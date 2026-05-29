"""Backfill / DataOps agent — Slack Socket Mode bot.

Listens for @-mentions and supports three capabilities:
  • trigger  — "backfill 20 screens" → triggers rico_pipeline via the Airflow REST API
  • diagnose — "is the pipeline healthy?" → inspects run/audit/paused state and reports
  • fix       — "fix the duplicate issue" → proposes a remediation and waits for a human
                to reply "confirm" before executing (confirm-gated; Level B).

Run:
    python -m agent.agent   (or: make agent)
"""

from __future__ import annotations

import logging
import re
import time

from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

from agent import airflow_client, diagnostics, llm_parser, remediation
from agent.airflow_client import AirflowError
from agent.config import settings
from agent.diagnostics import Diagnosis
from agent.remediation import RemediationError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
_log = logging.getLogger(__name__)

# Confirm-gate state: thread_key -> {"diagnosis": Diagnosis, "ts": epoch}
# Single-process Socket Mode, so an in-memory dict is sufficient.
_PENDING_FIXES: dict[str, dict] = {}
_CONFIRM_TTL_SECONDS = 600  # proposals expire after 10 minutes

_CONFIRM_RE = re.compile(r"^\s*(confirm|yes|do it|go ahead|approve[d]?)\s*$", re.IGNORECASE)
_CANCEL_RE = re.compile(r"^\s*(cancel|no|stop|abort|nevermind|never mind)\s*$", re.IGNORECASE)


def _strip_mention(text: str) -> str:
    """Remove the leading <@BOTID> mention from the message text."""
    return re.sub(r"^<@[A-Z0-9]+>\s*", "", text).strip()


def _airflow_run_url(dag_run_id: str) -> str:
    return (
        f"{settings.airflow_api_url}/dags/{settings.airflow_dag_id}"
        f"/grid?dag_run_id={dag_run_id}"
    )


def _purge_expired() -> None:
    now = time.time()
    for key in [k for k, v in _PENDING_FIXES.items() if now - v["ts"] > _CONFIRM_TTL_SECONDS]:
        _PENDING_FIXES.pop(key, None)


# ── Handlers for each capability ─────────────────────────────────────────────


def _handle_trigger(say, thread_ts: str, limit: int) -> None:
    """Trigger the DAG. Only confirms once the run is actually created."""
    try:
        dag_run_id = airflow_client.trigger_dag(limit)
    except AirflowError as exc:
        say(text=f"Could not trigger pipeline: {exc}", thread_ts=thread_ts)
        return
    except Exception as exc:
        _log.exception("Unexpected error calling Airflow")
        say(text=f"Unexpected error contacting Airflow: `{exc}`", thread_ts=thread_ts)
        return

    say(
        text=(
            f"Pipeline triggered successfully!\n"
            f"• *DAG*: `{settings.airflow_dag_id}`\n"
            f"• *LIMIT*: {limit}\n"
            f"• *dag_run_id*: `{dag_run_id}`\n"
            f"• *Airflow UI*: {_airflow_run_url(dag_run_id)}"
        ),
        thread_ts=thread_ts,
    )
    _log.info("Triggered dag_run_id=%s LIMIT=%d", dag_run_id, limit)


def _run_diagnosis(say, thread_ts: str) -> Diagnosis | None:
    try:
        return diagnostics.diagnose()
    except Exception as exc:
        _log.exception("Diagnosis failed")
        say(text=f"Couldn't complete diagnosis: `{exc}`", thread_ts=thread_ts)
        return None


def _handle_diagnose(say, thread_ts: str) -> None:
    diag = _run_diagnosis(say, thread_ts)
    if diag is None:
        return
    suffix = (
        f"\n\n_Recommended fix:_ *{diag.fix_label}*. "
        "Mention me with *fix* to proceed."
        if diag.fixable
        else ""
    )
    say(text=f"*{diag.title}*\n{diag.detail}{suffix}", thread_ts=thread_ts)


def _handle_fix_proposal(say, thread_key: str, thread_ts: str) -> None:
    diag = _run_diagnosis(say, thread_ts)
    if diag is None:
        return
    if not diag.fixable:
        say(
            text=f"*{diag.title}*\n{diag.detail}\n\nNothing to fix right now.",
            thread_ts=thread_ts,
        )
        _PENDING_FIXES.pop(thread_key, None)
        return

    _PENDING_FIXES[thread_key] = {"diagnosis": diag, "ts": time.time()}
    say(
        text=(
            f"*{diag.title}*\n{diag.detail}\n\n"
            f"*Proposed fix:* I will {diag.fix_label}.\n"
            "Reply *confirm* to proceed, or *cancel* to abort. (Expires in 10 min.)"
        ),
        thread_ts=thread_ts,
    )


def _handle_confirm(say, thread_key: str, thread_ts: str) -> None:
    pending = _PENDING_FIXES.pop(thread_key, None)
    if not pending:
        say(
            text="There's no pending fix to confirm. Mention me with *fix* first.",
            thread_ts=thread_ts,
        )
        return

    diag: Diagnosis = pending["diagnosis"]
    say(text=f"Applying fix: {diag.fix_label}…", thread_ts=thread_ts)
    try:
        result = remediation.execute(diag)
    except (RemediationError, AirflowError) as exc:
        say(text=f"Fix failed: {exc}", thread_ts=thread_ts)
        return
    except Exception as exc:
        _log.exception("Unexpected error during remediation")
        say(text=f"Unexpected error during fix: `{exc}`", thread_ts=thread_ts)
        return
    say(text=result, thread_ts=thread_ts)


def _handle_cancel(say, thread_key: str, thread_ts: str) -> None:
    if _PENDING_FIXES.pop(thread_key, None):
        say(text="Okay, I've cancelled the proposed fix.", thread_ts=thread_ts)
    else:
        say(text="Nothing to cancel.", thread_ts=thread_ts)


# ── Slack app ────────────────────────────────────────────────────────────────


def build_app() -> App:
    app = App(token=settings.slack_bot_token)

    @app.event("app_mention")
    def handle_mention(event: dict, say: object) -> None:  # type: ignore[type-arg]
        _purge_expired()
        # thread_ts groups the proposal and its confirmation reply together.
        thread_ts = event.get("thread_ts") or event.get("ts")
        thread_key = thread_ts
        user_message = _strip_mention(event.get("text", ""))
        _log.info("Received mention: %r", user_message)

        # 1. Confirm / cancel short-circuit the LLM (fast + reliable).
        if _CONFIRM_RE.match(user_message):
            _handle_confirm(say, thread_key, thread_ts)
            return
        if _CANCEL_RE.match(user_message):
            _handle_cancel(say, thread_key, thread_ts)
            return

        # 2. Classify intent.
        try:
            intent = llm_parser.parse(user_message)
        except Exception as exc:
            _log.exception("Unexpected error in llm_parser.parse")
            say(text=f"Sorry, I hit an error parsing your request: `{exc}`", thread_ts=thread_ts)
            return

        if intent is None:
            say(
                text=(
                    "I couldn't understand your request. Try:\n"
                    "• *backfill 20 screens* — run the pipeline\n"
                    "• *is the pipeline healthy?* — diagnose\n"
                    "• *fix the duplicate issue* — propose a fix (you approve before I act)"
                ),
                thread_ts=thread_ts,
            )
            return

        action = intent["action"]
        if action == "trigger_pipeline":
            _handle_trigger(say, thread_ts, intent["limit"])
        elif action == "diagnose":
            _handle_diagnose(say, thread_ts)
        elif action == "fix":
            _handle_fix_proposal(say, thread_key, thread_ts)
        else:
            say(text="I'm not sure what you'd like me to do.", thread_ts=thread_ts)

    return app


def main() -> None:
    settings.validate()
    app = build_app()
    handler = SocketModeHandler(app, settings.slack_app_token)
    _log.info(
        "DataOps agent starting (Socket Mode). Mention the bot to trigger, diagnose, or fix."
    )
    handler.start()


if __name__ == "__main__":
    main()
