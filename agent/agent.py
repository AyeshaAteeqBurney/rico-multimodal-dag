"""Backfill agent — Slack Socket Mode bot.

Listens for @-mentions, uses Ollama to parse intent, and triggers
rico_pipeline via the Airflow REST API.

Run:
    python -m agent.agent
or:
    make agent
"""

from __future__ import annotations

import logging
import re

from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

from agent import airflow_client, llm_parser
from agent.airflow_client import AirflowError
from agent.config import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
_log = logging.getLogger(__name__)


def _strip_mention(text: str) -> str:
    """Remove the leading <@BOTID> mention from the message text."""
    return re.sub(r"^<@[A-Z0-9]+>\s*", "", text).strip()


def _airflow_run_url(dag_run_id: str) -> str:
    return (
        f"{settings.airflow_api_url}/dags/{settings.airflow_dag_id}"
        f"/grid?dag_run_id={dag_run_id}"
    )


def build_app() -> App:
    app = App(token=settings.slack_bot_token)

    @app.event("app_mention")
    def handle_mention(event: dict, say: object) -> None:  # type: ignore[type-arg]
        thread_ts = event.get("ts")
        raw_text = event.get("text", "")
        user_message = _strip_mention(raw_text)
        _log.info("Received mention: %r", user_message)

        # ── 1. Parse intent via Ollama ──────────────────────────────────────
        try:
            intent = llm_parser.parse(user_message)
        except Exception as exc:
            _log.exception("Unexpected error in llm_parser.parse")
            say(
                text=f"Sorry, I hit an unexpected error parsing your request: `{exc}`",
                thread_ts=thread_ts,
            )
            return

        if intent is None:
            say(
                text=(
                    "I couldn't understand your request. "
                    "Try something like: *@DataBot backfill 20 screens*"
                ),
                thread_ts=thread_ts,
            )
            return

        limit = intent["limit"]

        # ── 2. Trigger Airflow (preflight + create run) ─────────────────────
        # Do NOT acknowledge before this call — only confirm once we know
        # the run was actually created and the DAG is not paused.
        try:
            dag_run_id = airflow_client.trigger_dag(limit)
        except AirflowError as exc:
            say(text=f"Could not trigger pipeline: {exc}", thread_ts=thread_ts)
            return
        except Exception as exc:
            _log.exception("Unexpected error calling Airflow")
            say(
                text=f"Unexpected error contacting Airflow: `{exc}`",
                thread_ts=thread_ts,
            )
            return

        # ── 3. Confirm — only reached if trigger truly succeeded ────────────
        run_url = _airflow_run_url(dag_run_id)
        say(
            text=(
                f"Pipeline triggered successfully!\n"
                f"• *DAG*: `{settings.airflow_dag_id}`\n"
                f"• *LIMIT*: {limit}\n"
                f"• *dag_run_id*: `{dag_run_id}`\n"
                f"• *Airflow UI*: {run_url}"
            ),
            thread_ts=thread_ts,
        )
        _log.info("Triggered dag_run_id=%s LIMIT=%d", dag_run_id, limit)

    return app


def main() -> None:
    settings.validate()
    app = build_app()
    handler = SocketModeHandler(app, settings.slack_app_token)
    _log.info(
        "Backfill agent starting (Socket Mode). Mention the bot in Slack to trigger a run."
    )
    handler.start()


if __name__ == "__main__":
    main()
