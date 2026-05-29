"""LLM intent parser — uses Ollama to classify a Slack message into an action.

Returns a dict ``{"action": <str>, "limit": <int|None>}``:
  - action="trigger_pipeline" → run the DAG (with "limit")
  - action="diagnose"         → report current pipeline health
  - action="fix"              → diagnose then propose a confirm-gated fix
  - action="unknown"          → message not understood (caller asks to clarify)

A regex fallback handles common cases (keywords + "backfill 15") if Ollama
returns malformed JSON or is unreachable, reducing demo failures.
"""

from __future__ import annotations

import json
import logging
import re
from json import JSONDecodeError

import requests

from agent.config import settings

_log = logging.getLogger(__name__)

_VALID_ACTIONS = {"trigger_pipeline", "diagnose", "fix", "unknown"}

_SYSTEM_PROMPT = """
You are a DataOps assistant mentioned in Slack. Classify the user's request into one action.

Actions:
- "trigger_pipeline": user wants to run / trigger / backfill / process N screens. Extract the
  number of screens as integer "limit" (default 5 if none given).
- "diagnose": user asks about pipeline health/status, what's wrong, why it failed, or to check it.
- "fix": user asks you to fix / repair / resolve / clean up a problem with the pipeline.
- "unknown": anything else (greetings, unrelated questions).

Respond ONLY with valid JSON, no markdown, no extra text.

Examples:
  "backfill 20 screens" -> {"action":"trigger_pipeline","limit":20}
  "run the pipeline for 50" -> {"action":"trigger_pipeline","limit":50}
  "is the pipeline healthy?" -> {"action":"diagnose","limit":null}
  "what went wrong with the last run?" -> {"action":"diagnose","limit":null}
  "can you fix the duplicate issue?" -> {"action":"fix","limit":null}
  "please repair the pipeline" -> {"action":"fix","limit":null}
  "what time is it?" -> {"action":"unknown","limit":null}
""".strip()

# Keyword fallbacks (checked before the integer fallback).
_FIX_RE = re.compile(r"\b(fix|repair|resolve|clean\s*up|cleanup|remediate)\b", re.IGNORECASE)
_DIAGNOSE_RE = re.compile(
    r"\b(diagnose|status|health|healthy|what'?s wrong|what went wrong|check|investigate|why)\b",
    re.IGNORECASE,
)


def _clamp_limit(raw_limit: object) -> int:
    try:
        limit = int(raw_limit) if raw_limit is not None else 5
    except (TypeError, ValueError):
        limit = 5
    if limit < 1 or limit > 500:
        _log.warning("limit %s out of range — clamping to 5", raw_limit)
        limit = 5
    return limit


def _regex_fallback(text: str) -> dict | None:
    """Keyword/number heuristics when the LLM output is unusable."""
    if _FIX_RE.search(text):
        return {"action": "fix", "limit": None}
    if _DIAGNOSE_RE.search(text):
        return {"action": "diagnose", "limit": None}
    match = re.search(r"\b(\d{1,4})\b", text)
    if match:
        limit = int(match.group(1))
        if 1 <= limit <= 9999:
            return {"action": "trigger_pipeline", "limit": min(limit, 500)}
    return None


def parse(user_message: str) -> dict | None:
    """Classify a Slack message. Returns an intent dict or None if unknown."""
    prompt = f"{_SYSTEM_PROMPT}\n\nUser message: {user_message}"
    try:
        response = requests.post(
            f"{settings.ollama_endpoint}/api/generate",
            json={"model": settings.ollama_model, "prompt": prompt, "stream": False},
            timeout=settings.llm_timeout,
        )
        response.raise_for_status()
        raw = response.json()["response"].strip()
        _log.debug("llm_parser raw response: %s", raw)
    except requests.RequestException as exc:
        _log.warning("Ollama request failed: %s — falling back to keywords", exc)
        return _regex_fallback(user_message)

    raw_clean = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.MULTILINE).strip()
    try:
        parsed = json.loads(raw_clean)
    except JSONDecodeError:
        _log.warning("LLM returned non-JSON (%r) — trying keyword fallback", raw_clean[:200])
        return _regex_fallback(user_message)

    action = parsed.get("action", "unknown")
    if action not in _VALID_ACTIONS:
        return _regex_fallback(user_message)

    if action == "unknown":
        # Give keywords a chance — the LLM sometimes misses "fix"/"diagnose".
        return _regex_fallback(user_message)

    if action == "trigger_pipeline":
        return {"action": "trigger_pipeline", "limit": _clamp_limit(parsed.get("limit"))}

    return {"action": action, "limit": None}
