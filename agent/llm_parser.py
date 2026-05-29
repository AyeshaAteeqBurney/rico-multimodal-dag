"""LLM intent parser — uses Ollama to extract action + LIMIT from a Slack message.

Returns a dict ``{"action": "trigger_pipeline", "limit": <int>}`` on success,
or ``None`` when the message is unrecognised or no valid limit can be extracted.

A regex fallback handles common cases (e.g. "backfill 15") if Ollama returns
malformed JSON, reducing demo failures.
"""

from __future__ import annotations

import json
import logging
import re
from json import JSONDecodeError

import requests

from agent.config import settings

_log = logging.getLogger(__name__)

_SYSTEM_PROMPT = """
You are a DataOps assistant. A user has mentioned you in Slack.
Decide whether they want to trigger the data pipeline, and if so extract how many screens.

Rules:
- If the user wants to run / trigger / backfill / process screens, respond with action "trigger_pipeline".
- Extract the number of screens as a positive integer (key "limit"). If no number is given, use 5.
- If the message is unrelated (greeting, question about status, etc.) respond with action "unknown".
- Respond ONLY with valid JSON. No extra text, no markdown.

Examples:
  User: "backfill 20 screens" → {"action":"trigger_pipeline","limit":20}
  User: "run the pipeline for 50" → {"action":"trigger_pipeline","limit":50}
  User: "hey can you process 10 new screens?" → {"action":"trigger_pipeline","limit":10}
  User: "what time is it?" → {"action":"unknown","limit":null}
""".strip()


def _regex_fallback(text: str) -> dict | None:
    """Try to extract a limit from plain text when LLM output is not valid JSON."""
    # Look for any standalone integer in the message.
    match = re.search(r"\b(\d{1,4})\b", text)
    if match:
        limit = int(match.group(1))
        if 1 <= limit <= 9999:
            return {"action": "trigger_pipeline", "limit": limit}
    return None


def parse(user_message: str) -> dict | None:
    """Parse a Slack message and return intent dict or None.

    Returns:
        {"action": "trigger_pipeline", "limit": int} — ready to trigger
        None — message not understood; caller should ask for clarification
    """
    prompt = f"{_SYSTEM_PROMPT}\n\nUser message: {user_message}"
    raw = ""
    try:
        response = requests.post(
            f"{settings.ollama_endpoint}/api/generate",
            json={
                "model": settings.ollama_model,
                "prompt": prompt,
                "stream": False,
            },
            timeout=settings.llm_timeout,
        )
        response.raise_for_status()
        raw = response.json()["response"].strip()
        _log.debug("llm_parser raw response: %s", raw)
    except requests.RequestException as exc:
        _log.warning("Ollama request failed: %s — falling back to regex", exc)
        return _regex_fallback(user_message)

    # Strip markdown code fences if present.
    raw_clean = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw, flags=re.MULTILINE).strip()

    try:
        parsed = json.loads(raw_clean)
    except JSONDecodeError:
        _log.warning("LLM returned non-JSON (%r) — trying regex fallback", raw_clean[:200])
        return _regex_fallback(user_message)

    action = parsed.get("action", "unknown")
    raw_limit = parsed.get("limit")

    if action != "trigger_pipeline":
        return None

    # Validate and clamp limit.
    try:
        limit = int(raw_limit) if raw_limit is not None else 5
    except (TypeError, ValueError):
        limit = 5

    if limit < 1 or limit > 500:
        _log.warning("LLM returned out-of-range limit %s — clamping to 5", limit)
        limit = 5

    return {"action": "trigger_pipeline", "limit": limit}
