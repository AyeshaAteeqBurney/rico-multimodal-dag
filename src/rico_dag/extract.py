"""LLM extraction stage.

For each screen, read the parsed text representation from MinIO, call Ollama,
parse the JSON response, and persist to ``screens_metadata``. Failures route
to ``screens_review_queue`` instead of crashing the task (Project 4 §3.5).
"""

from __future__ import annotations

import json
import logging
from json import JSONDecodeError
from pathlib import Path

import requests

from rico_dag.config import settings
from rico_dag.db import fingerprint, get_conn, logger_with_run_id
from rico_dag.storage import get_bytes

_log = logging.getLogger(__name__)
_PROMPT_PATH = Path(__file__).resolve().parent / "extract_v1.txt"
_PROMPT: str | None = None


def _prompt_template() -> str:
    global _PROMPT
    if _PROMPT is None:
        _PROMPT = _PROMPT_PATH.read_text(encoding="utf-8")
    return _PROMPT


def _call_ollama(prompt: str) -> str:
    response = requests.post(
        f"{settings.ollama_endpoint}/api/generate",
        json={
            "model": settings.ollama_model,
            "prompt": prompt,
            "stream": False,
        },
        timeout=120,
    )
    response.raise_for_status()
    return response.json()["response"]


def run(*, run_id: str, screen_ids: list[int]) -> dict:
    log = logger_with_run_id(_log, run_id)
    template = _prompt_template()
    succeeded = 0
    queued = 0

    with get_conn() as conn, conn.cursor() as cur:
        for screen_id in screen_ids:
            text = get_bytes(f"text/{screen_id}.txt").decode("utf-8")
            full_prompt = template.replace("{hierarchy_text}", text)
            fp = fingerprint(full_prompt.encode("utf-8"))

            raw = ""
            try:
                raw = _call_ollama(full_prompt)
                payload = json.loads(raw)
            except (JSONDecodeError, requests.RequestException) as exc:
                cur.execute(
                    """
                    INSERT INTO screens_review_queue (
                        screen_id, reason, raw_output, run_id, source_fingerprint
                    ) VALUES (%s, %s, %s, %s, %s)
                    """,
                    (screen_id, str(exc), raw, run_id, fp),
                )
                queued += 1
                log.warning("extract: screen_id=%s queued for review (%s)", screen_id, exc)
                continue

            confidence = float(payload.get("confidence") or 0.0)
            # Lab convention: confidence has its own column; strip it from the JSON body.
            body = {k: v for k, v in payload.items() if k != "confidence"}
            cur.execute(
                """
                UPDATE screens_metadata
                SET extraction_payload = %s::jsonb,
                    prompt_version    = %s,
                    confidence        = %s,
                    run_id            = %s,
                    source_fingerprint= %s,
                    updated_at        = NOW()
                WHERE screen_id = %s
                """,
                (
                    json.dumps(body),
                    settings.prompt_version,
                    confidence,
                    run_id,
                    fp,
                    screen_id,
                ),
            )
            succeeded += 1

        conn.commit()

    log.info("extract: %d succeeded, %d in review queue", succeeded, queued)
    return {"run_id": run_id, "screen_ids": screen_ids, "task": "extract", "rows_in": len(screen_ids), "rows_out": succeeded}
