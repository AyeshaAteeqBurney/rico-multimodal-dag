"""Post-embed load / finalization 
Verify expected rows, summarize counts, idempotent DB writes. 
"""

from __future__ import annotations


def run(*, run_id: str, screen_ids: list[int]) -> dict:
    return {"run_id": run_id, "screen_ids": screen_ids, "task": "load"}
