"""SBERT text embeddings  

Reads parsed text from MinIO ``text/{screen_id}.txt``.
"""

from __future__ import annotations


def run(*, run_id: str, screen_ids: list[int]) -> dict:
    return {"run_id": run_id, "screen_ids": screen_ids, "task": "embed_text"}
