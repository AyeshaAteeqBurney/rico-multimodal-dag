"""CLIP image embeddings 
Reads PNG bytes from MinIO ``screens/{screen_id}.png``. 
"""

from __future__ import annotations


def run(*, run_id: str, screen_ids: list[int]) -> dict:
    return {"run_id": run_id, "screen_ids": screen_ids, "task": "embed_image"}
