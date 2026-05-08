"""LLM extraction + review queue 

Input text: MinIO ``text/{screen_id}.txt``. Use ``OLLAMA_ENDPOINT`` (``ollama`` hostname in
Compose). 
"""

from __future__ import annotations


def run(*, run_id: str, screen_ids: list[int]) -> dict:
    return {"run_id": run_id, "screen_ids": screen_ids, "task": "extract"}
