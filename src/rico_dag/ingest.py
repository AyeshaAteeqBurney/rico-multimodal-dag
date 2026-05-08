"""Ingest stage: stream chosen screens from HuggingFace and persist metadata."""

from __future__ import annotations

import json
from pathlib import Path

from datasets import load_dataset

from rico_dag.config import settings
from rico_dag.db import fingerprint, get_conn
from rico_dag.storage import put_if_missing


def _read_chosen_ids() -> list[int]:
    chosen: list[int] = []
    for raw_line in Path(settings.chosen_screens_file).read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        chosen.append(int(line))
    return chosen


def run(*, run_id: str, limit: int) -> list[int]:
    chosen_ids = set(_read_chosen_ids()[:limit])
    if not chosen_ids:
        return []

    ds = load_dataset("rootsautomation/RICO-Screen2Words", split="train", streaming=True)
    processed: list[int] = []

    with get_conn() as conn, conn.cursor() as cur:
        for row in ds:
            screen_id = int(row["screenId"])
            if screen_id not in chosen_ids:
                continue

            png_bytes = row["image"]["bytes"]
            hierarchy_bytes = json.dumps(row["ui_obj"], ensure_ascii=True).encode("utf-8")
            png_key = f"screens/{screen_id}.png"
            hierarchy_key = f"screens/{screen_id}.hierarchy.json"

            put_if_missing(key=png_key, payload=png_bytes, content_type="image/png")
            put_if_missing(key=hierarchy_key, payload=hierarchy_bytes, content_type="application/json")

            cur.execute(
                """
                INSERT INTO screens_metadata (
                    screen_id, app_package, category, png_path, hierarchy_json_path,
                    run_id, source_fingerprint
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (screen_id) DO UPDATE
                SET app_package = EXCLUDED.app_package,
                    category = EXCLUDED.category,
                    png_path = EXCLUDED.png_path,
                    hierarchy_json_path = EXCLUDED.hierarchy_json_path,
                    run_id = EXCLUDED.run_id,
                    source_fingerprint = EXCLUDED.source_fingerprint,
                    updated_at = NOW()
                """,
                (
                    screen_id,
                    row.get("package"),
                    row.get("category"),
                    png_key,
                    hierarchy_key,
                    run_id,
                    fingerprint(png_bytes),
                ),
            )
            processed.append(screen_id)
            if len(processed) >= limit:
                break

        conn.commit()

    return processed
