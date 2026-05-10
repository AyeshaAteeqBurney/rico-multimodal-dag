"""Ingest stage: stream chosen screens from HuggingFace and persist metadata."""

from __future__ import annotations

import io
import json
from pathlib import Path

from datasets import load_dataset
from PIL import Image

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


def _image_to_png_bytes(image: object) -> bytes:
    """HF may return a dict with raw bytes (older) or a decoded PIL image (newer `datasets`)."""
    if isinstance(image, dict) and "bytes" in image:
        return image["bytes"]
    if isinstance(image, (bytes, bytearray, memoryview)):
        return bytes(image)
    if isinstance(image, Image.Image):
        buf = io.BytesIO()
        im = image
        if im.mode in ("P", "PA"):
            im = im.convert("RGBA")
        elif im.mode not in ("RGB", "RGBA", "L"):
            im = im.convert("RGB")
        im.save(buf, format="PNG")
        return buf.getvalue()
    raise TypeError(f"Unsupported image column type: {type(image)!r}")


def _hierarchy_json_bytes(row: dict) -> bytes:
    """HF schema varies: older rows used `ui_obj`; current RICO-Screen2Words uses `view_hierarchy`."""
    if "ui_obj" in row:
        raw = row["ui_obj"]
    elif "view_hierarchy" in row:
        raw = row["view_hierarchy"]
    else:
        raise KeyError(
            "Dataset row has no UI hierarchy column "
            "(expected 'ui_obj' or 'view_hierarchy'). "
            f"Available keys: {sorted(row.keys())}"
        )
    if isinstance(raw, str):
        return raw.encode("utf-8")
    return json.dumps(raw, ensure_ascii=True).encode("utf-8")


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

            png_bytes = _image_to_png_bytes(row["image"])
            hierarchy_bytes = _hierarchy_json_bytes(row)
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
                    row.get("package")
                    or row.get("app_package_name")
                    or row.get("app_package"),
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

    return {"run_id": run_id, "screen_ids": processed, "rows_in": 0, "rows_out": len(processed)}
