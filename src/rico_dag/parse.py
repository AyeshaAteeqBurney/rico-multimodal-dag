"""Parse stage: flatten hierarchy into a text representation."""

from __future__ import annotations

import json
from typing import Any

from rico_dag.storage import get_bytes, put_if_missing


def parse_hierarchy(raw_json: str) -> list[tuple[str, str, tuple[int, int, int, int]]]:
    root = json.loads(raw_json)
    out: list[tuple[str, str, tuple[int, int, int, int]]] = []

    def walk(node: dict[str, Any]) -> None:
        cls = str(node.get("class", ""))
        text = str(node.get("text", "")).strip()
        bounds = node.get("bounds", [0, 0, 0, 0])
        bbox = (
            int(bounds[0]) if len(bounds) > 0 else 0,
            int(bounds[1]) if len(bounds) > 1 else 0,
            int(bounds[2]) if len(bounds) > 2 else 0,
            int(bounds[3]) if len(bounds) > 3 else 0,
        )
        out.append((cls, text, bbox))
        for child in node.get("children", []):
            if isinstance(child, dict):
                walk(child)

    walk(root)
    return out


def _to_text_representation(parsed: list[tuple[str, str, tuple[int, int, int, int]]]) -> str:
    lines: list[str] = []
    for cls, text, bbox in parsed:
        if text:
            lines.append(f"{cls}|{text}|{bbox}")
    return "\n".join(lines)


def run(*, screen_ids: list[int]) -> list[int]:
    for screen_id in screen_ids:
        hierarchy_key = f"screens/{screen_id}.hierarchy.json"
        raw_hierarchy = get_bytes(hierarchy_key).decode("utf-8")
        parsed = parse_hierarchy(raw_hierarchy)
        text_repr = _to_text_representation(parsed).encode("utf-8")
        text_key = f"text/{screen_id}.txt"
        put_if_missing(key=text_key, payload=text_repr, content_type="text/plain")
    return screen_ids
