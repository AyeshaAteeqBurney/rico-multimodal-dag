"""CLIP image embeddings.

Reads PNG bytes from MinIO ``screens/{screen_id}.png``, encodes with open-clip,
L2-normalises so pgvector ``<->`` (L2 distance) equals cosine distance, and
writes each row idempotently to ``screens_embeddings``.
"""

from __future__ import annotations

import io
import logging

import open_clip
import torch
from pgvector.psycopg import register_vector
from PIL import Image

from rico_dag.config import settings
from rico_dag.db import fingerprint, get_conn, logger_with_run_id
from rico_dag.storage import get_bytes

_log = logging.getLogger(__name__)
_MODEL = None
_PREPROCESS = None
_MODEL_VERSION: str | None = None


def _split_clip_version(version: str) -> tuple[str, str]:
    arch, _, pretrained = version.partition("/")
    return arch, pretrained


def _model_version_string(arch: str, pretrained: str) -> str:
    # Matches the lab: open-clip-<arch>-<pretrained-with-dashes>
    return f"open-clip-{arch}-{pretrained.replace('_', '-')}"


def _model() -> tuple[torch.nn.Module, callable, str]:
    global _MODEL, _PREPROCESS, _MODEL_VERSION
    if _MODEL is None:
        arch, pretrained = _split_clip_version(settings.clip_version)
        model, _, preprocess = open_clip.create_model_and_transforms(
            arch, pretrained=pretrained
        )
        model.eval()
        _MODEL = model
        _PREPROCESS = preprocess
        _MODEL_VERSION = _model_version_string(arch, pretrained)
    return _MODEL, _PREPROCESS, _MODEL_VERSION


def run(*, run_id: str, screen_ids: list[int]) -> dict:
    log = logger_with_run_id(_log, run_id)
    model, preprocess, model_version = _model()
    inserted = 0

    with get_conn() as conn:
        register_vector(conn)
        with conn.cursor() as cur:
            for screen_id in screen_ids:
                png_bytes = get_bytes(f"screens/{screen_id}.png")
                image = Image.open(io.BytesIO(png_bytes)).convert("RGB")
                tensor = preprocess(image).unsqueeze(0)
                with torch.no_grad():
                    features = model.encode_image(tensor)
                    features = features / features.norm(dim=-1, keepdim=True)
                vector = features[0].cpu().numpy().astype("float32")
                cur.execute(
                    """
                    INSERT INTO screens_embeddings (
                        screen_id, model_name, model_version, embedding_kind,
                        vector, run_id, source_fingerprint
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (screen_id, model_name, model_version, embedding_kind)
                    DO NOTHING
                    """,
                    (
                        screen_id,
                        "open-clip",
                        model_version,
                        "image",
                        vector,
                        run_id,
                        fingerprint(png_bytes),
                    ),
                )
                if cur.rowcount > 0:
                    inserted += 1
        conn.commit()

    log.info(
        "embed_image: %d new rows, %d skipped by ON CONFLICT",
        inserted,
        len(screen_ids) - inserted,
    )
    return {"run_id": run_id, "screen_ids": screen_ids, "task": "embed_image"}
