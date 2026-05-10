"""SBERT text embeddings.

Reads parsed text from MinIO ``text/{screen_id}.txt``, encodes with
sentence-transformers (L2-normalised so pgvector ``<->`` equals cosine
distance), and writes each row idempotently to ``screens_embeddings``.
"""

from __future__ import annotations

import logging

from pgvector.psycopg import register_vector
from sentence_transformers import SentenceTransformer

from rico_dag.config import settings
from rico_dag.db import fingerprint, get_conn, logger_with_run_id
from rico_dag.storage import get_bytes

_log = logging.getLogger(__name__)
_MODEL: SentenceTransformer | None = None


def _model() -> SentenceTransformer:
    global _MODEL
    if _MODEL is None:
        _MODEL = SentenceTransformer(settings.sbert_version)
    return _MODEL


def _model_version_string() -> str:
    # Matches the lab: "sentence-transformers/<model>"
    raw = settings.sbert_version
    return raw if "/" in raw else f"sentence-transformers/{raw}"


def run(*, run_id: str, screen_ids: list[int]) -> dict:
    log = logger_with_run_id(_log, run_id)
    model = _model()
    model_version = _model_version_string()
    inserted = 0

    with get_conn() as conn:
        register_vector(conn)
        with conn.cursor() as cur:
            for screen_id in screen_ids:
                text_bytes = get_bytes(f"text/{screen_id}.txt")
                vector = model.encode(
                    text_bytes.decode("utf-8"),
                    normalize_embeddings=True,
                ).astype("float32")
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
                        "sentence-transformers",
                        model_version,
                        "text",
                        vector,
                        run_id,
                        fingerprint(text_bytes),
                    ),
                )
                if cur.rowcount > 0:
                    inserted += 1
        conn.commit()

    log.info(
        "embed_text: %d new rows, %d skipped by ON CONFLICT",
        inserted,
        len(screen_ids) - inserted,
    )
    return {"run_id": run_id, "screen_ids": screen_ids, "task": "embed_text"}
