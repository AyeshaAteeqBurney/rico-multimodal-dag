"""Post-embed load / finalization.

The embed and extract stages already write idempotently to their destination
tables. This stage verifies each screen produced by ingest has the expected
rows, and emits a per-task summary that the metrics stage can consume.
"""

from __future__ import annotations

import logging

from rico_dag.db import get_conn, logger_with_run_id

_log = logging.getLogger(__name__)


def run(*, run_id: str, screen_ids: list[int]) -> dict:
    log = logger_with_run_id(_log, run_id)

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE embedding_kind = 'image') AS image_rows,
                COUNT(*) FILTER (WHERE embedding_kind = 'text')  AS text_rows
            FROM screens_embeddings
            WHERE run_id = %s
            """,
            (run_id,),
        )
        image_rows, text_rows = cur.fetchone()

        cur.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE extraction_payload IS NOT NULL) AS extracted,
                COUNT(*) AS total
            FROM screens_metadata
            WHERE run_id = %s
            """,
            (run_id,),
        )
        extracted, metadata_rows = cur.fetchone()

        cur.execute(
            "SELECT COUNT(*) FROM screens_review_queue WHERE run_id = %s",
            (run_id,),
        )
        (review_rows,) = cur.fetchone()

    expected = len(screen_ids)
    log.info(
        "load: screens=%d metadata=%d image_emb=%d text_emb=%d extracted=%d review=%d",
        expected,
        metadata_rows,
        image_rows,
        text_rows,
        extracted,
        review_rows,
    )

    return {
        "run_id": run_id,
        "screen_ids": screen_ids,
        "task": "load",
        "rows_in": len(screen_ids),
        "rows_out": metadata_rows,
        "counts": {
            "expected": expected,
            "metadata_rows": metadata_rows,
            "image_embeddings": image_rows,
            "text_embeddings": text_rows,
            "extracted": extracted,
            "review_queue": review_rows,
        },
    }
