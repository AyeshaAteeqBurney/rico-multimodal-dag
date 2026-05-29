"""Duplicate-detection audit — circuit breaker for the pipeline."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from airflow.exceptions import AirflowException

from rico_dag.db import get_conn, logger_with_run_id

_log = logging.getLogger(__name__)

# Set by chaos/inject_duplicates.py for the §5 manual-corruption demo.
CHAOS_FINGERPRINT = "chaos-duplicate-inject-v1"


def run(*, run_id: str) -> dict:
    log = logger_with_run_id(_log, run_id)
    duplicates = {}

    with get_conn() as conn, conn.cursor() as cur:
        # §5 re-run: chaos row was inserted on a prior succeeded run_id; evaluate this run.
        cur.execute(
            """
            UPDATE screens_embeddings
            SET run_id = %s
            WHERE source_fingerprint = %s
            """,
            (run_id, CHAOS_FINGERPRINT),
        )
        if cur.rowcount:
            log.warning(
                "chaos demo: reassigned %s embedding row(s) to run_id=%s for duplicate audit",
                cur.rowcount,
                run_id,
            )

        cur.execute(
            """
            SELECT screen_id, model_name, model_version, embedding_kind, COUNT(*)
            FROM screens_embeddings
            WHERE run_id = %s
            GROUP BY screen_id, model_name, model_version, embedding_kind
            HAVING COUNT(*) > 1
            """,
            (run_id,),
        )
        embedding_dupes = cur.fetchall()
        if embedding_dupes:
            duplicates["screens_embeddings"] = [
                {
                    "screen_id": row[0],
                    "model_name": row[1],
                    "model_version": row[2],
                    "embedding_kind": row[3],
                    "count": row[4],
                }
                for row in embedding_dupes
            ]

        cur.execute(
            """
            SELECT screen_id, COUNT(*)
            FROM screens_metadata
            WHERE run_id = %s
            GROUP BY screen_id
            HAVING COUNT(*) > 1
            """,
            (run_id,),
        )
        metadata_dupes = cur.fetchall()
        if metadata_dupes:
            duplicates["screens_metadata"] = [
                {"screen_id": row[0], "count": row[1]}
                for row in metadata_dupes
            ]

        passed = len(duplicates) == 0

        cur.execute(
            """
            INSERT INTO audit_results (run_id, audit_name, passed, details, created_at)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                run_id,
                "duplicate_detection",
                passed,
                json.dumps(duplicates),
                datetime.now(timezone.utc),
            ),
        )
        conn.commit()

    if not passed:
        log.error("AUDIT FAILED — duplicates found: %s", json.dumps(duplicates, indent=2))
        raise AirflowException(f"Duplicate detection audit failed. Duplicates: {json.dumps(duplicates)}")

    log.info("audit passed — no duplicates found")
    return {"run_id": run_id, "task": "audit", "passed": True}
