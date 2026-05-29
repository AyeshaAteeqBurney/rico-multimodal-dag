"""Data-integrity audit — circuit breaker for the pipeline.

Runs a suite of integrity checks against the *current run's* rows and halts the
pipeline (raises AirflowException) if any check finds a violation. Checks:

  1. duplicate_embeddings  — same (screen_id, model, version, kind) appears twice
  2. duplicate_metadata    — same screen_id appears twice for the run
  3. invalid_vectors       — embedding vectors that are NULL or (near) zero-norm
  4. orphan_embeddings     — embeddings whose screen has no metadata for the run
  5. missing_embeddings    — metadata screens lacking an image and/or text embedding

All checks are scoped to ``run_id`` and are vacuously true on an empty run, so a
clean pipeline always passes. The result (pass/fail + per-check violation
details) is recorded in ``audit_results`` for traceability.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from airflow.exceptions import AirflowException

from rico_dag.db import get_conn, logger_with_run_id

_log = logging.getLogger(__name__)

# Set by chaos/inject_duplicates.py for the §5 manual-corruption demo.
CHAOS_FINGERPRINT = "chaos-duplicate-inject-v1"

# Embeddings are L2-normalised to unit norm by embed_image/embed_text, so a real
# vector has norm ~1.0. Anything below this is corrupt (all-zero / NaN).
_MIN_VECTOR_NORM = 1e-6

# audit_name is kept stable for downstream tooling (validate_project4.py asserts it).
AUDIT_NAME = "duplicate_detection"


def _reassign_chaos_rows(cur, run_id: str, log: logging.LoggerAdapter) -> None:
    """§5 demo: chaos rows inserted on a prior run are pulled into this run.

    Covers both embeddings and metadata so every chaos scenario (duplicate,
    zero-norm, orphan, missing) is evaluated against the current run. On a clean
    run this updates zero rows.
    """
    cur.execute(
        "UPDATE screens_embeddings SET run_id = %s WHERE source_fingerprint = %s",
        (run_id, CHAOS_FINGERPRINT),
    )
    emb = cur.rowcount
    cur.execute(
        "UPDATE screens_metadata SET run_id = %s WHERE source_fingerprint = %s",
        (run_id, CHAOS_FINGERPRINT),
    )
    meta = cur.rowcount
    if emb or meta:
        log.warning(
            "chaos demo: reassigned %s embedding + %s metadata row(s) to run_id=%s for audit",
            emb,
            meta,
            run_id,
        )


def _check_duplicate_embeddings(cur, run_id: str) -> list[dict]:
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
    return [
        {
            "screen_id": r[0],
            "model_name": r[1],
            "model_version": r[2],
            "embedding_kind": r[3],
            "count": r[4],
        }
        for r in cur.fetchall()
    ]


def _check_duplicate_metadata(cur, run_id: str) -> list[dict]:
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
    return [{"screen_id": r[0], "count": r[1]} for r in cur.fetchall()]


def _check_invalid_vectors(cur, run_id: str) -> list[dict]:
    """NULL or (near) zero-norm vectors — a corrupt/degenerate embedding."""
    cur.execute(
        """
        SELECT screen_id, model_name, model_version, embedding_kind,
               vector_norm(vector) AS norm
        FROM screens_embeddings
        WHERE run_id = %s
          AND (vector IS NULL OR vector_norm(vector) < %s)
        """,
        (run_id, _MIN_VECTOR_NORM),
    )
    return [
        {
            "screen_id": r[0],
            "model_name": r[1],
            "model_version": r[2],
            "embedding_kind": r[3],
            "norm": float(r[4]) if r[4] is not None else None,
        }
        for r in cur.fetchall()
    ]


def _check_orphan_embeddings(cur, run_id: str) -> list[dict]:
    """Embeddings whose screen has no metadata row for the same run (broken lineage)."""
    cur.execute(
        """
        SELECT e.screen_id, e.embedding_kind
        FROM screens_embeddings e
        LEFT JOIN screens_metadata m
          ON m.screen_id = e.screen_id AND m.run_id = e.run_id
        WHERE e.run_id = %s
          AND m.screen_id IS NULL
        """,
        (run_id,),
    )
    return [{"screen_id": r[0], "embedding_kind": r[1]} for r in cur.fetchall()]


def _check_missing_embeddings(cur, run_id: str) -> list[dict]:
    """Metadata screens lacking an image and/or text embedding for the run."""
    cur.execute(
        """
        SELECT m.screen_id,
               COALESCE(BOOL_OR(e.embedding_kind = 'image'), FALSE) AS has_image,
               COALESCE(BOOL_OR(e.embedding_kind = 'text'),  FALSE) AS has_text
        FROM screens_metadata m
        LEFT JOIN screens_embeddings e
          ON e.screen_id = m.screen_id AND e.run_id = m.run_id
        WHERE m.run_id = %s
        GROUP BY m.screen_id
        HAVING NOT (
            COALESCE(BOOL_OR(e.embedding_kind = 'image'), FALSE)
            AND COALESCE(BOOL_OR(e.embedding_kind = 'text'), FALSE)
        )
        """,
        (run_id,),
    )
    out = []
    for screen_id, has_image, has_text in cur.fetchall():
        missing = [k for k, present in (("image", has_image), ("text", has_text)) if not present]
        out.append({"screen_id": screen_id, "missing_kinds": missing})
    return out


# Each check: (key, human label, function). Order = report order.
_CHECKS = (
    ("duplicate_embeddings", "duplicate embedding rows", _check_duplicate_embeddings),
    ("duplicate_metadata", "duplicate metadata rows", _check_duplicate_metadata),
    ("invalid_vectors", "null / zero-norm vectors", _check_invalid_vectors),
    ("orphan_embeddings", "embeddings without metadata", _check_orphan_embeddings),
    ("missing_embeddings", "screens missing an embedding", _check_missing_embeddings),
)


def run(*, run_id: str) -> dict:
    log = logger_with_run_id(_log, run_id)
    violations: dict[str, list[dict]] = {}

    with get_conn() as conn, conn.cursor() as cur:
        _reassign_chaos_rows(cur, run_id, log)

        for key, _label, check_fn in _CHECKS:
            found = check_fn(cur, run_id)
            if found:
                violations[key] = found

        passed = len(violations) == 0

        cur.execute(
            """
            INSERT INTO audit_results (run_id, audit_name, passed, details, created_at)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                run_id,
                AUDIT_NAME,
                passed,
                json.dumps(violations),
                datetime.now(timezone.utc),
            ),
        )
        conn.commit()

    if not passed:
        summary = "; ".join(
            f"{label}: {len(violations[key])}"
            for key, label, _fn in _CHECKS
            if key in violations
        )
        log.error("AUDIT FAILED — %s\n%s", summary, json.dumps(violations, indent=2))
        raise AirflowException(f"Data integrity audit failed ({summary}). Details: {json.dumps(violations)}")

    log.info("audit passed — all integrity checks clean")
    return {"run_id": run_id, "task": "audit", "passed": True}
