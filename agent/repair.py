"""Generic, schema-level integrity repair for a failed run.

Unlike `chaos/inject_duplicates.py --cleanup` (which deletes rows by their chaos
tag), this repairs *any* offending rows by their data properties, so it fixes
real corruption as well as the chaos-demo rows. Each operation is scoped to the
failing run and mirrors exactly one audit check:

  audit check            repair action
  ---------------------  --------------------------------------------------
  duplicate_embeddings   de-duplicate, keeping one row per natural key
  duplicate_metadata     (covered by PK; de-dupe is a safety net)
  invalid_vectors        delete NULL / zero-norm embedding rows
  orphan_embeddings      delete embeddings whose screen has no metadata
  missing_embeddings     delete metadata rows that have no image+text embedding

After repair the table is internally consistent and the primary key is
restored. A subsequent re-run recomputes embeddings/metadata for in-scope
screens from source (idempotent), so real screens are rebuilt — only genuinely
un-embeddable rows (e.g. metadata with no source) stay removed.
"""

from __future__ import annotations

import logging

from agent.db import get_conn

_log = logging.getLogger(__name__)

_MIN_VECTOR_NORM = 1e-6
_EMBEDDINGS_PK = "screens_embeddings_pkey"


def _restore_pk(cur) -> bool:
    cur.execute(
        "SELECT 1 FROM pg_constraint WHERE conname = %s AND contype = 'p'",
        (_EMBEDDINGS_PK,),
    )
    if cur.fetchone():
        return False
    cur.execute(
        f"""
        ALTER TABLE screens_embeddings
        ADD CONSTRAINT {_EMBEDDINGS_PK}
        PRIMARY KEY (screen_id, model_name, model_version, embedding_kind)
        """
    )
    return True


def repair_run(run_id: str) -> dict:
    """Repair integrity violations for ``run_id``. Returns counts per operation."""
    counts: dict[str, int] = {}
    with get_conn() as conn, conn.cursor() as cur:
        # 1. De-duplicate embeddings globally (keep the earliest ctid per key).
        #    Done first so the primary key can be safely restored at the end.
        cur.execute(
            """
            DELETE FROM screens_embeddings a
            USING screens_embeddings b
            WHERE a.screen_id = b.screen_id
              AND a.model_name = b.model_name
              AND a.model_version = b.model_version
              AND a.embedding_kind = b.embedding_kind
              AND a.ctid > b.ctid
            """
        )
        counts["deduped_embeddings"] = cur.rowcount

        # 2. Delete NULL / zero-norm embeddings for the run.
        cur.execute(
            """
            DELETE FROM screens_embeddings
            WHERE run_id = %s
              AND (vector IS NULL OR vector_norm(vector) < %s)
            """,
            (run_id, _MIN_VECTOR_NORM),
        )
        counts["deleted_invalid_vectors"] = cur.rowcount

        # 3. Delete orphan embeddings (no metadata row for the same run).
        cur.execute(
            """
            DELETE FROM screens_embeddings e
            WHERE e.run_id = %s
              AND NOT EXISTS (
                  SELECT 1 FROM screens_metadata m
                  WHERE m.screen_id = e.screen_id AND m.run_id = e.run_id
              )
            """,
            (run_id,),
        )
        counts["deleted_orphan_embeddings"] = cur.rowcount

        # 4. Delete metadata rows lacking an image AND/OR text embedding for the
        #    run. Real screens are rebuilt from source on the next run; rows with
        #    no source (e.g. injected metadata) stay removed.
        cur.execute(
            """
            DELETE FROM screens_metadata m
            WHERE m.run_id = %s
              AND NOT (
                  EXISTS (
                      SELECT 1 FROM screens_embeddings e
                      WHERE e.screen_id = m.screen_id AND e.run_id = m.run_id
                        AND e.embedding_kind = 'image'
                  )
                  AND EXISTS (
                      SELECT 1 FROM screens_embeddings e
                      WHERE e.screen_id = m.screen_id AND e.run_id = m.run_id
                        AND e.embedding_kind = 'text'
                  )
              )
            """,
            (run_id,),
        )
        counts["deleted_incomplete_metadata"] = cur.rowcount

        # 5. Restore the canonical primary key if a prior corruption dropped it.
        counts["restored_primary_key"] = 1 if _restore_pk(cur) else 0

        conn.commit()

    _log.info("repair_run(%s) -> %s", run_id, counts)
    return counts


# Human labels for the result summary.
_OP_LABELS = {
    "deduped_embeddings": "de-duplicated embedding rows",
    "deleted_invalid_vectors": "deleted null/zero-norm embeddings",
    "deleted_orphan_embeddings": "deleted orphan embeddings",
    "deleted_incomplete_metadata": "deleted incomplete metadata rows",
    "restored_primary_key": "restored embeddings primary key",
}


def summarize(counts: dict) -> str:
    """One-line human summary of what the repair changed (skips no-op steps)."""
    parts = [
        f"{_OP_LABELS[k]}: {v}"
        for k, v in counts.items()
        if v and k in _OP_LABELS
    ]
    return "; ".join(parts) if parts else "no rows needed repair"
