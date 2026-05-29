#!/usr/bin/env python3
"""Inject data-integrity corruption for the §5 audit circuit-breaker demo.

The audit (src/rico_dag/audit.py) runs a 5-check integrity suite. This tool can
inject a tagged, fully-reversible corruption for each check so every branch of
the audit can be demonstrated:

  scenario    audit check that fires        what is injected
  ----------  ----------------------------  ------------------------------------
  duplicate   duplicate_embeddings          a clone of a real embedding row
  zero-norm   invalid_vectors               an embedding with a zero vector
  orphan      orphan_embeddings             an embedding for a screen with no metadata
  missing     missing_embeddings            a metadata row with no embeddings
  all         (all of the above)            one of each

Every injected row is tagged with ``source_fingerprint = CHAOS_FINGERPRINT`` and
inserted (never mutating real data), so ``--cleanup`` reverses any scenario by
deleting tagged rows, de-duplicating, and restoring the primary key. The audit
reassigns tagged rows to the current run, so the corruption is caught on the
*next* run (matching §5).

Workflow (Assignment §5):
  1. make dag-trigger LIMIT=5                 # successful run
  2. make chaos-inject SCENARIO=duplicate     # corrupt that run
  3. make dag-trigger LIMIT=5                 # audit_task fails, eval skipped
  4. make chaos-cleanup

Usage:
  python chaos/inject_duplicates.py                       # default scenario=duplicate
  python chaos/inject_duplicates.py --scenario orphan
  python chaos/inject_duplicates.py --scenario all
  python chaos/inject_duplicates.py --run-id <uuid>
  python chaos/inject_duplicates.py --cleanup
  python chaos/inject_duplicates.py --verify
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import psycopg

REPO_ROOT = Path(__file__).resolve().parents[1]

CHAOS_FINGERPRINT = "chaos-duplicate-inject-v1"
EMBEDDINGS_PK = "screens_embeddings_pkey"
PROD_UNIQUE_INDEX = "uq_screens_embeddings_prod"
METADATA_PK = "screens_metadata_pkey"

# Synthetic screen ids for insert-only corruption (well outside real RICO ids).
ORPHAN_SCREEN_ID = 990000001
MISSING_SCREEN_ID = 990000002
ZERO_NORM_MODEL = "chaos-zero-norm"

SCENARIOS = ("duplicate", "zero-norm", "orphan", "missing", "all")


def _load_dotenv() -> None:
    env_path = REPO_ROOT / ".env"
    if not env_path.is_file():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _apply_host_env_defaults() -> None:
    if os.getenv("POSTGRES_HOST", "localhost") in ("postgres", "db"):
        os.environ["POSTGRES_HOST"] = "localhost"


def _dsn() -> str:
    return (
        f"host={os.getenv('POSTGRES_HOST', 'localhost')} "
        f"port={os.getenv('POSTGRES_PORT', '5432')} "
        f"dbname={os.getenv('POSTGRES_DB', 'rico')} "
        f"user={os.getenv('POSTGRES_USER', 'rico')} "
        f"password={os.getenv('POSTGRES_PASSWORD', 'rico')}"
    )


def _connect() -> psycopg.Connection:
    return psycopg.connect(_dsn(), connect_timeout=10)


def _info(msg: str) -> None:
    print(f"[chaos] {msg}")


def _error(msg: str) -> None:
    print(f"[chaos] ERROR: {msg}", file=sys.stderr)


def _resolve_run_id(conn: psycopg.Connection, run_id: str | None) -> str:
    if run_id:
        return run_id
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT run_id::text
            FROM pipeline_runs
            WHERE run_id != '00000000-0000-0000-0000-000000000000'
              AND status = 'succeeded'
            ORDER BY started_at DESC
            LIMIT 1
            """
        )
        row = cur.fetchone()
    if not row:
        raise RuntimeError(
            "No succeeded pipeline_runs row found. Run the DAG first: make dag-trigger LIMIT=5"
        )
    return row[0]


def _pk_exists(conn: psycopg.Connection, table: str, pk_name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM pg_constraint c
            JOIN pg_class t ON t.oid = c.conrelid
            WHERE t.relname = %s AND c.conname = %s AND c.contype = 'p'
            """,
            (table, pk_name),
        )
        return cur.fetchone() is not None


def _index_exists(conn: psycopg.Connection, index_name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM pg_indexes WHERE indexname = %s",
            (index_name,),
        )
        return cur.fetchone() is not None


def _ensure_prod_unique_index(conn: psycopg.Connection) -> None:
    """Allow one production row per embedding key while chaos rows may duplicate it."""
    if _index_exists(conn, PROD_UNIQUE_INDEX):
        return
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE UNIQUE INDEX {PROD_UNIQUE_INDEX}
            ON screens_embeddings (screen_id, model_name, model_version, embedding_kind)
            WHERE source_fingerprint IS DISTINCT FROM %s
            """,
            (CHAOS_FINGERPRINT,),
        )
    conn.commit()
    _info(f"created {PROD_UNIQUE_INDEX} (embed ON CONFLICT keeps working)")


def _drop_pk(conn: psycopg.Connection) -> bool:
    if not _pk_exists(conn, "screens_embeddings", EMBEDDINGS_PK):
        return False
    with conn.cursor() as cur:
        cur.execute(f"ALTER TABLE screens_embeddings DROP CONSTRAINT {EMBEDDINGS_PK}")  # noqa: S608
    conn.commit()
    return True


def _restore_pk(conn: psycopg.Connection) -> None:
    if _pk_exists(conn, "screens_embeddings", EMBEDDINGS_PK):
        return
    with conn.cursor() as cur:
        cur.execute(
            f"""
            ALTER TABLE screens_embeddings
            ADD CONSTRAINT {EMBEDDINGS_PK}
            PRIMARY KEY (screen_id, model_name, model_version, embedding_kind)
            """
        )
    conn.commit()


def _dedupe_embeddings(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            DELETE FROM screens_embeddings a
            USING screens_embeddings b
            WHERE a.screen_id = b.screen_id
              AND a.model_name = b.model_name
              AND a.model_version = b.model_version
              AND a.embedding_kind = b.embedding_kind
              AND a.ctid < b.ctid
            """
        )
        return cur.rowcount


def cleanup(conn: psycopg.Connection) -> None:
    """Reverse any scenario: delete tagged rows, de-duplicate, restore PK."""
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM screens_embeddings WHERE source_fingerprint = %s",
            (CHAOS_FINGERPRINT,),
        )
        emb_deleted = cur.rowcount
        cur.execute(
            "DELETE FROM screens_metadata WHERE source_fingerprint = %s",
            (CHAOS_FINGERPRINT,),
        )
        meta_deleted = cur.rowcount
    conn.commit()
    deduped = _dedupe_embeddings(conn)
    _restore_pk(conn)
    _info(
        f"deleted {emb_deleted} chaos embedding row(s), {meta_deleted} chaos metadata row(s), "
        f"{deduped} deduped row(s); PK restored"
    )


# ── Integrity verification (mirrors src/rico_dag/audit.py, no airflow import) ──


def _count_violations(conn: psycopg.Connection, run_id: str) -> dict[str, int]:
    checks = {
        "duplicate_embeddings": """
            SELECT COUNT(*) FROM (
                SELECT 1 FROM screens_embeddings WHERE run_id = %s
                GROUP BY screen_id, model_name, model_version, embedding_kind
                HAVING COUNT(*) > 1
            ) t
        """,
        "duplicate_metadata": """
            SELECT COUNT(*) FROM (
                SELECT 1 FROM screens_metadata WHERE run_id = %s
                GROUP BY screen_id HAVING COUNT(*) > 1
            ) t
        """,
        "invalid_vectors": """
            SELECT COUNT(*) FROM screens_embeddings
            WHERE run_id = %s AND (vector IS NULL OR vector_norm(vector) < 1e-6)
        """,
        "orphan_embeddings": """
            SELECT COUNT(*) FROM screens_embeddings e
            LEFT JOIN screens_metadata m
              ON m.screen_id = e.screen_id AND m.run_id = e.run_id
            WHERE e.run_id = %s AND m.screen_id IS NULL
        """,
        "missing_embeddings": """
            SELECT COUNT(*) FROM (
                SELECT m.screen_id
                FROM screens_metadata m
                LEFT JOIN screens_embeddings e
                  ON e.screen_id = m.screen_id AND e.run_id = m.run_id
                WHERE m.run_id = %s
                GROUP BY m.screen_id
                HAVING NOT (
                    COALESCE(BOOL_OR(e.embedding_kind = 'image'), FALSE)
                    AND COALESCE(BOOL_OR(e.embedding_kind = 'text'), FALSE)
                )
            ) t
        """,
    }
    out: dict[str, int] = {}
    with conn.cursor() as cur:
        for name, sql in checks.items():
            cur.execute(sql, (run_id,))
            out[name] = cur.fetchone()[0]
    return out


def verify(conn: psycopg.Connection, run_id: str) -> int:
    counts = _count_violations(conn, run_id)
    total = sum(counts.values())
    _info(f"run_id={run_id}")
    for name, n in counts.items():
        flag = "  <-- audit will FAIL" if n else ""
        _info(f"  {name}: {n}{flag}")

    if not _pk_exists(conn, "screens_embeddings", EMBEDDINGS_PK):
        if _index_exists(conn, PROD_UNIQUE_INDEX):
            _info("PK absent (chaos demo) — embed uses UPDATE/INSERT, not ON CONFLICT")
        else:
            _error("screens_embeddings_pkey missing — run make chaos-cleanup or make db-repair")
    return 1 if total else 0


# ── Scenario injectors (all insert-only, all tagged CHAOS_FINGERPRINT) ─────────


def _real_seed(conn: psycopg.Connection, run_id: str) -> tuple:
    """Return (screen_id, model_name, model_version, embedding_kind) of a real row."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT screen_id, model_name, model_version, embedding_kind
            FROM screens_embeddings
            WHERE run_id = %s AND source_fingerprint != %s
            LIMIT 1
            """,
            (run_id, CHAOS_FINGERPRINT),
        )
        seed = cur.fetchone()
    if not seed:
        raise RuntimeError(f"No embeddings for run_id={run_id}")
    return seed


def inject_duplicate(conn: psycopg.Connection, run_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM screens_embeddings WHERE run_id = %s AND source_fingerprint = %s "
            "AND model_name != %s AND screen_id < %s",
            (run_id, CHAOS_FINGERPRINT, ZERO_NORM_MODEL, ORPHAN_SCREEN_ID),
        )
        if cur.fetchone():
            _info("duplicate: chaos row already present — skipping")
            return

    seed = _real_seed(conn, run_id)
    _ensure_prod_unique_index(conn)
    if _drop_pk(conn):
        _info(f"duplicate: dropped {EMBEDDINGS_PK} briefly to insert chaos duplicate")

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO screens_embeddings (
                screen_id, model_name, model_version, embedding_kind,
                vector, run_id, source_fingerprint
            )
            SELECT screen_id, model_name, model_version, embedding_kind,
                   vector, run_id, %s
            FROM screens_embeddings
            WHERE run_id = %s AND screen_id = %s AND model_name = %s
              AND model_version = %s AND embedding_kind = %s
              AND source_fingerprint != %s
            LIMIT 1
            """,
            (CHAOS_FINGERPRINT, run_id, seed[0], seed[1], seed[2], seed[3], CHAOS_FINGERPRINT),
        )
        if cur.rowcount != 1:
            raise RuntimeError("chaos duplicate INSERT did not insert exactly one row")
    conn.commit()
    _info(f"duplicate: inserted clone of screen_id={seed[0]} ({seed[3]}) -> duplicate_embeddings")


def inject_zero_norm(conn: psycopg.Connection, run_id: str) -> None:
    seed = _real_seed(conn, run_id)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM screens_embeddings WHERE run_id = %s AND source_fingerprint = %s "
            "AND model_name = %s",
            (run_id, CHAOS_FINGERPRINT, ZERO_NORM_MODEL),
        )
        if cur.fetchone():
            _info("zero-norm: chaos row already present — skipping")
            return
        # Distinct model_name => unique key => no PK conflict, isolates invalid_vectors.
        cur.execute(
            """
            INSERT INTO screens_embeddings (
                screen_id, model_name, model_version, embedding_kind,
                vector, run_id, source_fingerprint
            )
            VALUES (%s, %s, 'chaos', %s, '[0,0,0]'::vector, %s, %s)
            """,
            (seed[0], ZERO_NORM_MODEL, seed[3], run_id, CHAOS_FINGERPRINT),
        )
    conn.commit()
    _info(f"zero-norm: inserted zero-vector for screen_id={seed[0]} -> invalid_vectors")


def inject_orphan(conn: psycopg.Connection, run_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM screens_embeddings WHERE screen_id = %s AND source_fingerprint = %s",
            (ORPHAN_SCREEN_ID, CHAOS_FINGERPRINT),
        )
        if cur.fetchone():
            _info("orphan: chaos row already present — skipping")
            return
        # Synthetic screen_id with NO metadata row -> orphan. Copies a real (valid) vector.
        cur.execute(
            """
            INSERT INTO screens_embeddings (
                screen_id, model_name, model_version, embedding_kind,
                vector, run_id, source_fingerprint
            )
            SELECT %s, model_name, model_version, embedding_kind, vector, %s, %s
            FROM screens_embeddings
            WHERE run_id = %s AND source_fingerprint != %s
            LIMIT 1
            """,
            (ORPHAN_SCREEN_ID, run_id, CHAOS_FINGERPRINT, run_id, CHAOS_FINGERPRINT),
        )
        if cur.rowcount != 1:
            raise RuntimeError("chaos orphan INSERT did not insert exactly one row")
    conn.commit()
    _info(f"orphan: inserted embedding for unknown screen_id={ORPHAN_SCREEN_ID} -> orphan_embeddings")


def inject_missing(conn: psycopg.Connection, run_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM screens_metadata WHERE screen_id = %s AND source_fingerprint = %s",
            (MISSING_SCREEN_ID, CHAOS_FINGERPRINT),
        )
        if cur.fetchone():
            _info("missing: chaos row already present — skipping")
            return
        # Metadata row with NO embeddings -> missing_embeddings.
        cur.execute(
            """
            INSERT INTO screens_metadata (
                screen_id, png_path, hierarchy_json_path, run_id, source_fingerprint
            )
            VALUES (%s, 'chaos://missing.png', 'chaos://missing.json', %s, %s)
            ON CONFLICT (screen_id) DO NOTHING
            """,
            (MISSING_SCREEN_ID, run_id, CHAOS_FINGERPRINT),
        )
    conn.commit()
    _info(f"missing: inserted metadata-only screen_id={MISSING_SCREEN_ID} -> missing_embeddings")


_INJECTORS = {
    "duplicate": inject_duplicate,
    "zero-norm": inject_zero_norm,
    "orphan": inject_orphan,
    "missing": inject_missing,
}


def inject(conn: psycopg.Connection, run_id: str, scenario: str) -> None:
    targets = list(_INJECTORS) if scenario == "all" else [scenario]
    for name in targets:
        _INJECTORS[name](conn, run_id)


def main() -> int:
    parser = argparse.ArgumentParser(description="Chaos integrity-corruption injection for audit demo (§5).")
    parser.add_argument("--run-id", help="Pipeline run UUID (default: latest succeeded)")
    parser.add_argument(
        "--scenario",
        choices=SCENARIOS,
        default="duplicate",
        help="Which integrity violation to inject (default: duplicate)",
    )
    parser.add_argument("--cleanup", action="store_true", help="Remove chaos rows and restore PK")
    parser.add_argument("--verify", action="store_true", help="Show integrity violation counts for run_id")
    parser.add_argument(
        "--compose-env",
        action="store_true",
        help="Keep POSTGRES_HOST=postgres from .env (inside Docker)",
    )
    args = parser.parse_args()

    _load_dotenv()
    if not args.compose_env:
        _apply_host_env_defaults()

    try:
        with _connect() as conn:
            if args.cleanup:
                cleanup(conn)
                return 0

            run_id = _resolve_run_id(conn, args.run_id)
            if args.verify:
                return verify(conn, run_id)

            _info(f"Injecting audit demo corruption (scenario={args.scenario}, Assignment §5)")
            cleanup(conn)
            inject(conn, run_id, args.scenario)
            if verify(conn, run_id):
                _info("corruption confirmed for injected run_id")
            else:
                _error("inject ran but no violation detected for run_id")
                return 1
    except Exception as exc:  # noqa: BLE001
        _error(str(exc))
        return 1

    print()
    _info("Next: make dag-trigger LIMIT=5 (audit_task should fail, eval skipped)")
    _info("Then: make chaos-cleanup")
    return 0


if __name__ == "__main__":
    sys.exit(main())
