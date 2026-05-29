#!/usr/bin/env python3
"""Inject duplicate embedding rows for the §5 audit circuit-breaker demo.

Uses a partial unique index (see migrations/003_embeddings_chaos_safe.sql) so
production embed ON CONFLICT still works while a tagged chaos duplicate exists.

Workflow (Assignment §5):
  1. make dag-trigger LIMIT=5          # successful run
  2. make chaos-inject                 # duplicate on that run_id
  3. make dag-trigger LIMIT=5          # audit_task fails, eval skipped
  4. make chaos-cleanup

Usage:
  python chaos/inject_duplicates.py
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


def verify(conn: psycopg.Connection, run_id: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT screen_id, model_name, model_version, embedding_kind, COUNT(*)
            FROM screens_embeddings
            WHERE run_id = %s
            GROUP BY 1, 2, 3, 4
            HAVING COUNT(*) > 1
            """,
            (run_id,),
        )
        emb = cur.fetchall()

    _info(f"run_id={run_id}")
    if emb:
        _info(f"embedding duplicate groups: {len(emb)} (audit will FAIL for this run_id)")
        for row in emb[:5]:
            _info(f"  screen_id={row[0]} {row[1]}/{row[2]}/{row[3]} count={row[4]}")
    else:
        _info("embedding duplicate groups: 0 for this run_id (audit passes unless reassigned)")

    if not _pk_exists(conn, "screens_embeddings", EMBEDDINGS_PK):
        if _index_exists(conn, PROD_UNIQUE_INDEX):
            _info("PK absent (chaos demo) — embed uses UPDATE/INSERT, not ON CONFLICT")
        else:
            _error("screens_embeddings_pkey missing — run make chaos-cleanup or make db-repair")
    return 1 if emb else 0


def inject_embeddings(conn: psycopg.Connection, run_id: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1 FROM screens_embeddings
            WHERE run_id = %s AND source_fingerprint = %s
            """,
            (run_id, CHAOS_FINGERPRINT),
        )
        if cur.fetchone():
            _info("chaos embedding row already present — skipping")
            return

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

    _ensure_prod_unique_index(conn)
    if _drop_pk(conn):
        _info(f"dropped {EMBEDDINGS_PK} briefly to insert chaos duplicate")

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
            WHERE run_id = %s
              AND screen_id = %s
              AND model_name = %s
              AND model_version = %s
              AND embedding_kind = %s
              AND source_fingerprint != %s
            LIMIT 1
            """,
            (
                CHAOS_FINGERPRINT,
                run_id,
                seed[0],
                seed[1],
                seed[2],
                seed[3],
                CHAOS_FINGERPRINT,
            ),
        )
        if cur.rowcount != 1:
            raise RuntimeError("chaos embedding INSERT did not insert exactly one row")
    conn.commit()
    _info(
        f"inserted chaos duplicate for screen_id={seed[0]} ({seed[3]}); "
        "embed tasks update production rows (chaos row left untouched)"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Chaos duplicate injection for audit demo (§5).")
    parser.add_argument("--run-id", help="Pipeline run UUID (default: latest succeeded)")
    parser.add_argument("--cleanup", action="store_true", help="Remove chaos rows and restore PK")
    parser.add_argument("--verify", action="store_true", help="Show duplicate counts for run_id")
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

            _info("Injecting audit demo duplicate (Assignment §5)")
            cleanup(conn)
            inject_embeddings(conn, run_id)
            if verify(conn, run_id):
                _info("duplicates confirmed for injected run_id")
            else:
                _error("inject ran but no duplicate group for run_id")
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
