"""Read-only Postgres access for the agent's diagnosis feature.

The agent runs on the host and connects to the published Postgres port.
It only reads run/audit state — it never writes (remediation is done via
the Airflow API and the existing chaos-cleanup script).
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Iterator

import psycopg

from agent.config import settings

_log = logging.getLogger(__name__)

BOOTSTRAP_RUN_ID = "00000000-0000-0000-0000-000000000000"


def _dsn() -> str:
    return (
        f"host={settings.postgres_host} "
        f"port={settings.postgres_port} "
        f"dbname={settings.postgres_db} "
        f"user={settings.postgres_user} "
        f"password={settings.postgres_password}"
    )


@contextmanager
def get_conn() -> Iterator[psycopg.Connection]:
    with psycopg.connect(_dsn(), connect_timeout=10) as conn:
        yield conn


def latest_run() -> dict | None:
    """Return the most recent non-bootstrap pipeline run, or None."""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT run_id::text, dag_run_id, status, limit_param, started_at, ended_at
            FROM pipeline_runs
            WHERE run_id != %s
            ORDER BY started_at DESC
            LIMIT 1
            """,
            (BOOTSTRAP_RUN_ID,),
        )
        row = cur.fetchone()
    if not row:
        return None
    return {
        "run_id": row[0],
        "dag_run_id": row[1],
        "status": row[2],
        "limit_param": row[3],
        "started_at": row[4],
        "ended_at": row[5],
    }


def latest_audit(run_id: str) -> dict | None:
    """Return the most recent audit_results row for a run, or None."""
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT audit_name, passed, details, created_at
            FROM audit_results
            WHERE run_id = %s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
    if not row:
        return None
    return {
        "audit_name": row[0],
        "passed": row[1],
        "details": row[2],
        "created_at": row[3],
    }
