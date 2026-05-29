"""Database helpers for run-traceable writes."""

from __future__ import annotations

import hashlib
import logging
import os
import subprocess
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Iterator

import psycopg

from rico_dag.config import settings


def _postgres_dsn() -> str:
    return (
        f"host={settings.postgres_host} "
        f"port={settings.postgres_port} "
        f"dbname={settings.postgres_db} "
        f"user={settings.postgres_user} "
        f"password={settings.postgres_password}"
    )


@contextmanager
def get_conn() -> Iterator[psycopg.Connection]:
    with psycopg.connect(_postgres_dsn()) as conn:
        yield conn


def fingerprint(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def resolve_git_sha() -> str:
    env_sha = os.getenv("GIT_SHA")
    if env_sha:
        return env_sha
    try:
        out = subprocess.check_output(["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL)
        return out.decode("utf-8").strip()
    except Exception:
        return "unknown"


def start_run(*, dag_run_id: str, limit_param: int) -> str:
    run_id = str(uuid.uuid4())
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_runs (
                run_id, dag_run_id, status, limit_param, git_sha,
                clip_version, sbert_version, llm_model, prompt_version
            ) VALUES (%s, %s, 'running', %s, %s, %s, %s, %s, %s)
            """,
            (
                run_id,
                dag_run_id,
                limit_param,
                resolve_git_sha(),
                settings.clip_version,
                settings.sbert_version,
                settings.ollama_model,
                settings.prompt_version,
            ),
        )
        conn.commit()
    return run_id


def end_run(*, run_id: str, status: str) -> None:
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            UPDATE pipeline_runs
            SET status = %s, ended_at = %s
            WHERE run_id = %s
            """,
            (status, datetime.now(timezone.utc), run_id),
        )
        conn.commit()


def logger_with_run_id(base_logger: logging.Logger, run_id: str) -> logging.LoggerAdapter:
    """Attach ``run_id`` to log records (Project 4). Use this in embed/extract/load."""
    return logging.LoggerAdapter(base_logger, extra={"run_id": run_id})


CHAOS_FINGERPRINT = "chaos-duplicate-inject-v1"


def _embeddings_pk_exists(cur: psycopg.Cursor) -> bool:
    cur.execute(
        """
        SELECT 1 FROM pg_constraint
        WHERE conname = 'screens_embeddings_pkey' AND contype = 'p'
        """
    )
    return cur.fetchone() is not None


def upsert_screen_embedding(
    cur: psycopg.Cursor,
    *,
    screen_id: int,
    model_name: str,
    model_version: str,
    embedding_kind: str,
    vector,
    run_id: str,
    source_fingerprint: str,
) -> bool:
    """Insert or update a production embedding row.

    Works when ``screens_embeddings_pkey`` is present (normal runs) and when
    chaos-inject dropped the PK but a tagged duplicate row coexists.
    """
    cur.execute(
        """
        UPDATE screens_embeddings
        SET vector = %s,
            run_id = %s::uuid,
            source_fingerprint = %s
        WHERE screen_id = %s
          AND model_name = %s
          AND model_version = %s
          AND embedding_kind = %s
          AND source_fingerprint IS DISTINCT FROM %s
        """,
        (
            vector,
            run_id,
            source_fingerprint,
            screen_id,
            model_name,
            model_version,
            embedding_kind,
            CHAOS_FINGERPRINT,
        ),
    )
    if cur.rowcount > 0:
        return True

    if _embeddings_pk_exists(cur):
        cur.execute(
            """
            INSERT INTO screens_embeddings (
                screen_id, model_name, model_version, embedding_kind,
                vector, run_id, source_fingerprint
            ) VALUES (%s, %s, %s, %s, %s, %s::uuid, %s)
            ON CONFLICT ON CONSTRAINT screens_embeddings_pkey DO UPDATE SET
                vector = EXCLUDED.vector,
                run_id = EXCLUDED.run_id,
                source_fingerprint = EXCLUDED.source_fingerprint
            """,
            (
                screen_id,
                model_name,
                model_version,
                embedding_kind,
                vector,
                run_id,
                source_fingerprint,
            ),
        )
        return cur.rowcount > 0

    cur.execute(
        """
        INSERT INTO screens_embeddings (
            screen_id, model_name, model_version, embedding_kind,
            vector, run_id, source_fingerprint
        ) VALUES (%s, %s, %s, %s, %s, %s::uuid, %s)
        """,
        (
            screen_id,
            model_name,
            model_version,
            embedding_kind,
            vector,
            run_id,
            source_fingerprint,
        ),
    )
    return cur.rowcount > 0
