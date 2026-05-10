"""Compute and persist pipeline health and data quality metrics."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

import psycopg

from rico_dag.db import get_conn, logger_with_run_id

_log = logging.getLogger(__name__)


def compute_and_persist(*, run_id: str, context: dict | None = None, task_xcoms: dict | None = None) -> dict:
    log = logger_with_run_id(_log, run_id)
    _compute_data_quality(run_id=run_id, log=log)
    if context and context.get("dag_run"):
        _compute_pipeline_health(run_id=run_id, context=context, log=log, task_xcoms=task_xcoms)
    log_summary(run_id=run_id, log=log)
    return {"run_id": run_id, "task": "metrics"}


def _compute_data_quality(*, run_id: str, log: logging.LoggerAdapter) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM screens_metadata WHERE run_id = %s", (run_id,))
            (meta_count,) = cur.fetchone()
            _insert_metric(conn, run_id, "screens_metadata_row_count", meta_count, {})

            if meta_count > 0:
                cur.execute(
                    "SELECT COUNT(*) FILTER (WHERE extraction_payload IS NOT NULL) FROM screens_metadata WHERE run_id = %s",
                    (run_id,),
                )
                (extracted_count,) = cur.fetchone()
                _insert_metric(conn, run_id, "pct_extracted", extracted_count / meta_count * 100, {})

                cur.execute(
                    "SELECT COUNT(*) FILTER (WHERE confidence >= 0.5) FROM screens_metadata WHERE run_id = %s",
                    (run_id,),
                )
                (high_conf_count,) = cur.fetchone()
                _insert_metric(conn, run_id, "pct_high_confidence", high_conf_count / meta_count * 100, {})

            cur.execute("SELECT COUNT(*) FROM screens_review_queue WHERE run_id = %s", (run_id,))
            (review_count,) = cur.fetchone()
            pct_review = (review_count / meta_count * 100) if meta_count > 0 else 0
            _insert_metric(conn, run_id, "pct_in_review_queue", pct_review, {})

            cur.execute(
                """
                SELECT
                    model_version,
                    embedding_kind,
                    COUNT(*) AS n,
                    AVG(vector_dims(vector)) AS avg_dim,
                    COUNT(*) FILTER (WHERE vector_norm(vector) < 0.001) AS zero_norm_count
                FROM screens_embeddings
                WHERE run_id = %s
                GROUP BY model_version, embedding_kind
                """,
                (run_id,),
            )
            for model_version, embedding_kind, n, avg_dim, zero_norm_count in cur.fetchall():
                labels = {"model_version": model_version, "embedding_kind": embedding_kind}
                _insert_metric(conn, run_id, "embeddings_row_count", n, labels)
                if avg_dim:
                    _insert_metric(conn, run_id, "embeddings_avg_dim", float(avg_dim), labels)
                pct_zero = (zero_norm_count / n * 100) if n > 0 else 0
                _insert_metric(conn, run_id, "embeddings_pct_zero_norm", pct_zero, labels)

            cur.execute("SELECT COUNT(DISTINCT app_package) FROM screens_metadata WHERE run_id = %s", (run_id,))
            (distinct_packages,) = cur.fetchone()
            _insert_metric(conn, run_id, "distinct_app_packages", distinct_packages, {})

            cur.execute("SELECT COUNT(DISTINCT category) FROM screens_metadata WHERE run_id = %s", (run_id,))
            (distinct_categories,) = cur.fetchone()
            _insert_metric(conn, run_id, "distinct_categories", distinct_categories, {})

        conn.commit()
    log.info("data quality metrics computed")


def _compute_pipeline_health(*, run_id: str, context: dict, log: logging.LoggerAdapter, task_xcoms: dict | None = None) -> None:
    dag_run = context.get("dag_run")
    if not dag_run:
        return

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT started_at FROM pipeline_runs WHERE run_id = %s", (run_id,))
            (started_at,) = cur.fetchone()
        total_duration = (datetime.now(timezone.utc) - started_at).total_seconds()
        _insert_metric(conn, run_id, "total_run_duration_seconds", total_duration, {})

        for ti in dag_run.get_task_instances():
            if ti.task_id == "finalize_task":
                continue
            if ti.duration:
                _insert_metric(conn, run_id, "task_duration_seconds", ti.duration, {"task_id": ti.task_id})
            retries = max(0, (ti.try_number or 1) - 1)
            _insert_metric(conn, run_id, "task_retries", retries, {"task_id": ti.task_id})

        if task_xcoms:
            for task_id, xcom in task_xcoms.items():
                if not isinstance(xcom, dict):
                    continue
                if "rows_in" in xcom:
                    _insert_metric(conn, run_id, "task_rows_in", xcom["rows_in"], {"task_id": task_id})
                if "rows_out" in xcom:
                    _insert_metric(conn, run_id, "task_rows_out", xcom["rows_out"], {"task_id": task_id})

        conn.commit()
    log.info("pipeline health metrics computed")


def _insert_metric(
    conn: psycopg.Connection,
    run_id: str,
    metric_name: str,
    metric_value: float,
    metric_labels: dict,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_metrics (run_id, metric_name, metric_value, metric_labels, created_at)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                run_id,
                metric_name,
                float(metric_value),
                json.dumps(metric_labels),
                datetime.now(timezone.utc),
            ),
        )


def log_summary(*, run_id: str, log: logging.LoggerAdapter | None = None) -> None:
    if log is None:
        log = logger_with_run_id(_log, run_id)

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT metric_name, metric_value, metric_labels FROM pipeline_metrics WHERE run_id = %s ORDER BY metric_name",
            (run_id,),
        )
        rows = cur.fetchall()

    if not rows:
        return

    lines = ["=" * 70, f"PIPELINE METRICS  run_id={run_id}", "=" * 70]
    for name, value, labels in rows:
        label_str = ""
        if labels:
            label_str = "  [" + ", ".join(f"{k}={v}" for k, v in labels.items()) + "]"
        lines.append(f"  {name}: {value:.2f}{label_str}")
    lines.append("=" * 70)
    log.info("\n".join(lines))
