"""Slack webhook notifications for the pipeline."""

from __future__ import annotations

import logging
import os

import requests

from rico_dag.db import get_conn

_log = logging.getLogger(__name__)
_TIMEOUT = 10


def _webhook_url() -> str | None:
    return os.getenv("SLACK_WEBHOOK_URL")


def _pipeline_run_uuid(dag_run_id: str | None) -> str | None:
    if not dag_run_id:
        return None
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT run_id FROM pipeline_runs WHERE dag_run_id = %s",
                (dag_run_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            return str(row[0])
    except Exception as exc:
        _log.warning("_pipeline_run_uuid lookup failed: %s", exc)
        return None


def _post(payload: dict) -> None:
    url = _webhook_url()
    if not url:
        _log.warning("SLACK_WEBHOOK_URL not set — skipping notification")
        return
    try:
        resp = requests.post(url, json=payload, timeout=_TIMEOUT)
        resp.raise_for_status()
    except Exception as exc:
        _log.warning("Slack notification failed (non-fatal): %s", exc)


def notify_run_started(context: dict) -> None:
    try:
        dag_run = context.get("dag_run")
        params = context.get("params", {})
        limit = params.get("LIMIT", "?")
        dag_run_id = dag_run.run_id if dag_run else "unknown"
        pipeline_run_id = _pipeline_run_uuid(dag_run_id) or "pending"
        trigger = "manual" if (dag_run and dag_run.external_trigger) else "scheduled"

        _post({
            "text": (
                f"*Pipeline started*\n"
                f"• `dag_run_id`: `{dag_run_id}`\n"
                f"• `run_id`: `{pipeline_run_id}`\n"
                f"• `LIMIT`: {limit}\n"
                f"• triggered by: {trigger}"
            )
        })
    except Exception as exc:
        _log.warning("notify_run_started failed (non-fatal): %s", exc)


def notify_audit_failed(context: dict) -> None:
    try:
        dag_run = context.get("dag_run")
        ti = context.get("ti")
        exception = context.get("exception", "")
        dag_run_id = dag_run.run_id if dag_run else "unknown"
        pipeline_run_id = _pipeline_run_uuid(dag_run_id) or "unknown"
        log_url = ti.log_url if ti else "unavailable"

        _post({
            "text": (
                f"*Audit FAILED — pipeline halted*\n"
                f"• `dag_run_id`: `{dag_run_id}`\n"
                f"• `run_id`: `{pipeline_run_id}`\n"
                f"• details: ```{str(exception)[:500]}```\n"
                f"• logs: {log_url}"
            )
        })
    except Exception as exc:
        _log.warning("notify_audit_failed failed (non-fatal): %s", exc)


def _fetch_run_summary(dag_run_id: str) -> tuple[str, str, str]:
    """Return (pipeline_run_id, duration_str, metrics_line) for a completed run."""
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT run_id,
                       EXTRACT(EPOCH FROM (ended_at - started_at))::int AS duration_s
                FROM pipeline_runs
                WHERE dag_run_id = %s
                """,
                (dag_run_id,),
            )
            row = cur.fetchone()
            if not row:
                return "unknown", "unknown", "no metrics"
            uuid_run_id, duration_s = row
            duration_str = f"{duration_s}s" if duration_s is not None else "unknown"

            cur.execute(
                """
                SELECT metric_name, metric_value
                FROM pipeline_metrics
                WHERE run_id = %s
                  AND metric_name IN (
                    'screens_metadata_row_count',
                    'pct_extracted',
                    'pct_high_confidence',
                    'embeddings_pct_zero_norm'
                  )
                  AND metric_labels = '{}'::jsonb
                ORDER BY metric_name
                """,
                (str(uuid_run_id),),
            )
            metrics = {r[0]: r[1] for r in cur.fetchall()}
            parts = []
            if "screens_metadata_row_count" in metrics:
                parts.append(f"screens={int(metrics['screens_metadata_row_count'])}")
            if "pct_extracted" in metrics:
                parts.append(f"extracted={metrics['pct_extracted']:.0f}%")
            if "pct_high_confidence" in metrics:
                parts.append(f"high_conf={metrics['pct_high_confidence']:.0f}%")
            if "embeddings_pct_zero_norm" in metrics:
                parts.append(f"zero_norm={metrics['embeddings_pct_zero_norm']:.0f}%")
            return str(uuid_run_id), duration_str, "  ".join(parts) if parts else "no metrics"
    except Exception as exc:
        _log.warning("_fetch_run_summary failed: %s", exc)
        return "unknown", "unknown", "no metrics"


def _pipeline_run_status(dag_run_id: str) -> str | None:
    """Status from pipeline_runs (set by finalize_task), not Airflow's dag_run.state."""
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT status FROM pipeline_runs WHERE dag_run_id = %s",
                (dag_run_id,),
            )
            row = cur.fetchone()
            return row[0] if row else None
    except Exception as exc:
        _log.warning("_pipeline_run_status lookup failed: %s", exc)
        return None


def notify_run_finished(context: dict) -> None:
    try:
        dag_run = context.get("dag_run")
        dag_run_id = dag_run.run_id if dag_run else "unknown"
        airflow_state = dag_run.state if dag_run else "unknown"
        # finalize_task writes the canonical status; Airflow dag_run.state can disagree.
        status = _pipeline_run_status(dag_run_id) or airflow_state

        pipeline_run_id, duration_str, metrics_line = _fetch_run_summary(dag_run_id)

        ok = status in ("succeeded", "success")
        paused = status == "paused_by_audit"
        if ok:
            headline = "*Pipeline finished*"
        elif paused:
            headline = "*Pipeline finished (paused by audit)*"
        else:
            headline = "*Pipeline finished (with failures)*"
        _post({
            "text": (
                f"{headline}\n"
                f"• `dag_run_id`: `{dag_run_id}`\n"
                f"• `run_id`: `{pipeline_run_id}`\n"
                f"• status: `{status}`"
                + (f" (airflow: `{airflow_state}`)" if status != airflow_state else "")
                + f"\n• duration: {duration_str}\n"
                f"• summary: {metrics_line}"
            )
        })
    except Exception as exc:
        _log.warning("notify_run_finished failed (non-fatal): %s", exc)
