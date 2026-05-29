"""Airflow REST API client — triggers rico_pipeline with a dynamic LIMIT.

Uses the Airflow 2.x stable endpoint:
  POST /api/v1/dags/{dag_id}/dagRuns
  Body: {"conf": {"LIMIT": <int>}}
"""

from __future__ import annotations

import logging

import requests

from agent.config import settings

_log = logging.getLogger(__name__)
_TIMEOUT = 15

_UNPAUSE_MSG = (
    f"`{{dag_id}}` is paused in Airflow — the run will never start. "
    "Unpause it in the UI first: "
    f"{{url}}/dags/{{dag_id}}/grid"
)


class AirflowError(Exception):
    """Raised when the Airflow API returns an error or the DAG is in a bad state."""


def _auth() -> tuple[str, str]:
    return (settings.airflow_api_user, settings.airflow_api_password)


def _check_dag_paused() -> None:
    """Raise AirflowError before triggering if the DAG is paused.

    Airflow 2.x returns HTTP 200 even for paused DAGs — the run is created
    but sits in the queue and never executes.  Checking upfront prevents the
    agent from falsely confirming a trigger that will not run.
    """
    url = f"{settings.airflow_api_url}/api/v1/dags/{settings.airflow_dag_id}"
    try:
        resp = requests.get(url, auth=_auth(), timeout=_TIMEOUT)
    except requests.RequestException as exc:
        raise AirflowError(
            f"Could not reach Airflow at {settings.airflow_api_url}: {exc}"
        ) from exc

    if not resp.ok:
        # Non-fatal: if we can't read DAG state, proceed and let the trigger
        # call surface any real error.
        _log.warning("Could not verify DAG state (HTTP %s) — proceeding anyway", resp.status_code)
        return

    if resp.json().get("is_paused"):
        raise AirflowError(
            _UNPAUSE_MSG.format(dag_id=settings.airflow_dag_id, url=settings.airflow_api_url)
        )


def trigger_dag(limit: int) -> str:
    """Trigger rico_pipeline with the given LIMIT.

    Performs a preflight check to verify the DAG is not paused before
    creating a dag run, so the agent never confirms a run that won't execute.

    Returns:
        dag_run_id (str) — e.g. "manual__2026-05-29T10:00:00+00:00"

    Raises:
        AirflowError — if the DAG is paused, unreachable, or the API errors.
    """
    # Preflight: refuse to trigger if the DAG is paused.
    _check_dag_paused()

    url = f"{settings.airflow_api_url}/api/v1/dags/{settings.airflow_dag_id}/dagRuns"
    payload = {"conf": {"LIMIT": limit}}

    _log.info("Triggering DAG %s with LIMIT=%d via %s", settings.airflow_dag_id, limit, url)
    try:
        response = requests.post(
            url,
            json=payload,
            auth=_auth(),
            timeout=_TIMEOUT,
        )
    except requests.RequestException as exc:
        raise AirflowError(
            f"Could not reach Airflow at {settings.airflow_api_url}: {exc}"
        ) from exc

    if response.status_code == 409:
        raise AirflowError(
            _UNPAUSE_MSG.format(dag_id=settings.airflow_dag_id, url=settings.airflow_api_url)
        )

    if not response.ok:
        try:
            detail = response.json().get("detail", response.text[:300])
        except Exception:
            detail = response.text[:300]
        raise AirflowError(
            f"Airflow API returned HTTP {response.status_code}: {detail}"
        )

    body = response.json()
    dag_run_id: str = body["dag_run_id"]

    # Defensive: double-check the returned run state is not already failed.
    run_state: str = body.get("state", "")
    if run_state == "failed":
        raise AirflowError(
            f"Airflow accepted the trigger but immediately marked the run as failed "
            f"(dag_run_id=`{dag_run_id}`). Check the Airflow UI for details."
        )

    _log.info("DAG triggered successfully: dag_run_id=%s state=%s", dag_run_id, run_state)
    return dag_run_id
