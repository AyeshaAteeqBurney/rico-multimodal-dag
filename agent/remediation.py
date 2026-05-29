"""Execute confirm-gated fixes proposed by agent/diagnostics.py.

  - Integrity repair runs generic, schema-level SQL (agent.repair) that fixes
    *real* corruption by data properties — not by chaos tag.
  - Re-runs and unpause go through the Airflow REST API (agent.airflow_client).

Every fix is only ever called after a human confirms in Slack.
"""

from __future__ import annotations

import logging

from agent import airflow_client, diagnostics, repair
from agent.diagnostics import Diagnosis

_log = logging.getLogger(__name__)


class RemediationError(Exception):
    """Raised when a fix step fails."""


def _run_url(dag_run_id: str) -> str:
    return (
        f"{airflow_client.settings.airflow_api_url}/dags/"
        f"{airflow_client.settings.airflow_dag_id}/grid?dag_run_id={dag_run_id}"
    )


def execute(diagnosis: Diagnosis) -> str:
    """Carry out the recommended fix. Returns a Slack-formatted result message.

    Raises:
        RemediationError / AirflowError on failure (caller reports to Slack).
    """
    action = diagnosis.fix_action
    limit = diagnosis.limit

    if action == diagnostics.FIX_NONE:
        return "Nothing to fix — the pipeline is healthy."

    steps: list[str] = []

    if action == diagnostics.FIX_REPAIR_AND_RERUN:
        if not diagnosis.run_id:
            raise RemediationError("repair requested but no run_id in diagnosis")
        counts = repair.repair_run(diagnosis.run_id)
        steps.append(f"Repaired data integrity ({repair.summarize(counts)})")

    if action == diagnostics.FIX_UNPAUSE_AND_RERUN:
        airflow_client.set_paused(False)
        steps.append("Unpaused the DAG")

    # All fixable actions end by (re)running the pipeline.
    dag_run_id = airflow_client.trigger_dag(limit)
    steps.append(
        f"Triggered a new run (LIMIT={limit})\n"
        f"• *dag_run_id*: `{dag_run_id}`\n"
        f"• *Airflow UI*: {_run_url(dag_run_id)}"
    )

    numbered = "\n".join(f"{i}. {s}" for i, s in enumerate(steps, start=1))
    return f"Fix applied:\n{numbered}"
