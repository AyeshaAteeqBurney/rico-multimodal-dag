"""Smoke test: trigger rico_pipeline directly without Slack.

Useful for verifying Airflow API auth and DAG state before running the full agent.

Usage:
    python -m agent.trigger_smoke          # default LIMIT=5
    python -m agent.trigger_smoke 10       # custom LIMIT
"""

from __future__ import annotations

import sys

from agent.airflow_client import AirflowError, trigger_dag
from agent.config import settings


def main() -> None:
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 5
    print(f"Smoke test: triggering {settings.airflow_dag_id} with LIMIT={limit}")
    print(f"  Airflow URL  : {settings.airflow_api_url}")
    print(f"  Auth user    : {settings.airflow_api_user}")
    try:
        dag_run_id = trigger_dag(limit)
        print(f"\nSuccess! dag_run_id = {dag_run_id}")
        print(
            f"Open: {settings.airflow_api_url}/dags/{settings.airflow_dag_id}"
            f"/grid?dag_run_id={dag_run_id}"
        )
    except AirflowError as exc:
        print(f"\nFailed: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
