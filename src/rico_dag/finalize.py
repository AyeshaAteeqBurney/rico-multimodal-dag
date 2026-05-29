"""Finalize run status and persist end-of-run metrics."""

from __future__ import annotations

from rico_dag.db import end_run
from rico_dag.metrics import compute_and_persist

_TRACKED_TASKS = [
    "ingest_task",
    "parse_task",
    "embed_image_task",
    "embed_text_task",
    "extract_task",
    "load_task",
]


def _norm_state(state: object | None) -> str | None:
    if state is None:
        return None
    raw = state.value if hasattr(state, "value") else state
    return str(raw).split(".")[-1].lower()


def _resolve_final_status(*, by_task: dict[str, object]) -> str:
    audit_state = _norm_state(by_task.get("audit_task"))
    eval_state = _norm_state(by_task.get("eval_task"))
    any_failed = any(_norm_state(state) == "failed" for state in by_task.values())

    # §3.4 / §3.5: succeeded | failed | paused-by-audit
    if audit_state == "failed":
        return "paused_by_audit"
    if any_failed or audit_state not in ("success",) or eval_state not in ("success",):
        return "failed"
    return "succeeded"


def run(*, context: dict) -> dict:
    """Compute final pipeline status and persist metrics."""
    ti = context["ti"]
    dag_run = context["dag_run"]
    ingest_payload = ti.xcom_pull(task_ids="ingest_task")
    run_id = ingest_payload["run_id"]

    task_xcoms = {task_id: ti.xcom_pull(task_ids=task_id) for task_id in _TRACKED_TASKS}
    by_task = {task.task_id: task.state for task in dag_run.get_task_instances()}
    status = _resolve_final_status(by_task=by_task)

    end_run(run_id=run_id, status=status)
    compute_and_persist(
        run_id=run_id,
        context=context,
        task_xcoms=task_xcoms,
        final_status=status,
    )
    return {"run_id": run_id, "final_status": status, "task": "finalize"}
