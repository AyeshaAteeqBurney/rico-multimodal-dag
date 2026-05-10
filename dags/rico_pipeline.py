"""Thin Airflow DAG orchestrating the Project 4 pipeline."""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param

from rico_dag import audit, embed_image, embed_text, eval as eval_stage
from rico_dag import extract, ingest, load, metrics, parse
from rico_dag.db import end_run, start_run
from rico_dag.slack import notify_audit_failed, notify_run_finished, notify_run_started

@dag(
    dag_id="rico_pipeline",
    description="RICO notebook pipeline translated to an idempotent DAG.",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    params={"LIMIT": Param(5, type="integer", minimum=1)},
    on_success_callback=notify_run_finished,
    on_failure_callback=notify_run_finished,
    tags=["project4", "rico"],
)
def rico_pipeline():
    @task
    def ingest_task(**context):  # noqa: ANN003
        limit = int(context["params"]["LIMIT"])
        dag_run_id = context["dag_run"].run_id
        run_id = start_run(dag_run_id=dag_run_id, limit_param=limit)
        notify_run_started(context)
        result = ingest.run(run_id=run_id, limit=limit)
        return {"run_id": run_id, "screen_ids": result["screen_ids"], "rows_in": result["rows_in"], "rows_out": result["rows_out"]}

    @task
    def parse_task(payload: dict):
        result = parse.run(screen_ids=payload["screen_ids"])
        return {"run_id": payload["run_id"], "screen_ids": result["screen_ids"], "rows_in": result["rows_in"], "rows_out": result["rows_out"]}

    @task
    def embed_image_task(payload: dict):
        return embed_image.run(run_id=payload["run_id"], screen_ids=payload["screen_ids"])

    @task
    def embed_text_task(payload: dict):
        return embed_text.run(run_id=payload["run_id"], screen_ids=payload["screen_ids"])

    @task
    def extract_task(payload: dict):
        return extract.run(run_id=payload["run_id"], screen_ids=payload["screen_ids"])

    @task
    def load_task(payload: dict):
        return load.run(run_id=payload["run_id"], screen_ids=payload["screen_ids"])

    @task(on_failure_callback=notify_audit_failed)
    def audit_task(load_out: dict):
        # Pass whole load_task XCom dict — loaded["run_id"] looks for a separate XCom key and fails.
        return audit.run(run_id=load_out["run_id"])

    @task(trigger_rule="all_success")
    def eval_task(audit_out: dict):
        return eval_stage.run(run_id=audit_out["run_id"])

    @task(trigger_rule="all_done")
    def finalize_task(**context):  # noqa: ANN003
        ti = context["ti"]
        dag_run = context["dag_run"]
        ingest_payload = ti.xcom_pull(task_ids="ingest_task")
        run_id = ingest_payload["run_id"]
        _tracked_tasks = ["ingest_task", "parse_task", "embed_image_task", "embed_text_task", "extract_task", "load_task"]
        task_xcoms = {tid: ti.xcom_pull(task_ids=tid) for tid in _tracked_tasks}

        def _norm_state(state: object | None) -> str | None:
            if state is None:
                return None
            raw = state.value if hasattr(state, "value") else state
            return str(raw).split(".")[-1].lower()

        by_task = {t.task_id: t.state for t in dag_run.get_task_instances()}
        audit_state = _norm_state(by_task.get("audit_task"))
        eval_state = _norm_state(by_task.get("eval_task"))

        if audit_state == "failed":
            status = "paused_by_audit"
        elif audit_state == "success" and eval_state == "success":
            status = "succeeded"
        else:
            status = "failed"
        metrics.compute_and_persist(run_id=run_id, context=context, task_xcoms=task_xcoms)
        end_run(run_id=run_id, status=status)

    ingest_out = ingest_task()
    parsed_out = parse_task(ingest_out)

    image_out = embed_image_task(parsed_out)
    text_out = embed_text_task(parsed_out)
    extract_out = extract_task(parsed_out)

    # Use extract_out as payload (run_id + screen_ids); do not pass parsed_out or load gets a
    # direct parse_task → load_task edge and appears to bypass the parallel embed/extract steps.
    loaded = load_task(extract_out)
    [image_out, text_out] >> loaded

    audited = audit_task(loaded)
    evaluated = eval_task(audited)
    finalize_task() << [audited, evaluated]


rico_pipeline()
