"""Thin Airflow DAG orchestrating the Project 4 pipeline."""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param

# Lightweight imports only — heavy deps (torch, CLIP, SBERT, datasets) load inside tasks.
from rico_dag.db import start_run
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
        from rico_dag import ingest

        limit = int(context["params"]["LIMIT"])
        dag_run_id = context["dag_run"].run_id
        run_id = start_run(dag_run_id=dag_run_id, limit_param=limit)
        notify_run_started(context)
        result = ingest.run(run_id=run_id, limit=limit)
        return {
            "run_id": run_id,
            "screen_ids": result["screen_ids"],
            "rows_in": result["rows_in"],
            "rows_out": result["rows_out"],
        }

    @task
    def parse_task(payload: dict):
        from rico_dag import parse

        result = parse.run(screen_ids=payload["screen_ids"])
        return {
            "run_id": payload["run_id"],
            "screen_ids": result["screen_ids"],
            "rows_in": result["rows_in"],
            "rows_out": result["rows_out"],
        }

    @task
    def embed_image_task(payload: dict):
        from rico_dag import embed_image

        return embed_image.run(run_id=payload["run_id"], screen_ids=payload["screen_ids"])

    @task
    def embed_text_task(payload: dict):
        from rico_dag import embed_text

        return embed_text.run(run_id=payload["run_id"], screen_ids=payload["screen_ids"])

    @task
    def extract_task(payload: dict):
        from rico_dag import extract

        return extract.run(run_id=payload["run_id"], screen_ids=payload["screen_ids"])

    @task
    def load_task(payload: dict):
        from rico_dag import load

        return load.run(run_id=payload["run_id"], screen_ids=payload["screen_ids"])

    @task(on_failure_callback=notify_audit_failed)
    def audit_task(load_out: dict):
        from rico_dag import audit

        return audit.run(run_id=load_out["run_id"])

    @task(trigger_rule="all_success")
    def eval_task(audit_out: dict):
        from rico_dag import eval as eval_stage

        return eval_stage.run(run_id=audit_out["run_id"])

    @task(trigger_rule="all_done")
    def finalize_task(**context):  # noqa: ANN003
        from rico_dag import finalize

        return finalize.run(context=context)

    ingest_out = ingest_task()
    parsed_out = parse_task(ingest_out)

    image_out = embed_image_task(parsed_out)
    text_out = embed_text_task(parsed_out)
    extract_out = extract_task(parsed_out)

    loaded = load_task(extract_out)
    [image_out, text_out] >> loaded

    audited = audit_task(loaded)
    evaluated = eval_task(audited)
    finalize_task() << [audited, evaluated]


rico_pipeline()
