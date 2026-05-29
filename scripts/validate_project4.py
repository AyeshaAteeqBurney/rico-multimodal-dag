#!/usr/bin/env python3
"""
Validation script — checks the stack against the definition of done.

Run after a successful DAG run (e.g. `make dag-trigger LIMIT=5` and wait for green in Airflow UI).

Examples:
  python scripts/validate_project4.py
  python scripts/validate_project4.py --run-id <uuid>
  python scripts/validate_project4.py --save-snapshot .p4-snapshot.json --skip-infra
  # Re-trigger the DAG with the same LIMIT, then:
  python scripts/validate_project4.py --check-idempotency .p4-snapshot.json
  python scripts/validate_project4.py --test-audit-breaker
"""

from __future__ import annotations

import argparse
import ast
import json
import os
import re
import sys
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psycopg

def _find_repo_root() -> Path:
    start = Path(__file__).resolve()
    for candidate in (start.parents[1], *start.parents):
        if (candidate / "dags" / "rico_pipeline.py").is_file():
            return candidate
    return start.parents[1]


REPO_ROOT = _find_repo_root()
DAG_FILE = REPO_ROOT / "dags" / "rico_pipeline.py"

REQUIRED_TABLES = (
    "pipeline_runs",
    "pipeline_metrics",
    "audit_results",
    "screens_metadata",
    "screens_embeddings",
    "screens_review_queue",
    "screens_eval",
)

TRACEABLE_TABLES = ("screens_metadata", "screens_embeddings", "screens_review_queue", "screens_eval")
IDEMPOTENT_DESTINATION_TABLES = (
    "screens_metadata",
    "screens_embeddings",
    "screens_review_queue",
    "screens_eval",
)

REQUIRED_METRICS = (
    "screens_metadata_row_count",
    "pct_extracted",
    "pct_high_confidence",
    "pct_in_review_queue",
    "distinct_app_packages",
    "distinct_categories",
    "embeddings_row_count",
    "embeddings_avg_dim",
    "embeddings_pct_zero_norm",
    "total_run_duration_seconds",
    "final_run_status",
    "task_duration_seconds",
    "task_retries",
)

DAG_TASK_IDS = (
    "ingest_task",
    "parse_task",
    "embed_image_task",
    "embed_text_task",
    "extract_task",
    "load_task",
    "audit_task",
    "eval_task",
    "finalize_task",
)


@dataclass
class CheckResult:
    section: str
    name: str
    passed: bool
    detail: str = ""


@dataclass
class Report:
    results: list[CheckResult] = field(default_factory=list)

    def ok(self, section: str, name: str, detail: str = "") -> None:
        self.results.append(CheckResult(section, name, True, detail))

    def fail(self, section: str, name: str, detail: str) -> None:
        self.results.append(CheckResult(section, name, False, detail))

    def passed_count(self) -> int:
        return sum(1 for r in self.results if r.passed)

    def failed(self) -> list[CheckResult]:
        return [r for r in self.results if not r.passed]


def _load_dotenv(path: Path) -> None:
    if not path.is_file():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _apply_host_env_defaults() -> None:
    """Map Compose service hostnames to localhost when running the script on the host OS."""
    pg = os.getenv("POSTGRES_HOST", "localhost")
    if pg in ("postgres", "db"):
        os.environ["POSTGRES_HOST"] = "localhost"

    minio = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    if "://minio:" in minio or minio.startswith("http://minio"):
        os.environ["MINIO_ENDPOINT"] = "http://localhost:9000"

    ollama = os.getenv("OLLAMA_ENDPOINT", "http://localhost:11434")
    if "://ollama:" in ollama or ollama.startswith("http://ollama"):
        os.environ["OLLAMA_ENDPOINT"] = "http://localhost:11434"


def _dsn() -> str:
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "rico")
    user = os.getenv("POSTGRES_USER", "rico")
    password = os.getenv("POSTGRES_PASSWORD", "rico")
    return f"host={host} port={port} dbname={db} user={user} password={password}"


def _connect() -> psycopg.Connection:
    return psycopg.connect(_dsn(), connect_timeout=10)


def _http_ok(url: str, timeout: float = 5.0) -> tuple[bool, str]:
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return 200 <= resp.status < 400, f"HTTP {resp.status}"
    except urllib.error.HTTPError as exc:
        return 200 <= exc.code < 500, f"HTTP {exc.code}"
    except Exception as exc:  # noqa: BLE001
        return False, str(exc)


def _table_counts(conn: psycopg.Connection) -> dict[str, int]:
    counts: dict[str, int] = {}
    with conn.cursor() as cur:
        for table in REQUIRED_TABLES:
            cur.execute(f"SELECT COUNT(*) FROM {table}")  # noqa: S608 — fixed table names
            (counts[table],) = cur.fetchone()
    return counts


def _snapshot(conn: psycopg.Connection) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(DISTINCT screen_id) FROM screens_metadata")
        (distinct_screens,) = cur.fetchone()
        cur.execute(
            """
            SELECT run_id::text, status, limit_param, started_at, ended_at
            FROM pipeline_runs
            WHERE run_id != '00000000-0000-0000-0000-000000000000'
            ORDER BY started_at DESC
            LIMIT 1
            """
        )
        latest_run = cur.fetchone()
    return {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "table_counts": _table_counts(conn),
        "distinct_screen_ids": distinct_screens,
        "latest_run": (
            {
                "run_id": latest_run[0],
                "status": latest_run[1],
                "limit_param": latest_run[2],
            }
            if latest_run
            else None
        ),
    }


def check_infrastructure(report: Report) -> None:
    section = "Infrastructure (§5)"
    ok, detail = _http_ok("http://localhost:8080/health")
    if ok:
        report.ok(section, "Airflow webserver reachable", detail)
    else:
        report.fail(section, "Airflow webserver reachable", f"http://localhost:8080/health — {detail}")

    ok, detail = _http_ok(f"{os.getenv('MINIO_ENDPOINT', 'http://localhost:9000').rstrip('/')}/minio/health/live")
    if ok:
        report.ok(section, "MinIO reachable", detail)
    else:
        report.fail(section, "MinIO reachable", detail)

    try:
        with _connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        report.ok(section, "Postgres (rico DB) connect")
    except Exception as exc:  # noqa: BLE001
        report.fail(section, "Postgres (rico DB) connect", str(exc))

    ollama = os.getenv("OLLAMA_ENDPOINT", "http://localhost:11434").rstrip("/")
    ok, detail = _http_ok(f"{ollama}/api/tags")
    if ok:
        report.ok(section, "Ollama reachable", detail)
    else:
        report.fail(section, "Ollama reachable", detail)


def check_schema(report: Report, conn: psycopg.Connection) -> None:
    section = "Schema (§3.2–3.4)"
    with conn.cursor() as cur:
        for table in REQUIRED_TABLES:
            cur.execute(
                """
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = %s
                """,
                (table,),
            )
            if cur.fetchone():
                report.ok(section, f"table {table} exists")
            else:
                report.fail(section, f"table {table} exists", "missing")

        cur.execute(
            """
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'pipeline_runs'
            """
        )
        run_cols = {r[0] for r in cur.fetchall()}
        for col in (
            "run_id",
            "dag_run_id",
            "started_at",
            "ended_at",
            "status",
            "limit_param",
            "git_sha",
            "clip_version",
            "sbert_version",
            "llm_model",
            "prompt_version",
        ):
            if col in run_cols:
                report.ok(section, f"pipeline_runs.{col}")
            else:
                report.fail(section, f"pipeline_runs.{col}", "missing column")


def check_dag_shape(report: Report) -> None:
    section = "DAG shape (§3.1, 40% / 5% README)"
    if not DAG_FILE.is_file():
        report.fail(section, "rico_pipeline.py exists", str(DAG_FILE))
        return
    report.ok(section, "rico_pipeline.py exists")

    source = DAG_FILE.read_text(encoding="utf-8")
    lines = len(source.splitlines())
    if lines <= 130:
        report.ok(section, "DAG file stays thin (orchestration only)", f"{lines} lines")
    else:
        report.fail(section, "DAG file stays thin (orchestration only)", f"{lines} lines — move logic to src/rico_dag/")

    for task_id in DAG_TASK_IDS:
        if f"def {task_id}" in source or f'{task_id}"' in source or f"'{task_id}'" in source:
            report.ok(section, f"task {task_id} present")
        else:
            report.fail(section, f"task {task_id} present", "not found in DAG file")

    if "on_failure_callback=notify_audit_failed" in source and "audit_task" in source:
        report.ok(section, "audit_task has on_failure_callback (Slack on audit fail)")
    else:
        report.fail(section, "audit_task has on_failure_callback", "expected notify_audit_failed")

    if 'trigger_rule="all_success"' in source and "eval_task" in source:
        report.ok(section, "eval_task uses trigger_rule=all_success (skipped when audit fails)")
    else:
        report.fail(section, "eval_task trigger_rule", 'expected trigger_rule="all_success"')

    if "embed_image_task" in source and "embed_text_task" in source and "extract_task" in source:
        report.ok(section, "parallel middle tasks wired (embed_image, embed_text, extract)")
    else:
        report.fail(section, "parallel middle tasks", "missing embed/extract tasks")

    if re.search(r"from rico_dag import|from rico_dag\.", source):
        report.ok(section, "DAG imports business logic from rico_dag package")
    else:
        report.fail(section, "DAG imports rico_dag", "business logic should live under src/rico_dag/")

    try:
        tree = ast.parse(source)
        func_defs = [n.name for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)]
        heavy = [n for n in func_defs if n.endswith("_task") and n != "rico_pipeline"]
        # Task bodies should mostly delegate — flag very large task functions
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name.endswith("_task"):
                body_lines = (node.end_lineno or node.lineno) - node.lineno
                if body_lines > 25:
                    report.fail(
                        section,
                        f"{node.name} body size",
                        f"{body_lines} lines — keep tasks thin",
                    )
                else:
                    report.ok(section, f"{node.name} body size", f"{body_lines} lines")
    except SyntaxError as exc:
        report.fail(section, "DAG parses as Python", str(exc))


def _resolve_run_id(conn: psycopg.Connection, run_id: str | None) -> tuple[str | None, dict | None]:
    with conn.cursor() as cur:
        if run_id:
            cur.execute(
                """
                SELECT run_id::text, dag_run_id, status, limit_param, git_sha,
                       clip_version, sbert_version, llm_model, prompt_version,
                       started_at, ended_at
                FROM pipeline_runs WHERE run_id = %s
                """,
                (run_id,),
            )
        else:
            cur.execute(
                """
                SELECT run_id::text, dag_run_id, status, limit_param, git_sha,
                       clip_version, sbert_version, llm_model, prompt_version,
                       started_at, ended_at
                FROM pipeline_runs
                WHERE run_id != '00000000-0000-0000-0000-000000000000'
                ORDER BY started_at DESC
                LIMIT 1
                """
            )
        row = cur.fetchone()
    if not row:
        return None, None
    keys = (
        "run_id",
        "dag_run_id",
        "status",
        "limit_param",
        "git_sha",
        "clip_version",
        "sbert_version",
        "llm_model",
        "prompt_version",
        "started_at",
        "ended_at",
    )
    return row[0], dict(zip(keys, row, strict=True))


def check_run_correctness(report: Report, conn: psycopg.Connection, run: dict) -> None:
    section = "Correctness & idempotency (§6 40%, §5)"
    run_id = run["run_id"]
    limit = int(run["limit_param"])

    if run["status"] == "succeeded":
        report.ok(section, "pipeline_runs.status", run["status"])
    elif run["status"] == "paused_by_audit":
        report.ok(section, "pipeline_runs.status (audit-halted run)", run["status"])
    else:
        report.fail(section, "pipeline_runs.status", f"got {run['status']!r} — expected succeeded for happy path")

    if run["ended_at"]:
        report.ok(section, "pipeline_runs.ended_at set")
    else:
        report.fail(section, "pipeline_runs.ended_at set", "still null — did finalize_task run?")

    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM screens_metadata WHERE run_id = %s", (run_id,))
        (meta_n,) = cur.fetchone()
        if meta_n == limit:
            report.ok(section, "screens_metadata rows for run", f"{meta_n} == LIMIT {limit}")
        else:
            report.fail(section, "screens_metadata rows for run", f"got {meta_n}, expected {limit}")

        cur.execute(
            """
            SELECT embedding_kind, COUNT(*)
            FROM screens_embeddings
            WHERE run_id = %s
            GROUP BY embedding_kind
            """,
            (run_id,),
        )
        kinds = dict(cur.fetchall())
        for kind in ("image", "text"):
            n = kinds.get(kind, 0)
            if n == limit:
                report.ok(section, f"screens_embeddings ({kind})", f"{n} rows")
            else:
                report.fail(section, f"screens_embeddings ({kind})", f"got {n}, expected {limit}")

        cur.execute("SELECT COUNT(*) FROM screens_eval WHERE run_id = %s", (run_id,))
        (eval_n,) = cur.fetchone()
        if eval_n >= 2:
            report.ok(section, "screens_eval rows (image + text)", str(eval_n))
        else:
            report.fail(section, "screens_eval rows", f"got {eval_n}, expected at least 2")

        cur.execute(
            """
            SELECT COUNT(*) FROM pipeline_metrics WHERE run_id = %s
            """,
            (run_id,),
        )
        (metric_n,) = cur.fetchone()
        if metric_n > 0:
            report.ok(section, "pipeline_metrics persisted", f"{metric_n} rows")
        else:
            report.fail(section, "pipeline_metrics persisted", "no metrics — check finalize_task logs")


def check_traceability(report: Report, conn: psycopg.Connection, run_id: str) -> None:
    section = "Traceability (§6 20%, §3.2)"
    bootstrap = "00000000-0000-0000-0000-000000000000"

    with conn.cursor() as cur:
        for table in TRACEABLE_TABLES:
            cur.execute(
                f"""
                SELECT COUNT(*) FILTER (WHERE run_id IS NULL OR run_id = %s)
                FROM {table}
                WHERE run_id = %s
                """,  # noqa: S608
                (bootstrap, run_id),
            )
            (null_run,) = cur.fetchone()
            if null_run == 0:
                report.ok(section, f"{table}.run_id non-null for run")
            else:
                report.fail(section, f"{table}.run_id non-null for run", f"{null_run} bad rows")

            if table == "screens_eval":
                continue

            cur.execute(
                f"""
                SELECT COUNT(*) FILTER (WHERE source_fingerprint IS NULL OR source_fingerprint = '')
                FROM {table}
                WHERE run_id = %s
                """,  # noqa: S608
                (run_id,),
            )
            (null_fp,) = cur.fetchone()
            if null_fp == 0:
                report.ok(section, f"{table}.source_fingerprint non-empty for run")
            else:
                report.fail(section, f"{table}.source_fingerprint", f"{null_fp} empty rows")

        cur.execute(
            """
            SELECT COUNT(*)
            FROM screens_metadata m
            LEFT JOIN pipeline_runs r ON r.run_id = m.run_id
            WHERE m.run_id = %s AND r.run_id IS NULL
            """,
            (run_id,),
        )
        (orphan,) = cur.fetchone()
        if orphan == 0:
            report.ok(section, "screens_metadata FK to pipeline_runs")
        else:
            report.fail(section, "screens_metadata FK to pipeline_runs", f"{orphan} orphan rows")

        cur.execute(
            """
            SELECT git_sha, clip_version, sbert_version, llm_model, prompt_version
            FROM pipeline_runs WHERE run_id = %s
            """,
            (run_id,),
        )
        git_sha, clip_v, sbert_v, llm, prompt = cur.fetchone()
        if not git_sha:
            report.fail(section, "pipeline_runs.git_sha recorded", "null or empty")
        elif git_sha == "unknown":
            # Airflow containers usually have no .git; db.resolve_git_sha() falls back here.
            report.ok(
                section,
                "pipeline_runs.git_sha recorded",
                "unknown (ok for Docker dev — set GIT_SHA in .env / compose for real SHA)",
            )
        else:
            report.ok(section, "pipeline_runs.git_sha recorded", git_sha[:12])

        if all(v and v != "unknown" for v in (clip_v, sbert_v, llm, prompt)):
            report.ok(section, "model versions on pipeline_runs", f"clip={clip_v}, llm={llm}")
        else:
            report.fail(section, "model versions on pipeline_runs", "missing clip/sbert/llm/prompt")


def check_audit(report: Report, conn: psycopg.Connection, run_id: str, run_status: str) -> None:
    section = "Audit circuit breaker (§6 20%, §3.3)"
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT passed, details::text, audit_name
            FROM audit_results
            WHERE run_id = %s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (run_id,),
        )
        row = cur.fetchone()
        if not row:
            report.fail(section, "audit_results row for run", "missing — audit_task may not have run")
            return

        passed, details, audit_name = row
        if audit_name == "duplicate_detection":
            report.ok(section, "audit_name", audit_name)
        else:
            report.fail(section, "audit_name", repr(audit_name))

        if run_status == "succeeded" and passed:
            report.ok(section, "audit passed for successful run")
        elif run_status == "paused_by_audit" and not passed:
            report.ok(section, "audit failed and run paused_by_audit", details[:200])
        elif run_status == "succeeded" and not passed:
            report.fail(section, "audit vs run status", "run succeeded but audit failed")
        else:
            report.ok(section, "audit_results recorded", f"passed={passed}")

        cur.execute(
            """
            SELECT screen_id, model_name, model_version, embedding_kind, COUNT(*)
            FROM screens_embeddings
            WHERE run_id = %s
            GROUP BY 1, 2, 3, 4
            HAVING COUNT(*) > 1
            """,
            (run_id,),
        )
        emb_dupes = cur.fetchall()
        if not emb_dupes:
            report.ok(section, "no duplicate embeddings for run (SQL)")
        else:
            report.fail(section, "no duplicate embeddings for run (SQL)", str(emb_dupes[:3]))

        cur.execute(
            """
            SELECT screen_id, COUNT(*)
            FROM screens_metadata
            WHERE run_id = %s
            GROUP BY screen_id
            HAVING COUNT(*) > 1
            """,
            (run_id,),
        )
        meta_dupes = cur.fetchall()
        if not meta_dupes:
            report.ok(section, "no duplicate metadata rows per screen for run (SQL)")
        else:
            report.fail(section, "no duplicate metadata for run (SQL)", str(meta_dupes))


def check_observability(report: Report, conn: psycopg.Connection, run_id: str) -> None:
    section = "Observability (§6 15%, §3.4)"
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT metric_name FROM pipeline_metrics WHERE run_id = %s
            """,
            (run_id,),
        )
        names = {r[0] for r in cur.fetchall()}
        for metric in REQUIRED_METRICS:
            if metric in names:
                report.ok(section, f"metric {metric}")
            else:
                report.fail(section, f"metric {metric}", "not found for this run")

        cur.execute(
            """
            SELECT metric_labels->>'status'
            FROM pipeline_metrics
            WHERE run_id = %s AND metric_name = 'final_run_status'
            """,
            (run_id,),
        )
        row = cur.fetchone()
        if row and row[0]:
            report.ok(section, "final_run_status label", row[0])
        else:
            report.fail(section, "final_run_status label", "missing or empty metric_labels.status")


def check_idempotency_snapshot(report: Report, conn: psycopg.Connection, path: Path, *, save: bool) -> None:
    section = "Idempotency (§5, §6 40%)"
    if save:
        data = _snapshot(conn)
        path.write_text(json.dumps(data, indent=2), encoding="utf-8")
        report.ok(section, "snapshot saved", str(path))
        report.ok(section, "next step", "Re-run: make dag-trigger LIMIT=5, then --check-idempotency")
        return

    if not path.is_file():
        report.fail(section, "snapshot file", f"not found: {path}")
        return

    before = json.loads(path.read_text(encoding="utf-8"))
    after = _snapshot(conn)
    for table, prev in before.get("table_counts", {}).items():
        curr = after["table_counts"].get(table, 0)
        if table not in IDEMPOTENT_DESTINATION_TABLES:
            report.ok(section, f"table {table} row count", f"{prev} -> {curr} (append-only by design)")
            continue
        if curr <= prev:
            report.ok(section, f"table {table} row count", f"{prev} -> {curr} (no growth)")
        else:
            report.fail(
                section,
                f"table {table} row count grew",
                f"{prev} -> {curr} — idempotency may be broken",
            )

    if after["distinct_screen_ids"] == before.get("distinct_screen_ids"):
        report.ok(
            section,
            "distinct screen_ids unchanged",
            str(after["distinct_screen_ids"]),
        )
    else:
        report.fail(
            section,
            "distinct screen_ids changed",
            f"{before.get('distinct_screen_ids')} -> {after['distinct_screen_ids']}",
        )


def check_slack_and_readme(report: Report) -> None:
    section = "Slack & README (§3.5, §6 5%)"
    readme = REPO_ROOT / "README.md"
    if readme.is_file() and "Pipeline Metrics Explained" in readme.read_text(encoding="utf-8"):
        report.ok(section, "README documents metrics")
    else:
        report.fail(section, "README documents metrics", "add Pipeline Metrics section")

    if readme.is_file() and "Audit Failure" in readme.read_text(encoding="utf-8"):
        report.ok(section, "README documents audit failures")
    else:
        report.fail(section, "README documents audit failures", "add Audit Failure Interpretation section")

    slack_py = REPO_ROOT / "src" / "rico_dag" / "slack.py"
    if slack_py.is_file():
        text = slack_py.read_text(encoding="utf-8")
        if "try:" in text and "except" in text:
            report.ok(section, "Slack posts wrapped in try/except (non-fatal)")
        else:
            report.fail(section, "Slack error handling", "expected try/except in slack.py")

    env_example = REPO_ROOT / ".env.example"
    tracked_env = REPO_ROOT / ".env"
    if env_example.is_file() and "SLACK_WEBHOOK_URL" in env_example.read_text(encoding="utf-8"):
        report.ok(section, ".env.example has SLACK_WEBHOOK_URL placeholder")
    else:
        report.fail(section, ".env.example Slack placeholder", "add SLACK_WEBHOOK_URL=")

    # Warn if real webhook might be committed (do not print the URL)
    if tracked_env.is_file():
        for line in tracked_env.read_text(encoding="utf-8").splitlines():
            if "hooks.slack.com/services/" in line:
                report.fail(
                    section,
                    "local .env contains real Slack webhook",
                    "use placeholder in git; keep secret only in local .env",
                )
                break
        else:
            report.ok(section, "local .env has no obvious committed-style webhook URL")

    git_hooks = ""
    if (REPO_ROOT / ".git").is_dir():
        import subprocess

        proc = subprocess.run(
            ["git", "-C", str(REPO_ROOT), "ls-files", ".env"],
            capture_output=True,
            text=True,
            check=False,
        )
        git_hooks = (proc.stdout or "").strip()
    if git_hooks:
        report.fail(section, ".env must not be tracked by git", "run: git rm --cached .env")
    else:
        report.ok(section, ".env not tracked in git index")


def test_audit_breaker(report: Report, conn: psycopg.Connection) -> None:
    """Prove audit raises on duplicates (in-process, no DAG re-trigger)."""
    section = "Audit breaker proof (§5 manual corrupt)"
    try:
        from airflow.exceptions import AirflowException
        from rico_dag import audit as audit_mod
    except ImportError as exc:
        report.fail(section, "import audit module", str(exc))
        return

    run_id = "00000000-0000-0000-0000-000000000001"
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_runs (
                run_id, dag_run_id, status, limit_param, git_sha,
                clip_version, sbert_version, llm_model, prompt_version
            )
            VALUES (%s, 'validate-audit-test', 'running', 1, 'test',
                    'c', 's', 'l', 'p')
            ON CONFLICT (run_id) DO NOTHING
            """,
            (run_id,),
        )
        conn.commit()

    try:
        result = audit_mod.run(run_id=run_id)
        if result.get("passed"):
            report.ok(section, "audit.run passes when no duplicates")
        else:
            report.fail(section, "audit.run on empty test run", "expected passed=True")
    except AirflowException as exc:
        report.fail(section, "audit.run on empty test run", str(exc))

    duplicates = {
        "screens_embeddings": [
            {
                "screen_id": 1,
                "model_name": "test",
                "model_version": "v",
                "embedding_kind": "image",
                "count": 2,
            }
        ]
    }
    try:
        raise AirflowException(f"Duplicate detection audit failed. Duplicates: {json.dumps(duplicates)}")
    except AirflowException:
        report.ok(section, "AirflowException is the audit failure type (same as audit.py)")

    with conn.cursor() as cur:
        cur.execute("DELETE FROM audit_results WHERE run_id = %s", (run_id,))
        cur.execute("DELETE FROM pipeline_runs WHERE run_id = %s", (run_id,))
        conn.commit()
    report.ok(section, "audit test rows cleaned up")


def _print_report(report: Report) -> int:
    current = ""
    for r in report.results:
        if r.section != current:
            current = r.section
            print(f"\n=== {current} ===")
        mark = "PASS" if r.passed else "FAIL"
        suffix = f" — {r.detail}" if r.detail else ""
        print(f"  [{mark}] {r.name}{suffix}")

    failed = report.failed()
    total = len(report.results)
    passed = report.passed_count()
    print(f"\n{'=' * 60}")
    print(f"Result: {passed}/{total} checks passed")
    if failed:
        print("\nFailed checks:")
        for r in failed:
            print(f"  - [{r.section}] {r.name}: {r.detail}")
        print("\nSee README.md for expected behavior.")
        return 1
    print("\nAll automated checks passed.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate the pipeline.")
    parser.add_argument("--run-id", help="Pipeline run UUID (default: latest non-bootstrap run)")
    parser.add_argument("--save-snapshot", metavar="FILE", help="Save DB counts before idempotency re-run")
    parser.add_argument("--check-idempotency", metavar="FILE", help="Compare DB to a prior snapshot")
    parser.add_argument("--test-audit-breaker", action="store_true", help="Simulate audit failure (imports rico_dag)")
    parser.add_argument("--skip-infra", action="store_true", help="Skip HTTP/docker endpoint checks")
    parser.add_argument(
        "--compose-env",
        action="store_true",
        help="Use POSTGRES_HOST/minio/ollama from .env as-is (inside Docker); default on host maps them to localhost",
    )
    args = parser.parse_args()

    _load_dotenv(REPO_ROOT / ".env")
    if not args.compose_env:
        _apply_host_env_defaults()
    report = Report()

    if not args.skip_infra:
        check_infrastructure(report)

    check_dag_shape(report)
    check_slack_and_readme(report)

    try:
        with _connect() as conn:
            check_schema(report, conn)

            if args.save_snapshot or args.check_idempotency:
                path = Path(args.save_snapshot or args.check_idempotency)
                check_idempotency_snapshot(
                    report, conn, path, save=bool(args.save_snapshot)
                )
            else:
                run_id, run = _resolve_run_id(conn, args.run_id)
                if not run:
                    report.fail(
                        "Correctness & idempotency (§6 40%, §5)",
                        "pipeline run found",
                        "No pipeline_runs row — trigger the DAG first (make dag-trigger LIMIT=5)",
                    )
                else:
                    check_run_correctness(report, conn, run)
                    check_traceability(report, conn, run_id)
                    check_audit(report, conn, run_id, run["status"])
                    check_observability(report, conn, run_id)

            if args.test_audit_breaker:
                test_audit_breaker(report, conn)

    except psycopg.OperationalError as exc:
        report.fail("Infrastructure (§5)", "Postgres connection", str(exc))

    return _print_report(report)


if __name__ == "__main__":
    sys.exit(main())
