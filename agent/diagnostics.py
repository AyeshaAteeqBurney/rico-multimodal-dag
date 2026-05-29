"""Diagnose the pipeline's current health and recommend a fix.

Inspects (read-only):
  - whether the DAG is paused (Airflow API)
  - the latest pipeline_runs row (Postgres)
  - the latest audit_results row for that run (Postgres)

Produces a structured Diagnosis with a recommended, confirm-gated fix action.
Remediation itself lives in agent/remediation.py.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from agent import airflow_client, db
from agent.airflow_client import AirflowError

_log = logging.getLogger(__name__)

# Fix action keys understood by agent/remediation.py
FIX_NONE = "none"
FIX_TRIGGER = "trigger"
FIX_RERUN = "rerun"
FIX_UNPAUSE_AND_RERUN = "unpause_and_rerun"
FIX_REPAIR_AND_RERUN = "repair_and_rerun"


@dataclass
class Diagnosis:
    issue: str           # machine key: healthy | no_runs | dag_paused | audit_integrity | run_failed
    title: str           # short human headline
    detail: str          # multi-line explanation (Slack-formatted)
    fix_action: str      # one of the FIX_* keys
    fixable: bool
    limit: int           # LIMIT to use when (re)running
    run_id: str | None = None  # the run to repair, when fix_action needs it

    @property
    def fix_label(self) -> str:
        return {
            FIX_TRIGGER: "trigger a new pipeline run",
            FIX_RERUN: "re-run the pipeline",
            FIX_UNPAUSE_AND_RERUN: "unpause the DAG and re-run the pipeline",
            FIX_REPAIR_AND_RERUN: (
                "repair the data-integrity violations (de-dupe, drop invalid/orphan/"
                "incomplete rows) and re-run the pipeline"
            ),
        }.get(self.fix_action, "take no action")


# Human labels for the audit's violation keys (see src/rico_dag/audit.py).
_VIOLATION_LABELS = {
    "duplicate_embeddings": "duplicate embedding rows",
    "duplicate_metadata": "duplicate metadata rows",
    "invalid_vectors": "null / zero-norm vectors",
    "orphan_embeddings": "embeddings without metadata",
    "missing_embeddings": "screens missing an embedding",
}


def _summarize_violations(details: dict | None) -> str:
    if not details:
        return "Integrity violations were detected."
    lines = []
    for key, rows in details.items():
        if not isinstance(rows, list):
            continue
        label = _VIOLATION_LABELS.get(key, key)
        lines.append(f"• *{label}*: {len(rows)}")
        for row in rows[:3]:
            sid = row.get("screen_id", "?")
            if "missing_kinds" in row:
                lines.append(f"    - screen_id={sid} missing={','.join(row['missing_kinds'])}")
            elif "norm" in row:
                lines.append(
                    f"    - screen_id={sid} {row.get('embedding_kind','?')} norm={row.get('norm')}"
                )
            elif "embedding_kind" in row:
                kind = row.get("embedding_kind", "?")
                # orphan rows have no model_name — show just the kind.
                desc = f"{row['model_name']}/{kind}" if row.get("model_name") else kind
                lines.append(
                    f"    - screen_id={sid} {desc}"
                    + (f" (count={row['count']})" if "count" in row else "")
                )
            else:
                lines.append(f"    - screen_id={sid} (count={row.get('count','?')})")
    return "\n".join(lines) if lines else "Integrity violations were detected."


def diagnose() -> Diagnosis:
    """Inspect current state and return a Diagnosis with a recommended fix."""
    # 1. Paused DAG — runs created while paused never execute.
    try:
        paused = airflow_client.is_paused()
    except AirflowError as exc:
        return Diagnosis(
            issue="airflow_unreachable",
            title="Cannot reach Airflow",
            detail=str(exc),
            fix_action=FIX_NONE,
            fixable=False,
            limit=5,
        )

    run = db.latest_run()
    limit = int(run["limit_param"]) if run and run.get("limit_param") else 5
    limit = max(1, min(limit, 500))

    if paused:
        return Diagnosis(
            issue="dag_paused",
            title="DAG is paused",
            detail=(
                f"`{airflow_client.settings.airflow_dag_id}` is paused, so any triggered run "
                "stays queued and never executes."
            ),
            fix_action=FIX_UNPAUSE_AND_RERUN,
            fixable=True,
            limit=limit,
        )

    # 2. No runs yet.
    if run is None:
        return Diagnosis(
            issue="no_runs",
            title="No pipeline runs yet",
            detail="There are no pipeline runs recorded. I can trigger the first one.",
            fix_action=FIX_TRIGGER,
            fixable=True,
            limit=limit,
        )

    # 3. Audit failure — highest-priority data issue.
    audit = db.latest_audit(run["run_id"])
    if audit and not audit["passed"]:
        details = audit["details"] or {}
        return Diagnosis(
            issue="audit_integrity",
            title="Audit failed — data integrity violation",
            detail=(
                f"Run `{run['run_id']}` failed the *audit*:\n"
                f"{_summarize_violations(details)}\n"
                "I can repair the offending rows and re-run — re-running alone won't help."
            ),
            fix_action=FIX_REPAIR_AND_RERUN,
            fixable=True,
            limit=limit,
            run_id=run["run_id"],
        )

    # 4. Run still in progress — nothing to fix yet.
    if run["status"] == "running":
        return Diagnosis(
            issue="running",
            title="A run is still in progress",
            detail=(
                f"Run `{run['run_id']}` (LIMIT={run['limit_param']}) is currently *running*. "
                "Wait for it to finish before diagnosing or fixing."
            ),
            fix_action=FIX_NONE,
            fixable=False,
            limit=limit,
        )

    # 5. Run failed/paused-by-audit for another reason.
    if run["status"] in ("failed", "paused_by_audit"):
        return Diagnosis(
            issue="run_failed",
            title=f"Last run ended as `{run['status']}`",
            detail=(
                f"Run `{run['run_id']}` (LIMIT={run['limit_param']}) ended with status "
                f"`{run['status']}` and no audit failure recorded. "
                "A re-run will start a fresh attempt."
            ),
            fix_action=FIX_RERUN,
            fixable=True,
            limit=limit,
        )

    # 6. Healthy.
    return Diagnosis(
        issue="healthy",
        title="Pipeline looks healthy",
        detail=(
            f"Last run `{run['run_id']}` succeeded (LIMIT={run['limit_param']}). "
            "No action needed — but I can still trigger a new run if you want."
        ),
        fix_action=FIX_NONE,
        fixable=False,
        limit=limit,
    )


def _main() -> int:
    """One-shot CLI: print a diagnosis without Slack (handy for demos/tests)."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    diag = diagnose()
    print(f"issue   : {diag.issue}")
    print(f"title   : {diag.title}")
    print(f"fixable : {diag.fixable}  (action={diag.fix_action}, limit={diag.limit})")
    print("detail  :")
    print(diag.detail)
    if diag.fixable:
        print(f"\nrecommended fix: {diag.fix_label}")
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
