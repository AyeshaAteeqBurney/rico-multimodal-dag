# Test Guide

Scripts and checklists for validating the core pipeline (Project 4) and the bonus Slack agent. Run from the repo root in PowerShell unless noted.

**Do not use `make clean`** if you want to keep the Ollama model volume — it runs `docker compose down -v` and wipes all volumes. Use `make down` + `make up` or `make reset` instead.

---

## 0. Prerequisites

```powershell
cp .env.example .env          # if not done yet
make build
make up
make pull-models              # idempotent; skips re-download if cached
docker compose exec airflow-webserver airflow dags unpause rico_pipeline
```

Verify stack:

```powershell
docker compose ps
make validate-docker            # or: python scripts/validate_project4.py --skip-infra
```

---

## 1. Happy path (core pipeline)

**Goal:** DAG succeeds, tables populated, rubric passes.

```powershell
make dag-trigger LIMIT=5
# Wait for green in Airflow UI: http://localhost:8080
make validate-docker
```

**Spot checks (SQL):**

```powershell
docker compose exec postgres psql -U rico -d rico -c "SELECT run_id, status, git_sha, limit_param FROM pipeline_runs ORDER BY started_at DESC LIMIT 1;"
docker compose exec postgres psql -U rico -d rico -c "SELECT COUNT(*) AS metadata_rows FROM screens_metadata;"
docker compose exec postgres psql -U rico -d rico -c "SELECT embedding_kind, COUNT(*) FROM screens_embeddings GROUP BY embedding_kind;"
```

**Expected:** Latest run `status = succeeded`; metadata count matches `LIMIT`; image + text embedding rows present; `git_sha` not `unknown`.

---

## 2. Idempotency (Layer A — write guardrails)

**Goal:** Re-running with the same `LIMIT` does not add destination rows. Tests `ON CONFLICT` / upserts — **not** the audit circuit breaker.

```powershell
python scripts/validate_project4.py --save-snapshot .p4-snapshot.json --skip-infra
make dag-trigger LIMIT=5
# wait for success
python scripts/validate_project4.py --check-idempotency .p4-snapshot.json --skip-infra
```

**Expected:** Script reports counts unchanged for `screens_metadata`, `screens_embeddings`, `screens_review_queue`, `screens_eval`.

---

## 3. Audit circuit breaker — in-process (no DAG)

**Goal:** Prove audit raises on duplicates without triggering Airflow.

```powershell
python scripts/validate_project4.py --test-audit-breaker --skip-infra
```

**Expected:** All audit-breaker checks pass.

---

## 4. Audit circuit breaker — chaos scenarios (Layer C)

**Goal:** Bad data exists in Postgres → re-run DAG → `audit_task` fails → `eval_task` skipped → run `paused_by_audit`.

**Workflow for every scenario:**

```powershell
make chaos-inject SCENARIO=<name>
make dag-trigger LIMIT=5
# Airflow: audit_task failed, eval_task skipped/upstream_failed
make chaos-cleanup
make dag-trigger LIMIT=5
make validate-docker
```

| Scenario | Command | Audit check that fires |
|----------|---------|------------------------|
| Duplicate embeddings | `make chaos-inject SCENARIO=duplicate` | `duplicate_embeddings` |
| Zero-norm vector | `make chaos-inject SCENARIO=zero-norm` | `invalid_vectors` |
| Orphan embedding | `make chaos-inject SCENARIO=orphan` | `orphan_embeddings` |
| Missing embedding | `make chaos-inject SCENARIO=missing` | `missing_embeddings` |
| All at once | `make chaos-inject SCENARIO=all` | all five checks |

**Verify violations before re-run:**

```powershell
python chaos/inject_duplicates.py --verify
```

**Inspect audit result after failed run:**

```powershell
docker compose exec postgres psql -U rico -d rico -c "SELECT run_id, passed, details FROM audit_results ORDER BY created_at DESC LIMIT 1;"
```

**Notes:**

- Chaos rows are tagged `source_fingerprint = chaos-duplicate-inject-v1`; `make chaos-cleanup` removes them and restores the embeddings PK.
- The duplicate scenario **drops the PK briefly** so a true duplicate can exist — this tests audit, not early-stage PK rejection.
- After `chaos-inject`, always **`make dag-trigger`** (do not cleanup first) so audit reassigns chaos rows to the current run and fails.

**If embed fails with ON CONFLICT error after chaos:**

```powershell
docker compose exec postgres psql -U rico -d rico -f /docker-entrypoint-initdb.d/003_embeddings_chaos_safe.sql
# or: make db-repair
```

---

## 5. Mimic instructor fresh check (no agent, model preserved)

**Goal:** Clean stack state like a grader would see — volumes and Ollama model kept.

```powershell
make down
make build
make up
docker compose exec airflow-webserver airflow dags unpause rico_pipeline
make reset                      # truncate tables + clear MinIO; keeps volumes
make dag-trigger LIMIT=5
make validate-docker
```

Optional audit demo:

```powershell
make chaos-inject SCENARIO=duplicate
make dag-trigger LIMIT=5
make chaos-cleanup
make dag-trigger LIMIT=5
make validate-docker
```

---

## 6. Bonus agent — smoke (no Slack)

```powershell
make agent-install
make agent-smoke LIMIT=5
make agent-diagnose
```

**Expected:** Smoke prints a real `dag_run_id`; diagnose reports healthy or the latest issue.

---

## 7. Bonus agent — Slack video flow

Terminal 1:

```powershell
make agent
```

Terminal 2 (for failure demo):

```powershell
make chaos-inject SCENARIO=all
make dag-trigger LIMIT=5
```

**Slack sequence:**

| Step | Message | Expected |
|------|---------|----------|
| 1 | `@DataBot backfill 5 screens` | Triggers DAG; replies with `dag_run_id` |
| 2 | `@DataBot why did the pipeline fail` | Diagnosis with violation breakdown |
| 3 | `@DataBot fix` | Proposed repair; asks for `confirm` |
| 4 | `@DataBot confirm` | Generic repair + re-trigger; summary of rows removed |
| 5 | — | `make validate-docker` passes after run completes |

Reply `@DataBot cancel` to abort a pending fix.

---

## 8. What each test layer proves

| Layer | Test | Proves |
|-------|------|--------|
| **A — Prevent** | §2 idempotency snapshot | Re-runs don't create duplicate rows (`ON CONFLICT`) |
| **B — Verify counts** | Happy path + load logs | Metadata/extract coverage for the batch |
| **C — Circuit breaker** | §4 chaos + re-run | Audit halts eval when bad state exists |
| **D — Remediate** | Agent `fix` → `confirm` or `make chaos-cleanup` | Bad rows removed; pipeline succeeds again |

Chaos inject tests **Layer C** by putting bad rows in Postgres (sometimes bypassing PK). Idempotency tests **Layer A**. Both are required for a complete picture.

---

## 9. Audit failure reference

Checks in `src/rico_dag/audit.py` (scoped to current `run_id`):

| Key | Catches |
|-----|---------|
| `duplicate_embeddings` | Same `(screen_id, model, version, kind)` twice |
| `duplicate_metadata` | Same `screen_id` twice for the run |
| `invalid_vectors` | NULL or near-zero norm vectors |
| `orphan_embeddings` | Embedding with no metadata for the run |
| `missing_embeddings` | Metadata screen missing image and/or text embedding |

**On failure:** `audit_task` fails → `eval_task` skipped → status `paused_by_audit` → Slack alert (if webhook configured). Early stages do **not** roll back writes; fix with `make chaos-cleanup`, agent repair, or `make reset`.

---

## 10. Pipeline metrics (quick reference)

After each run, check `pipeline_metrics` or the end-of-run log summary:

| Metric | Healthy hint |
|--------|----------------|
| `screens_metadata_row_count` | ≈ `LIMIT` |
| `pct_extracted` | High (90%+) |
| `pct_high_confidence` | High (80%+) |
| `pct_in_review_queue` | Low (0–5%) |
| `embeddings_pct_zero_norm` | 0% |
| `task_retries` | 0 |
| `final_run_status` | `succeeded` |

---

## 11. Troubleshooting

**Ingest / HuggingFace DNS inside Airflow:**

```powershell
docker compose up -d --force-recreate airflow-init airflow-webserver airflow-scheduler
docker compose exec airflow-scheduler python -c "import socket; print(socket.gethostbyname('huggingface.co'))"
```

Set `COMPOSE_DNS_SERVER_1` / `COMPOSE_DNS_SERVER_2` in `.env` if needed.

**DAG paused after recreate:**

```powershell
docker compose exec airflow-webserver airflow dags unpause rico_pipeline
```

**Agent cannot reach Airflow:** `AIRFLOW_API_URL` must use `localhost:8080` on the host, not `airflow-webserver`.

**Full data wipe (keeps volumes, unlike `make clean`):**

```powershell
make reset
```
