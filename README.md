# RICO Multimodal DAG 

This project implements an end-to-end multimodal data pipeline as an Airflow DAG. It ingests UI screens, parses hierarchy structure, computes image and text embeddings, performs extraction, loads results into Postgres + pgvector, runs a duplicate-detection audit as a circuit breaker, and executes evaluation.

The pipeline is production-oriented: scheduled orchestration, idempotent writes, row-level traceability, and observable run outcomes.

## Pipeline Flow

Simple DAG structure:

`ingest -> parse -> [embed_image, embed_text, extract] -> load -> audit -> eval`

The DAG executes the following sequence:

- The three middle tasks (`embed_image`, `embed_text`, `extract`) run in parallel.
- A `LIMIT` DAG parameter controls batch size for development and demo runs.
- The audit task can halt downstream execution by failing on duplicate detection.

## Core Capabilities

- **Idempotency**
  - Re-running with the same inputs does not create duplicate rows or duplicate objects.
  - Database writes use conflict-safe patterns and deterministic keys.

- **Traceability**
  - Every pipeline run is recorded in `pipeline_runs`.
  - Destination rows carry `run_id` and `source_fingerprint`.
  - Fingerprints allow exact input provenance tracking.

- **Audit Circuit Breaker**
  - Duplicate-detection audit runs after load and before eval.
  - Audit failure blocks `eval` and marks the run as failed/paused.
  - Violations are logged and persisted for investigation.

- **Observability Foundation**
  - Schema includes `pipeline_metrics` and `audit_results`.
  - Run lifecycle hooks are wired to support status tracking.

## Tech Stack

- **Orchestration:** Apache Airflow
- **Database:** PostgreSQL + pgvector
- **Object Storage:** MinIO (S3-compatible)
- **LLM Runtime:** Ollama
- **ML/Data Libraries:** HuggingFace datasets, open-clip, sentence-transformers

## Repository Structure

```text
rico-multimodal-dag/
  dags/                  # DAG definitions (orchestration only)
  src/rico_dag/          # Pipeline business logic modules
  migrations/            # Database schema migrations
  data/                  # Local run inputs/config data
  docker-compose.yml     # Full local stack
  Makefile               # Common lifecycle commands
  pyproject.toml         # Python dependencies
  .env.example           # Environment variable template
```

## Setup

1. Copy environment template:

```bash
cp .env.example .env
```

2. Start the full stack:

```bash
make up
```

This stack builds a custom Airflow image (`Dockerfile.airflow`) and installs project dependencies during image build. You do not need to run manual `pip install` commands inside Airflow containers after startup.

3. Open Airflow UI:

- <http://localhost:8080>
- Default credentials: `admin` / `admin`

## Running the Pipeline

Trigger the DAG manually with a small development batch:

```bash
make dag-trigger LIMIT=5
```

For larger runs, increase `LIMIT`:

```bash
make dag-trigger LIMIT=50
```

## Validation (§6)

After a successful DAG run (`make dag-trigger LIMIT=5`), verify the rubric from the assignment PDF:

```bash
make validate
# or (from your laptop — maps postgres/minio/ollama in .env to localhost automatically)
python scripts/validate_project4.py --skip-infra
```

If `.env` uses Docker service names (`POSTGRES_HOST=postgres`), run validation **on the host** without flags: the script rewrites them to `localhost` for published ports. Inside a container use `make validate-docker` or pass `--compose-env`.

**Idempotency check** (Definition of Done §5):

```bash
python scripts/validate_project4.py --save-snapshot .p4-snapshot.json --skip-infra
make dag-trigger LIMIT=5
# wait for run to finish
python scripts/validate_project4.py --check-idempotency .p4-snapshot.json --skip-infra
```

**Audit circuit breaker** (optional in-process proof):

```bash
python scripts/validate_project4.py --test-audit-breaker
```

**Audit circuit breaker demo** (Assignment §5 — corrupt data, re-run, eval skipped):

```bash
# After a successful run:
make chaos-inject
make dag-trigger LIMIT=5
# In Airflow UI: audit_task failed, eval_task upstream_failed/skipped

make chaos-cleanup
make dag-trigger LIMIT=5
# Should succeed again after cleanup
```

**Important:** After `chaos-inject`, run **`make dag-trigger`** again (do not skip cleanup first). Embed keeps working via a partial unique index; `audit_task` reassigns the chaos row to the current `run_id` and fails per §5. Then **`make chaos-cleanup`** before normal operation.

If embed fails with `no unique or exclusion constraint matching the ON CONFLICT specification`, apply the DB repair migration:

```bash
docker compose exec postgres psql -U rico -d rico -f /docker-entrypoint-initdb.d/003_embeddings_chaos_safe.sql
```

(Or `make clean` on a dev volume.)

Script: `chaos/inject_duplicates.py` (same idea as sess8 `chaos/inject_duplicates.py`). It tags chaos rows with `source_fingerprint=chaos-duplicate-inject-v1` and temporarily drops the embeddings PK so a true duplicate key exists for `audit_task`.

See **Audit Failure Interpretation** below for SQL/log interpretation.

## Operational Commands

- Start services: `make up`
- Validate rubric: `make validate`
- Stop services: `make down`
- Full reset (remove volumes): `make clean`
- Data reset (truncate tables + clear bucket): `make reset`
- Pull LLM model: `make pull-models`
- Tail logs: `make logs`

## Data Model Summary

- `screens_metadata`: per-screen metadata and extraction fields
- `screens_embeddings`: vector outputs keyed by screen/model/kind
- `screens_review_queue`: extraction issues requiring review
- `screens_eval`: evaluation results
- `pipeline_runs`: run-level metadata and status
- `audit_results`: audit outcomes and details
- `pipeline_metrics`: health and quality metrics per run

## Pipeline Metrics Explained

The `pipeline_metrics` table records health and data quality metrics after each run. Key metrics:

- **`screens_metadata_row_count`**: Total screens ingested in this run. Expected: matches `LIMIT`.
- **`pct_extracted`**: Percentage of screens where LLM extraction succeeded (extraction_payload is non-null). Expected: 90-100%. Below 50% indicates extraction issues.
- **`pct_high_confidence`**: Percentage of screens with extraction confidence >= 0.5. Expected: 80-100%. Below 70% may indicate low-quality extractions.
- **`pct_in_review_queue`**: Percentage of screens flagged for manual review (screens_review_queue). Expected: 0-5%. Above 10% indicates systematic extraction problems.
- **`distinct_app_packages`**: Count of unique Android app packages in this run. Indicator of dataset diversity.
- **`distinct_categories`**: Count of unique app categories in this run. Indicator of category diversity.
- **`embeddings_pct_zero_norm`** (by model/kind): Percentage of embeddings with near-zero norm (vector_norm < 0.001). Expected: 0%. Non-zero indicates malformed or degenerate embeddings.
- **`embeddings_avg_dim`** (by model/kind): Average dimensionality of computed embeddings. Sanity check that embeddings are being computed at all.
- **`task_duration_seconds`**: Wall-clock duration per task. Used to identify bottlenecks (`extract_task` is typically slowest).
- **`task_retries`**: Retry count per task. Expected: 0. Non-zero indicates task instability.
- **`total_run_duration_seconds`**: Total pipeline end-to-end duration. Baseline for performance tracking.
- **`final_run_status`**: Final pipeline outcome label in metric labels (`succeeded`, `failed`, `paused_by_audit`).

## Audit Failure Interpretation

The audit task checks for duplicates and acts as a circuit breaker. **If audit fails:**

1. **Check `audit_results` table** for the failing run:
   - `passed = false` indicates duplicates were found
   - `details` (JSON) lists which table(s) had violations and which screen_ids

2. **Duplicate types:**
   - **Metadata duplicates**: Same `screen_id` appears twice in `screens_metadata` for the same run. Root cause: idempotency bug in ingest/load, or concurrent writes.
   - **Embedding duplicates**: Same `(screen_id, model_name, model_version, embedding_kind)` appears twice for the same run. Root cause: idempotency bug in embed tasks, or data corruption.

3. **Next steps:**
   - **Investigate root cause**: Check logs for the failing task and the run before it. Did the previous run succeed? Did inputs change?
   - **Data corruption scenario**: If duplicates are in old data (different run_id), they won't block the current run. Audit only checks the current run.
   - **Fix and re-run**: Once root cause is fixed, truncate the problematic table (`make reset`) and trigger the DAG again.
   - **Manual review**: Visit `http://localhost:8080`, click the failed DAG run, check task logs for details.

4. **Expected behavior on audit failure:**
   - `audit_task` fails with an AirflowException.
   - `eval_task` is skipped (trigger_rule="all_success" prevents it from running).
   - Run status is marked as `failed` or `paused_by_audit` in `pipeline_runs`.
   - Slack notification (if configured) alerts that audit failed with violation details.

## Troubleshooting: ingest and Slack (DNS / Hub connectivity)

If **`ingest_task`** fails with `ConnectionError: Couldn't reach 'rootsautomation/RICO-Screen2Words' on the Hub`, or Slack logs show **`Failed to resolve 'hooks.slack.com'`**, the Airflow container usually cannot resolve public DNS or reach the internet.

1. **Recreate Airflow after compose DNS** — `docker-compose.yml` sets public DNS (`8.8.8.8`, `8.8.4.4`) on `airflow-init`, `airflow-webserver`, and `airflow-scheduler`. After pulling changes, run:
   `docker compose up -d --force-recreate airflow-init airflow-webserver airflow-scheduler`
2. **Override DNS** — set `COMPOSE_DNS_SERVER_1` / `COMPOSE_DNS_SERVER_2` in `.env` (see `.env.example`) if your network requires different resolvers.
3. **Verify inside the scheduler** — `docker compose exec airflow-scheduler python -c "import socket; print(socket.gethostbyname('huggingface.co'))"` should print an IPv4 address.
4. **Corporate / offline** — if outbound HTTPS is blocked, use VPN or proxy (`HTTP_PROXY` / `HTTPS_PROXY`), or pre-populate an HF cache and point `HF_HOME` at a volume with the dataset already downloaded.

`ingest` retries `load_dataset` a few times with backoff for transient hub errors; persistent DNS or firewall issues require the steps above.

## Implementation Principles

- Keep DAG files thin; place business logic in `src/rico_dag/`.
- Keep migrations additive and versioned.
- Treat audit as an enforcement gate, not a warning.
- Prefer deterministic keys and conflict-safe writes to preserve idempotency.
