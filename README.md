# RICO Multimodal DAG

End-to-end multimodal pipeline as an Airflow DAG: ingest RICO screens, parse hierarchy, embed images (CLIP) and text (SBERT), extract structured JSON (Ollama), load into Postgres + pgvector, run a data-integrity audit as a circuit breaker, then evaluate.

Production-oriented: idempotent writes, row-level traceability, observability metrics, Slack alerts.

## Pipeline Flow

`ingest → parse → [embed_image, embed_text, extract] → load → audit → eval`

- Middle three tasks run in parallel.
- `LIMIT` DAG param controls batch size (`make dag-trigger LIMIT=5`).
- Audit runs **after load, before eval** — failure skips eval and marks the run `paused_by_audit`.

## Core Capabilities

| Area | Behavior |
|------|----------|
| **Idempotency** | `ON CONFLICT` upserts; re-run with same `LIMIT` adds no duplicate rows |
| **Traceability** | `pipeline_runs` + `run_id` / `source_fingerprint` on every destination row |
| **Audit** | 5-check integrity suite; fails loudly, blocks eval, persists to `audit_results` |
| **Observability** | `pipeline_metrics` + end-of-run log summary + Slack notifications |

## Tech Stack

Airflow · PostgreSQL + pgvector · MinIO · Ollama · HuggingFace · open-clip · sentence-transformers

## Repository Structure

```text
rico-multimodal-dag/
  dags/              # DAG definitions (orchestration only)
  src/rico_dag/      # Pipeline business logic
  agent/             # Bonus: Slack DataOps agent
  chaos/             # Audit circuit-breaker demo injector
  migrations/        # Database schema
  scripts/           # validate_project4.py rubric checker
  tests.md           # Full test guide (validation, chaos, agent)
  docker-compose.yml
  Makefile
```

## Setup

```bash
cp .env.example .env
make build
make up
make pull-models
```

Airflow UI: <http://localhost:8080> (`admin` / `admin`). Unpause `rico_pipeline` after first start.

## Running the Pipeline

```bash
make dag-trigger LIMIT=5
```

## Validation & Testing

See **[tests.md](tests.md)** for step-by-step scripts covering:

- Happy path + rubric validation (`make validate` / `make validate-docker`)
- Idempotency re-run check
- Chaos / audit circuit-breaker scenarios (`duplicate`, `zero-norm`, `orphan`, `missing`, `all`)
- Instructor-style fresh stack check (without wiping Ollama volumes)
- Bonus agent smoke + Slack demo flow

Quick rubric check after a successful run:

```bash
make validate-docker
```

## Operational Commands

| Command | Purpose |
|---------|---------|
| `make up` / `make down` | Start / stop stack (volumes preserved) |
| `make reset` | Truncate tables + clear MinIO bucket |
| `make clean` | Stop stack **and wipe volumes** (re-pull Ollama model) |
| `make logs` | Tail compose logs |
| `make chaos-inject` / `make chaos-cleanup` | Audit demo corruption / cleanup |
| `make agent-install` / `make agent` | Bonus Slack agent |

## Data Model

`screens_metadata` · `screens_embeddings` · `screens_review_queue` · `screens_eval` · `pipeline_runs` · `audit_results` · `pipeline_metrics`

Audit checks (per `run_id`): duplicate embeddings/metadata, invalid (zero-norm) vectors, orphan embeddings, missing embeddings. Details in [tests.md §9](tests.md#9-audit-failure-reference).

## Troubleshooting

**Ingest / HuggingFace DNS:** Recreate Airflow after compose DNS changes; verify `socket.gethostbyname('huggingface.co')` inside the scheduler. See [tests.md §11](tests.md#11-troubleshooting).

**Embed ON CONFLICT after chaos:** `make db-repair` or apply migration `003_embeddings_chaos_safe.sql`.

---

## Bonus: Backfill Agent (ChatOps)

Two-way Slack bot: natural-language commands → Ollama intent parsing → Airflow REST API + Postgres diagnostics. Does **not** modify the DAG.

| Capability | Example | Action |
|------------|---------|--------|
| **Trigger** | `@DataBot backfill 20 screens` | Triggers run; replies with `dag_run_id` |
| **Diagnose** | `@DataBot is the pipeline healthy?` | Reads run/audit/DAG state |
| **Fix (gated)** | `@DataBot fix` → `@DataBot confirm` | Generic repair + re-run after human approval |

**Confirmation gate:** Audit is a circuit breaker — the bot proposes fixes but only executes after explicit `confirm` in-thread (`cancel` to abort; 10 min expiry).

### Architecture

```
@DataBot message → agent/agent.py (Socket Mode)
  → llm_parser.py (Ollama) → intent
  → trigger: airflow_client.py → POST dagRuns
  → diagnose/fix: diagnostics.py + db.py → audit_results
  → confirm: remediation.py → repair.py + re-trigger
```

**Generic repair** (`agent/repair.py`): de-dupe, drop invalid/orphan/incomplete rows, restore PK if needed — then re-run. Real screens are rebuilt idempotently on re-run.

### Prerequisites

1. `make up` + `make pull-models`
2. Slack App tokens in `.env` (`SLACK_BOT_TOKEN`, `SLACK_APP_TOKEN`)
3. `rico_pipeline` unpaused in Airflow UI
4. Postgres reachable on `localhost:5432` (agent maps compose hostnames automatically)

### Slack App setup (one-time)

1. <https://api.slack.com/apps> → Create App → **Socket Mode** on → App-Level Token (`xapp-...`, scope `connections:write`)
2. **OAuth & Permissions** → scopes: `app_mentions:read`, `chat:write`
3. **Event Subscriptions** → `app_mention`
4. Install to workspace → Bot Token (`xoxb-...`)
5. Add to `.env`, invite bot: `/invite @YourBotName`

### Running

```bash
make agent-install   # first time
make agent           # Ctrl-C to stop
make agent-smoke     # Airflow trigger without Slack
make agent-diagnose  # one-shot diagnosis CLI
```

### Example interactions

| Slack | Bot |
|-------|-----|
| `@DataBot backfill 5 screens` | Triggers LIMIT=5, returns `dag_run_id` |
| `@DataBot why did the pipeline fail` | Audit violation summary + recommended fix |
| `@DataBot fix` | Proposes repair; waits for `confirm` |
| `@DataBot confirm` | Runs repair + re-trigger |

Full video demo script: see [tests.md §7](tests.md#7-bonus-agent--slack-video-flow).

### Agent troubleshooting

| Symptom | Fix |
|---------|-----|
| Missing `SLACK_BOT_TOKEN` / `SLACK_APP_TOKEN` | Add to `.env` |
| DAG paused | Unpause in Airflow UI (agent can offer to fix) |
| Cannot reach Airflow | `AIRFLOW_API_URL=http://localhost:8080` on host |
| Ollama timeout | `AGENT_LLM_TIMEOUT`; run `make pull-models` |
| No pending fix on `confirm` | Send `fix` first, same thread, within 10 min |

## Principles

- Thin DAG files; logic in `src/rico_dag/`
- Audit is enforcement, not a warning
- Deterministic keys + conflict-safe writes
