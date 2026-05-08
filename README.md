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

## Operational Commands

- Start services: `make up`
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

## Definition of Done Checklist

- DAG is visible and healthy in Airflow UI.
- A run with `LIMIT=5` populates destination tables and creates a `pipeline_runs` entry.
- Re-running with same `LIMIT` does not create duplicates.
- Manually inserted duplicates are caught by audit, and `eval` is skipped.
- Destination rows have non-null `run_id` and `source_fingerprint`.

## Implementation Principles

- Keep DAG files thin; place business logic in `src/rico_dag/`.
- Keep migrations additive and versioned.
- Treat audit as an enforcement gate, not a warning.
- Prefer deterministic keys and conflict-safe writes to preserve idempotency.
