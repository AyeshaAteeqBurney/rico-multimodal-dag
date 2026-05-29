.PHONY: help build up down clean pull-models reset airflow-init logs dag-trigger validate chaos-inject chaos-cleanup agent-install agent agent-smoke db-repair

COMPOSE := docker compose

OLLAMA_MODEL     ?= qwen2.5:3b
POSTGRES_USER    ?= rico
POSTGRES_DB      ?= rico
MINIO_ACCESS_KEY ?= minioadmin
MINIO_SECRET_KEY ?= minioadmin
MINIO_BUCKET     ?= rico-raw
LIMIT            ?= 5

help:
	@echo "Project 4 targets:"
	@echo "  build         build custom airflow image with project dependencies"
	@echo "  up            start postgres+minio+ollama+airflow"
	@echo "  airflow-init  run airflow db init/migrate + admin user creation"
	@echo "  pull-models   pull qwen2.5:3b into ollama"
	@echo "  dag-trigger   trigger rico_pipeline with LIMIT (default 5)"
	@echo "  down          stop services (volumes preserved)"
	@echo "  clean         stop services and wipe volumes (full reset)"
	@echo "  reset         truncate tables + clear MinIO bucket"
	@echo "  logs          tail compose logs"
	@echo "  validate      run Project 4 rubric checks (scripts/validate_project4.py)"
	@echo "  chaos-inject  inject duplicate rows for audit circuit-breaker demo"
	@echo "  chaos-cleanup remove chaos rows and restore PKs"
	@echo "  agent-install install agent dependencies (slack-bolt etc.)"
	@echo "  agent         start the Slack backfill agent (Socket Mode)"
	@echo "  agent-smoke   smoke-test Airflow API trigger without Slack"

build:
	$(COMPOSE) build airflow-init airflow-webserver airflow-scheduler

up:
	$(COMPOSE) up -d --wait postgres minio ollama
	$(COMPOSE) up -d minio-init ollama-init
	$(COMPOSE) up -d airflow-init
	$(COMPOSE) up -d airflow-webserver airflow-scheduler

airflow-init:
	$(COMPOSE) up airflow-init

down:
	$(COMPOSE) down

clean:
	$(COMPOSE) down -v

pull-models:
	$(COMPOSE) exec ollama ollama pull $(OLLAMA_MODEL)

reset:
	$(COMPOSE) exec postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -c \
	  "TRUNCATE TABLE screens_metadata, screens_embeddings, screens_review_queue, screens_eval RESTART IDENTITY CASCADE;"
	$(COMPOSE) exec minio mc alias set local http://minio:9000 $(MINIO_ACCESS_KEY) $(MINIO_SECRET_KEY) >/dev/null
	$(COMPOSE) exec minio mc rm --recursive --force local/$(MINIO_BUCKET)/ >/dev/null 2>&1 || true
	@echo "state truncated"

logs:
	$(COMPOSE) logs -f --tail=100

dag-trigger:
	$(COMPOSE) exec airflow-webserver airflow dags trigger rico_pipeline --conf "{\"LIMIT\":$(LIMIT)}"

validate:
	python scripts/validate_project4.py --skip-infra

validate-docker:
	$(COMPOSE) exec -e POSTGRES_HOST=postgres airflow-scheduler python /opt/airflow/scripts/validate_project4.py --skip-infra

chaos-inject:
	python chaos/inject_duplicates.py

chaos-inject-docker:
	$(COMPOSE) exec -e POSTGRES_HOST=postgres airflow-scheduler python /opt/airflow/chaos/inject_duplicates.py

chaos-cleanup:
	python chaos/inject_duplicates.py --cleanup

chaos-cleanup-docker:
	$(COMPOSE) exec -e POSTGRES_HOST=postgres airflow-scheduler python /opt/airflow/chaos/inject_duplicates.py --cleanup

# ── Bonus: Backfill Agent ────────────────────────────────────────────────────

agent-install:
	pip install -r requirements-agent.txt

agent:
	python -m agent.agent

agent-smoke:
	python -m agent.trigger_smoke $(LIMIT)

db-repair:
	$(COMPOSE) exec -T postgres psql -U $(POSTGRES_USER) -d $(POSTGRES_DB) -f /docker-entrypoint-initdb.d/003_embeddings_chaos_safe.sql
