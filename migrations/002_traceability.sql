\c rico

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          UUID PRIMARY KEY,
    dag_run_id      TEXT NOT NULL UNIQUE,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ended_at        TIMESTAMPTZ,
    status          TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'failed', 'paused_by_audit')),
    limit_param     INTEGER NOT NULL,
    git_sha         TEXT NOT NULL,
    clip_version    TEXT NOT NULL,
    sbert_version   TEXT NOT NULL,
    llm_model       TEXT NOT NULL,
    prompt_version  TEXT NOT NULL
);

ALTER TABLE screens_metadata
    ADD COLUMN IF NOT EXISTS run_id UUID,
    ADD COLUMN IF NOT EXISTS source_fingerprint TEXT;

ALTER TABLE screens_embeddings
    ADD COLUMN IF NOT EXISTS run_id UUID,
    ADD COLUMN IF NOT EXISTS source_fingerprint TEXT;

ALTER TABLE screens_review_queue
    ADD COLUMN IF NOT EXISTS run_id UUID,
    ADD COLUMN IF NOT EXISTS source_fingerprint TEXT;

UPDATE screens_metadata SET run_id = '00000000-0000-0000-0000-000000000000' WHERE run_id IS NULL;
UPDATE screens_metadata SET source_fingerprint = '' WHERE source_fingerprint IS NULL;
UPDATE screens_embeddings SET run_id = '00000000-0000-0000-0000-000000000000' WHERE run_id IS NULL;
UPDATE screens_embeddings SET source_fingerprint = '' WHERE source_fingerprint IS NULL;
UPDATE screens_review_queue SET run_id = '00000000-0000-0000-0000-000000000000' WHERE run_id IS NULL;
UPDATE screens_review_queue SET source_fingerprint = '' WHERE source_fingerprint IS NULL;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.table_constraints
    WHERE table_name = 'pipeline_runs'
      AND constraint_name = 'pipeline_runs_run_id_key'
  ) THEN
    EXECUTE 'ALTER TABLE pipeline_runs DROP CONSTRAINT pipeline_runs_run_id_key';
  END IF;
END $$;

INSERT INTO pipeline_runs (
    run_id, dag_run_id, started_at, ended_at, status, limit_param, git_sha,
    clip_version, sbert_version, llm_model, prompt_version
)
VALUES (
    '00000000-0000-0000-0000-000000000000',
    'bootstrap-pretraceability',
    NOW(),
    NOW(),
    'succeeded',
    0,
    'unknown',
    'unknown',
    'unknown',
    'unknown',
    'unknown'
)
ON CONFLICT (run_id) DO NOTHING;

ALTER TABLE screens_metadata
    ALTER COLUMN run_id SET NOT NULL,
    ALTER COLUMN source_fingerprint SET NOT NULL;

ALTER TABLE screens_embeddings
    ALTER COLUMN run_id SET NOT NULL,
    ALTER COLUMN source_fingerprint SET NOT NULL;

ALTER TABLE screens_review_queue
    ALTER COLUMN run_id SET NOT NULL,
    ALTER COLUMN source_fingerprint SET NOT NULL;

ALTER TABLE screens_metadata
    ADD CONSTRAINT fk_screens_metadata_run_id
    FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id);

ALTER TABLE screens_embeddings
    ADD CONSTRAINT fk_screens_embeddings_run_id
    FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id);

ALTER TABLE screens_review_queue
    ADD CONSTRAINT fk_screens_review_queue_run_id
    FOREIGN KEY (run_id) REFERENCES pipeline_runs(run_id);

CREATE TABLE IF NOT EXISTS audit_results (
    id          BIGSERIAL PRIMARY KEY,
    run_id      UUID NOT NULL REFERENCES pipeline_runs(run_id),
    audit_name  TEXT NOT NULL,
    passed      BOOLEAN NOT NULL,
    details     JSONB NOT NULL DEFAULT '{}'::JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id            BIGSERIAL PRIMARY KEY,
    run_id        UUID NOT NULL REFERENCES pipeline_runs(run_id),
    metric_name   TEXT NOT NULL,
    metric_value  DOUBLE PRECISION NOT NULL,
    metric_labels JSONB NOT NULL DEFAULT '{}'::JSONB,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
