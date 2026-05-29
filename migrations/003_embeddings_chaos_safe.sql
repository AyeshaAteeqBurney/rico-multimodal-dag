\c rico

-- Repair DBs where chaos-inject dropped screens_embeddings_pkey without restoring it.
DELETE FROM screens_embeddings WHERE source_fingerprint = 'chaos-duplicate-inject-v1';

DELETE FROM screens_embeddings a
USING screens_embeddings b
WHERE a.screen_id = b.screen_id
  AND a.model_name = b.model_name
  AND a.model_version = b.model_version
  AND a.embedding_kind = b.embedding_kind
  AND a.ctid < b.ctid;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'screens_embeddings_pkey'
  ) THEN
    ALTER TABLE screens_embeddings
      ADD CONSTRAINT screens_embeddings_pkey
      PRIMARY KEY (screen_id, model_name, model_version, embedding_kind);
  END IF;
END $$;

-- Lets embed use ON CONFLICT while a chaos duplicate row coexists (chaos-inject only).
CREATE UNIQUE INDEX IF NOT EXISTS uq_screens_embeddings_prod
  ON screens_embeddings (screen_id, model_name, model_version, embedding_kind)
  WHERE source_fingerprint IS DISTINCT FROM 'chaos-duplicate-inject-v1';
