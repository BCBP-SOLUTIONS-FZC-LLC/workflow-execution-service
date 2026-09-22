-- Instance identity becomes a subject and a key unique within it (LLD §4.2).
-- One opaque business_key could not express a calling domain whose entity runs
-- several workflows at once: the second collided with the first, and an
-- instance left paused or degraded held that entity's key indefinitely.
--
-- Both columns stay opaque to the engine. NOT NULL with no default: there is no
-- deployed data, and a nullable subject would silently weaken the uniqueness
-- rule below, since Postgres treats NULLs as distinct in a unique index.
ALTER TABLE workflow_instance
    ADD COLUMN subject_id TEXT NOT NULL;

-- Uniqueness moves from (tenant, key) to (tenant, subject, key), keeping the
-- same partial predicate: a key is reusable once its instance reaches a
-- terminal state, and DEGRADED does not count as terminal because it still
-- needs admin resolution (LLD §4.2).
DROP INDEX IF EXISTS uq_workflow_instance_business_key;

CREATE UNIQUE INDEX uq_workflow_instance_business_key
    ON workflow_instance (tenant_id, subject_id, business_key)
    WHERE status NOT IN ('COMPLETED', 'TERMINATED', 'FAILED');

-- "Every live instance for this subject" as an index scan. Same predicate as
-- the uniqueness rule, so one subject's live set is exactly what it constrains.
CREATE INDEX idx_workflow_instance_subject
    ON workflow_instance (tenant_id, subject_id)
    WHERE status NOT IN ('COMPLETED', 'TERMINATED', 'FAILED');
