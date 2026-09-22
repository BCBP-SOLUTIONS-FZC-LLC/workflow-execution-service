-- Recreating the narrower uniqueness rule fails if two live instances share a
-- business_key under different subjects, which is exactly what the up migration
-- made legal. That failure is the correct outcome: rolling back cannot silently
-- pick one of them to keep.
DROP INDEX IF EXISTS idx_workflow_instance_subject;

DROP INDEX IF EXISTS uq_workflow_instance_business_key;

CREATE UNIQUE INDEX uq_workflow_instance_business_key
    ON workflow_instance (tenant_id, business_key)
    WHERE status NOT IN ('COMPLETED', 'TERMINATED', 'FAILED');

ALTER TABLE workflow_instance
    DROP COLUMN IF EXISTS subject_id;
