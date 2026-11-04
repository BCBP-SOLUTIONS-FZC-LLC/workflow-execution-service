-- Purge bookkeeping for offboarded tenants (LLD §4.13). No RLS and no foreign
-- key: like active_task_queues and event_recency_guard it is operational
-- state read across tenants by the purge sweep, and it outlives the tenant's
-- own rows.
CREATE TABLE tenant_retention (
    tenant_id           UUID        PRIMARY KEY,
    offboarded_at       TIMESTAMPTZ,
    history_purge_after TIMESTAMPTZ,
    purged_at           TIMESTAMPTZ,
    returned_at         TIMESTAMPTZ,
    attempts            INTEGER     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at     TIMESTAMPTZ NOT NULL,
    record_version      BIGINT      NOT NULL DEFAULT 1 CHECK (record_version > 0),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK ((offboarded_at IS NULL) = (history_purge_after IS NULL)),
    CHECK (offboarded_at IS NOT NULL OR returned_at IS NOT NULL)
);

CREATE INDEX idx_tenant_retention_due ON tenant_retention (history_purge_after) WHERE purged_at IS NULL;
