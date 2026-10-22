-- The tenant's current lifecycle status as last applied from a TenantStateChanged
-- event (LLD §5.5), read by the start gate. No RLS and no foreign key: like
-- active_task_queues it is operational state read before a tenant context
-- exists. A change older than the stored one never overwrites it.
CREATE TABLE tenant_state (
    tenant_id  UUID        PRIMARY KEY,
    status     TEXT        NOT NULL,
    changed_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
