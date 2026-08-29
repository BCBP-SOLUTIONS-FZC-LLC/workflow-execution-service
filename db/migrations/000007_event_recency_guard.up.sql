-- Generic <=-skip recency guard for out-of-order-delivery protection across
-- TenantStateChanged and UserAvailabilityChanged (LLD §6.2 items 4/6,
-- Appendix A #25/#26). One generic table, not two purpose-named ones: both
-- scopes reduce to the identical check+store shape, with no FK relationships
-- to enforce. No RLS: scope_key already encodes tenant/user identity where
-- relevant — this is bookkeeping state, not tenant business data (same
-- rationale as active_task_queues and processed_event).
CREATE TABLE event_recency_guard (
    scope_key       TEXT        PRIMARY KEY,
    last_applied_at TIMESTAMPTZ NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
