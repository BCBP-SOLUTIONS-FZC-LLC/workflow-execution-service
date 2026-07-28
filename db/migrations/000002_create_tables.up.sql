-- One row per running/completed workflow instance — the dashboard-facing /
-- projection of Temporal's authoritative execution state (LLD §4.2).
CREATE TABLE workflow_instance (
    id                    UUID                     PRIMARY KEY,
    tenant_id             UUID                     NOT NULL,
    workflow_id           UUID                     NOT NULL,  -- Definition Service's workflow.id; lineage/reporting join only, no FK (cross-service, cross-schema)
    workflow_version_id   UUID                     NOT NULL,  -- Definition Service's workflow_version.id; same cross-service caveat
    business_key          TEXT                     NOT NULL,
    temporal_workflow_id  TEXT                     NOT NULL,
    temporal_run_id       TEXT,
    status                workflow_instance_status NOT NULL,
    current_node_keys     TEXT[]                   NOT NULL,
    saved_node_keys       TEXT[]                   NOT NULL DEFAULT '{}',
    context_json          JSONB,
    override_map          JSONB,
    task_queue            TEXT                     NOT NULL,
    started_by_user_id    UUID                     NOT NULL,
    started_at            TIMESTAMPTZ,
    completed_at          TIMESTAMPTZ,
    record_version        BIGINT                   NOT NULL DEFAULT 1 CHECK (record_version > 0),
    created_at            TIMESTAMPTZ              NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ              NOT NULL DEFAULT now()
);

-- One row per dispatched stage/task (prep/review/approve, unrecognized-Type
-- passthrough, call_pool admin-stub) (LLD §4.3).
CREATE TABLE workflow_task (
    id                    UUID                 PRIMARY KEY,
    tenant_id             UUID                 NOT NULL,
    workflow_instance_id  UUID                 NOT NULL REFERENCES workflow_instance(id) ON DELETE RESTRICT,
    node_key              TEXT                 NOT NULL,
    -- Snapshotted from the compiled plan at task creation.
    department_id         UUID                 NOT NULL,
    status                workflow_task_status NOT NULL,
    record_version        BIGINT               NOT NULL DEFAULT 1 CHECK (record_version > 0),
    assignee_mode         TEXT                 NOT NULL,  -- 'single' | 'all'
    extras_json           JSONB,
    deferred_from_task_id UUID                 REFERENCES workflow_task(id) ON DELETE RESTRICT,
    due_at                TIMESTAMPTZ,
    follow_up_at          TIMESTAMPTZ,
    created_at            TIMESTAMPTZ          NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ          NOT NULL DEFAULT now(),
    completed_at          TIMESTAMPTZ
);

-- One row per assignee on a task; carries claim/completion/reassignment state (LLD §4.4).
CREATE TABLE workflow_task_assignment (
    id            UUID        PRIMARY KEY,
    tenant_id     UUID        NOT NULL,
    task_id       UUID        NOT NULL REFERENCES workflow_task(id) ON DELETE RESTRICT,
    user_id       UUID        NOT NULL,
    assigned_by   UUID,
    reason        TEXT,
    is_lead       BOOL        NOT NULL DEFAULT false,
    is_active     BOOL        NOT NULL DEFAULT true,
    assigned_at   TIMESTAMPTZ,
    claimed_at    TIMESTAMPTZ,
    completed_at  TIMESTAMPTZ,
    result_json   JSONB,
    vacated_at    TIMESTAMPTZ,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Registry of currently-active tenant-isolated Temporal task queues (LLD §4.6).
-- No RLS (see 000004_enable_rls) — Workers need to read every currently-active
-- queue across every tenant in one query.
CREATE TABLE active_task_queues (
    id             UUID         PRIMARY KEY,
    tenant_id      UUID         NOT NULL,
    queue_name     TEXT         NOT NULL UNIQUE,
    registered_at  TIMESTAMPTZ  NOT NULL,
    updated_at     TIMESTAMPTZ  NOT NULL
);

-- Consumer-idempotency dedup table for inbound events (LLD §6), following the
-- same platform-wide convention as Definition Service and iam-user-profile.
-- No RLS, no tenant_id — infrastructure dedup state, not tenant business data.
CREATE TABLE processed_event (
    event_id     UUID        NOT NULL,
    consumer     TEXT        NOT NULL,
    event_type   TEXT,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, consumer)
);

-- Per-user Data Encryption Keys backing crypto-shredding for content-risk jsonb
-- fields (LLD §4.12, §9.6). One row per (tenant_id, user_id), created lazily.
CREATE TABLE workflow_data_keys (
    tenant_id    UUID        NOT NULL,
    user_id      UUID        NOT NULL,
    wrapped_dek  BYTEA       NOT NULL,
    kms_key_id   TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at   TIMESTAMPTZ,
    PRIMARY KEY (tenant_id, user_id)
);

-- Node-override's own audit record (LLD §4.13, §5.4). Insert-only, immutable:
-- no record_version/updated_at/deleted_at, same reasoning as every audit-bearing
-- row in this schema.
CREATE TABLE assignee_overrides (
    id                    UUID        PRIMARY KEY,
    tenant_id             UUID        NOT NULL,
    workflow_instance_id  UUID        NOT NULL REFERENCES workflow_instance(id) ON DELETE RESTRICT,
    node_key              TEXT        NOT NULL,
    previous_user_id      UUID        NOT NULL,
    new_user_id           UUID        NOT NULL,
    reason                TEXT,
    actor_user_id         UUID        NOT NULL,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- outbox_events / outbox_dead_letters are NOT created here — platform-events
-- owns that DDL (outbox.ApplySchema, applied by cmd/server's migrate subcommand
-- before this file's migrations run). This service only adds indexes
-- (000003_create_indexes) and an RLS policy (000005_outbox_rls) on top.
