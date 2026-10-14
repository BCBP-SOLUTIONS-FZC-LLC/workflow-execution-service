-- sqlc schema-awareness only, NOT a migration — outbox_events is created by
-- platform-events' own outbox.ApplySchema. golang-migrate never reads this
-- directory; this file exists only so sqlc can type-check outbox_events.sql.
CREATE TABLE IF NOT EXISTS outbox_events (
    id           UUID        PRIMARY KEY,
    event_type   TEXT        NOT NULL,
    payload      JSONB       NOT NULL,
    tenant_id    TEXT        NOT NULL DEFAULT '',
    trace_id     TEXT        NOT NULL DEFAULT '',
    attempts     INT         NOT NULL DEFAULT 0,
    last_error   TEXT        NOT NULL DEFAULT '',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ
);
