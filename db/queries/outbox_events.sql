-- name: OutboxEventExistsForTask :one
-- RecordSLAWarning/RecordSLABreach's own idempotency check: these are
-- audit-only (no status mutation to gate a retry on), so a retried activity
-- checks the outbox itself for whether it already recorded this exact
-- event type for this task before enqueueing a second one.
SELECT EXISTS(
    SELECT 1 FROM outbox_events
    WHERE event_type = @event_type::text
      AND payload -> 'data' ->> 'task_id' = @task_id::text
) AS exists;

-- name: ListOutboxEventsByInstance :many
-- payload is the whole serialized envelope, so business fields live under
-- its own "data" key, not at payload's top level.
SELECT * FROM outbox_events
WHERE tenant_id = @tenant_id::text
  AND payload -> 'data' ->> 'workflow_instance_id' = @workflow_instance_id::text
  AND (
    sqlc.narg('cursor_created_at')::timestamptz IS NULL
    OR (created_at, id) < (sqlc.narg('cursor_created_at')::timestamptz, sqlc.narg('cursor_id')::uuid)
  )
ORDER BY created_at DESC, id DESC
LIMIT @page_limit::int;
