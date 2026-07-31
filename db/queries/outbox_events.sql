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
