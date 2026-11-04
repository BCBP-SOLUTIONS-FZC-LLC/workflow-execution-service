-- name: RecencyGuardShouldApply :one
SELECT NOT EXISTS (
    SELECT 1 FROM event_recency_guard
    WHERE scope_key = sqlc.arg(scope_key) AND last_applied_at >= sqlc.arg(event_time)
) AS should_apply;

-- name: RecencyGuardCheckAndCommit :one
INSERT INTO event_recency_guard (scope_key, last_applied_at, updated_at)
VALUES (sqlc.arg(scope_key), sqlc.arg(event_time), now())
ON CONFLICT (scope_key) DO UPDATE
    SET last_applied_at = EXCLUDED.last_applied_at, updated_at = now()
    WHERE event_recency_guard.last_applied_at < EXCLUDED.last_applied_at
RETURNING scope_key;

-- name: RecencyGuardCommit :exec
INSERT INTO event_recency_guard (scope_key, last_applied_at, updated_at)
VALUES (sqlc.arg(scope_key), sqlc.arg(event_time), now())
ON CONFLICT (scope_key) DO UPDATE
    SET last_applied_at = GREATEST(event_recency_guard.last_applied_at, EXCLUDED.last_applied_at),
        updated_at = now();
