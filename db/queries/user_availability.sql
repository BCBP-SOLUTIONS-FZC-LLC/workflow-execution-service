-- name: UpsertUserAvailability :exec
INSERT INTO user_availability (tenant_id, user_id, status, changed_at)
VALUES ($1, $2, $3, $4)
ON CONFLICT (tenant_id, user_id) DO UPDATE
SET status = EXCLUDED.status,
    changed_at = EXCLUDED.changed_at,
    record_version = user_availability.record_version + 1,
    updated_at = now()
WHERE EXCLUDED.changed_at >= user_availability.changed_at;

-- name: GetUserAvailability :one
SELECT status FROM user_availability WHERE tenant_id = $1 AND user_id = $2;
