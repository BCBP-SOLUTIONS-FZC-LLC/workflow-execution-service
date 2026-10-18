-- name: UpsertTenantState :exec
INSERT INTO tenant_state (tenant_id, status, changed_at, updated_at)
VALUES ($1, $2, $3, now())
ON CONFLICT (tenant_id) DO UPDATE
SET status = EXCLUDED.status, changed_at = EXCLUDED.changed_at, updated_at = now()
WHERE EXCLUDED.changed_at >= tenant_state.changed_at;

-- name: GetTenantState :one
SELECT status FROM tenant_state WHERE tenant_id = $1;

-- name: PurgeTenantState :execrows
DELETE FROM tenant_state WHERE tenant_id = $1;
