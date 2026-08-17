-- name: GetWorkflowInstance :one
SELECT * FROM workflow_instance WHERE id = $1;

-- name: ListWorkflowInstancesByTenant :many
-- Backs InstanceService.List's filterable query (GET /instances) -- every
-- filter is optional (NULL = unfiltered).
SELECT * FROM workflow_instance
WHERE tenant_id = $1
  AND (sqlc.narg('status')::workflow_instance_status IS NULL OR status = sqlc.narg('status')::workflow_instance_status)
  AND (sqlc.narg('workflow_version_id')::uuid IS NULL OR workflow_version_id = sqlc.narg('workflow_version_id')::uuid)
  AND (sqlc.narg('started_after')::timestamptz IS NULL OR started_at > sqlc.narg('started_after')::timestamptz)
  AND (sqlc.narg('started_before')::timestamptz IS NULL OR started_at < sqlc.narg('started_before')::timestamptz)
  AND (
    sqlc.narg('cursor_created_at')::timestamptz IS NULL
    OR (created_at, id) < (sqlc.narg('cursor_created_at')::timestamptz, sqlc.narg('cursor_id')::uuid)
  )
ORDER BY created_at DESC, id DESC
LIMIT $2;

-- name: CreateWorkflowInstance :one
INSERT INTO workflow_instance (
    id, tenant_id, workflow_id, workflow_version_id, business_key,
    temporal_workflow_id, temporal_run_id, status, current_node_keys,
    context_json, override_map, task_queue, started_by_user_id, started_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14
)
RETURNING *;

-- name: UpdateWorkflowInstanceStatus :one
-- completed_at is set only when the new status is terminal (COMPLETED,
-- TERMINATED, FAILED) — a non-terminal transition (e.g. RUNNING -> DEGRADED)
-- leaves the existing value (NULL) untouched.
UPDATE workflow_instance
SET status = $2,
    completed_at = CASE WHEN $2::workflow_instance_status IN ('COMPLETED', 'TERMINATED', 'FAILED')
                       THEN now() ELSE completed_at END,
    updated_at = now(), record_version = record_version + 1
WHERE id = $1 AND record_version = $3
RETURNING *;

-- name: CountActiveInstancesByWorkflow :one
-- Backs ArchiveGuard.CheckActiveInstances(tenant_id, workflow_id) -- uses
-- idx_workflow_instance_workflow_active.
SELECT count(*) FROM workflow_instance
WHERE tenant_id = $1 AND workflow_id = $2
  AND status IN ('RUNNING', 'PAUSED', 'DEGRADED');

-- name: CountActiveInstancesByTaskQueue :one
-- Backs TenantLifecycleReconciler's plan-downgrade check (LLD §3.2 item 3):
-- a tenant's isolated queue is never deregistered while any instance
-- started on it is still running.
SELECT count(*) FROM workflow_instance
WHERE tenant_id = $1 AND task_queue = $2
  AND status IN ('RUNNING', 'PAUSED', 'DEGRADED');

-- name: UpdateWorkflowInstanceCurrentNodeKeys :one
UPDATE workflow_instance
SET current_node_keys = $2, updated_at = now(), record_version = record_version + 1
WHERE id = $1 AND record_version = $3
RETURNING *;
