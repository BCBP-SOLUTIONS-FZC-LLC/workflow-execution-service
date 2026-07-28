-- name: GetWorkflowInstance :one
SELECT * FROM workflow_instance WHERE id = $1;

-- name: ListWorkflowInstancesByTenant :many
SELECT * FROM workflow_instance
WHERE tenant_id = $1
ORDER BY created_at DESC, id DESC
LIMIT $2;

-- name: CreateWorkflowInstance :one
INSERT INTO workflow_instance (
    id, tenant_id, workflow_id, workflow_version_id, business_key,
    temporal_workflow_id, temporal_run_id, status, current_node_keys,
    task_queue, started_by_user_id, started_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
)
RETURNING *;

-- name: UpdateWorkflowInstanceStatus :one
UPDATE workflow_instance
SET status = $2, updated_at = now(), record_version = record_version + 1
WHERE id = $1 AND record_version = $3
RETURNING *;
