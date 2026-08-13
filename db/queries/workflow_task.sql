-- name: GetWorkflowTask :one
SELECT * FROM workflow_task WHERE id = $1;

-- name: ListWorkflowTasksByInstance :many
SELECT * FROM workflow_task
WHERE workflow_instance_id = $1
  AND (
    sqlc.narg('cursor_created_at')::timestamptz IS NULL
    OR (created_at, id) < (sqlc.narg('cursor_created_at')::timestamptz, sqlc.narg('cursor_id')::uuid)
  )
ORDER BY created_at DESC, id DESC
LIMIT $2;

-- name: CreateWorkflowTask :one
INSERT INTO workflow_task (
    id, tenant_id, workflow_instance_id, node_key, department_id,
    status, assignee_mode, connector_type, extras_json, deferred_from_task_id,
    due_at, follow_up_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
)
RETURNING *;

-- name: UpdateWorkflowTaskStatus :one
UPDATE workflow_task
SET status = $2, updated_at = now(), record_version = record_version + 1
WHERE id = $1 AND record_version = $3
RETURNING *;
