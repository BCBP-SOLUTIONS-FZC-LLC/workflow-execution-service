-- name: GetWorkflowTask :one
SELECT * FROM workflow_task WHERE id = $1;

-- name: ListWorkflowTasksByInstance :many
SELECT * FROM workflow_task
WHERE workflow_instance_id = $1
ORDER BY created_at DESC, id DESC;

-- name: CreateWorkflowTask :one
INSERT INTO workflow_task (
    id, tenant_id, workflow_instance_id, node_key, department_id,
    status, assignee_mode, due_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8
)
RETURNING *;

-- name: UpdateWorkflowTaskStatus :one
UPDATE workflow_task
SET status = $2, updated_at = now(), record_version = record_version + 1
WHERE id = $1 AND record_version = $3
RETURNING *;
