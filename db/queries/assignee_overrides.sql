-- name: ListAssigneeOverridesByInstance :many
SELECT * FROM assignee_overrides
WHERE workflow_instance_id = $1
ORDER BY created_at DESC;

-- name: CreateAssigneeOverride :one
INSERT INTO assignee_overrides (
    id, tenant_id, workflow_instance_id, node_key,
    previous_user_id, new_user_id, reason, actor_user_id
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8
)
RETURNING *;
