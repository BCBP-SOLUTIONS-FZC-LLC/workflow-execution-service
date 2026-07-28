-- name: GetWorkflowDataKey :one
SELECT * FROM workflow_data_keys WHERE tenant_id = $1 AND user_id = $2;

-- name: CreateWorkflowDataKey :one
INSERT INTO workflow_data_keys (tenant_id, user_id, wrapped_dek, kms_key_id)
VALUES ($1, $2, $3, $4)
RETURNING *;

-- name: RevokeWorkflowDataKey :exec
UPDATE workflow_data_keys
SET revoked_at = now(), updated_at = now()
WHERE tenant_id = $1 AND user_id = $2;
