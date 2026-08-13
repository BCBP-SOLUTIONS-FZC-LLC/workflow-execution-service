-- name: GetWorkflowTaskAssignment :one
SELECT * FROM workflow_task_assignment WHERE id = $1;

-- name: ListActiveAssignmentsByTask :many
SELECT * FROM workflow_task_assignment
WHERE task_id = $1 AND is_active;

-- name: ListActiveAssignmentsByUser :many
SELECT * FROM workflow_task_assignment
WHERE tenant_id = $1 AND user_id = $2 AND is_active;

-- name: CreateWorkflowTaskAssignment :one
INSERT INTO workflow_task_assignment (
    id, tenant_id, task_id, user_id, assigned_by, reason, is_lead, assigned_at
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8
)
RETURNING *;

-- name: VacateWorkflowTaskAssignment :one
UPDATE workflow_task_assignment
SET is_active = false, vacated_at = now(), updated_at = now()
WHERE id = $1
RETURNING *;

-- name: CompleteWorkflowTaskAssignment :one
UPDATE workflow_task_assignment
SET completed_at = now(), result_json = $2, is_active = false, updated_at = now()
WHERE id = $1
RETURNING *;

-- name: ClearOtherTaskAssignmentLeads :exec
UPDATE workflow_task_assignment
SET is_lead = false, updated_at = now()
WHERE task_id = $1 AND id != $2 AND is_lead;

-- name: SetTaskAssignmentLead :one
UPDATE workflow_task_assignment
SET is_lead = true, updated_at = now()
WHERE id = $1
RETURNING *;
