-- name: GetWorkflowTaskAssignment :one
SELECT * FROM workflow_task_assignment WHERE id = $1;

-- name: ListActiveAssignmentsByTask :many
SELECT * FROM workflow_task_assignment
WHERE task_id = $1 AND is_active;

-- name: ListActiveAssignmentsByUser :many
SELECT * FROM workflow_task_assignment
WHERE tenant_id = $1 AND user_id = $2 AND is_active;

-- name: ListActiveTasksByUser :many
-- Backs TaskService.ActiveByUser -- workflow_task_assignment has no created_at
-- column of its own (assigned_at is nullable) so the keyset orders on the
-- joined task's created_at/id instead, the same (created_at DESC, id DESC)
-- convention every other list query in this schema uses.
SELECT t.id AS task_id, t.workflow_instance_id, t.node_key, a.user_id, t.department_id, t.status, t.record_version,
       t.created_at
FROM workflow_task_assignment a
JOIN workflow_task t ON t.id = a.task_id
WHERE a.tenant_id = $1 AND a.user_id = $2 AND a.is_active
  AND (
    sqlc.narg('cursor_created_at')::timestamptz IS NULL
    OR (t.created_at, t.id) < (sqlc.narg('cursor_created_at')::timestamptz, sqlc.narg('cursor_id')::uuid)
  )
ORDER BY t.created_at DESC, t.id DESC
LIMIT $3;

-- name: VacateAllActiveAssignmentsByUser :many
-- Backs UserSafetyNetReconciler.VacateAssignments -- per-assignment, tenant-
-- wide, no scope filter (LLD §6.2 item 3): every active assignment for a
-- deleted user is vacated, regardless of instance/department/delegation tag.
UPDATE workflow_task_assignment
SET is_active = false, vacated_at = now(), updated_at = now()
WHERE tenant_id = $1 AND user_id = $2 AND is_active
RETURNING *;

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
