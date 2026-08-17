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

-- name: ListWorkflowTasksByTenant :many
-- Backs TaskService.List's tenant-wide, filterable query (GET /tasks) —
-- every filter is optional (NULL = unfiltered); uses idx_workflow_task_tenant_keyset
-- for the base scan.
SELECT t.* FROM workflow_task t
WHERE t.tenant_id = $1
  AND (sqlc.narg('status')::workflow_task_status IS NULL OR t.status = sqlc.narg('status')::workflow_task_status)
  AND (sqlc.narg('workflow_instance_id')::uuid IS NULL OR t.workflow_instance_id = sqlc.narg('workflow_instance_id')::uuid)
  AND (sqlc.narg('department_id')::uuid IS NULL OR t.department_id = sqlc.narg('department_id')::uuid)
  AND (sqlc.narg('due_before')::timestamptz IS NULL OR t.due_at < sqlc.narg('due_before')::timestamptz)
  AND (
    sqlc.narg('assignee_user_id')::uuid IS NULL
    OR EXISTS (
      SELECT 1 FROM workflow_task_assignment a
      WHERE a.task_id = t.id AND a.user_id = sqlc.narg('assignee_user_id')::uuid AND a.is_active
    )
  )
  AND (
    sqlc.narg('cursor_created_at')::timestamptz IS NULL
    OR (t.created_at, t.id) < (sqlc.narg('cursor_created_at')::timestamptz, sqlc.narg('cursor_id')::uuid)
  )
ORDER BY t.created_at DESC, t.id DESC
LIMIT $2;

-- name: GetWorkflowTaskByInstanceAndNode :one
-- Returns the current (most recently created) task at this node — a node
-- revisited via force-back/an exclusive-gateway back-edge can have more than
-- one workflow_task row over an instance's lifetime (VisitCount-derived
-- task IDs, LLD's deterministic-task-ID convention), so this is always the
-- latest, not necessarily the only, row for (instance, node_key).
SELECT * FROM workflow_task
WHERE workflow_instance_id = $1 AND node_key = $2
ORDER BY created_at DESC
LIMIT 1;

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
-- completed_at is set whenever the task leaves the active (READY/IN_PROGRESS)
-- state for any terminal status — COMPLETED, DEFERRED, FAILED, or SUPERSEDED
-- all mean the task is no longer actionable, matching how the instance-level
-- completed_at covers TERMINATED/FAILED alongside COMPLETED.
UPDATE workflow_task
SET status = $2,
    completed_at = CASE WHEN $2::workflow_task_status IN ('COMPLETED', 'DEFERRED', 'FAILED', 'SUPERSEDED')
                       THEN now() ELSE completed_at END,
    updated_at = now(), record_version = record_version + 1
WHERE id = $1 AND record_version = $3
RETURNING *;

-- name: BumpWorkflowTaskRecordVersion :one
-- Optimistic-concurrency guard for TaskAssignmentRepository.Complete/SetLead:
-- the LLD frames the task, not the assignment (which carries no
-- record_version of its own), as claim/complete's contested resource.
UPDATE workflow_task
SET updated_at = now(), record_version = record_version + 1
WHERE id = $1 AND record_version = $2
RETURNING *;
