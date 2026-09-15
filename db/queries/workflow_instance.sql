-- name: GetWorkflowInstance :one
SELECT * FROM workflow_instance WHERE id = $1;

-- name: ListWorkflowInstancesByTenant :many
-- Every filter is optional (NULL = unfiltered). enforce_scope=false is the
-- admin/unscoped path; when true, an instance is visible only if it has at
-- least one task whose department is in scope_department_ids or the caller
-- has an active assignment on it. Instances carry no department_id of their
-- own, so scope is a join through workflow_task.
SELECT wi.* FROM workflow_instance wi
WHERE wi.tenant_id = $1
  AND (sqlc.narg('status')::workflow_instance_status IS NULL OR wi.status = sqlc.narg('status')::workflow_instance_status)
  -- statuses is the set form of status, for sweeps that must select every
  -- non-terminal instance in ONE query: splitting that into a query per
  -- status lets an instance changing status between two of those queries
  -- fall through both. Empty array = unfiltered.
  --
  -- text[] rather than workflow_instance_status[]: pgx has no encode plan
  -- for an array of a custom enum unless that enum's OID is registered on
  -- the connection, and the driver reports it only at query time, as
  -- "unable to encode ... unknown type (OID ...)".
  AND (
    cardinality(sqlc.arg('statuses')::text[]) = 0
    OR wi.status::text = ANY(sqlc.arg('statuses')::text[])
  )
  AND (sqlc.narg('workflow_version_id')::uuid IS NULL OR wi.workflow_version_id = sqlc.narg('workflow_version_id')::uuid)
  AND (sqlc.narg('started_after')::timestamptz IS NULL OR wi.started_at > sqlc.narg('started_after')::timestamptz)
  AND (sqlc.narg('started_before')::timestamptz IS NULL OR wi.started_at < sqlc.narg('started_before')::timestamptz)
  AND (
    NOT sqlc.arg('enforce_scope')::bool
    OR EXISTS (
      SELECT 1 FROM workflow_task t
      WHERE t.workflow_instance_id = wi.id
        AND (
          t.department_id = ANY(sqlc.arg('scope_department_ids')::uuid[])
          OR EXISTS (
            SELECT 1 FROM workflow_task_assignment a
            WHERE a.task_id = t.id AND a.user_id = sqlc.arg('scope_caller_user_id')::uuid AND a.is_active
          )
        )
    )
  )
  AND (
    sqlc.narg('cursor_created_at')::timestamptz IS NULL
    OR (wi.created_at, wi.id) < (sqlc.narg('cursor_created_at')::timestamptz, sqlc.narg('cursor_id')::uuid)
  )
ORDER BY wi.created_at DESC, wi.id DESC
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
