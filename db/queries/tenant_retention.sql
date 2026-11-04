-- name: RecordTenantOffboarded :exec
INSERT INTO tenant_retention (tenant_id, offboarded_at, history_purge_after, next_attempt_at)
VALUES ($1, $2, $3, $2)
ON CONFLICT (tenant_id) DO UPDATE SET
    offboarded_at = EXCLUDED.offboarded_at,
    history_purge_after = EXCLUDED.history_purge_after,
    purged_at = NULL,
    returned_at = NULL,
    attempts = 0,
    next_attempt_at = EXCLUDED.next_attempt_at,
    record_version = tenant_retention.record_version + 1,
    updated_at = now()
WHERE tenant_retention.returned_at IS NOT NULL
  AND tenant_retention.returned_at < EXCLUDED.offboarded_at;

-- name: RecordTenantReturned :exec
INSERT INTO tenant_retention (tenant_id, returned_at, next_attempt_at)
VALUES ($1, $2, $2)
ON CONFLICT (tenant_id) DO UPDATE SET
    returned_at = EXCLUDED.returned_at,
    record_version = tenant_retention.record_version + 1,
    updated_at = now()
WHERE (tenant_retention.offboarded_at IS NULL OR tenant_retention.offboarded_at < EXCLUDED.returned_at)
  AND (tenant_retention.returned_at IS NULL OR tenant_retention.returned_at < EXCLUDED.returned_at);

-- name: LockCurrentTenantRetention :one
SELECT record_version FROM tenant_retention
WHERE tenant_id = $1 AND record_version = $2 AND returned_at IS NULL
FOR SHARE;

-- name: ListTenantsDueForPurge :many
SELECT tenant_id, attempts, record_version FROM tenant_retention
WHERE purged_at IS NULL AND returned_at IS NULL
  AND history_purge_after <= $1 AND next_attempt_at <= $1
ORDER BY history_purge_after, tenant_id
LIMIT $2;

-- name: MarkTenantPurged :one
UPDATE tenant_retention
SET purged_at = $3, attempts = 0, record_version = record_version + 1, updated_at = now()
WHERE tenant_id = $1 AND record_version = $2
RETURNING record_version;

-- name: RecordTenantPurgeFailure :one
UPDATE tenant_retention
SET attempts = $3, next_attempt_at = $4, record_version = record_version + 1, updated_at = now()
WHERE tenant_id = $1 AND record_version = $2
RETURNING record_version;

-- name: PurgeTenantAssigneeOverrides :execrows
DELETE FROM assignee_overrides
WHERE id IN (SELECT o.id FROM assignee_overrides o WHERE o.tenant_id = $1 LIMIT $2);

-- name: PurgeTenantTaskAssignments :execrows
DELETE FROM workflow_task_assignment
WHERE id IN (SELECT a.id FROM workflow_task_assignment a WHERE a.tenant_id = $1 LIMIT $2);

-- name: UnlinkTenantDeferredTasks :execrows
UPDATE workflow_task SET deferred_from_task_id = NULL
WHERE id IN (SELECT t.id FROM workflow_task t WHERE t.tenant_id = $1 AND t.deferred_from_task_id IS NOT NULL LIMIT $2);

-- name: PurgeTenantTasks :execrows
DELETE FROM workflow_task
WHERE id IN (SELECT t.id FROM workflow_task t WHERE t.tenant_id = $1 LIMIT $2);

-- name: PurgeTenantInstances :execrows
DELETE FROM workflow_instance
WHERE id IN (SELECT i.id FROM workflow_instance i WHERE i.tenant_id = $1 LIMIT $2);

-- name: PurgeTenantConnectorDocuments :execrows
DELETE FROM connector_documents
WHERE id IN (SELECT d.id FROM connector_documents d WHERE d.tenant_id = $1 LIMIT $2);

-- name: PurgeTenantConnectorDocumentAttempts :execrows
DELETE FROM connector_document_attempts
WHERE id IN (SELECT d.id FROM connector_document_attempts d WHERE d.tenant_id = $1 LIMIT $2);

-- name: PurgeTenantConnectorSendIntents :execrows
DELETE FROM connector_send_intents
WHERE id IN (SELECT s.id FROM connector_send_intents s WHERE s.tenant_id = $1 LIMIT $2);

-- name: PurgeTenantUserAvailability :execrows
DELETE FROM user_availability
WHERE (tenant_id, user_id) IN (SELECT u.tenant_id, u.user_id FROM user_availability u WHERE u.tenant_id = $1 LIMIT $2);

-- name: PurgeTenantQueues :execrows
DELETE FROM active_task_queues WHERE tenant_id = $1;

-- name: PurgeTenantRecencyGuards :execrows
DELETE FROM event_recency_guard
WHERE scope_key IN (
    SELECT g.scope_key FROM event_recency_guard g
    WHERE g.scope_key = $1 OR g.scope_key LIKE $2
    LIMIT $3
);

-- name: TenantHasRows :one
SELECT (
    EXISTS (SELECT 1 FROM assignee_overrides o WHERE o.tenant_id = @tenant_id::uuid)
    OR EXISTS (SELECT 1 FROM workflow_task_assignment a WHERE a.tenant_id = @tenant_id::uuid)
    OR EXISTS (SELECT 1 FROM workflow_task t WHERE t.tenant_id = @tenant_id::uuid)
    OR EXISTS (SELECT 1 FROM workflow_instance i WHERE i.tenant_id = @tenant_id::uuid)
    OR EXISTS (SELECT 1 FROM connector_documents d WHERE d.tenant_id = @tenant_text::text)
    OR EXISTS (SELECT 1 FROM connector_document_attempts da WHERE da.tenant_id = @tenant_text::text)
    OR EXISTS (SELECT 1 FROM connector_send_intents s WHERE s.tenant_id = @tenant_text::text)
    OR EXISTS (SELECT 1 FROM user_availability u WHERE u.tenant_id = @tenant_id::uuid)
    OR EXISTS (SELECT 1 FROM active_task_queues q WHERE q.tenant_id = @tenant_id::uuid)
    OR EXISTS (SELECT 1 FROM event_recency_guard g WHERE g.scope_key = @tenant_scope::text OR g.scope_key LIKE @user_scope::text)
)::boolean AS has_rows;
