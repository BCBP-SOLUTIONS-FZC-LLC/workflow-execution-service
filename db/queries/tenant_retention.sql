-- name: RecordTenantOffboarded :exec
INSERT INTO tenant_retention (tenant_id, offboarded_at, history_purge_after, updated_at)
VALUES ($1, $2, $3, now())
ON CONFLICT (tenant_id) DO NOTHING;

-- name: ListTenantsDueForPurge :many
SELECT tenant_id FROM tenant_retention
WHERE purged_at IS NULL AND history_purge_after <= $1
ORDER BY history_purge_after, tenant_id
LIMIT $2;

-- name: MarkTenantPurged :exec
UPDATE tenant_retention SET purged_at = $2, updated_at = now() WHERE tenant_id = $1;

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

-- name: PurgeTenantQueues :execrows
DELETE FROM active_task_queues WHERE tenant_id = $1;

-- name: PurgeTenantRecencyGuards :execrows
DELETE FROM event_recency_guard
WHERE scope_key IN (
    SELECT g.scope_key FROM event_recency_guard g
    WHERE g.scope_key = $1 OR g.scope_key LIKE $2
    LIMIT $3
);
