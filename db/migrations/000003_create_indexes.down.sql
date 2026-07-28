DROP INDEX IF EXISTS idx_outbox_events_task;
DROP INDEX IF EXISTS idx_outbox_events_instance_created;

DROP INDEX IF EXISTS idx_processed_event_processed_at;

DROP INDEX IF EXISTS idx_assignee_overrides_instance_node;

DROP INDEX IF EXISTS idx_workflow_task_assignment_user_active;
DROP INDEX IF EXISTS idx_workflow_task_assignment_task_active;
DROP INDEX IF EXISTS uq_workflow_task_assignment_active;

DROP INDEX IF EXISTS idx_workflow_task_tenant_keyset;
DROP INDEX IF EXISTS idx_workflow_task_instance_status;
DROP INDEX IF EXISTS idx_workflow_task_tenant_dept_status;

DROP INDEX IF EXISTS idx_workflow_instance_version_active;
DROP INDEX IF EXISTS idx_workflow_instance_task_queue_active;
DROP INDEX IF EXISTS idx_workflow_instance_tenant_status;
DROP INDEX IF EXISTS uq_workflow_instance_business_key;
