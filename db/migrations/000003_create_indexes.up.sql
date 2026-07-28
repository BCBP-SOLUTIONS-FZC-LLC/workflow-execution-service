-- workflow_instance -----------------------------------------------------

-- Idempotent-instantiation key. Partial to allow business_key reuse once an
-- instance reaches a terminal state. DEGRADED does NOT count as terminal here —
-- it still needs admin resolution, so its business_key isn't reusable yet (LLD §4.2).
CREATE UNIQUE INDEX uq_workflow_instance_business_key
    ON workflow_instance (tenant_id, business_key)
    WHERE status NOT IN ('COMPLETED', 'TERMINATED', 'FAILED');

CREATE INDEX idx_workflow_instance_tenant_status
    ON workflow_instance (tenant_id, status);

-- The tenant task-queue downgrade guard: never remove a tenant's isolated queue
-- from the registry while any instance is still running/paused/degraded on it.
CREATE INDEX idx_workflow_instance_task_queue_active
    ON workflow_instance (task_queue)
    WHERE status IN ('RUNNING', 'PAUSED', 'DEGRADED');

-- The archive-guard query behind Definition Service's CheckActiveInstances call.
CREATE INDEX idx_workflow_instance_version_active
    ON workflow_instance (workflow_version_id)
    WHERE status IN ('RUNNING', 'PAUSED', 'DEGRADED');

-- workflow_task -----------------------------------------------------------

CREATE INDEX idx_workflow_task_tenant_dept_status
    ON workflow_task (tenant_id, department_id, status);

CREATE INDEX idx_workflow_task_instance_status
    ON workflow_task (workflow_instance_id, status);

-- Keyset-pagination-friendly (created_at DESC, id DESC tiebreaker) — no
-- OFFSET-oriented index anywhere in this schema.
CREATE INDEX idx_workflow_task_tenant_keyset
    ON workflow_task (tenant_id, created_at DESC, id DESC);

-- workflow_task_assignment --------------------------------------------------

-- Correctness constraint (also serves as a performance index): prevents two
-- active rows for the same (task_id, user_id) pair — also the backstop against
-- duplicate assignment under redelivered delegation events (LLD §6).
CREATE UNIQUE INDEX uq_workflow_task_assignment_active
    ON workflow_task_assignment (task_id, user_id) WHERE is_active;

CREATE INDEX idx_workflow_task_assignment_task_active
    ON workflow_task_assignment (task_id) WHERE is_active;

-- "Show me my active tasks" — the single most common dashboard query.
CREATE INDEX idx_workflow_task_assignment_user_active
    ON workflow_task_assignment (tenant_id, user_id) WHERE is_active;

-- assignee_overrides ---------------------------------------------------------

CREATE INDEX idx_assignee_overrides_instance_node
    ON assignee_overrides (workflow_instance_id, node_key);

-- processed_event -------------------------------------------------------------

CREATE INDEX idx_processed_event_processed_at
    ON processed_event (processed_at);

-- outbox_events (platform-events-owned table; these two indexes are this
-- service's own addition on top of it, giving the merged audit trail its query
-- access, LLD §4.5/§4.9/§4.10) --------------------------------------------

CREATE INDEX idx_outbox_events_instance_created
    ON outbox_events (tenant_id, (payload->>'workflow_instance_id'), created_at DESC, id DESC);

CREATE INDEX idx_outbox_events_task
    ON outbox_events ((payload->>'task_id')) WHERE payload->>'task_id' IS NOT NULL;
