-- Reverse of 000006: restore every swapped policy to its original
-- 000004/000005 bare-comparison form, then drop the audit-logging objects.

DROP POLICY tenant_isolation_policy ON outbox_dead_letters;
CREATE POLICY tenant_isolation_policy ON outbox_dead_letters
    USING (tenant_id = app_tenant_id()::text);

DROP POLICY tenant_isolation_policy ON outbox_events;
CREATE POLICY tenant_isolation_policy ON outbox_events
    USING (tenant_id = app_tenant_id()::text);

DROP POLICY tenant_isolation_policy ON assignee_overrides;
CREATE POLICY tenant_isolation_policy ON assignee_overrides
    USING      (tenant_id = app_tenant_id())
    WITH CHECK (tenant_id = app_tenant_id());

DROP POLICY tenant_isolation_policy ON workflow_data_keys;
CREATE POLICY tenant_isolation_policy ON workflow_data_keys
    USING      (tenant_id = app_tenant_id())
    WITH CHECK (tenant_id = app_tenant_id());

DROP POLICY tenant_isolation_policy ON workflow_task_assignment;
CREATE POLICY tenant_isolation_policy ON workflow_task_assignment
    USING      (tenant_id = app_tenant_id())
    WITH CHECK (tenant_id = app_tenant_id());

DROP POLICY tenant_isolation_policy ON workflow_task;
CREATE POLICY tenant_isolation_policy ON workflow_task
    USING      (tenant_id = app_tenant_id())
    WITH CHECK (tenant_id = app_tenant_id());

DROP POLICY tenant_isolation_policy ON workflow_instance;
CREATE POLICY tenant_isolation_policy ON workflow_instance
    USING      (tenant_id = app_tenant_id())
    WITH CHECK (tenant_id = app_tenant_id());

DROP FUNCTION rls_check_tenant(uuid, text);
DROP FUNCTION log_rls_violation(text, uuid, text);
DROP TABLE rls_violation_log;
