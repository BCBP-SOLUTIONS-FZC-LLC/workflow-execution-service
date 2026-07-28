-- Row-Level Security. Every policy uses the centralized app_tenant_id()
-- function (000001_create_schema) rather than inlining
-- current_setting('app.tenant_id')::uuid per-policy — see .claude/CLAUDE.local.md
-- for why this deviates from definition_service's own migrations.
--
-- FORCE ROW LEVEL SECURITY: prevents the table owner from bypassing RLS.
-- REVOKE ALL FROM PUBLIC: strips default public access.

REVOKE ALL ON workflow_instance FROM PUBLIC;
ALTER TABLE workflow_instance ENABLE ROW LEVEL SECURITY;
ALTER TABLE workflow_instance FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation_policy ON workflow_instance
    USING      (tenant_id = app_tenant_id())
    WITH CHECK (tenant_id = app_tenant_id());

REVOKE ALL ON workflow_task FROM PUBLIC;
ALTER TABLE workflow_task ENABLE ROW LEVEL SECURITY;
ALTER TABLE workflow_task FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation_policy ON workflow_task
    USING      (tenant_id = app_tenant_id())
    WITH CHECK (tenant_id = app_tenant_id());

REVOKE ALL ON workflow_task_assignment FROM PUBLIC;
ALTER TABLE workflow_task_assignment ENABLE ROW LEVEL SECURITY;
ALTER TABLE workflow_task_assignment FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation_policy ON workflow_task_assignment
    USING      (tenant_id = app_tenant_id())
    WITH CHECK (tenant_id = app_tenant_id());

REVOKE ALL ON workflow_data_keys FROM PUBLIC;
ALTER TABLE workflow_data_keys ENABLE ROW LEVEL SECURITY;
ALTER TABLE workflow_data_keys FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation_policy ON workflow_data_keys
    USING      (tenant_id = app_tenant_id())
    WITH CHECK (tenant_id = app_tenant_id());

REVOKE ALL ON assignee_overrides FROM PUBLIC;
ALTER TABLE assignee_overrides ENABLE ROW LEVEL SECURITY;
ALTER TABLE assignee_overrides FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation_policy ON assignee_overrides
    USING      (tenant_id = app_tenant_id())
    WITH CHECK (tenant_id = app_tenant_id());

-- NO RLS on active_task_queues, despite the tenant_id column (LLD §4.6, §4.8).
-- Workers need to read every currently-active queue across every tenant in one
-- query to compute their own registration set — forcing that through a
-- per-tenant GUC context switch would be backwards for what is fundamentally an
-- operational/infra table, not tenant business data.

-- NO RLS on processed_event, and no tenant_id column at all (LLD §4.7, §4.8):
-- infrastructure dedup state keyed by a globally-unique envelope ID, not tenant
-- business data. Matches Definition Service's own processed_event table exactly.
