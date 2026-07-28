DROP POLICY IF EXISTS tenant_isolation_policy ON assignee_overrides;
ALTER TABLE assignee_overrides NO FORCE ROW LEVEL SECURITY;
ALTER TABLE assignee_overrides DISABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation_policy ON workflow_data_keys;
ALTER TABLE workflow_data_keys NO FORCE ROW LEVEL SECURITY;
ALTER TABLE workflow_data_keys DISABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation_policy ON workflow_task_assignment;
ALTER TABLE workflow_task_assignment NO FORCE ROW LEVEL SECURITY;
ALTER TABLE workflow_task_assignment DISABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation_policy ON workflow_task;
ALTER TABLE workflow_task NO FORCE ROW LEVEL SECURITY;
ALTER TABLE workflow_task DISABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation_policy ON workflow_instance;
ALTER TABLE workflow_instance NO FORCE ROW LEVEL SECURITY;
ALTER TABLE workflow_instance DISABLE ROW LEVEL SECURITY;
