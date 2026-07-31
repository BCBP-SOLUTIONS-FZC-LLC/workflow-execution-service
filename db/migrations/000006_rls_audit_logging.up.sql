-- RLS-violation audit logging, additive to T1.2's ticket scope — ported from
-- iam-user-profile's rls_violation_log/log_rls_violation/rls_check_tenant
-- pattern (LLD §9.5).

CREATE TABLE rls_violation_log (
    id                bigserial PRIMARY KEY,
    table_name        text NOT NULL,
    row_tenant_id     uuid,
    app_tenant_id     uuid,
    violation_type    text NOT NULL,
    user_id           uuid,
    session_role      text DEFAULT SESSION_USER,
    client_addr       inet DEFAULT inet_client_addr(),
    application_name  text DEFAULT current_setting('application_name', true),
    query_text        text,
    occurred_at       timestamptz NOT NULL DEFAULT now()
);

-- RLS disabled: log_rls_violation fires from inside another table's RLS
-- check, so an INSERT here under RLS would recurse into its own policy.
ALTER TABLE rls_violation_log DISABLE ROW LEVEL SECURITY;
ALTER TABLE rls_violation_log NO FORCE ROW LEVEL SECURITY;

CREATE OR REPLACE FUNCTION log_rls_violation(
    p_table_name     text,
    p_row_tenant_id  uuid,
    p_violation_type text
) RETURNS void
    LANGUAGE plpgsql SECURITY DEFINER
    SET search_path = public AS $$
BEGIN
    IF random() > 0.01 THEN RETURN; END IF;
    INSERT INTO rls_violation_log (
        table_name, row_tenant_id, app_tenant_id, violation_type, query_text
    ) VALUES (
        p_table_name, p_row_tenant_id, app_tenant_id(), p_violation_type, current_query()
    );
EXCEPTION WHEN OTHERS THEN
    NULL;
END;
$$;

CREATE OR REPLACE FUNCTION rls_check_tenant(
    p_tenant_id  uuid,
    p_table_name text
) RETURNS boolean
    LANGUAGE plpgsql STABLE STRICT SECURITY DEFINER
    SET search_path = public AS $$
DECLARE
    v_app uuid;
BEGIN
    v_app := app_tenant_id();
    IF v_app IS NULL THEN
        PERFORM log_rls_violation(p_table_name, p_tenant_id, 'missing_or_invalid_guc');
        RETURN false;
    END IF;
    IF p_tenant_id <> v_app THEN
        PERFORM log_rls_violation(p_table_name, p_tenant_id, 'cross_tenant_access');
        RETURN false;
    END IF;
    RETURN true;
END;
$$;

DROP POLICY tenant_isolation_policy ON workflow_instance;
CREATE POLICY tenant_isolation_policy ON workflow_instance
    USING      (rls_check_tenant(tenant_id, 'workflow_instance'))
    WITH CHECK (rls_check_tenant(tenant_id, 'workflow_instance'));

DROP POLICY tenant_isolation_policy ON workflow_task;
CREATE POLICY tenant_isolation_policy ON workflow_task
    USING      (rls_check_tenant(tenant_id, 'workflow_task'))
    WITH CHECK (rls_check_tenant(tenant_id, 'workflow_task'));

DROP POLICY tenant_isolation_policy ON workflow_task_assignment;
CREATE POLICY tenant_isolation_policy ON workflow_task_assignment
    USING      (rls_check_tenant(tenant_id, 'workflow_task_assignment'))
    WITH CHECK (rls_check_tenant(tenant_id, 'workflow_task_assignment'));

DROP POLICY tenant_isolation_policy ON workflow_data_keys;
CREATE POLICY tenant_isolation_policy ON workflow_data_keys
    USING      (rls_check_tenant(tenant_id, 'workflow_data_keys'))
    WITH CHECK (rls_check_tenant(tenant_id, 'workflow_data_keys'));

DROP POLICY tenant_isolation_policy ON assignee_overrides;
CREATE POLICY tenant_isolation_policy ON assignee_overrides
    USING      (rls_check_tenant(tenant_id, 'assignee_overrides'))
    WITH CHECK (rls_check_tenant(tenant_id, 'assignee_overrides'));

-- tenant_id is text here (platform-events-owned), so the row value casts up
-- to uuid — opposite direction from 000005's app_tenant_id()::text. USING
-- only, no WITH CHECK/FORCE, matching 000005's existing shape.
DROP POLICY tenant_isolation_policy ON outbox_events;
CREATE POLICY tenant_isolation_policy ON outbox_events
    USING (rls_check_tenant(tenant_id::uuid, 'outbox_events'));

DROP POLICY tenant_isolation_policy ON outbox_dead_letters;
CREATE POLICY tenant_isolation_policy ON outbox_dead_letters
    USING (rls_check_tenant(tenant_id::uuid, 'outbox_dead_letters'));
