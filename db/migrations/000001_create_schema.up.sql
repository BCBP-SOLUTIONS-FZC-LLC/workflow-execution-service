CREATE TYPE workflow_instance_status AS ENUM (
    'RUNNING', 'PAUSED', 'COMPLETED', 'TERMINATED', 'FAILED', 'DEGRADED'
);

CREATE TYPE workflow_task_status AS ENUM (
    'READY', 'IN_PROGRESS', 'COMPLETED', 'DEFERRED', 'FAILED', 'SUPERSEDED'
);

-- Centralized RLS tenant-check function , SECURITY DEFINER (invoker cannot
-- poison the search path), fail-closed: returns NULL rather than raising on a
-- missing/empty GUC or a malformed UUID, so every policy's tenant_id = NULL
-- comparison is always false under SQL three-valued logic.
CREATE OR REPLACE FUNCTION app_tenant_id() RETURNS uuid
    LANGUAGE plpgsql STABLE SECURITY DEFINER AS $$
DECLARE v text;
BEGIN
    v := current_setting('app.tenant_id', true);
    IF v IS NULL OR v = '' THEN RETURN NULL; END IF;
    RETURN v::uuid;
EXCEPTION WHEN OTHERS THEN RETURN NULL;
END;
$$;
