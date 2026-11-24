-- The delegations that have ended, remembered by id (LLD §6.2) so that a
-- DelegationStarted delivered after its DelegationEnded moves nothing. One row
-- per delegation, written when the end is applied.
CREATE TABLE ended_delegation (
    tenant_id     UUID        NOT NULL,
    delegation_id UUID        NOT NULL,
    ended_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, delegation_id)
);

REVOKE ALL ON ended_delegation FROM PUBLIC;
ALTER TABLE ended_delegation ENABLE ROW LEVEL SECURITY;
ALTER TABLE ended_delegation FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation_policy ON ended_delegation
    USING      (rls_check_tenant(tenant_id, 'ended_delegation'))
    WITH CHECK (rls_check_tenant(tenant_id, 'ended_delegation'));

-- The time of the last cancel of every delegation to a delegate (the cancel
-- that names no delegation), so a start of a delegation that began before it
-- moves nothing. One row per delegate; a later cancel moves the time forward.
CREATE TABLE ended_delegate (
    tenant_id        UUID        NOT NULL,
    delegate_user_id UUID        NOT NULL,
    ended_at         TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, delegate_user_id)
);

REVOKE ALL ON ended_delegate FROM PUBLIC;
ALTER TABLE ended_delegate ENABLE ROW LEVEL SECURITY;
ALTER TABLE ended_delegate FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation_policy ON ended_delegate
    USING      (rls_check_tenant(tenant_id, 'ended_delegate'))
    WITH CHECK (rls_check_tenant(tenant_id, 'ended_delegate'));
