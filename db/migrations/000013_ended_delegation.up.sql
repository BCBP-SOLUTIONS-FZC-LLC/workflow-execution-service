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
