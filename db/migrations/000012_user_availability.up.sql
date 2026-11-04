-- The last availability status applied for each user from UserAvailabilityChanged
-- (LLD §4.14), read when a delegation ends so that work handed back to an
-- out-of-office delegator is held. One row per user, keyed by identity; an
-- older change never overwrites a newer one.
CREATE TYPE user_availability_status AS ENUM ('available', 'busy', 'ooo');

CREATE TABLE user_availability (
    tenant_id      UUID                     NOT NULL,
    user_id        UUID                     NOT NULL,
    status         user_availability_status NOT NULL,
    changed_at     TIMESTAMPTZ              NOT NULL,
    record_version BIGINT                   NOT NULL DEFAULT 1 CHECK (record_version > 0),
    created_at     TIMESTAMPTZ              NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ              NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, user_id)
);

REVOKE ALL ON user_availability FROM PUBLIC;
ALTER TABLE user_availability ENABLE ROW LEVEL SECURITY;
ALTER TABLE user_availability FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation_policy ON user_availability
    USING      (rls_check_tenant(tenant_id, 'user_availability'))
    WITH CHECK (rls_check_tenant(tenant_id, 'user_availability'));
