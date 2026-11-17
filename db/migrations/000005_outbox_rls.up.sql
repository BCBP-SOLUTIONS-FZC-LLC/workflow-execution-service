-- The outbox_events / outbox_dead_letters tables are created by
-- platform-events outbox.ApplySchema (run before these domain migrations).
-- The RLS policies are a service-specific decision and live here, using the
-- centralized app_tenant_id() function like every other policy in this schema.
-- platform-events' tenant_id column is TEXT (not uuid), so app_tenant_id()'s
-- uuid return value is cast to text here — every other policy in this schema
-- compares uuid = uuid directly and needs no such cast.
--
-- FORCE ROW LEVEL SECURITY, as on every other tenant table: the role that owns
-- these tables (the application role when migrations run under it) does not
-- bypass the policy. The outbox relay reads across tenants under its own
-- BYPASSRLS role.
ALTER TABLE outbox_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE outbox_events FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation_policy ON outbox_events
    USING (tenant_id = app_tenant_id()::text);

ALTER TABLE outbox_dead_letters ENABLE ROW LEVEL SECURITY;
ALTER TABLE outbox_dead_letters FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation_policy ON outbox_dead_letters
    USING (tenant_id = app_tenant_id()::text);
