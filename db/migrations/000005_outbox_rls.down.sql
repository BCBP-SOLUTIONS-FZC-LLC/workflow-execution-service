DROP POLICY IF EXISTS tenant_isolation_policy ON outbox_dead_letters;
ALTER TABLE outbox_dead_letters DISABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS tenant_isolation_policy ON outbox_events;
ALTER TABLE outbox_events DISABLE ROW LEVEL SECURITY;
