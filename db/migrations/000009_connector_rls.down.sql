DROP POLICY IF EXISTS tenant_isolation_policy ON connector_documents;
ALTER TABLE connector_documents NO FORCE ROW LEVEL SECURITY;
ALTER TABLE connector_documents DISABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation_policy ON connector_document_attempts;
ALTER TABLE connector_document_attempts NO FORCE ROW LEVEL SECURITY;
ALTER TABLE connector_document_attempts DISABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation_policy ON connector_send_intents;
ALTER TABLE connector_send_intents NO FORCE ROW LEVEL SECURITY;
ALTER TABLE connector_send_intents DISABLE ROW LEVEL SECURITY;
