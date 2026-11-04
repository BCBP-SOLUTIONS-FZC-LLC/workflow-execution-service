-- The connector tables are created by workflow-connectors' ApplySchema (run before
-- these domain migrations). Their tenant_id is text, so the policy casts it to uuid.

REVOKE ALL ON connector_documents FROM PUBLIC;
ALTER TABLE connector_documents ENABLE ROW LEVEL SECURITY;
ALTER TABLE connector_documents FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation_policy ON connector_documents
    USING      (rls_check_tenant(tenant_id::uuid, 'connector_documents'))
    WITH CHECK (rls_check_tenant(tenant_id::uuid, 'connector_documents'));

REVOKE ALL ON connector_document_attempts FROM PUBLIC;
ALTER TABLE connector_document_attempts ENABLE ROW LEVEL SECURITY;
ALTER TABLE connector_document_attempts FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation_policy ON connector_document_attempts
    USING      (rls_check_tenant(tenant_id::uuid, 'connector_document_attempts'))
    WITH CHECK (rls_check_tenant(tenant_id::uuid, 'connector_document_attempts'));

REVOKE ALL ON connector_send_intents FROM PUBLIC;
ALTER TABLE connector_send_intents ENABLE ROW LEVEL SECURITY;
ALTER TABLE connector_send_intents FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation_policy ON connector_send_intents
    USING      (rls_check_tenant(tenant_id::uuid, 'connector_send_intents'))
    WITH CHECK (rls_check_tenant(tenant_id::uuid, 'connector_send_intents'));
