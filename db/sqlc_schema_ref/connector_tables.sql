-- sqlc schema-awareness only, NOT a migration — these tables are created by
-- workflow-connectors' sqlstore.ApplySchema (documents and send intents).
-- golang-migrate never reads this directory; the file exists only so sqlc can
-- type-check the tenant purge queries.
CREATE TABLE IF NOT EXISTS connector_documents (
    id         uuid        PRIMARY KEY,
    tenant_id  text        NOT NULL
);

CREATE TABLE IF NOT EXISTS connector_document_attempts (
    id         bigserial   PRIMARY KEY,
    tenant_id  text        NOT NULL
);

CREATE TABLE IF NOT EXISTS connector_send_intents (
    id         uuid        PRIMARY KEY,
    tenant_id  text        NOT NULL
);
