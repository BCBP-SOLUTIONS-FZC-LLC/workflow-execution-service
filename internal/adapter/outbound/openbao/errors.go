// Package openbao is the KV-v2 READ counterpart to definition_service's
// WRITE-only OpenBao client (definition_service/internal/adapter/outbound/http/secrets_client.go).
// cmd/connector-worker uses it to resolve a connector-task's secret_ref
// fields to their real credential value immediately before calling
// workflow-connectors' Connector.Execute — the raw value never touches
// context_json, Temporal history, or the connector-task-created event/Stream
// payload (LLD workflow_connectors.md §6.2, Decision #5).
package openbao

import "errors"

var (
	// ErrUnauthorized mirrors definition_service's own sentinel for a 403.
	ErrUnauthorized = errors.New("openbao: unauthorized")

	// ErrSecretNotFound classifies a missing credential as an author-time
	// misconfiguration, not an infra blip — the caller (cmd/connector-worker's
	// dispatch pool) should treat this as an unsafe-to-retry dispatch
	// failure, distinct from ErrUpstreamUnavailable.
	ErrSecretNotFound = errors.New("openbao: secret not found")

	// ErrUpstreamUnavailable covers a transport failure, a decode failure,
	// or any status this package doesn't classify more narrowly.
	ErrUpstreamUnavailable = errors.New("openbao: upstream unavailable")

	// ErrInvalidPath rejects a path/fieldName that doesn't match
	// definition_service's own write-side path convention before any HTTP
	// call is attempted — see reader.go's validSecretPath/validFieldName
	// doc comment for why this guard is non-negotiable.
	ErrInvalidPath = errors.New("openbao: invalid secret path")
)
