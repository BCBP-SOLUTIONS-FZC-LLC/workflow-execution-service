package openbao

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"
)

// validSecretPath matches definition_service's own write-side path
// convention exactly: connectors/{tenantID}/{connectorType}/{fieldName}
// (definition_service/internal/core/service/connector_service.go's
// WriteCredential). This package can't import that service's own
// validConnectorSegment regex directly — execution_service can't depend on
// a sibling service's internal packages — so it's duplicated here,
// non-negotiably: a path arrives inside a connector-task-created event
// payload, untrusted input, not a value this package can assume is already
// safe, given the 2026-08-14 path-traversal vulnerability fixed on the
// write side.
var (
	validSecretPath = regexp.MustCompile(`^connectors/[a-zA-Z0-9_-]+/[a-zA-Z0-9_-]+/[a-zA-Z0-9_-]+$`)
	validFieldName  = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)
)

// Reader is the KV-v2 read client. addr/token/mount mirror
// definition_service's SecretsClient constructor exactly.
type Reader struct {
	addr, token, mount string
	client             *http.Client
}

func NewReader(addr, token, mount string, timeout time.Duration) *Reader {
	return &Reader{addr: addr, token: token, mount: mount, client: &http.Client{Timeout: timeout}}
}

type openBaoReadResponse struct {
	Data struct {
		Data map[string]string `json:"data"`
	} `json:"data"`
}

// Read GETs {addr}/v1/{mount}/data/{path} and returns data.data[fieldName] —
// the KV-v2 response envelope's inner data map, keyed by the same fieldName
// WriteCredential used as both the map key and the path's own last segment.
func (r *Reader) Read(ctx context.Context, path, fieldName string) (string, error) {
	if !validSecretPath.MatchString(path) {
		return "", fmt.Errorf("%w: %q", ErrInvalidPath, path)
	}
	if !validFieldName.MatchString(fieldName) {
		return "", fmt.Errorf("%w: field name %q", ErrInvalidPath, fieldName)
	}

	url := fmt.Sprintf("%s/v1/%s/data/%s", strings.TrimRight(r.addr, "/"), r.mount, strings.TrimLeft(path, "/"))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", fmt.Errorf("build openbao read request: %w", err)
	}
	req.Header.Set("X-Vault-Token", r.token)

	resp, err := r.client.Do(req)
	if err != nil {
		return "", fmt.Errorf("%w: openbao read %q: %s", ErrUpstreamUnavailable, path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusOK:
		return r.extractField(resp.Body, path, fieldName)
	case http.StatusNotFound:
		return "", fmt.Errorf("%w: %q", ErrSecretNotFound, path)
	case http.StatusForbidden:
		return "", fmt.Errorf("%w: openbao read %q", ErrUnauthorized, path)
	default:
		respBody, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("%w: openbao read %q: status %d: %s", ErrUpstreamUnavailable, path, resp.StatusCode, string(respBody))
	}
}

func (r *Reader) extractField(body io.Reader, path, fieldName string) (string, error) {
	var parsed openBaoReadResponse
	if err := json.NewDecoder(body).Decode(&parsed); err != nil {
		return "", fmt.Errorf("%w: openbao read %q: decode response: %s", ErrUpstreamUnavailable, path, err)
	}
	value, ok := parsed.Data.Data[fieldName]
	if !ok {
		return "", fmt.Errorf("%w: %q has no field %q", ErrSecretNotFound, path, fieldName)
	}
	return value, nil
}
