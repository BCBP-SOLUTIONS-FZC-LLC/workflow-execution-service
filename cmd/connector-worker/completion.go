package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	adapterhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"

	"github.com/google/uuid"
)

type completionClient struct {
	addr string
	http *http.Client
}

func newCompletionClient(addr string, timeout time.Duration) *completionClient {
	return &completionClient{
		addr: strings.TrimRight(addr, "/"),
		http: adapterhttp.NewClient(timeout),
	}
}

type completeRequest struct {
	TenantID uuid.UUID      `json:"tenant_id"`
	Output   map[string]any `json:"output"`
}

type failRequest struct {
	TenantID   uuid.UUID      `json:"tenant_id"`
	ErrorClass string         `json:"error_class"`
	Output     map[string]any `json:"output,omitempty"`
}

func (c *completionClient) Complete(ctx context.Context, tenantID, taskID uuid.UUID, output map[string]any) error {
	body, err := json.Marshal(completeRequest{TenantID: tenantID, Output: output})
	if err != nil {
		return fmt.Errorf("encode complete request: %w", err)
	}
	return c.post(ctx, tenantID, fmt.Sprintf("/api/v1/internal/connector-tasks/%s/complete", taskID), body)
}

func (c *completionClient) Fail(ctx context.Context, tenantID, taskID uuid.UUID, errorClass string, output map[string]any) error {
	body, err := json.Marshal(failRequest{TenantID: tenantID, ErrorClass: errorClass, Output: output})
	if err != nil {
		return fmt.Errorf("encode fail request: %w", err)
	}
	return c.post(ctx, tenantID, fmt.Sprintf("/api/v1/internal/connector-tasks/%s/fail", taskID), body)
}

func (c *completionClient) post(ctx context.Context, tenantID uuid.UUID, path string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.addr+path, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-user-id", domain.SystemCaller)
	req.Header.Set("x-tenant-roles", domain.SystemCaller)
	req.Header.Set("x-tenant-id", tenantID.String())

	gincommon.InjectTraceContext(ctx, req.Header)
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("call %s: %w", path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 300 {
		snippet, _ := io.ReadAll(io.LimitReader(resp.Body, callbackBodySnippetBytes))
		return &callbackStatusError{Path: path, Status: resp.StatusCode, Body: strings.TrimSpace(string(snippet))}
	}
	return nil
}

const callbackBodySnippetBytes = 200

// callbackStatusError carries what the Execution API answered a callback with,
// so the log shows a wrong principal or a missing route and not only "failed".
type callbackStatusError struct {
	Path   string
	Status int
	Body   string
}

func (e *callbackStatusError) Error() string {
	return fmt.Sprintf("call %s: status %d: %q", e.Path, e.Status, e.Body)
}
