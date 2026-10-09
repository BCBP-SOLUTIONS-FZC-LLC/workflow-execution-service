package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	"github.com/google/uuid"
)

type completionClient struct {
	addr  string
	token string
	http  *http.Client
}

func newCompletionClient(addr, token string, timeout time.Duration) *completionClient {
	return &completionClient{
		addr: strings.TrimRight(addr, "/"), token: token,
		http: &http.Client{Timeout: timeout},
	}
}

type completeRequest struct {
	TenantID uuid.UUID      `json:"tenant_id"`
	Output   map[string]any `json:"output"`
}

type failRequest struct {
	TenantID   uuid.UUID `json:"tenant_id"`
	ErrorClass string    `json:"error_class"`
}

func (c *completionClient) Complete(ctx context.Context, tenantID, taskID uuid.UUID, output map[string]any) error {
	body, err := json.Marshal(completeRequest{TenantID: tenantID, Output: output})
	if err != nil {
		return fmt.Errorf("encode complete request: %w", err)
	}
	return c.post(ctx, fmt.Sprintf("/api/v1/internal/connector-tasks/%s/complete", taskID), body)
}

func (c *completionClient) Fail(ctx context.Context, tenantID, taskID uuid.UUID, errorClass string) error {
	body, err := json.Marshal(failRequest{TenantID: tenantID, ErrorClass: errorClass})
	if err != nil {
		return fmt.Errorf("encode fail request: %w", err)
	}
	return c.post(ctx, fmt.Sprintf("/api/v1/internal/connector-tasks/%s/fail", taskID), body)
}

func (c *completionClient) post(ctx context.Context, path string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.addr+path, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-internal-token", c.token)

	gincommon.InjectTraceContext(req.Context(), req.Header)
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("call %s: %w", path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 300 {
		return fmt.Errorf("call %s: status %d", path, resp.StatusCode)
	}
	return nil
}
