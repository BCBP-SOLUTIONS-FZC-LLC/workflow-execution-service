package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

const (
	maxDefinitionAttempts = 3
	internalTokenHeader   = "x-internal-token"
)

var _ port.DefinitionServiceClient = (*DefinitionClient)(nil)

type DefinitionClient struct {
	baseURL string
	token   string
	client  *http.Client
}

func NewDefinitionClient(baseURL, token string, timeout time.Duration) *DefinitionClient {
	return &DefinitionClient{baseURL: baseURL, token: token, client: &http.Client{Timeout: timeout}}
}

type compiledPlanResp struct {
	WorkflowID    string          `json:"workflow_id"`
	VersionID     string          `json:"version_id"`
	VersionNumber *int32          `json:"version_number"`
	Status        string          `json:"status"`
	IsValid       bool            `json:"is_valid"`
	CompiledPlan  json.RawMessage `json:"compiled_plan"`
}

func (c *DefinitionClient) GetCompiledWorkflow(ctx context.Context, tenantID, workflowVersionID uuid.UUID) (*port.CompiledWorkflow, error) {
	rawURL := c.baseURL + "/api/v1/internal/workflow-versions/" + workflowVersionID.String() +
		"/compiled-plan?" + url.Values{"tenant_id": {tenantID.String()}}.Encode()

	var lastErr error
	for attempt := 1; attempt <= maxDefinitionAttempts; attempt++ {
		cw, retryable, err := c.tryGet(ctx, rawURL)
		if err == nil {
			return cw, nil
		}
		lastErr = err
		if !retryable || attempt == maxDefinitionAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("%w: compiled plan %s: %w", port.ErrDefinitionUnavailable, workflowVersionID, ctx.Err())
		case <-time.After(retryBackoff(attempt)):
		}
	}
	return nil, lastErr
}

func (c *DefinitionClient) tryGet(ctx context.Context, rawURL string) (*port.CompiledWorkflow, bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, false, fmt.Errorf("%w: build request: %w", port.ErrDefinitionRejected, err)
	}
	if c.token != "" {
		req.Header.Set(internalTokenHeader, c.token)
	}
	gincommon.InjectTraceContext(req.Context(), req.Header)
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, ctx.Err() == nil, fmt.Errorf("%w: compiled plan: %w", port.ErrDefinitionUnavailable, err)
	}
	defer resp.Body.Close() //nolint:errcheck

	switch {
	case resp.StatusCode >= 500:
		return nil, true, fmt.Errorf("%w: compiled plan returned %d", port.ErrDefinitionUnavailable, resp.StatusCode)
	case resp.StatusCode != http.StatusOK:
		return nil, false, fmt.Errorf("%w: compiled plan returned %d", port.ErrDefinitionRejected, resp.StatusCode)
	}
	var body compiledPlanResp
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, false, fmt.Errorf("%w: decode compiled plan: %w", port.ErrDefinitionRejected, err)
	}
	cw, err := toCompiledWorkflow(body)
	return cw, false, err
}

func toCompiledWorkflow(b compiledPlanResp) (*port.CompiledWorkflow, error) {
	workflowID, err := uuid.Parse(b.WorkflowID)
	if err != nil {
		return nil, fmt.Errorf("%w: workflow_id %q is not a UUID", port.ErrDefinitionRejected, b.WorkflowID)
	}
	versionID, err := uuid.Parse(b.VersionID)
	if err != nil {
		return nil, fmt.Errorf("%w: version_id %q is not a UUID", port.ErrDefinitionRejected, b.VersionID)
	}
	cw := &port.CompiledWorkflow{WorkflowID: workflowID, VersionID: versionID, Status: b.Status, IsValid: b.IsValid}
	if b.VersionNumber != nil {
		cw.VersionNumber = *b.VersionNumber
	}
	if plan := bytes.TrimSpace(b.CompiledPlan); len(plan) > 0 && !bytes.Equal(plan, []byte("null")) {
		cw.CompiledPlanJSON = string(plan)
	}
	return cw, nil
}
