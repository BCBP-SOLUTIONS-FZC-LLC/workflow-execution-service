package http

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

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
)

const (
	maxEntitlementsBody = 1 << 20
	// entitlementsIdleConnsPerHost matches the grants cache's cap of 32 concurrent reads, so a burst reuses connections.
	entitlementsIdleConnsPerHost = 32
)

const (
	callOutcomeSuccess = "success"
	callOutcomeError   = "error"

	failTransport = "transport"
	failStatus    = "status"
	failResponse  = "response"
)

var _ port.EntitlementsReader = (*EntitlementsClient)(nil)

type EntitlementsClient struct {
	baseURL string
	client  *http.Client
}

func NewEntitlementsClient(baseURL string, timeout time.Duration) *EntitlementsClient {
	transport := http.DefaultTransport.(*http.Transport).Clone() //nolint:forcetypeassert // the standard library's default is a *http.Transport
	transport.MaxIdleConnsPerHost = entitlementsIdleConnsPerHost
	client := NewClient(timeout)
	client.Transport = transport
	return &EntitlementsClient{baseURL: strings.TrimRight(baseURL, "/"), client: client}
}

// FeatureFlags makes one attempt: the grants cache's backoff is the retry policy.
func (c *EntitlementsClient) FeatureFlags(ctx context.Context, tenantID uuid.UUID) ([]string, error) {
	start := time.Now()
	flags, failure, err := c.fetch(ctx, tenantID)
	outcome := callOutcomeSuccess
	if err != nil {
		outcome = callOutcomeError
		observability.IncCounterVec(observability.EntitlementsCallErrorsTotal, failure)
	}
	observability.ObserveHistogramVec(observability.EntitlementsCallDurationSeconds, time.Since(start).Seconds(), outcome)
	return flags, err
}

// fetch also names the failure class for the call metric.
func (c *EntitlementsClient) fetch(ctx context.Context, tenantID uuid.UUID) ([]string, string, error) {
	rawURL := fmt.Sprintf("%s/api/v1/internal/tenants/%s/entitlements", c.baseURL, tenantID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, failTransport, fmt.Errorf("%w: build entitlements request: %w", port.ErrEntitlementsUnavailable, err)
	}
	req.Header.Set("x-user-id", domain.SystemCaller)
	req.Header.Set("x-tenant-roles", domain.SystemCaller)
	req.Header.Set("x-tenant-id", tenantID.String())

	gincommon.InjectTraceContext(ctx, req.Header)
	resp, err := c.client.Do(req)
	if err != nil {
		return nil, failTransport, fmt.Errorf("%w: entitlements read: %w", port.ErrEntitlementsUnavailable, err)
	}
	defer resp.Body.Close() //nolint:errcheck // a failed close of a response body is not actionable

	if resp.StatusCode != http.StatusOK {
		return nil, failStatus, fmt.Errorf("%w: entitlements endpoint returned %d", port.ErrEntitlementsUnavailable, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxEntitlementsBody+1))
	if err != nil {
		return nil, failTransport, fmt.Errorf("%w: read entitlements response: %w", port.ErrEntitlementsUnavailable, err)
	}
	if len(body) > maxEntitlementsBody {
		return nil, failResponse, fmt.Errorf("%w: entitlements response is larger than %d bytes", port.ErrEntitlementsUnavailable, maxEntitlementsBody)
	}
	flags, err := decodeFeatureFlags(body, tenantID)
	if err != nil {
		return nil, failResponse, err
	}
	return flags, "", nil
}

func decodeFeatureFlags(body []byte, tenantID uuid.UUID) ([]string, error) {
	var out struct {
		TenantID     json.RawMessage `json:"tenant_id"`
		FeatureFlags json.RawMessage `json:"feature_flags"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("%w: decode entitlements response: %w", port.ErrEntitlementsUnavailable, err)
	}
	if err := checkEntitlementsTenant(out.TenantID, tenantID); err != nil {
		return nil, err
	}
	if out.FeatureFlags == nil {
		return nil, fmt.Errorf("%w: entitlements response has no feature_flags", port.ErrEntitlementsUnavailable)
	}
	var raw []*string
	if err := json.Unmarshal(out.FeatureFlags, &raw); err != nil || raw == nil {
		return nil, fmt.Errorf("%w: entitlements feature_flags is not an array of strings", port.ErrEntitlementsUnavailable)
	}
	flags := make([]string, len(raw))
	for i, flag := range raw {
		if flag == nil {
			return nil, fmt.Errorf("%w: entitlements feature_flags holds a null entry", port.ErrEntitlementsUnavailable)
		}
		flags[i] = *flag
	}
	return flags, nil
}

func checkEntitlementsTenant(raw json.RawMessage, want uuid.UUID) error {
	if raw == nil || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return fmt.Errorf("%w: entitlements response has no tenant_id", port.ErrEntitlementsUnavailable)
	}
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		return fmt.Errorf("%w: entitlements tenant_id is not a string: %w", port.ErrEntitlementsUnavailable, err)
	}
	got, err := uuid.Parse(text)
	if err != nil {
		return fmt.Errorf("%w: entitlements tenant_id is not a uuid: %w", port.ErrEntitlementsUnavailable, err)
	}
	if got != want {
		return fmt.Errorf("%w: entitlements answered for tenant %s, not %s", port.ErrEntitlementsUnavailable, got, want)
	}
	return nil
}
