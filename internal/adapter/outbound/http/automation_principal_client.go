package http

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

const iamSystemUserID = "00000000-0000-0000-0000-0000000000a1"

var _ port.AutomationPrincipalResolver = (*AutomationPrincipalClient)(nil)

type AutomationPrincipalClient struct {
	baseURL string
	client  *http.Client
	ttl     time.Duration

	mu    sync.Mutex
	known map[uuid.UUID]cachedPrincipal
}

type cachedPrincipal struct {
	principal port.AutomationPrincipal
	expires   time.Time
}

func NewAutomationPrincipalClient(baseURL string, timeout, cacheTTL time.Duration) *AutomationPrincipalClient {
	return &AutomationPrincipalClient{
		baseURL: baseURL,
		client:  &http.Client{Timeout: timeout},
		ttl:     cacheTTL,
		known:   make(map[uuid.UUID]cachedPrincipal),
	}
}

func (c *AutomationPrincipalClient) AutomationPrincipal(ctx context.Context, tenantID uuid.UUID) (port.AutomationPrincipal, error) {
	if c.baseURL == "" {
		return port.AutomationPrincipal{}, fmt.Errorf("token service not configured: %w", port.ErrNoAutomationPrincipal)
	}
	if p, ok := c.cached(tenantID); ok {
		return p, nil
	}

	url := fmt.Sprintf("%s/api/v1/internal/tenants/%s/service-accounts/platform-automation", c.baseURL, tenantID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return port.AutomationPrincipal{}, fmt.Errorf("build token service request: %w", err)
	}
	req.Header.Set("x-user-id", iamSystemUserID)
	req.Header.Set("x-tenant-id", tenantID.String())

	gincommon.InjectTraceContext(ctx, req.Header)
	resp, err := c.client.Do(req)
	if err != nil {
		return port.AutomationPrincipal{}, fmt.Errorf("%w: token service: %v", ErrUpstreamUnavailable, err)
	}
	defer func() { _ = resp.Body.Close() }()

	switch {
	case resp.StatusCode == http.StatusNotFound:
		return port.AutomationPrincipal{}, port.ErrNoAutomationPrincipal
	case resp.StatusCode != http.StatusOK:
		return port.AutomationPrincipal{}, fmt.Errorf("%w: token service answered %d", ErrUpstreamUnavailable, resp.StatusCode)
	}

	var body struct {
		PrincipalID  uuid.UUID `json:"principal_id"`
		PrincipalSub string    `json:"principal_sub"`
		Status       string    `json:"status"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&body); err != nil || body.PrincipalID == uuid.Nil {
		return port.AutomationPrincipal{}, fmt.Errorf("%w: token service answered without a principal_id", ErrUpstreamUnavailable)
	}

	if body.Status != "active" {
		return port.AutomationPrincipal{}, port.ErrNoAutomationPrincipal
	}
	if body.PrincipalSub == "" {
		return port.AutomationPrincipal{}, fmt.Errorf("%w: token service answered without a principal_sub", ErrUpstreamUnavailable)
	}

	principal := port.AutomationPrincipal{ID: body.PrincipalID, Subject: body.PrincipalSub}
	c.remember(tenantID, principal)
	return principal, nil
}

func (c *AutomationPrincipalClient) cached(tenantID uuid.UUID) (port.AutomationPrincipal, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.known[tenantID]
	if !ok || time.Now().After(entry.expires) {
		return port.AutomationPrincipal{}, false
	}
	return entry.principal, true
}

func (c *AutomationPrincipalClient) remember(tenantID uuid.UUID, principal port.AutomationPrincipal) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.known[tenantID] = cachedPrincipal{principal: principal, expires: time.Now().Add(c.ttl)}
}
