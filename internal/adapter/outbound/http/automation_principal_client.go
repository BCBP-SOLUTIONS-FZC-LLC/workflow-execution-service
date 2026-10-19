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
	id      uuid.UUID
	expires time.Time
}

func NewAutomationPrincipalClient(baseURL string, timeout, cacheTTL time.Duration) *AutomationPrincipalClient {
	return &AutomationPrincipalClient{
		baseURL: baseURL,
		client:  &http.Client{Timeout: timeout},
		ttl:     cacheTTL,
		known:   make(map[uuid.UUID]cachedPrincipal),
	}
}

func (c *AutomationPrincipalClient) AutomationPrincipal(ctx context.Context, tenantID uuid.UUID) (uuid.UUID, error) {
	if c.baseURL == "" {
		return uuid.Nil, fmt.Errorf("token service not configured: %w", port.ErrNoAutomationPrincipal)
	}
	if id, ok := c.cached(tenantID); ok {
		return id, nil
	}

	url := fmt.Sprintf("%s/api/v1/internal/tenants/%s/service-accounts/platform-automation", c.baseURL, tenantID)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return uuid.Nil, fmt.Errorf("build token service request: %w", err)
	}
	req.Header.Set("x-user-id", iamSystemUserID)
	req.Header.Set("x-tenant-id", tenantID.String())

	gincommon.InjectTraceContext(ctx, req.Header)
	resp, err := c.client.Do(req)
	if err != nil {
		return uuid.Nil, fmt.Errorf("%w: token service: %v", ErrUpstreamUnavailable, err)
	}
	defer func() { _ = resp.Body.Close() }()

	switch {
	case resp.StatusCode == http.StatusNotFound:
		return uuid.Nil, port.ErrNoAutomationPrincipal
	case resp.StatusCode != http.StatusOK:
		return uuid.Nil, fmt.Errorf("%w: token service answered %d", ErrUpstreamUnavailable, resp.StatusCode)
	}

	var body struct {
		PrincipalID uuid.UUID `json:"principal_id"`
		Status      string    `json:"status"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&body); err != nil || body.PrincipalID == uuid.Nil {
		return uuid.Nil, fmt.Errorf("%w: token service answered without a principal_id", ErrUpstreamUnavailable)
	}

	if body.Status != "active" {
		return uuid.Nil, port.ErrNoAutomationPrincipal
	}

	c.remember(tenantID, body.PrincipalID)
	return body.PrincipalID, nil
}

func (c *AutomationPrincipalClient) cached(tenantID uuid.UUID) (uuid.UUID, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.known[tenantID]
	if !ok || time.Now().After(entry.expires) {
		return uuid.Nil, false
	}
	return entry.id, true
}

func (c *AutomationPrincipalClient) remember(tenantID, principalID uuid.UUID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.known[tenantID] = cachedPrincipal{id: principalID, expires: time.Now().Add(c.ttl)}
}
