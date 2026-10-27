package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var ErrUpstreamUnavailable = errors.New("upstream service unavailable")

const maxEligibilityAttempts = 3

const maxEligibilityBatchConcurrency = 20

var _ port.EligibilityChecker = (*EligibilityClient)(nil)

type EligibilityClient struct {
	baseURL string
	client  *http.Client
	log     port.Logger
}

func NewEligibilityClient(baseURL string, timeout time.Duration) *EligibilityClient {
	return &EligibilityClient{
		baseURL: strings.TrimRight(baseURL, "/"),
		client:  &http.Client{Timeout: timeout},
	}
}

func (c *EligibilityClient) WithLogger(log port.Logger) *EligibilityClient {
	c.log = log
	return c
}

const systemPrincipal = "iam-system"

func (c *EligibilityClient) CheckEligibilityBatch(
	ctx context.Context,
	tenantID uuid.UUID,
	requests []port.EligibilityCheckRequest,
) ([]port.EligibilityResult, error) {
	results := make([]port.EligibilityResult, len(requests))
	var wg sync.WaitGroup
	sem := make(chan struct{}, maxEligibilityBatchConcurrency)
	for i, req := range requests {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, req port.EligibilityCheckRequest) {
			defer wg.Done()
			defer func() { <-sem }()
			eligible, err := c.CheckEligibility(ctx, tenantID, req.NewUserID, req.DepartmentID, req.RequiredLevel)
			results[i] = port.EligibilityResult{Eligible: eligible, Err: err}
		}(i, req)
	}
	wg.Wait()
	return results, nil
}

func (c *EligibilityClient) CheckEligibility(
	ctx context.Context,
	tenantID, newUserID, departmentID uuid.UUID,
	requiredLevel string,
) (bool, error) {
	if !domain.ValidRoleLevel(requiredLevel) {
		return false, nil
	}
	params := url.Values{"department_id": {departmentID.String()}, "required_level": {requiredLevel}}
	rawURL := fmt.Sprintf("%s/api/v1/internal/tenants/%s/members/%s/eligibility?%s",
		c.baseURL, tenantID, newUserID, params.Encode())

	var lastErr error
	for attempt := 1; attempt <= maxEligibilityAttempts; attempt++ {
		answer, retryable, err := c.tryCheckEligibility(ctx, rawURL, tenantID)
		if err == nil {
			if !answer.eligible && c.log != nil {
				c.log.Info("assignee refused by membership eligibility", map[string]any{
					"tenant_id": tenantID.String(), "user_id": newUserID.String(), "department_id": departmentID.String(),
					"required_level": requiredLevel, "reason": answer.reason,
				})
			}
			return answer.eligible, nil
		}
		lastErr = err
		if attempt == maxEligibilityAttempts || !retryable {
			break
		}
		select {
		case <-ctx.Done():
			return false, fmt.Errorf("%w: eligibility check: %w", ErrUpstreamUnavailable, ctx.Err())
		case <-time.After(retryBackoff(attempt)):
		}
	}
	return false, lastErr
}

type eligibilityAnswer struct {
	eligible bool
	reason   string
}

func (c *EligibilityClient) tryCheckEligibility(
	ctx context.Context,
	rawURL string,
	tenantID uuid.UUID,
) (answer eligibilityAnswer, retryable bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return eligibilityAnswer{}, false, fmt.Errorf("build eligibility request: %w", err)
	}
	req.Header.Set("x-user-id", systemPrincipal)
	req.Header.Set("x-tenant-roles", systemPrincipal)
	req.Header.Set("x-tenant-id", tenantID.String())

	gincommon.InjectTraceContext(ctx, req.Header)
	resp, err := c.client.Do(req)
	if err != nil {
		return eligibilityAnswer{}, true, fmt.Errorf("%w: eligibility check: %w", ErrUpstreamUnavailable, err)
	}
	defer resp.Body.Close() //nolint:errcheck // a failed close of a response body is not actionable

	switch {
	case resp.StatusCode >= 500:
		return eligibilityAnswer{}, true, fmt.Errorf("%w: eligibility endpoint returned %d", ErrUpstreamUnavailable, resp.StatusCode)
	case resp.StatusCode != http.StatusOK:
		return eligibilityAnswer{}, false, fmt.Errorf("%w: eligibility endpoint returned %d", ErrUpstreamUnavailable, resp.StatusCode)
	}

	var out struct {
		Eligible *bool  `json:"eligible"`
		Reason   string `json:"reason"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return eligibilityAnswer{}, false, fmt.Errorf("%w: decode eligibility response: %w", ErrUpstreamUnavailable, err)
	}
	if out.Eligible == nil {
		return eligibilityAnswer{}, false, fmt.Errorf("%w: eligibility response missing \"eligible\" field", ErrUpstreamUnavailable)
	}
	return eligibilityAnswer{eligible: *out.Eligible, reason: out.Reason}, false, nil
}

func retryBackoff(attempt int) time.Duration {
	d := 50 * time.Millisecond << (attempt - 1)
	if d > 500*time.Millisecond {
		d = 500 * time.Millisecond
	}
	return d
}
