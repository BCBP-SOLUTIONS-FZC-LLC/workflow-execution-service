// Package http is the outbound IAM eligibility HTTP client (LLD §5.4 step 2,
// §9.2), the sole implementation of port.EligibilityChecker.
package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var ErrUpstreamUnavailable = errors.New("upstream service unavailable")

const maxEligibilityAttempts = 3

var _ port.EligibilityChecker = (*EligibilityClient)(nil)

// EligibilityClient calls Org & Membership's assignee-eligibility endpoint.
// Request authentication, if any, is injected via a custom http.Client.
type EligibilityClient struct {
	baseURL string
	client  *http.Client
}

func NewEligibilityClient(baseURL string, timeout time.Duration) *EligibilityClient {
	return &EligibilityClient{
		baseURL: baseURL,
		client:  &http.Client{Timeout: timeout},
	}
}

type eligibilityReq struct {
	NewUserID     uuid.UUID `json:"new_user_id"`
	DepartmentID  uuid.UUID `json:"department_id"`
	RequiredLevel string    `json:"required_level"`
	ActorID       uuid.UUID `json:"actor_id"`
}

// CheckEligibilityBatch fans out concurrently over CheckEligibility, one real
// HTTP call per request — a correct, working implementation today, but not
// yet the round-trip reduction LLD §5.5/§6.7 actually wants (Org & Membership
// has no confirmed real batch endpoint yet). Swapping in a true server-side
// batch call later needs no port.EligibilityChecker change, only this
// method's body.
func (c *EligibilityClient) CheckEligibilityBatch(
	ctx context.Context,
	requests []port.EligibilityCheckRequest,
	actorID uuid.UUID,
) ([]bool, error) {
	results := make([]bool, len(requests))
	errs := make([]error, len(requests))
	var wg sync.WaitGroup
	for i, req := range requests {
		wg.Add(1)
		go func(i int, req port.EligibilityCheckRequest) {
			defer wg.Done()
			eligible, err := c.CheckEligibility(ctx, req.NewUserID, req.DepartmentID, req.RequiredLevel, actorID)
			results[i], errs[i] = eligible, err
		}(i, req)
	}
	wg.Wait()

	for _, err := range errs {
		if err != nil {
			return nil, err
		}
	}
	return results, nil
}

func (c *EligibilityClient) CheckEligibility(
	ctx context.Context,
	newUserID, departmentID uuid.UUID,
	requiredLevel string,
	actorID uuid.UUID,
) (bool, error) {
	body, err := json.Marshal(eligibilityReq{
		NewUserID:     newUserID,
		DepartmentID:  departmentID,
		RequiredLevel: requiredLevel,
		ActorID:       actorID,
	})
	if err != nil {
		return false, fmt.Errorf("marshal eligibility request: %w", err)
	}
	rawURL := c.baseURL + "/eligibility"

	var lastErr error
	for attempt := 1; attempt <= maxEligibilityAttempts; attempt++ {
		eligible, retryable, err := c.tryCheckEligibility(ctx, rawURL, body)
		if err == nil {
			return eligible, nil
		}
		lastErr = err
		if attempt == maxEligibilityAttempts || !retryable {
			break
		}
		select {
		case <-ctx.Done():
			return false, fmt.Errorf("%w: eligibility check: %w", ErrUpstreamUnavailable, ctx.Err())
		case <-time.After(eligibilityBackoff(attempt)):
		}
	}
	return false, lastErr
}

func (c *EligibilityClient) tryCheckEligibility(
	ctx context.Context,
	rawURL string,
	body []byte,
) (eligible, retryable bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rawURL, bytes.NewReader(body))
	if err != nil {
		return false, false, fmt.Errorf("build eligibility request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return false, true, fmt.Errorf("%w: eligibility check: %w", ErrUpstreamUnavailable, err)
	}
	defer resp.Body.Close() //nolint:errcheck

	switch {
	case resp.StatusCode >= 500:
		return false, true, fmt.Errorf("%w: eligibility endpoint returned %d", ErrUpstreamUnavailable, resp.StatusCode)
	case resp.StatusCode != http.StatusOK:
		return false, false, fmt.Errorf("%w: eligibility endpoint returned %d", ErrUpstreamUnavailable, resp.StatusCode)
	}

	// Eligible is a pointer so a response body missing the field entirely
	// (an unrecognized shape) is distinguishable from an explicit false —
	// the former fails closed via ErrUpstreamUnavailable rather than being
	// silently read as "ineligible".
	var out struct {
		Eligible *bool `json:"eligible"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return false, false, fmt.Errorf("%w: decode eligibility response: %w", ErrUpstreamUnavailable, err)
	}
	if out.Eligible == nil {
		return false, false, fmt.Errorf("%w: eligibility response missing \"eligible\" field", ErrUpstreamUnavailable)
	}
	return *out.Eligible, false, nil
}

func eligibilityBackoff(attempt int) time.Duration {
	d := 50 * time.Millisecond << (attempt - 1)
	if d > 500*time.Millisecond {
		d = 500 * time.Millisecond
	}
	return d
}
