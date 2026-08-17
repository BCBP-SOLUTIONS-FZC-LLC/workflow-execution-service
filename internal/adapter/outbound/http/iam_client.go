package http

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// ErrIAMContractNotConfirmed marks GetUserStatus as not yet wired to a real
// IAM/Org & Membership endpoint — the request/response contract for a live
// per-user status lookup (LLD Appendix B's undesigned claim/complete/defer
// pre-check) has not been confirmed with the IAM team. Once confirmed,
// replace this stub's body with a real HTTP call, mirroring EligibilityClient's
// own request/retry pattern in this same file — the constructor/struct shape
// below is deliberately already in that shape so the swap needs no caller
// changes.
//
// Callers should treat this specific error as fail-open (log and proceed),
// the same way this design already treats Valkey/IAM unavailability
// elsewhere for a non-critical-path safety net — only a successful call
// reporting IsDeleted/IsOOO/a live delegate should ever block a task action.
var ErrIAMContractNotConfirmed = errors.New("iam client: user-status endpoint contract not yet confirmed")

var _ port.IAMClient = (*IAMClient)(nil)

type IAMClient struct {
	baseURL string
	client  *http.Client
}

func NewIAMClient(baseURL string, timeout time.Duration) *IAMClient {
	return &IAMClient{
		baseURL: baseURL,
		client:  &http.Client{Timeout: timeout},
	}
}

func (c *IAMClient) GetUserStatus(_ context.Context, _, _ uuid.UUID) (port.UserStatus, error) {
	return port.UserStatus{}, ErrIAMContractNotConfirmed
}
