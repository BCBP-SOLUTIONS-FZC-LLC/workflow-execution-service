package http

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

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
