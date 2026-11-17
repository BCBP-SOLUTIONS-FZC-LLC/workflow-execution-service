package port

import (
	"context"

	"github.com/google/uuid"
)

// EndedDelegationRepository remembers which delegations have ended, so a
// DelegationStarted that arrives after its DelegationEnded moves nothing.
type EndedDelegationRepository interface {
	MarkEnded(ctx context.Context, tenantID, delegationID uuid.UUID) error
	IsEnded(ctx context.Context, tenantID, delegationID uuid.UUID) (bool, error)
}
