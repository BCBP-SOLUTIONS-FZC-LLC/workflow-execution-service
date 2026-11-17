package port

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// DelegationScope is the lock a delegation's start and the cancel that names it
// take, so the two never run at the same time.
func DelegationScope(tenantID, delegationID uuid.UUID) string {
	return "delegation:" + tenantID.String() + ":" + delegationID.String()
}

// DelegateScope is the lock a delegation's start and a cancel of every
// delegation to the same delegate take.
func DelegateScope(tenantID, delegateID uuid.UUID) string {
	return "delegate:" + tenantID.String() + ":" + delegateID.String()
}

// EndedDelegationRepository remembers which delegations have ended, so a
// DelegationStarted that arrives after its DelegationEnded moves nothing.
type EndedDelegationRepository interface {
	MarkEnded(ctx context.Context, tenantID, delegationID uuid.UUID) error
	IsEnded(ctx context.Context, tenantID, delegationID uuid.UUID) (bool, error)
	// MarkDelegateEnded records that every delegation to the delegate was
	// cancelled at endedAt; an older time never replaces a newer one.
	MarkDelegateEnded(ctx context.Context, tenantID, delegateID uuid.UUID, endedAt time.Time) error
	// DelegateEndedAt returns when every delegation to the delegate was last
	// cancelled, and false when none was.
	DelegateEndedAt(ctx context.Context, tenantID, delegateID uuid.UUID) (endedAt time.Time, found bool, err error)
}
