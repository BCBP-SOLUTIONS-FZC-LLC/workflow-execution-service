package port

import (
	"context"

	"github.com/google/uuid"
)

type ReassignDelegateInput struct {
	TenantID      uuid.UUID
	OldDelegateID uuid.UUID
	NewDelegateID uuid.UUID
	DelegationID  *uuid.UUID
}

type CancelByDelegateInput struct {
	TenantID       uuid.UUID
	DelegateUserID uuid.UUID
	DelegationID   *uuid.UUID
}

type DelegateImpactInput struct {
	TenantID       uuid.UUID
	DelegateUserID uuid.UUID
	DelegationID   *uuid.UUID
	Page           Page
}

// DelegateImpactResult keeps ReassignedCount (a cheap, unpaginated total) separate
// from WorkflowIDs (the paginated preview) per LLD §5.9's rev-1.10 correction —
// PageResult[T] alone has no slot for a second, non-paginated count.
type DelegateImpactResult struct {
	ReassignedCount int
	WorkflowIDs     PageResult[uuid.UUID]
}

// WorkflowClient is the port the three /internal/workflows/* routes (LLD
// §5.8) code against; internal/core/service implements it separately,
// handler tests fake it directly.
type WorkflowClient interface {
	ReassignDelegate(ctx context.Context, in ReassignDelegateInput) (reassigned int, err error)
	CancelByDelegate(ctx context.Context, in CancelByDelegateInput) (cancelled int, err error)
	DelegateImpact(ctx context.Context, in DelegateImpactInput) (DelegateImpactResult, error)
}
