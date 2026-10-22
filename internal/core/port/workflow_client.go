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

type DelegateImpactResult struct {
	ReassignedCount int
	WorkflowIDs     PageResult[uuid.UUID]
}

type WorkflowClient interface {
	ReassignDelegate(ctx context.Context, in ReassignDelegateInput) (reassigned int, err error)
	CancelByDelegate(ctx context.Context, in CancelByDelegateInput) (cancelled int, err error)
	DelegateImpact(ctx context.Context, in DelegateImpactInput) (DelegateImpactResult, error)
}
