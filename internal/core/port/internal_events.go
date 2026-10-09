package port

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type DelegationRerouteInput struct {
	TenantID     uuid.UUID
	DelegationID uuid.UUID
	DelegatorID  uuid.UUID
	DelegateID   uuid.UUID
	Scope        string
	ScopeID      *string
	StartsAt     time.Time
	EndsAt       *time.Time
}
type DelegationReversalInput struct {
	TenantID     uuid.UUID
	DelegationID uuid.UUID
	DelegatorID  uuid.UUID
	DelegateID   uuid.UUID
	EndedReason  string
}

type DelegationReconciler interface {
	Reroute(ctx context.Context, in DelegationRerouteInput) error
	Reverse(ctx context.Context, in DelegationReversalInput) error
}

type UserDeletedInput struct {
	TenantID  uuid.UUID
	UserID    uuid.UUID
	DeletedAt time.Time
}

type UserSafetyNetReconciler interface {
	VacateAssignments(ctx context.Context, in UserDeletedInput) error
}

type UserAvailabilityInput struct {
	TenantID       uuid.UUID
	UserID         uuid.UUID
	Status         string
	OOOFrom        *time.Time
	OOOUntil       *time.Time
	DelegateUserID *uuid.UUID
}

type OOOAvailabilityReconciler interface {
	Apply(ctx context.Context, in UserAvailabilityInput) error
}

type TenantLifecycleInput struct {
	TenantID       uuid.UUID
	Status         string
	PreviousStatus string
	Plan           string
	PreviousPlan   string
	ChangedAt      time.Time
	Cause          string
}

type TenantLifecycleReconciler interface {
	Apply(ctx context.Context, in TenantLifecycleInput) error
}
