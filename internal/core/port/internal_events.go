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
	Scope        string // "all" | "department" | "business_key"
	ScopeID      *string
	StartsAt     time.Time
	EndsAt       *time.Time // nil for an open-ended delegation
}
type DelegationReversalInput struct {
	TenantID     uuid.UUID
	DelegationID uuid.UUID
	DelegatorID  uuid.UUID
	DelegateID   uuid.UUID
	EndedReason  string // "expired" | "cancelled" | "delegate_removed" | other (treated as generic end)
}

// DelegationReconciler's real implementation (deferred to a future
// internal/core/service task) drives the bulk reroute/reversal transaction
// + signal loop (LLD §6.2 items 1-2). Reroute's caller times
// delegation_reroute_duration_seconds around this call, success only.
type DelegationReconciler interface {
	Reroute(ctx context.Context, in DelegationRerouteInput) error
	Reverse(ctx context.Context, in DelegationReversalInput) error
}

type UserDeletedInput struct {
	TenantID  uuid.UUID
	UserID    uuid.UUID
	DeletedAt time.Time
}

// UserSafetyNetReconciler's real implementation vacates per-assignment,
// never instance-wide (LLD §6.2 item 3) — a multi-assignee task's instance
// survives if a co-assignee remains active.
type UserSafetyNetReconciler interface {
	VacateAssignments(ctx context.Context, in UserDeletedInput) error
}

type UserAvailabilityInput struct {
	TenantID       uuid.UUID
	UserID         uuid.UUID
	Status         string // "ooo" | "available" | other
	OOOFrom        *time.Time
	OOOUntil       *time.Time
	DelegateUserID *uuid.UUID // informational only — never a reroute driver
}

// OOOAvailabilityReconciler.Apply's real implementation dispatches
// pause(initiator=ooo)/resume(initiator=ooo) internally off Status. A
// per-instance instance-pause signal rejected by signal validation
// (DEGRADED != RUNNING, LLD §3) must be logged and skipped there, never
// abort the whole batch — a contract note for that future implementation,
// not something this task's own code loops over.
type OOOAvailabilityReconciler interface {
	Apply(ctx context.Context, in UserAvailabilityInput) error
}

type TenantLifecycleInput struct {
	TenantID       uuid.UUID
	Status         string // trial | active | past_due | cancelled | suspended | trial_expired | offboarded
	PreviousStatus string
	Plan           string
	PreviousPlan   string
	ChangedAt      time.Time
	Cause          string // source lifecycle event type, e.g. "TenantSuspended"
}

// TenantLifecycleReconciler.Apply covers every sub-transaction the event
// carries (status transition + plan change, when both present) in one call
// — deliberately not split into ApplyStatus/ApplyPlan, so the handler's own
// "commit recency once, after Apply returns" rule (LLD §6.2 item 4.3) is
// correct by construction. Same DEGRADED-rejection contract note as
// OOOAvailabilityReconciler applies to its pause-driving transitions.
type TenantLifecycleReconciler interface {
	Apply(ctx context.Context, in TenantLifecycleInput) error
}

type TemplatePublishedInput struct {
	TenantID      uuid.UUID
	WorkflowID    uuid.UUID
	WorkflowKey   string // unique per tenant, not globally
	VersionID     uuid.UUID
	VersionNumber int
	ArtifactHash  string
	PublishedBy   uuid.UUID
	// PromotedFromVersion is nil for a fresh publish, set when this version
	// was promoted from an existing draft/version.
	PromotedFromVersion *uuid.UUID
}

// TemplateCachePrewarmer.Prewarm is fail-open by handler contract (LLD §6.2
// item 5): any returned error is logged by the caller and still yields 200.
// It still returns an error, rather than swallowing one itself, so the real
// implementation can report cause and callers can assert the fail-open path.
type TemplateCachePrewarmer interface {
	Prewarm(ctx context.Context, in TemplatePublishedInput) error
}
