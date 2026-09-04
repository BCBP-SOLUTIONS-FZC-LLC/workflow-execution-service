package port

import (
	"context"
	"time"
)

// RecencyGuard is the generic <=-skip-with-tie-resolves-to-skip out-of-order
// delivery guard shared by TenantStateChanged and UserAvailabilityChanged
// (LLD §6.2 items 4/6, Appendix A #25/#26). scopeKey conventions:
// "tenant:<tenant_id>", "user_availability:<tenant_id>:<user_id>" — a
// Keycloak user_id is only unique per tenant, so tenant_id must be part of
// every multi-tenant scope key or two tenants sharing a user id would
// collide on one recency row.
type RecencyGuard interface {
	// ShouldApply is a pure read: true when eventTime is strictly newer than
	// the stored value (or no row exists yet), false otherwise. It performs
	// no write — callers that need the check-and-commit to happen in one
	// atomic step should use CheckAndCommit instead.
	ShouldApply(ctx context.Context, scopeKey string, eventTime time.Time) (bool, error)

	// CheckAndCommit performs ShouldApply's check and the commit in one
	// atomic statement — only safe for callers whose guarded operation can't
	// itself fail in a way that needs a retry. Any caller whose side effect
	// can fail and must be retried on a later, equally-timed redelivery
	// should use ShouldApply before the side effect and Commit after it
	// succeeds instead — committing here first and then having the side
	// effect fail would advance the guard past an event that was never
	// actually applied. Its one caller was workflow.template.published,
	// which platform-events retired; no other event type's reconciler is
	// side-effect-free enough to use this safely, so it is currently unused.
	CheckAndCommit(ctx context.Context, scopeKey string, eventTime time.Time) (applied bool, err error)

	// Commit unconditionally, monotonically records eventTime (never lowers
	// the stored value). TenantStateChanged and UserAvailabilityChanged both
	// call ShouldApply up front, run their reconciler, then Commit exactly
	// once after it succeeds — never before, and never per sub-transaction
	// for TenantStateChanged specifically (LLD §6.2 item 4.3, Appendix A #26).
	Commit(ctx context.Context, scopeKey string, eventTime time.Time) error

	// WithLock serializes concurrent callers sharing scopeKey by holding a
	// session-level lock for fn's entire duration. Required because
	// ShouldApply/Apply/Commit are separate statements/transactions that no
	// single DB transaction spans (the reconciler opens its own
	// sub-transactions per instance) — a transaction-scoped lock can't cover
	// the sequence, only a session-held one wrapping the whole call can. This
	// closes the real race IAM's own event-delivery model creates: standard
	// (non-FIFO) SNS/SQS gives no ordering guarantee, and IAM's docs confirm
	// concurrent/overlapping delivery for the same tenant/user is expected
	// during bursts, not rare — so two conflicting events for the same scope
	// key can otherwise have their Apply calls execute out of timestamp
	// order even though Commit correctly records the newer timestamp.
	WithLock(ctx context.Context, scopeKey string, fn func(ctx context.Context) error) error
}
