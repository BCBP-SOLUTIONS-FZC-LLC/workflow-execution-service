package port

import (
	"context"
	"time"
)

// RecencyGuard is the generic <=-skip-with-tie-resolves-to-skip out-of-order
// delivery guard shared by TenantStateChanged, UserAvailabilityChanged, and
// workflow.template.published (LLD §6.2 items 4/6, Appendix A #25/#26).
// scopeKey conventions: "tenant:<tenant_id>", "user_availability:<user_id>",
// "template:<tenant_id>:<workflow_key>" — workflow_key is only unique per
// tenant, so the tenant_id must be part of the key or two tenants sharing a
// business key would collide on one recency row.
type RecencyGuard interface {
	// ShouldApply is a pure read: true when eventTime is strictly newer than
	// the stored value (or no row exists yet), false otherwise. It performs
	// no write — callers that need the check-and-commit to happen in one
	// atomic step should use CheckAndCommit instead.
	ShouldApply(ctx context.Context, scopeKey string, eventTime time.Time) (bool, error)

	// CheckAndCommit performs ShouldApply's check and the commit in one
	// atomic statement — only safe for callers whose guarded operation can't
	// itself fail in a way that needs a retry (e.g. template prewarm, which
	// is fail-open by handler contract). Any caller whose side effect can
	// fail and must be retried on a later, equally-timed redelivery should
	// use ShouldApply before the side effect and Commit after it succeeds
	// instead — committing here first and then having the side effect fail
	// would advance the guard past an event that was never actually applied.
	CheckAndCommit(ctx context.Context, scopeKey string, eventTime time.Time) (applied bool, err error)

	// Commit unconditionally, monotonically records eventTime (never lowers
	// the stored value). TenantStateChanged and UserAvailabilityChanged both
	// call ShouldApply up front, run their reconciler, then Commit exactly
	// once after it succeeds — never before, and never per sub-transaction
	// for TenantStateChanged specifically (LLD §6.2 item 4.3, Appendix A #26).
	Commit(ctx context.Context, scopeKey string, eventTime time.Time) error
}
