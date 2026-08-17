package port

import (
	"context"

	"github.com/google/uuid"
)

// UserStatus is a snapshot of a user's live status, sourced from IAM/Org &
// Membership. Distinct from EligibilityChecker (which asks "is this user
// eligible for this department+level"): this asks "is this user still a
// valid actor at all, right now" — the live claim/complete/defer/reassign
// pre-check LLD Appendix B documents as undesigned ("Task actions never
// re-verify the assignee's live OOO/delegation/deleted status at the moment
// of the action").
type UserStatus struct {
	IsDeleted      bool
	IsOOO          bool
	DelegateUserID *uuid.UUID
}

// IAMClient is the outbound contract for live per-user status lookups. Its
// real implementation's endpoint contract is not yet confirmed with the IAM
// team — see internal/adapter/outbound/http/iam_client.go's own doc comment
// for the stub this port currently has.
type IAMClient interface {
	GetUserStatus(ctx context.Context, tenantID, userID uuid.UUID) (UserStatus, error)
}
