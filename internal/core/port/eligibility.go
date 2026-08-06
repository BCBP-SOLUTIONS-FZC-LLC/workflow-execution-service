package port

import (
	"context"

	"github.com/google/uuid"
)

// EligibilityChecker is the contract for the outbound IAM assignee-eligibility
// check (LLD §5.4 step 2, §9.2)
type EligibilityChecker interface {
	CheckEligibility(
		ctx context.Context,
		newUserID, departmentID uuid.UUID,
		requiredLevel string,
		actorID uuid.UUID,
	) (eligible bool, err error)
}
