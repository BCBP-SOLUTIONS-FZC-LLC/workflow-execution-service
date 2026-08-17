package port

import (
	"context"

	"github.com/google/uuid"
)

// EligibilityCheckRequest is one (user, department, level) triple in a
// CheckEligibilityBatch call.
type EligibilityCheckRequest struct {
	NewUserID     uuid.UUID
	DepartmentID  uuid.UUID
	RequiredLevel string
}

// EligibilityChecker is the contract for the outbound IAM assignee-eligibility
// check (LLD §5.4 step 2, §9.2)
type EligibilityChecker interface {
	CheckEligibility(
		ctx context.Context,
		newUserID, departmentID uuid.UUID,
		requiredLevel string,
		actorID uuid.UUID,
	) (eligible bool, err error)

	// CheckEligibilityBatch batches every per-node eligibility question
	// InstanceService.Start's bulk default-assignee re-validation (LLD §5.5)
	// needs into one call site, cutting the caller's own round-trip count
	// from one per node to one per Start call. Results are returned in the
	// same order as requests. The LLD's "batch by distinct (department,
	// level) pair" language describes the goal — few calls, not N — not a
	// mandated collapse of requests naming different users; a real
	// server-side batch endpoint's exact contract isn't confirmed with the
	// IAM team yet (see EligibilityClient's own doc comment for its current,
	// fan-out-over-the-existing-single-user-endpoint implementation).
	CheckEligibilityBatch(
		ctx context.Context,
		requests []EligibilityCheckRequest,
		actorID uuid.UUID,
	) (eligible []bool, err error)
}
