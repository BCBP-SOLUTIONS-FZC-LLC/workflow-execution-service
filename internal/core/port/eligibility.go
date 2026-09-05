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

// EligibilityResult is one request's outcome within a CheckEligibilityBatch
// call. Err is set when that specific request's underlying call failed (e.g.
// a transient upstream error) — it is per-item, not a whole-batch failure, so
// one bad request never discards the results already obtained for the rest
// of the batch. Callers treat a non-nil Err the same as Eligible=false
// (fail-closed): an eligibility question we couldn't answer is not one we
// can allow through.
type EligibilityResult struct {
	Eligible bool
	Err      error
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
	// same order as requests, one EligibilityResult per request — a single
	// request's error never discards the others' already-obtained results.
	// The top-level err return is reserved for a genuine whole-batch failure
	// (e.g. malformed input); the current fan-out-over-single-user-endpoint
	// implementation never has one, since every possible failure is per-item.
	// The LLD's "batch by distinct (department, level) pair" language
	// describes the goal — few calls, not N — not a mandated collapse of
	// requests naming different users; a real server-side batch endpoint's
	// exact contract isn't confirmed with the IAM team yet (see
	// EligibilityClient's own doc comment for its current implementation).
	CheckEligibilityBatch(
		ctx context.Context,
		requests []EligibilityCheckRequest,
		actorID uuid.UUID,
	) (results []EligibilityResult, err error)
}
