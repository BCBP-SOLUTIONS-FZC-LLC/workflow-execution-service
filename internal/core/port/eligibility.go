package port

import (
	"context"

	"github.com/google/uuid"
)

type EligibilityCheckRequest struct {
	NewUserID     uuid.UUID
	DepartmentID  uuid.UUID
	RequiredLevel string
}

type EligibilityResult struct {
	Eligible bool
	Err      error
}

type EligibilityChecker interface {
	CheckEligibility(
		ctx context.Context,
		newUserID, departmentID uuid.UUID,
		requiredLevel string,
		actorID uuid.UUID,
	) (eligible bool, err error)

	CheckEligibilityBatch(
		ctx context.Context,
		requests []EligibilityCheckRequest,
		actorID uuid.UUID,
	) (results []EligibilityResult, err error)
}
