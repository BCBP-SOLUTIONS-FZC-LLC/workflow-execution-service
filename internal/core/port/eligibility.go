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
		tenantID, newUserID, departmentID uuid.UUID,
		requiredLevel string,
	) (eligible bool, err error)

	CheckEligibilityBatch(
		ctx context.Context,
		tenantID uuid.UUID,
		requests []EligibilityCheckRequest,
	) (results []EligibilityResult, err error)
}
