package temporal

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	pgdomain "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"
	"github.com/google/uuid"
	"go.temporal.io/sdk/temporal"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
)

func withTenantGUC(ctx context.Context, tenantID uuid.UUID) context.Context {
	return pgcommon.WithGUCSet(ctx, pgdomain.GUCSet{TenantID: tenantID.String()})
}

func nonRetryable(errType string, err error) error {
	return temporal.NewNonRetryableApplicationError(err.Error(), errType, err) //nolint:wrapcheck // must stay *temporal.ApplicationError for NonRetryableErrorTypes matching
}

// retryableOrNotFound lets an unknown row end the activity instead of retrying it
// without limit; every other error is returned as it came.
func retryableOrNotFound(err error) error {
	var appErr *temporal.ApplicationError
	if err == nil || errors.As(err, &appErr) || !errors.Is(err, domain.ErrNotFound) {
		return err
	}
	return nonRetryable("NotFoundError", err)
}

// deptUUID reads the department id the plan carries; an absent id stays the
// zero id, a malformed one is refused.
func deptUUID(iamDeptID string) (uuid.UUID, error) {
	if iamDeptID == "" {
		return uuid.Nil, nil
	}
	id, err := uuid.Parse(iamDeptID)
	if err != nil {
		return uuid.Nil, fmt.Errorf("parse department id %q: %w", iamDeptID, err)
	}
	return id, nil
}

func deterministicTaskID(instanceID uuid.UUID, nodeKey string, visitCount int64) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("execution_service:task:"+instanceID.String()+"/"+nodeKey+"/"+strconv.FormatInt(visitCount, 10)))
}

func (d *Deps) instanceCore(ctx context.Context, tenantID, instanceID uuid.UUID) (domain.CommonCore, error) {
	inst, err := d.Instances.GetByID(ctx, tenantID, instanceID)
	if err != nil {
		return domain.CommonCore{}, fmt.Errorf("get instance for event core: %w", err)
	}
	return domain.CommonCore{
		WorkflowInstanceID: instanceID,
		SubjectID:          inst.SubjectID,
		BusinessKey:        inst.BusinessKey,
		WorkflowVersionID:  inst.WorkflowVersionID,
	}, nil
}

func deterministicAssignmentID(taskID, userID uuid.UUID) uuid.UUID {
	return domain.DeterministicAssignmentID(taskID, userID)
}
