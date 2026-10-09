package temporal

import (
	"context"
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
	return temporal.NewApplicationErrorWithCause(err.Error(), errType, err) //nolint:wrapcheck // must stay *temporal.ApplicationError for NonRetryableErrorTypes matching
}

// deptUUID must resolve identically to instance_service.go's own copy —
// their outputs are compared directly in delegation_reconciler.go.
func deptUUID(iamDeptID string) uuid.UUID {
	id, _ := uuid.Parse(iamDeptID)
	return id
}

func deterministicTaskID(instanceID uuid.UUID, nodeKey string, visitCount int64) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("execution_service:task:"+instanceID.String()+"/"+nodeKey+"/"+strconv.FormatInt(visitCount, 10)))
}

// instanceCore builds an event's common core from the instance the task belongs
// to. Built from a read rather than from the task alone because an instance's
// identity — subject, key, version — lives on the instance row; a core carrying
// only the instance id ships a payload whose identifiers are empty strings and
// zero UUIDs, which the schema validator accepts (present satisfies `required`,
// and `format` is annotation-only).
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
