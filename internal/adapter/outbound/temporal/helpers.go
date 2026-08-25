package temporal

import (
	"context"
	"strconv"

	pgdomain "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"
	"github.com/google/uuid"
	"go.temporal.io/sdk/temporal"
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

func deterministicAssignmentID(taskID, userID uuid.UUID) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("execution_service:assignment:"+taskID.String()+"/"+userID.String()))
}

func deterministicRegressionTaskID(deferredTaskID uuid.UUID) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("execution_service:regression-task:"+deferredTaskID.String()))
}
