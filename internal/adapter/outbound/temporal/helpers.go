package temporal

import (
	"context"
	"strings"

	pgdomain "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"
	"github.com/google/uuid"
	"go.temporal.io/sdk/temporal"
)

// withTenantGUC sets the app.tenant_id GUC on ctx before a Transactor.RunInTx
// call composing multiple repo calls + an outbox enqueue in one transaction —
// RunInTx acquires its connection using ctx before its callback runs, so a
// repo method's own per-call GUC re-assertion from inside that callback comes
// too late (internal/adapter/outbound/postgres's own package doc).
func withTenantGUC(ctx context.Context, tenantID uuid.UUID) context.Context {
	return pgcommon.WithGUCSet(ctx, pgdomain.GUCSet{TenantID: tenantID.String()})
}

// nonRetryable wraps err as a Temporal ApplicationError of errType, matching
// internal/workflow/activities.go's own NonRetryableErrorTypes
// classification (ValidationError/NotFoundError for DB-write activities,
// DefinitionServiceClientError for the external-call one) — the interpreter
// side is only reused here, never modified.
func nonRetryable(errType string, err error) error {
	return temporal.NewApplicationErrorWithCause(err.Error(), errType, err) //nolint:wrapcheck // must stay *temporal.ApplicationError for NonRetryableErrorTypes matching
}

// deptUUID derives a stable, deterministic UUID from a compiled plan's
// DepartmentDef.ID — today a display-slug lane name, never a real IAM
// department UUID (execution_service LLD §4.3: reading one out of a BPMN
// lane's extensionElements is a still-open TODO in definition_service's own
// compiler). An explicit, temporary placeholder — replace with the compiled
// plan's own real UUID once that compiler fix lands, not with anything
// smarter here.
func deptUUID(deptID string) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("execution_service:department:"+deptID))
}

// deptIDFromNodeKey splits a "deptID/rest" NodeKey back into just the deptID
// half — the only encoding stageNodeKey (internal/workflow/stage.go) uses,
// mirrored here since this package cannot import that one.
func deptIDFromNodeKey(nodeKey string) string {
	deptID, _, _ := strings.Cut(nodeKey, "/")
	return deptID
}

// deterministicTaskID derives CreateTask's workflow_task.id from stable,
// replay-safe inputs (instanceID+NodeKey), the standard idempotent-side-effect
// pattern for a Temporal Activity: a retried attempt after a lost ack
// re-derives the exact same ID, so its INSERT hits its own primary key
// instead of creating a second real task row (mapErr classifies that
// conflict as domain.ErrAlreadyExists — see CreateTask's own doc comment).
func deterministicTaskID(instanceID uuid.UUID, nodeKey string) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("execution_service:task:"+instanceID.String()+"/"+nodeKey))
}

// deterministicAssignmentID derives a task assignment's id from its (already
// deterministic) task ID and the assignee — same idempotent-retry rationale
// as deterministicTaskID.
func deterministicAssignmentID(taskID, userID uuid.UUID) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("execution_service:assignment:"+taskID.String()+"/"+userID.String()))
}

// deterministicRegressionTaskID derives DeferTask's regression task's id
// from the deferred task's own ID — same idempotent-retry rationale as
// deterministicTaskID, but keyed off the source task rather than
// instance+NodeKey, since a regression task deliberately reuses its
// original task's own NodeKey (createRegressionTask), which
// deterministicTaskID would collide on.
func deterministicRegressionTaskID(deferredTaskID uuid.UUID) uuid.UUID {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("execution_service:regression-task:"+deferredTaskID.String()))
}
