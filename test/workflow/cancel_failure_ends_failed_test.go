package workflow_test

import (
	"strings"
	"testing"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/wflclock"
)

func TestExecute_ACancelThatFailsForGoodEndsTheInstanceFailed(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	collab := twoDeptCollaboration()
	var statusWrites []port.UpdateInstanceStatusInput
	registerFakeActivities(env, collab, &activityHooks{
		cancelInstance: func(port.CancelInstanceInput) error {
			return temporal.NewNonRetryableApplicationError("stale record version", "ConflictError", nil)
		},
		updateInstanceStatus: func(in port.UpdateInstanceStatusInput) { statusWrites = append(statusWrites, in) },
	})
	wflclock.After(env, func() {
		env.SignalWorkflow("instance-cancel:instance-1", adminSignalWire{AdminUserID: "admin-1", RecordVersion: 1})
	}, tick)

	env.ExecuteWorkflow(wfengine.Execute, wfengine.ExecuteInput{TenantID: "tenant-1", InstanceID: "instance-1", VersionID: "version-1"})

	err := env.GetWorkflowError()
	if err == nil || !strings.Contains(err.Error(), "CancelInstanceActivity") {
		t.Fatalf("workflow error = %v, want one that names the failed cancel", err)
	}
	if len(statusWrites) != 1 {
		t.Fatalf("status writes = %+v, want exactly one", statusWrites)
	}
	got := statusWrites[0]
	if got.Status != domain.InstanceStatusFailed || got.CompletedAt.IsZero() {
		t.Errorf("status write = %+v, want FAILED with a completion time", got)
	}
}
