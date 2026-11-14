package workflow_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// When the last failed branch of a park is resolved but the write that puts
// the instance back to RUNNING gives up, the run fails with that error: the
// write is not retried, and the instance is finally written FAILED (not left
// DEGRADED, not reported as running). The activity's non-retryable failure is
// a ValidationError (an id it cannot parse); it stands in here for any
// non-retryable status write failure.
func TestExecute_FailureToWriteRunningAfterAParkDrainsFailsTheInstance(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	collab := parallelCollaboration()
	billingIAM := uuid.New()
	collab.Plans[0].Departments[1].IAMDepartmentID = billingIAM.String()
	fakes := wflNewFakes()
	fakes.failFor = func(name string, in any) error {
		switch v := in.(type) {
		case port.CreateTaskInput:
			if v.NodeKey == "billing/prep" {
				return temporal.NewApplicationError("card declined", "ValidationError")
			}
		case port.UpdateInstanceStatusInput:
			if v.Status == domain.InstanceStatusRunning {
				return temporal.NewApplicationError("parse instance_id: invalid UUID length", "ValidationError")
			}
		}
		return nil
	}
	wflRegisterAll(env, fakes, collab)
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDoneStage("warehouse", "prep", 1))
	wflSignalAt(env, 5*time.Second, "instance-force-forward", adminSignalWire{
		AdminUserID: "admin-1", TargetDeptID: "billing", TargetNodeKey: "billing/prep", RecordVersion: 3,
	})

	_, err := wflStart(t, env, fakes, "")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "workflow: UpdateInstanceStatusActivity: ")
	assert.Contains(t, err.Error(), "parse instance_id")
	assert.Equal(t, []domain.InstanceStatus{
		domain.InstanceStatusDegraded, domain.InstanceStatusRunning, domain.InstanceStatusFailed,
	}, wflStatuses(fakes), "the RUNNING write was tried once, then the run was failed")
	writes := wflInputs[port.UpdateInstanceStatusInput](fakes, port.ActivityUpdateInstanceStatus)
	for _, w := range writes {
		assert.Equal(t, wflTenant, w.TenantID)
		assert.Equal(t, wflInstance, w.InstanceID)
	}
	require.Len(t, writes[0].FailedBranches, 1)
	assert.Equal(t, billingIAM, writes[0].FailedBranches[0].DepartmentID)
	assert.Empty(t, writes[1].FailedBranches)
}
