package workflow_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

func wflForkOnly() *dsl.CompiledCollaboration {
	collab := wflIntakeThenFork()
	collab.Plans[0].Execution.Steps = collab.Plans[0].Execution.Steps[1:]
	return collab
}

// A force-forward of a branch inside a parallel step fails the run when the
// current-node write that follows the committed route record is refused: the
// branch is not left waiting on a task the record superseded.
func TestExecute_ForceForwardInAParallelStepWhoseCurrentNodeWriteIsRefusedFailsTheRun(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	writes := 0
	fakes.failFor = func(name string, in any) error {
		if v, ok := in.(port.UpdateInstanceNodesInput); ok && len(v.NodeKeys) == 1 && v.NodeKeys[0] == "billing/Charge" {
			writes++
			if writes == 2 {
				return wflRefuse(name)
			}
		}
		return nil
	}
	wflRegisterAll(env, fakes, wflForkOnly())
	wflSignalAt(env, 5*time.Second, "instance-force-forward", adminSignalWire{AdminUserID: "admin-7", TargetDeptID: "billing", TargetNodeKey: "billing/Charge", RecordVersion: 9})

	_, err := wflStart(t, env, fakes, "")

	require.Error(t, err)
	assert.Contains(t, err.Error(), port.ActivityUpdateInstanceNodes)
	assert.Equal(t, []domain.InstanceStatus{domain.InstanceStatusFailed}, wflStatuses(fakes))
	audits := wflInputs[port.RecordForceRouteInput](fakes, port.ActivityRecordForceRoute)
	require.Len(t, audits, 1)
	assert.Equal(t, wflTenant, audits[0].TenantID)
	assert.Equal(t, wflInstance, audits[0].InstanceID)
}

// The same refusal while the failed branch is parked fails the run too.
func TestExecute_ForceForwardOfAParkedBranchWhoseCurrentNodeWriteIsRefusedFailsTheRun(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	fakes.failFor = func(name string, in any) error {
		switch v := in.(type) {
		case port.CreateTaskInput:
			if v.NodeKey == "billing/Charge" {
				return wflRefuse("create billing/Charge")
			}
		case port.UpdateInstanceNodesInput:
			if len(v.NodeKeys) == 1 && v.NodeKeys[0] == "billing/Charge" {
				return wflRefuse(name)
			}
		}
		return nil
	}
	wflRegisterAll(env, fakes, wflForkOnly())
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("warehouse", "Pack", 1))
	wflSignalAt(env, 5*time.Second, "instance-force-forward", adminSignalWire{AdminUserID: "admin-7", TargetNodeKey: "billing/Charge", RecordVersion: 9})

	_, err := wflStart(t, env, fakes, "")

	require.Error(t, err)
	assert.Contains(t, err.Error(), port.ActivityUpdateInstanceNodes)
	statuses := wflStatuses(fakes)
	assert.Equal(t, domain.InstanceStatusFailed, statuses[len(statuses)-1])
	audits := wflInputs[port.RecordForceRouteInput](fakes, port.ActivityRecordForceRoute)
	require.Len(t, audits, 1)
	assert.Equal(t, wflTenant, audits[0].TenantID)
}

// A force-forward inside a parallel step whose route record is refused changes
// nothing: the branch keeps its task and the instance still completes.
func TestExecute_ForceForwardInAParallelStepWhoseAuditWriteIsRefusedIsDropped(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	fakes.failFor = func(name string, _ any) error {
		if name == port.ActivityRecordForceRoute {
			return wflRefuse(name)
		}
		return nil
	}
	wflRegisterAll(env, fakes, wflForkOnly())
	wflSignalAt(env, 5*time.Second, "instance-force-forward", adminSignalWire{AdminUserID: "admin-7", TargetDeptID: "billing", TargetNodeKey: "billing/Charge", RecordVersion: 9})
	wflSignalAt(env, 8*time.Second, "stage-transition", wflDone("warehouse", "Pack", 1))
	wflSignalAt(env, 10*time.Second, "stage-transition", wflDone("billing", "Charge", 1))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	assert.Equal(t, 1, fakes.attemptsOf(port.ActivityRecordForceRoute))
	assert.Equal(t, 1, countNode(wflNodesCreated(fakes), "billing/Charge"))
}
