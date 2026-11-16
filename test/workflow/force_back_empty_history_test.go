package workflow_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"go.temporal.io/sdk/testsuite"
)

// A force-back that arrives when no completed step is left to return to
// changes nothing: the open task stays open, no task is created again and no
// route is recorded. The first force-back uses up the only completed step.
func TestExecute_ForceBackWithNoCompletedStepLeftChangesNothing(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, wflTwoDeptsCollab())
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDone("deptA", "A1", 1))
	wflSignalAt(env, 4*time.Second, "instance-force-back", adminSignalWire{AdminUserID: "admin-1", RecordVersion: 3})
	wflSignalAt(env, 6*time.Second, "instance-force-back", adminSignalWire{AdminUserID: "admin-2", RecordVersion: 4})
	wflSignalAt(env, 8*time.Second, "stage-transition", wflDone("deptA", "A1", 2))
	wflSignalAt(env, 10*time.Second, "stage-transition", wflDone("deptB", "B1", 2))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	assert.Equal(t, []domain.NodeKey{"deptA/A1", "deptB/B1", "deptA/A1", "deptB/B1"}, wflNodesCreated(fakes),
		"the refused force-back did not restart the stage")
	audits := wflInputs[port.RecordForceRouteInput](fakes, port.ActivityRecordForceRoute)
	require.Len(t, audits, 1)
	assert.Equal(t, "admin-1", audits[0].AdminUserID)
	assert.Equal(t, wflTenant, audits[0].TenantID)
	assert.Equal(t, wflInstance, audits[0].InstanceID)
	for _, in := range wflInputs[port.UpdateTaskStatusInput](fakes, port.ActivityUpdateTaskStatus) {
		assert.NotEqual(t, domain.TaskStatusSuperseded, in.Status, "task %s was superseded", in.TaskID)
	}
}
