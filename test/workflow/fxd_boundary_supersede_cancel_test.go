package workflow_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/wflclock"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// An interrupting boundary supersedes its host task. When the run is cancelled
// while that write is in flight, the write is repeated on a context that
// outlives the cancel, so the host task is never left open (a cancel does not
// supersede it: the boundary already took the stage out of the pending set).
func TestExecute_CancelWhileAnInterruptingBoundarySupersedesItsTaskStillSupersedesIt(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	collab := &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main",
			Departments: []dsl.DepartmentDef{
				{ID: "ops", Stages: []dsl.StageDef{
					{Type: "prep", NodeID: "Prepare", BoundaryTimer: &dsl.BoundaryTimer{Duration: "1h", Interrupting: true, TargetDept: "esc"}},
				}},
				{ID: "esc", Stages: wflStages("Escalate")},
			},
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"ops"}}}},
		}},
	}
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, collab)
	record := func(_ context.Context, in port.UpdateTaskStatusInput) error {
		return fakes.begin(port.ActivityUpdateTaskStatus, in)
	}
	env.OnActivity(port.ActivityUpdateTaskStatus, mock.Anything, mock.MatchedBy(func(in port.UpdateTaskStatusInput) bool {
		return in.Status == domain.TaskStatusSuperseded
	})).After(3 * time.Second).Return(record).Once()
	env.OnActivity(port.ActivityUpdateTaskStatus, mock.Anything, mock.Anything).Return(record)
	// The boundary fires at one hour and its first supersede write takes three
	// seconds, so the cancel lands while that write is in flight and the write
	// never finishes; the repeat on the outliving context runs at once.
	wflclock.After(env, func() {
		env.SignalWorkflow("instance-cancel:"+wflInstance, adminSignalWire{AdminUserID: "admin-9", RecordVersion: 4})
	}, time.Hour+time.Second)

	_, _ = wflStart(t, env, fakes, "")

	var superseded int
	for _, u := range wflInputs[port.UpdateTaskStatusInput](fakes, port.ActivityUpdateTaskStatus) {
		if u.TaskID == "ops/Prepare" && u.Status == domain.TaskStatusSuperseded {
			require.Equal(t, wflTenant, u.TenantID)
			superseded++
		}
	}
	assert.Equal(t, 1, superseded, "the repeat after the cancel superseded the task")
	assert.Equal(t, []domain.NodeKey{"ops/Prepare"}, wflNodesCreated(fakes), "the escalation path did not start")
}
