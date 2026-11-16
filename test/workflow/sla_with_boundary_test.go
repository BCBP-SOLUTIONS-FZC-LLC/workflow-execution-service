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

// A non-interrupting boundary that fires on an open task starts its side path
// and leaves the task's SLA timers running: the overdue task still records its
// warning and its breach, and the task then completes normally.
func TestExecute_NonInterruptingBoundaryKeepsTheTasksSLATimers(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	collab := &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main",
			Departments: []dsl.DepartmentDef{
				{ID: "ops", Stages: []dsl.StageDef{{
					Type: "prep", NodeID: "Prepare",
					FollowUpDate:  "2026-01-01T01:00:00Z",
					DueDate:       "2026-01-01T02:00:00Z",
					BoundaryTimer: &dsl.BoundaryTimer{Duration: "30m", Interrupting: false, TargetDept: "esc"},
				}}},
				{ID: "esc", Stages: wflStages("Escalate")},
			},
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"ops"}}}},
		}},
	}
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, collab)
	wflSignalAt(env, 4*time.Hour, "stage-transition", wflDone("ops", "Prepare", 1))
	wflSignalAt(env, 4*time.Hour, "stage-transition", wflDone("esc", "Escalate", 1))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	assert.Equal(t, []domain.NodeKey{"ops/Prepare", "esc/Escalate"}, wflNodesCreated(fakes), "the side path started")
	warnings := wflInputs[port.RecordSLAWarningInput](fakes, port.ActivityRecordSLAWarning)
	require.Len(t, warnings, 1, "the follow-up date still raised its warning")
	assert.Equal(t, "ops/Prepare", warnings[0].TaskID)
	assert.Equal(t, wflTenant, warnings[0].TenantID)
	assert.Equal(t, wflInstance, warnings[0].InstanceID)
	breaches := wflInputs[port.RecordSLABreachInput](fakes, port.ActivityRecordSLABreach)
	require.Len(t, breaches, 1, "the due date still raised its breach")
	assert.Equal(t, "ops/Prepare", breaches[0].TaskID)
	assert.Equal(t, wflTenant, breaches[0].TenantID)
	assert.Equal(t, wflInstance, breaches[0].InstanceID)
}
