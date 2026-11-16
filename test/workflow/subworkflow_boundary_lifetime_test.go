package workflow_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// Once an interrupting timer path of a call has fired, the call's other
// boundaries stop listening: a message sent by the timer path is not taken by
// the call's message path but waits for the step that receives it.
func TestExecute_CallBoundariesStopListeningOnceAnInterruptingPathFired(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	collab := &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main",
			Departments: []dsl.DepartmentDef{
				{ID: "review", Stages: wflStages("Review")},
				{ID: "esc", Stages: []dsl.StageDef{{Type: "send_task", NodeID: "Notify", Extras: map[string]string{"message": "note"}}}},
				{ID: "after", Stages: []dsl.StageDef{{Type: "receive_task", NodeID: "Wait", Extras: map[string]string{"message": "note"}}}},
			},
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{
				{SubWorkflow: &dsl.SubWorkflowStep{
					NodeID: "Call", Name: "call",
					Plan:         dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"review"}}}},
					TimerPaths:   []dsl.TimerPath{{Duration: "1h", Interrupting: true, TargetDept: "esc"}},
					MessagePaths: []dsl.MessagePath{{MessageName: "note", Interrupting: false}},
				}},
				{Sequential: []string{"after"}},
			}},
		}},
	}
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, collab)

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	assert.Equal(t, []domain.NodeKey{"review/Review"}, wflNodesCreated(fakes))
}

// After a non-interrupting timer path of a call has fired and the call is
// still running, a failure inside the call that matches one of its error paths
// still takes that path: the error path runs and the run goes on after the call.
func TestExecute_CallErrorPathStillFiresAfterANonInterruptingPathFired(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	collab := &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main",
			Departments: []dsl.DepartmentDef{
				{ID: "review", Stages: wflStages("Review")},
				{ID: "esc", Stages: wflStages("Escalate")},
				{ID: "handler", Stages: wflStages("Handle")},
				{ID: "after", Stages: wflStages("Finish")},
			},
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{
				{SubWorkflow: &dsl.SubWorkflowStep{
					NodeID: "Call", Name: "call",
					Plan:       dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"review"}}}},
					TimerPaths: []dsl.TimerPath{{Duration: "1h", Interrupting: false, TargetDept: "esc"}},
					ErrorPaths: []dsl.ErrorPath{{TargetDept: "handler"}},
				}},
				{Sequential: []string{"after"}},
			}},
		}},
	}
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, collab)
	wflSignalAt(env, 2*time.Hour, "stage-fail", stageFailWire{DeptID: "review", NodeID: "Review", ErrorClass: "rejected", VisitCount: 1})
	wflSignalAt(env, 3*time.Hour, "stage-transition", wflDone("handler", "Handle", 1))
	wflSignalAt(env, 4*time.Hour, "stage-transition", wflDone("after", "Finish", 1))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	assert.Equal(t, []domain.NodeKey{"review/Review", "esc/Escalate", "handler/Handle", "after/Finish"}, wflNodesCreated(fakes))
}
