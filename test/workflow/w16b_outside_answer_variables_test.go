package workflow_test

import (
	"reflect"
	"testing"

	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/wflclock"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// An outside answer with no variables still completes the wait and leaves the instance's variables as they were.
func TestExecute_OutsideAnswerWithNoVariablesCompletesTheWaitAndMergesNothing(t *testing.T) {
	cases := []struct {
		name      string
		variables map[string]any
	}{
		{name: "null variables", variables: nil},
		{name: "an empty object", variables: map[string]any{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			collab := opsCollaboration(
				dsl.StageDef{Type: "receive_task", NodeID: "Receive_Bid", Extras: map[string]string{"message": "bid-received"}},
				dsl.StageDef{Type: "prep", NodeID: "Award"},
			)
			rec := &contextRecorder{}
			registerFakeActivities(env, collab, rec.hooks())

			wflclock.After(env, func() {
				env.SignalWorkflow(port.SignalInstanceExternalAnswer+":instance-1", externalAnswerWire{
					Key: "Ops/Receive_Bid", Variables: tc.variables,
				})
			}, afterStart)
			wflclock.After(env, func() {
				env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
					DeptID: "Ops", NodeID: "Award", ResultJSON: `{}`, RecordVersion: 1, VisitCount: 1,
				})
			}, afterAnswer)

			if got := runToCompletion(t, env, collab, `{"tenderId":"T-7"}`); got != domain.InstanceStatusCompleted {
				t.Fatalf("Status = %v, want COMPLETED: the answer must still complete the wait", got)
			}
			if got, want := rec.variables(t, "Ops/Award"), map[string]any{"tenderId": "T-7"}; !reflect.DeepEqual(got, want) {
				t.Errorf("Award was created with %v, want the variables unchanged: %v", got, want)
			}
		})
	}
}
