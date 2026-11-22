package workflow_test

import (
	"reflect"
	"testing"

	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// A key that was answered leaves the waiting list, and a second answer for it changes nothing:
// the instance stays in the next wait and the late answer's variables are not merged.
func TestExecute_OutsideWaitAnsweredTwice_ConsumedKeyLeavesTheQueryAndTheRepeatIsDropped(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	collab := opsCollaboration(
		dsl.StageDef{Type: "receive_task", NodeID: "Receive_A", Extras: map[string]string{"message": "a-received"}},
		dsl.StageDef{Type: "receive_task", NodeID: "Receive_B", Extras: map[string]string{"message": "b-received"}},
		dsl.StageDef{Type: "prep", NodeID: "Final"},
	)
	rec := &contextRecorder{}
	registerFakeActivities(env, collab, rec.hooks())

	var waitingBefore, waitingAfterAnswer, waitingAfterRepeat []string
	query := func(into *[]string) func() {
		return func() {
			result, err := env.QueryWorkflow(port.QueryWaitingExternal)
			if err != nil {
				t.Errorf("QueryWorkflow: %v", err)
				return
			}
			if err := result.Get(into); err != nil {
				t.Errorf("decoding query result: %v", err)
			}
		}
	}
	signal := func(key string, vars map[string]any) func() {
		return func() {
			env.SignalWorkflow(port.SignalInstanceExternalAnswer+":instance-1", externalAnswerWire{Key: key, Variables: vars})
		}
	}
	env.RegisterDelayedCallback(query(&waitingBefore), 1*tick)
	env.RegisterDelayedCallback(signal("Ops/Receive_A", map[string]any{"bid": "first"}), 2*tick)
	env.RegisterDelayedCallback(query(&waitingAfterAnswer), 3*tick)
	env.RegisterDelayedCallback(signal("Ops/Receive_A", map[string]any{"bid": "second", "late": "yes"}), 4*tick)
	env.RegisterDelayedCallback(query(&waitingAfterRepeat), 5*tick)
	env.RegisterDelayedCallback(signal("Ops/Receive_B", nil), 6*tick)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "Ops", NodeID: "Final", ResultJSON: `{}`, RecordVersion: 1, VisitCount: 1,
		})
	}, 7*tick)

	if status := runToCompletion(t, env, collab, ""); status != domain.InstanceStatusCompleted {
		t.Fatalf("Status = %v, want COMPLETED", status)
	}

	if want := []string{"Ops/Receive_A"}; !reflect.DeepEqual(waitingBefore, want) {
		t.Errorf("waiting before the answer = %v, want %v", waitingBefore, want)
	}
	if want := []string{"Ops/Receive_B"}; !reflect.DeepEqual(waitingAfterAnswer, want) {
		t.Errorf("waiting after the answer = %v, want only the next wait %v", waitingAfterAnswer, want)
	}
	if want := []string{"Ops/Receive_B"}; !reflect.DeepEqual(waitingAfterRepeat, want) {
		t.Errorf("waiting after the repeated answer = %v, want %v", waitingAfterRepeat, want)
	}
	vars := rec.variables(t, "Ops/Final")
	if vars["bid"] != "first" {
		t.Errorf("bid = %v, want the first answer's value", vars["bid"])
	}
	if _, merged := vars["late"]; merged {
		t.Errorf("the repeated answer's variables were merged: %v", vars)
	}
}
