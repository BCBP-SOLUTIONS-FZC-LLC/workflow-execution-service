package workflow_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

const (
	w28aHost        = "Ops/Prepare"
	w28aBoundaryKey = "Ops/Prepare#B1"
)

// w28aHostWithBoundary is a department whose only task has a message boundary
// nobody sends to, so it is fired by hand; Next and Side are the departments
// the boundary may lead to.
func w28aHostWithBoundary(m dsl.MessagePath) *dsl.CompiledCollaboration {
	m.MessageName, m.BoundaryID = "outside", "B1"
	return &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main",
			Departments: []dsl.DepartmentDef{
				{ID: "Ops", Stages: []dsl.StageDef{{Type: "prep", NodeID: "Prepare", BoundaryMessage: &m}}},
				{ID: "Next", Stages: []dsl.StageDef{{Type: "prep", NodeID: "After"}}},
			},
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"Ops"}}}},
		}},
	}
}

func w28aContextOf(t *testing.T, fakes *wflFakes, node domain.NodeKey) map[string]any {
	t.Helper()
	for _, in := range wflInputs[port.CreateTaskInput](fakes, port.ActivityCreateTask) {
		if in.NodeKey != node {
			continue
		}
		vars := map[string]any{}
		require.NoError(t, json.Unmarshal([]byte(in.ContextJSON), &vars))
		return vars
	}
	t.Fatalf("no task was created for %s; created: %v", node, wflNodesCreated(fakes))
	return nil
}

func w28aSupersededOf(fakes *wflFakes) []string { return w19bSupersededTasks(fakes) }

func TestExecute_OutsideAnswerOverAnExistingVariable_TheAnswerWinsAndTheWaitIsReleased(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	collab := opsCollaboration(
		dsl.StageDef{Type: "receive_task", NodeID: "Receive_Bid", Extras: map[string]string{"message": "bid-received"}},
		dsl.StageDef{Type: "prep", NodeID: "Award"},
		dsl.StageDef{Type: "prep", NodeID: "Close"},
	)
	wflRegisterAll(env, fakes, collab)
	var before, after []string
	var tookBefore, tookAfter bool
	wflQueryWaitingAt(t, env, 1*time.Second, &before, &tookBefore)
	wflSignalAt(env, 2*time.Second, "instance-external-answer", wflAnswerWire{
		Key: "Ops/Receive_Bid", Variables: map[string]any{"bid": "new", "nested": map[string]any{"b": 2}},
	})
	wflQueryWaitingAt(t, env, 3*time.Second, &after, &tookAfter)
	wflSignalAt(env, 4*time.Second, "instance-external-answer", wflAnswerWire{
		Key: "Ops/Receive_Bid", Variables: map[string]any{"bid": "second"},
	})
	wflSignalAt(env, 6*time.Second, "stage-transition", wflDone("Ops", "Award", 1))
	wflSignalAt(env, 8*time.Second, "stage-transition", wflDone("Ops", "Close", 1))

	status, err := wflStart(t, env, fakes, `{"bid":"old","tender":"T-7","nested":{"a":1}}`)

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	require.True(t, tookBefore && tookAfter)
	assert.Equal(t, []string{"Ops/Receive_Bid"}, before, "the wait is listed while the instance waits")
	assert.Empty(t, after, "the wait is gone once the answer was accepted")
	want := map[string]any{"bid": "new", "tender": "T-7", "nested": map[string]any{"b": float64(2)}}
	assert.Equal(t, want, w28aContextOf(t, fakes, "Ops/Award"), "the answer wins over the old value; a nested object is replaced, not merged")
	assert.Equal(t, want, w28aContextOf(t, fakes, "Ops/Close"), "a second answer to the released wait changed nothing")
}

func TestExecute_FiringAnInterruptingMessageBoundaryContinuesFromItsTargetWithTheVariablesMerged(t *testing.T) {
	cases := []struct {
		name string
		path dsl.MessagePath
	}{
		{name: "target department", path: dsl.MessagePath{Interrupting: true, TargetDept: "Next"}},
		{name: "path steps", path: dsl.MessagePath{Interrupting: true, Steps: []dsl.ExecutionStep{{Sequential: []string{"Next"}}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			fakes := wflNewFakes()
			wflRegisterAll(env, fakes, w28aHostWithBoundary(tc.path))
			var before, after []string
			var tookBefore, tookAfter bool
			wflQueryWaitingAt(t, env, 1*time.Second, &before, &tookBefore)
			wflSignalAt(env, 2*time.Second, "instance-external-answer", wflAnswerWire{
				Key: w28aBoundaryKey, Variables: map[string]any{"reason": "withdrawn", "tender": "T-9", "nested": map[string]any{"c": 3}},
			})
			wflQueryWaitingAt(t, env, 4*time.Second, &after, &tookAfter)
			wflSignalAt(env, 6*time.Second, "stage-transition", wflDone("Next", "After", 1))

			status, err := wflStart(t, env, fakes, `{"tender":"T-7","nested":{"b":2}}`)

			require.NoError(t, err)
			assert.Equal(t, domain.InstanceStatusCompleted, status)
			require.True(t, tookBefore && tookAfter)
			assert.Equal(t, []string{w28aBoundaryKey}, before)
			assert.Empty(t, after, "the boundary's wait is released by the fire")
			assert.Equal(t, []domain.NodeKey{"Ops/Prepare", "Next/After"}, wflNodesCreated(fakes), "the path continues from the target")
			assert.Equal(t, []string{w28aHost}, w28aSupersededOf(fakes), "the interrupted host is superseded")
			assert.Equal(t, map[string]any{"reason": "withdrawn", "tender": "T-9", "nested": map[string]any{"c": float64(3)}},
				w28aContextOf(t, fakes, "Next/After"), "the variables were merged, top-level keys overwritten, before the path started")
			completions := wflInputs[port.CompleteAssignmentInput](fakes, port.ActivityCompleteAssignment)
			require.Len(t, completions, 1, "the host never completed; only the target's task did")
			assert.Equal(t, "Next/After", completions[0].TaskID)
			assert.Equal(t, wflTenant, completions[0].TenantID)
		})
	}
}

func TestExecute_FiringANonInterruptingMessageBoundaryRunsItsPathBesideAnOpenHost(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, w28aHostWithBoundary(dsl.MessagePath{Interrupting: false, TargetDept: "Next"}))
	var before, after []string
	var tookBefore, tookAfter bool
	wflQueryWaitingAt(t, env, 1*time.Second, &before, &tookBefore)
	wflSignalAt(env, 2*time.Second, "instance-external-answer", wflAnswerWire{Key: w28aBoundaryKey})
	wflQueryWaitingAt(t, env, 3*time.Second, &after, &tookAfter)
	var beside wfengine.WorkflowStatusQuery
	wflQueryStatusAt(t, env, 4*time.Second, &beside)
	wflSignalAt(env, 6*time.Second, "stage-transition", wflDone("Next", "After", 1))
	wflSignalAt(env, 8*time.Second, "stage-transition", wflDone("Ops", "Prepare", 1))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	require.True(t, tookBefore && tookAfter)
	assert.Equal(t, []string{w28aBoundaryKey}, before)
	assert.Empty(t, after, "the wait is released by the first fire, so the boundary cannot fire twice")
	assert.Equal(t, []domain.NodeKey{"Next/After", "Ops/Prepare"}, beside.ActiveTasks, "the host and the fired path's task were open together")
	assert.Equal(t, []domain.NodeKey{"Ops/Prepare", "Next/After"}, wflNodesCreated(fakes))
	assert.Empty(t, w28aSupersededOf(fakes), "the host was not superseded")
	completions := wflInputs[port.CompleteAssignmentInput](fakes, port.ActivityCompleteAssignment)
	require.Len(t, completions, 2)
	assert.Equal(t, "Next/After", completions[0].TaskID)
	assert.Equal(t, "Ops/Prepare", completions[1].TaskID, "completing the host later still resolved its stage")
	for _, c := range completions {
		assert.Equal(t, wflTenant, c.TenantID)
	}
}

func TestExecute_FiringAMessageBoundaryOfASubWorkflow_SupersedesTheChildsOpenTaskBeforeTheFiredPath(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	collab := &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main",
			Departments: []dsl.DepartmentDef{
				{ID: "Ops", Stages: []dsl.StageDef{{Type: "prep", NodeID: "Inner"}}},
				{ID: "Next", Stages: []dsl.StageDef{{Type: "prep", NodeID: "After"}}},
			},
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{SubWorkflow: &dsl.SubWorkflowStep{
				NodeID: "Sub", Name: "Sub",
				Plan:         dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"Ops"}}}},
				MessagePaths: []dsl.MessagePath{{MessageName: "cancel", BoundaryID: "Boundary_Cancel", Interrupting: true, TargetDept: "Next"}},
			}}}},
		}},
	}
	order := &w28aOrder{}
	fakes.failFor = func(_ string, in any) error {
		switch v := in.(type) {
		case port.CreateTaskInput:
			order.add("create:" + string(v.NodeKey))
		case port.UpdateTaskStatusInput:
			order.add("status:" + string(v.Status) + ":" + v.TaskID)
		}
		return nil
	}
	wflRegisterAll(env, fakes, collab)
	wflSignalAt(env, 2*time.Second, "instance-external-answer", wflAnswerWire{Key: "Sub#Boundary_Cancel"})
	wflSignalAt(env, 6*time.Second, "stage-transition", wflDone("Next", "After", 1))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	assert.Equal(t, []domain.NodeKey{"Ops/Inner", "Next/After"}, wflNodesCreated(fakes), "the fired path started after the child's task")
	assert.Equal(t, []string{"Ops/Inner"}, w28aSupersededOf(fakes), "the child's open task is superseded by the fire")
	superseding := order.indexOf("status:"+string(domain.TaskStatusSuperseded)+":Ops/Inner", 1)
	firedPath := order.indexOf("create:Next/After", 1)
	require.NotEqual(t, -1, superseding, "events: %v", order.snapshot())
	require.NotEqual(t, -1, firedPath, "events: %v", order.snapshot())
	assert.Less(t, superseding, firedPath, "the child's task was superseded before the fired path started; events: %v", order.snapshot())
	superseded := wflInputs[port.UpdateTaskStatusInput](fakes, port.ActivityUpdateTaskStatus)
	require.NotEmpty(t, superseded)
	assert.Equal(t, wflTenant, superseded[0].TenantID)
}
