package workflow_test

import (
	"encoding/json"
	"reflect"
	"sync"
	"testing"

	"go.temporal.io/sdk/testsuite"
	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// externalAnswerWire mirrors internal/workflow's unexported external-answer
// payload by field name.
type externalAnswerWire struct {
	Key       string
	Variables map[string]any
}

// The first checkpoint after the activity round trips (see
// TestExecute_GetWorkflowStatusQuery for why 1ms is not enough).
const (
	afterStart  = 1 * tick
	afterAnswer = 2 * tick
	afterThat   = 3 * tick
)

// contextRecorder records, per node key, the context_json each task was
// created with.
type contextRecorder struct {
	mu   sync.Mutex
	seen map[string]string
}

func (r *contextRecorder) hooks() *activityHooks {
	r.seen = map[string]string{}
	return &activityHooks{createTask: func(in port.CreateTaskInput) (port.CreateTaskOutput, error) {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.seen[string(in.NodeKey)] = in.ContextJSON
		return port.CreateTaskOutput{TaskID: string(in.NodeKey)}, nil
	}}
}

func (r *contextRecorder) variables(t *testing.T, nodeKey string) map[string]any {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	raw, ok := r.seen[nodeKey]
	if !ok {
		t.Fatalf("no task was created for %s; tasks: %v", nodeKey, r.seen)
	}
	vars := map[string]any{}
	if raw != "" {
		if err := json.Unmarshal([]byte(raw), &vars); err != nil {
			t.Fatalf("context_json of %s is not an object: %v", nodeKey, err)
		}
	}
	return vars
}

func runToCompletion(t *testing.T, env *testsuite.TestWorkflowEnvironment, collab *dsl.CompiledCollaboration, ctxJSON string) domain.InstanceStatus {
	t.Helper()
	env.ExecuteWorkflow(wfengine.Execute, wfengine.ExecuteInput{
		TenantID: "tenant-1", InstanceID: "instance-1", VersionID: "version-1", ContextJSON: ctxJSON,
	})
	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	var out wfengine.ExecuteOutput
	if err := env.GetWorkflowResult(&out); err != nil {
		t.Fatalf("GetWorkflowResult: %v", err)
	}
	return out.Status
}

func opsCollaboration(stages ...dsl.StageDef) *dsl.CompiledCollaboration {
	return &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name:        "main",
			Departments: []dsl.DepartmentDef{{ID: "Ops", Stages: stages}},
			Execution:   dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"Ops"}}}},
		}},
	}
}

// TestExecute_CompletedTaskResultsBecomeVariables pins that what a task
// completes with is an instance variable the tasks after it are created with:
// a connector's mapped output reaches a later connector's input this way.
func TestExecute_CompletedTaskResultsBecomeVariables(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	collab := opsCollaboration(
		dsl.StageDef{Type: "prep", NodeID: "First"},
		dsl.StageDef{Type: "prep", NodeID: "Second"},
	)
	rec := &contextRecorder{}
	registerFakeActivities(env, collab, rec.hooks())

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "Ops", NodeID: "First", ResultJSON: `{"answer":"yes","count":2}`, RecordVersion: 1, VisitCount: 1,
		})
	}, afterStart)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "Ops", NodeID: "Second", ResultJSON: `{"answer":"later"}`, RecordVersion: 1, VisitCount: 1,
		})
	}, afterAnswer)

	if got := runToCompletion(t, env, collab, `{"tenderId":"T-7"}`); got != domain.InstanceStatusCompleted {
		t.Fatalf("Status = %v, want COMPLETED", got)
	}
	want := map[string]any{"tenderId": "T-7", "answer": "yes", "count": float64(2)}
	if got := rec.variables(t, "Ops/Second"); !reflect.DeepEqual(got, want) {
		t.Errorf("Second was created with %v, want %v", got, want)
	}
}

// TestExecute_TaskResults_BeforeTheChangeAreNotVariables pins that a history
// recorded before change id task-result-variables replays as it ran: the task
// after one is created with the start context alone.
func TestExecute_TaskResults_BeforeTheChangeAreNotVariables(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.OnGetVersion("task-result-variables", wf.DefaultVersion, 1).Return(wf.DefaultVersion)
	collab := opsCollaboration(
		dsl.StageDef{Type: "prep", NodeID: "First"},
		dsl.StageDef{Type: "prep", NodeID: "Second"},
	)
	rec := &contextRecorder{}
	registerFakeActivities(env, collab, rec.hooks())

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "Ops", NodeID: "First", ResultJSON: `{"answer":"yes"}`, RecordVersion: 1, VisitCount: 1,
		})
	}, afterStart)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "Ops", NodeID: "Second", ResultJSON: `{}`, RecordVersion: 1, VisitCount: 1,
		})
	}, afterAnswer)

	if got := runToCompletion(t, env, collab, `{"tenderId":"T-7"}`); got != domain.InstanceStatusCompleted {
		t.Fatalf("Status = %v, want COMPLETED", got)
	}
	if got := rec.variables(t, "Ops/Second"); !reflect.DeepEqual(got, map[string]any{"tenderId": "T-7"}) {
		t.Errorf("Second was created with %v, want the start context alone", got)
	}
}

// TestExecute_OutsideAnswerCompletesAReceiveTask pins the way in for an
// outside party: a receive task no send task feeds waits for an answer that
// names its node key, and the answer's variables reach the tasks after it.
func TestExecute_OutsideAnswerCompletesAReceiveTask(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	collab := opsCollaboration(
		dsl.StageDef{Type: "receive_task", NodeID: "Receive_Bid", Extras: map[string]string{"message": "bid-received"}},
		dsl.StageDef{Type: "prep", NodeID: "Award"},
	)
	rec := &contextRecorder{}
	registerFakeActivities(env, collab, rec.hooks())

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(port.SignalInstanceExternalAnswer+":instance-1", externalAnswerWire{
			Key: "Ops/Receive_Bid", Variables: map[string]any{"bid": "42"},
		})
	}, afterStart)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "Ops", NodeID: "Award", ResultJSON: `{}`, RecordVersion: 1, VisitCount: 1,
		})
	}, afterAnswer)

	if got := runToCompletion(t, env, collab, ""); got != domain.InstanceStatusCompleted {
		t.Fatalf("Status = %v, want COMPLETED", got)
	}
	if got := rec.variables(t, "Ops/Award"); !reflect.DeepEqual(got, map[string]any{"bid": "42"}) {
		t.Errorf("Award was created with %v, want the answer's bid", got)
	}
}

// TestExecute_AReceiveTaskIsTheInstancesCurrentNode pins that an instance
// waiting on a message or an outside answer reports that wait in
// current_node_keys (execution LLD §4.1), so a caller can see which node to
// answer. A Definition plan whose main pool starts at a message wait showed
// no node at all.
func TestExecute_AReceiveTaskIsTheInstancesCurrentNode(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	collab := opsCollaboration(
		dsl.StageDef{Type: "receive_task", NodeID: "Receive_Bid", Extras: map[string]string{"message": "bid-received"}},
		dsl.StageDef{Type: "prep", NodeID: "Award"},
	)
	var mu sync.Mutex
	var recorded [][]domain.NodeKey
	hooks := &activityHooks{updateInstanceNodes: func(in port.UpdateInstanceNodesInput) {
		mu.Lock()
		defer mu.Unlock()
		recorded = append(recorded, in.NodeKeys)
	}}
	registerFakeActivities(env, collab, hooks)

	var atWait [][]domain.NodeKey
	env.RegisterDelayedCallback(func() {
		mu.Lock()
		atWait = append(atWait, recorded...)
		mu.Unlock()
		env.SignalWorkflow(port.SignalInstanceExternalAnswer+":instance-1", externalAnswerWire{Key: "Ops/Receive_Bid"})
	}, afterStart)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "Ops", NodeID: "Award", ResultJSON: `{}`, RecordVersion: 1, VisitCount: 1,
		})
	}, afterAnswer)

	if got := runToCompletion(t, env, collab, ""); got != domain.InstanceStatusCompleted {
		t.Fatalf("Status = %v, want COMPLETED", got)
	}
	want := [][]domain.NodeKey{{"Ops/Receive_Bid"}}
	if !reflect.DeepEqual(atWait, want) {
		t.Errorf("current nodes recorded while waiting = %v, want %v", atWait, want)
	}
}

// TestExecute_OutsideAnswerBeforeTheWait_IsDropped pins that an answer for a
// step the instance has not reached is not kept: it must not complete the
// step later, or leak its variables.
func TestExecute_OutsideAnswerBeforeTheWait_IsDropped(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	collab := opsCollaboration(
		dsl.StageDef{Type: "prep", NodeID: "Prepare"},
		dsl.StageDef{Type: "receive_task", NodeID: "Receive_Bid", Extras: map[string]string{"message": "bid-received"}},
		dsl.StageDef{Type: "prep", NodeID: "Award"},
	)
	rec := &contextRecorder{}
	registerFakeActivities(env, collab, rec.hooks())

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(port.SignalInstanceExternalAnswer+":instance-1", externalAnswerWire{
			Key: "Ops/Receive_Bid", Variables: map[string]any{"early": "yes"},
		})
	}, afterStart)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "Ops", NodeID: "Prepare", ResultJSON: `{}`, RecordVersion: 1, VisitCount: 1,
		})
	}, afterAnswer)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(port.SignalInstanceExternalAnswer+":instance-1", externalAnswerWire{
			Key: "Ops/Receive_Bid", Variables: map[string]any{"bid": "42"},
		})
	}, afterThat)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "Ops", NodeID: "Award", ResultJSON: `{}`, RecordVersion: 1, VisitCount: 1,
		})
	}, 4*tick)

	if got := runToCompletion(t, env, collab, ""); got != domain.InstanceStatusCompleted {
		t.Fatalf("Status = %v, want COMPLETED", got)
	}
	if got := rec.variables(t, "Ops/Award"); !reflect.DeepEqual(got, map[string]any{"bid": "42"}) {
		t.Errorf("Award was created with %v, want only the answer given while Receive_Bid waited", got)
	}
}

// TestExecute_OutsideAnswerFiresAMessageBoundary pins the way in for a
// message boundary no send task feeds: it is addressed by its host and its
// own id, and a terminating one ends the instance's path.
func TestExecute_OutsideAnswerFiresAMessageBoundary(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	collab := opsCollaboration(dsl.StageDef{
		Type: "prep", NodeID: "Prepare",
		BoundaryMessage: &dsl.MessagePath{MessageName: "withdraw", BoundaryID: "Boundary_Withdraw", Interrupting: true, Terminates: true},
	})
	var superseded []string
	var mu sync.Mutex
	hooks := &activityHooks{updateTaskStatus: func(in port.UpdateTaskStatusInput) error {
		mu.Lock()
		defer mu.Unlock()
		if in.Status == domain.TaskStatusSuperseded {
			superseded = append(superseded, in.TaskID)
		}
		return nil
	}}
	registerFakeActivities(env, collab, hooks)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(port.SignalInstanceExternalAnswer+":instance-1", externalAnswerWire{Key: "Ops/Prepare#Boundary_Withdraw"})
	}, afterStart)

	if got := runToCompletion(t, env, collab, ""); got != domain.InstanceStatusCompleted {
		t.Fatalf("Status = %v, want COMPLETED", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(superseded, []string{"Ops/Prepare"}) {
		t.Errorf("superseded tasks = %v, want the interrupted Prepare", superseded)
	}
}

// TestExecute_WaitingExternalQuery pins what the Execution Service asks before
// it accepts an answer: the keys of the outside waits the instance is in.
func TestExecute_WaitingExternalQuery(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	collab := opsCollaboration(
		dsl.StageDef{Type: "receive_task", NodeID: "Receive_Bid", Extras: map[string]string{"message": "bid-received"}},
	)
	registerFakeActivities(env, collab, nil)

	var got []string
	env.RegisterDelayedCallback(func() {
		result, err := env.QueryWorkflow(port.QueryWaitingExternal)
		if err != nil {
			t.Errorf("QueryWorkflow: %v", err)
			return
		}
		if err := result.Get(&got); err != nil {
			t.Errorf("decoding query result: %v", err)
		}
	}, afterStart)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(port.SignalInstanceExternalAnswer+":instance-1", externalAnswerWire{Key: "Ops/Receive_Bid"})
	}, afterAnswer)

	if status := runToCompletion(t, env, collab, ""); status != domain.InstanceStatusCompleted {
		t.Fatalf("Status = %v, want COMPLETED", status)
	}
	if !reflect.DeepEqual(got, []string{"Ops/Receive_Bid"}) {
		t.Errorf("waiting external = %v, want [Ops/Receive_Bid]", got)
	}
}

// TestExternalAnswerNames_MatchThePort pins that the signal and query names the
// service layer sends and asks are the ones the interpreter listens on.
func TestExternalAnswerNames_MatchThePort(t *testing.T) {
	if port.SignalInstanceExternalAnswer != wfengine.SignalInstanceExternalAnswer {
		t.Errorf("signal: port %q, interpreter %q", port.SignalInstanceExternalAnswer, wfengine.SignalInstanceExternalAnswer)
	}
	if port.QueryWaitingExternal != wfengine.QueryWaitingExternal {
		t.Errorf("query: port %q, interpreter %q", port.QueryWaitingExternal, wfengine.QueryWaitingExternal)
	}
}

// TestExecute_OutsideAnswerFiresASubWorkflowsMessageBoundary pins the host of
// a boundary on a subprocess or call: the step's own id.
func TestExecute_OutsideAnswerFiresASubWorkflowsMessageBoundary(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	collab := &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name:        "main",
			Departments: []dsl.DepartmentDef{{ID: "Ops", Stages: []dsl.StageDef{{Type: "prep", NodeID: "Inner"}}}},
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{SubWorkflow: &dsl.SubWorkflowStep{
				NodeID: "Sub", Name: "Sub",
				Plan:         dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"Ops"}}}},
				MessagePaths: []dsl.MessagePath{{MessageName: "cancel", BoundaryID: "Boundary_Cancel", Interrupting: true, Terminates: true}},
			}}}},
		}},
	}
	registerFakeActivities(env, collab, nil)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(port.SignalInstanceExternalAnswer+":instance-1", externalAnswerWire{Key: "Sub#Boundary_Cancel"})
	}, afterStart)

	if got := runToCompletion(t, env, collab, ""); got != domain.InstanceStatusCompleted {
		t.Fatalf("Status = %v, want COMPLETED", got)
	}
}
