package workflow_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// wflFakes is a set of hand-rolled activity bodies that record every input
// they receive (tenant, instance and resource ids included) and fail on
// demand, attempt by attempt.
type wflFakes struct {
	mu       sync.Mutex
	failWith func(name string, attempt int) error
	failFor  func(name string, in any) error
	attempts map[string]int
	inputs   map[string][]any
}

func wflNewFakes() *wflFakes {
	return &wflFakes{attempts: map[string]int{}, inputs: map[string][]any{}}
}

func (f *wflFakes) begin(name string, in any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attempts[name]++
	f.inputs[name] = append(f.inputs[name], in)
	if f.failFor != nil {
		if err := f.failFor(name, in); err != nil {
			return err
		}
	}
	if f.failWith == nil {
		return nil
	}
	return f.failWith(name, f.attempts[name])
}

func (f *wflFakes) attemptsOf(name string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.attempts[name]
}

func wflInputs[In any](f *wflFakes, name string) []In {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]In, 0, len(f.inputs[name]))
	for _, in := range f.inputs[name] {
		out = append(out, in.(In))
	}
	return out
}

func wflRegisterVoid[In any](env *testsuite.TestWorkflowEnvironment, f *wflFakes, name string) {
	env.RegisterActivityWithOptions(
		func(_ context.Context, in In) error { return f.begin(name, in) },
		activity.RegisterOptions{Name: name},
	)
}

func wflRegisterValue[In, Out any](env *testsuite.TestWorkflowEnvironment, f *wflFakes, name string, result func(In) Out) {
	env.RegisterActivityWithOptions(
		func(_ context.Context, in In) (Out, error) {
			if err := f.begin(name, in); err != nil {
				var zero Out
				return zero, err
			}
			return result(in), nil
		},
		activity.RegisterOptions{Name: name},
	)
}

// wflRegisterAll registers every activity the interpreter calls; the compiled
// plan is served from collab and every task id is its node key.
func wflRegisterAll(env *testsuite.TestWorkflowEnvironment, f *wflFakes, collab *dsl.CompiledCollaboration) {
	wflRegisterValue(env, f, port.ActivityGetCompiledPlan, func(port.GetCompiledPlanInput) port.GetCompiledPlanOutput {
		out := *collab
		if out.SchemaVersion == 0 {
			out.SchemaVersion = dsl.CurrentSchemaVersion
		}
		return port.GetCompiledPlanOutput{Collaboration: out}
	})
	wflRegisterValue(env, f, port.ActivityCreateTask, func(in port.CreateTaskInput) port.CreateTaskOutput {
		return port.CreateTaskOutput{TaskID: string(in.NodeKey)}
	})
	wflRegisterValue(env, f, port.ActivityCompleteAssignment, func(port.CompleteAssignmentInput) port.CompleteAssignmentOutput {
		return port.CompleteAssignmentOutput{AllDone: true}
	})
	wflRegisterVoid[port.UpdateInstanceNodesInput](env, f, port.ActivityUpdateInstanceNodes)
	wflRegisterVoid[port.UpdateInstanceStatusInput](env, f, port.ActivityUpdateInstanceStatus)
	wflRegisterVoid[port.DeferTaskInput](env, f, port.ActivityDeferTask)
	wflRegisterVoid[port.RecordForceRouteInput](env, f, port.ActivityRecordForceRoute)
	wflRegisterVoid[port.RecordSLAWarningInput](env, f, port.ActivityRecordSLAWarning)
	wflRegisterVoid[port.RecordSLABreachInput](env, f, port.ActivityRecordSLABreach)
	wflRegisterVoid[port.PauseInstanceInput](env, f, port.ActivityPauseInstance)
	wflRegisterVoid[port.ResumeInstanceInput](env, f, port.ActivityResumeInstance)
	wflRegisterVoid[port.CancelInstanceInput](env, f, port.ActivityCancelInstance)
	wflRegisterVoid[port.ReassignAssignmentInput](env, f, port.ActivityReassignAssignment)
	wflRegisterVoid[port.UpdateTaskStatusInput](env, f, port.ActivityUpdateTaskStatus)
}

const (
	wflTenant   = "tenant-1"
	wflInstance = "instance-1"
)

// wflAnswerWire mirrors the unexported outside-answer signal payload by field
// name.
type wflAnswerWire struct {
	Key       string
	Variables map[string]any
}

// wflStart starts Execute for the standard test instance and waits for it to
// end; the returned error is the workflow's own, the status is the one the
// last status write carried.
func wflStart(t *testing.T, env *testsuite.TestWorkflowEnvironment, f *wflFakes, ctxJSON string) (domain.InstanceStatus, error) {
	t.Helper()
	env.SetStartTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	env.ExecuteWorkflow(wfengine.Execute, wfengine.ExecuteInput{
		TenantID: wflTenant, InstanceID: wflInstance, VersionID: "version-1", ContextJSON: ctxJSON,
	})
	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	err := env.GetWorkflowError()
	if err != nil {
		return "", err
	}
	var out wfengine.ExecuteOutput
	if gerr := env.GetWorkflowResult(&out); gerr != nil {
		t.Fatalf("GetWorkflowResult: %v", gerr)
	}
	return out.Status, nil
}

// wflSignalAt sends a signal to the standard instance after delay.
func wflSignalAt(env *testsuite.TestWorkflowEnvironment, delay time.Duration, signal string, payload any) {
	env.RegisterDelayedCallback(func() { env.SignalWorkflow(signal+":"+wflInstance, payload) }, delay)
}

// wflDone is the completion signal of a task stage.
func wflDone(dept, node string, visit int64) stageTransitionWire {
	return stageTransitionWire{DeptID: dept, NodeID: node, ResultJSON: "{}", RecordVersion: 1, VisitCount: visit}
}

// wflDoneStage is the completion signal of a stage that has no node id, named
// by its type.
func wflDoneStage(dept, stage string, visit int64) stageTransitionWire {
	return stageTransitionWire{DeptID: dept, ToStage: stage, ResultJSON: "{}", RecordVersion: 1, VisitCount: visit}
}

// wflOneDept is a collaboration with one department and a sequential run of
// it.
func wflOneDept(id string, stages ...dsl.StageDef) *dsl.CompiledCollaboration {
	return &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name:        "main",
			Departments: []dsl.DepartmentDef{{ID: id, Stages: stages}},
			Execution:   dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{id}}}},
		}},
	}
}

// wflStatuses lists the statuses the instance status activity was asked to
// write, in order.
func wflStatuses(f *wflFakes) []domain.InstanceStatus {
	var out []domain.InstanceStatus
	for _, in := range wflInputs[port.UpdateInstanceStatusInput](f, port.ActivityUpdateInstanceStatus) {
		out = append(out, in.Status)
	}
	return out
}
