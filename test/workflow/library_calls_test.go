package workflow_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl/dsltest"
)

// goldenCollaboration is workflow-models' dsltest.LibraryCalls as the
// GetCompiledPlan activity hands it to the workflow: decoded, calls expanded.
func goldenCollaboration(t *testing.T) *dsl.CompiledCollaboration {
	t.Helper()
	var c dsl.CompiledCollaboration
	if err := json.Unmarshal(dsltest.LibraryCalls(), &c); err != nil {
		t.Fatalf("decode golden: %v", err)
	}
	expanded, err := dsl.ExpandCalls(&c, domain.MaxPlanStages)
	if err != nil {
		t.Fatalf("expand golden: %v", err)
	}
	return expanded
}

type goldenTask struct{ dept, node, iam string }

// Each call of the one module runs its own tasks, in order, in the caller
// lane's IAM department, and the instance completes once all are done.
func TestExecute_LibraryCalls_EachCallRunsItsOwnTasks(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	const (
		engIAM = "2200a5ae-2466-54f7-a1d8-243106e05705"
		opsIAM = "92fd92cf-4b2c-58dc-bf98-f3d0af729876"
	)
	want := []goldenTask{
		{"CA_Eng::Process_Review", "Review_Task", engIAM},
		{"CA_Eng::Review_Check::Process_Check", "Check_Task", engIAM},
		{"CA_Ops::Process_Review", "Review_Task", opsIAM},
		{"CA_Ops::Review_Check::Process_Check", "Check_Task", opsIAM},
	}
	var created []goldenTask
	registerFakeActivities(env, goldenCollaboration(t), &activityHooks{
		createTask: func(in port.CreateTaskInput) (port.CreateTaskOutput, error) {
			dept, node, _ := strings.Cut(string(in.NodeKey), "/")
			created = append(created, goldenTask{dept, node, in.IAMDepartmentID})
			return port.CreateTaskOutput{TaskID: string(in.NodeKey)}, nil
		},
	})
	for i, task := range want {
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
				DeptID: task.dept, NodeID: task.node, ToStage: "review", ResultJSON: "{}", RecordVersion: 1, VisitCount: 1,
			})
		}, time.Duration(i+1)*time.Minute)
	}

	env.ExecuteWorkflow(wfengine.Execute, wfengine.ExecuteInput{TenantID: "tenant-1", InstanceID: "instance-1", VersionID: "version-1"})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	var out wfengine.ExecuteOutput
	if err := env.GetWorkflowResult(&out); err != nil {
		t.Fatalf("GetWorkflowResult: %v", err)
	}
	if out.Status != domain.InstanceStatusCompleted {
		t.Errorf("Status = %v, want COMPLETED", out.Status)
	}
	if len(created) != len(want) {
		t.Fatalf("created %+v, want %+v", created, want)
	}
	for i := range want {
		if created[i] != want[i] {
			t.Errorf("task %d = %+v, want %+v", i, created[i], want[i])
		}
	}
}

// goldenBoundaries is dsltest.LibraryCallBoundaries as the activity hands it
// to the workflow.
func goldenBoundaries(t *testing.T) *dsl.CompiledCollaboration {
	t.Helper()
	var c dsl.CompiledCollaboration
	if err := json.Unmarshal(dsltest.LibraryCallBoundaries(), &c); err != nil {
		t.Fatalf("decode golden: %v", err)
	}
	expanded, err := dsl.ExpandCalls(&c, domain.MaxPlanStages)
	if err != nil {
		t.Fatalf("expand golden: %v", err)
	}
	return expanded
}

func runGoldenBoundaries(t *testing.T, signals []goldenTask) (domain.InstanceStatus, []string) {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	var created []string
	registerFakeActivities(env, goldenBoundaries(t), &activityHooks{
		createTask: func(in port.CreateTaskInput) (port.CreateTaskOutput, error) {
			created = append(created, string(in.NodeKey))
			return port.CreateTaskOutput{TaskID: string(in.NodeKey)}, nil
		},
	})
	for i, task := range signals {
		env.RegisterDelayedCallback(func() {
			env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
				DeptID: task.dept, NodeID: task.node, ToStage: "review", ResultJSON: "{}", RecordVersion: 1, VisitCount: 1,
			})
		}, time.Duration(i+1)*time.Minute)
	}
	env.ExecuteWorkflow(wfengine.Execute, wfengine.ExecuteInput{TenantID: "tenant-1", InstanceID: "instance-1", VersionID: "version-1"})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	var out wfengine.ExecuteOutput
	if err := env.GetWorkflowResult(&out); err != nil {
		t.Fatalf("GetWorkflowResult: %v", err)
	}
	return out.Status, created
}

// A call's P5D timer boundary leading to an end event ends the instance when
// it fires; nothing after the call runs.
func TestExecute_LibraryCallBoundaries_TimerEndsThePath(t *testing.T) {
	status, created := runGoldenBoundaries(t, nil)
	if status != domain.InstanceStatusCompleted {
		t.Errorf("Status = %v, want COMPLETED once P5D elapses", status)
	}
	for _, key := range created {
		if strings.HasPrefix(key, "CA_Message::") {
			t.Errorf("created %s after the timer ended the path", key)
		}
	}
}

// A call's message boundary fires on a message a send task delivered, and
// ends the instance without the call's tasks being done.
func TestExecute_LibraryCallBoundaries_MessageEndsThePath(t *testing.T) {
	status, _ := runGoldenBoundaries(t, []goldenTask{
		{dept: "CA_Timer::Process_Review", node: "Review_Task"},
		{dept: "CA_Timer::Review_Check::Process_Check", node: "Check_Task"},
	})
	if status != domain.InstanceStatusCompleted {
		t.Errorf("Status = %v, want COMPLETED through CA_Message's withdraw boundary", status)
	}
}
