package workflow

import (
	"context"
	"testing"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// newCompletionEnv registers a CompleteAssignment returning the given gate and
// records every UpdateTaskStatus the interpreter makes.
func newCompletionEnv(allDone bool, seen *[]port.UpdateTaskStatusInput) *testsuite.TestWorkflowEnvironment {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivityWithOptions(
		func(_ context.Context, _ port.CompleteAssignmentInput) (port.CompleteAssignmentOutput, error) {
			return port.CompleteAssignmentOutput{AllDone: allDone}, nil
		},
		activity.RegisterOptions{Name: port.ActivityCompleteAssignment},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, in port.UpdateTaskStatusInput) error {
			*seen = append(*seen, in)
			return nil
		},
		activity.RegisterOptions{Name: port.ActivityUpdateTaskStatus},
	)
	return env
}

func resolveOnce(t *testing.T, allDone bool) []port.UpdateTaskStatusInput {
	t.Helper()

	var seen []port.UpdateTaskStatusInput
	env := newCompletionEnv(allDone, &seen)
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		in := newInterpreter("tenant", "instance", "", nil, nil)
		_, _, err := in.resolveTaskStage(ctx, "sales/approval",
			port.CreateTaskOutput{TaskID: "task-1"},
			stageTransitionSignal{UserID: "user-1", ResultJSON: "{}"})
		return err
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	return seen
}

// A stage whose last assignment completes must reach COMPLETED. The gate is
// what makes the connector callback's re-completion guard and the documented
// READY/IN_PROGRESS → COMPLETED transition (LLD §4.3) real.
func TestResolveTaskStage_LastAssignmentWritesCompleted(t *testing.T) {
	seen := resolveOnce(t, true)

	if len(seen) != 1 {
		t.Fatalf("UpdateTaskStatus calls = %d, want 1", len(seen))
	}
	if seen[0].Status != domain.TaskStatusCompleted {
		t.Errorf("status = %v, want %v", seen[0].Status, domain.TaskStatusCompleted)
	}
	if seen[0].TaskID != "task-1" || seen[0].TenantID != "tenant" {
		t.Errorf("wrote status for %s/%s, want tenant/task-1", seen[0].TenantID, seen[0].TaskID)
	}
}

// A co-assignee still outstanding leaves the task open.
func TestResolveTaskStage_OutstandingAssigneeLeavesTaskOpen(t *testing.T) {
	if seen := resolveOnce(t, false); len(seen) != 0 {
		t.Fatalf("UpdateTaskStatus calls = %d, want 0", len(seen))
	}
}

// An instance whose history predates the change must not gain the activity on
// replay. The version is read before the AllDone check for this reason: read
// the other way round, whether an instance ever writes COMPLETED would depend
// on how many assignees its first completed stage happened to have, since
// GetVersion caches per run on the first call that reaches it.
func TestResolveTaskStage_OlderHistoryWritesNoStatus(t *testing.T) {
	var seen []port.UpdateTaskStatusInput
	env := newCompletionEnv(true, &seen)
	// DefaultVersion for this change id is what an execution started before the
	// change replays as.
	env.OnGetVersion(taskCompletedStatusChangeID, wf.DefaultVersion, 1).Return(wf.DefaultVersion)

	env.ExecuteWorkflow(func(ctx wf.Context) error {
		in := newInterpreter("tenant", "instance", "", nil, nil)
		_, _, err := in.resolveTaskStage(ctx, "sales/approval",
			port.CreateTaskOutput{TaskID: "task-1"},
			stageTransitionSignal{UserID: "user-1", ResultJSON: "{}"})
		return err
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	if len(seen) != 0 {
		t.Fatalf("UpdateTaskStatus calls = %d, want 0 — an older history must replay unchanged", len(seen))
	}
}
