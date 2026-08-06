package workflow

import (
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"
	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

func TestAddTimerCaseInvalidDurationIsANoOp(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	var fired bool
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		sel := wf.NewSelector(ctx)
		cancel := addTimerCase(ctx, sel, "not-a-duration", false, "dept", func(boundaryFire) { fired = true })
		cancel()
		return nil
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	if fired {
		t.Error("addTimerCase with an invalid duration string should never fire")
	}
}

func TestAddTimerCaseEmptyDurationIsANoOp(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.ExecuteWorkflow(func(ctx wf.Context) error {
		sel := wf.NewSelector(ctx)
		cancel := addTimerCase(ctx, sel, "", false, "dept", func(boundaryFire) {})
		cancel()
		return nil
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
}

func TestAddTimerCaseFires(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.SetStartTime(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))

	var fired boundaryFire
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		sel := wf.NewSelector(ctx)
		addTimerCase(ctx, sel, "1s", true, "escalation", func(f boundaryFire) { fired = f })
		sel.Select(ctx)
		return nil
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	if fired.Kind != "timer" || fired.TargetDept != "escalation" || !fired.Interrupting {
		t.Errorf("addTimerCase fired = %+v, want timer/escalation/interrupting", fired)
	}
}

func TestAddMessageCaseFires(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	var fired boundaryFire
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		buf := newMessageBuffer()
		buf.Send(ctx, "escalate", "sender:send")
		sel := wf.NewSelector(ctx)
		addMessageCase(ctx, sel, buf, "escalate", false, "escalation", func(f boundaryFire) { fired = f })
		sel.Select(ctx)
		return nil
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	if fired.Kind != "message" || fired.TargetDept != "escalation" || fired.Interrupting {
		t.Errorf("addMessageCase fired = %+v, want message/escalation/non-interrupting", fired)
	}
}

func TestAddMessageCaseEmptyNameIsANoOp(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.ExecuteWorkflow(func(ctx wf.Context) error {
		buf := newMessageBuffer()
		sel := wf.NewSelector(ctx)
		addMessageCase(ctx, sel, buf, "", false, "dept", func(boundaryFire) {})
		return nil
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
}

func TestAddErrorCaseFiresOnMatchingCode(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	var fired boundaryFire
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		errCh := wf.NewBufferedChannel(ctx, 1)
		errCh.Send(ctx, "E1")
		sel := wf.NewSelector(ctx)
		addErrorCase(ctx, sel, errCh, []dsl.ErrorPath{{ErrorCode: "E1", TargetDept: "handler"}}, func(f boundaryFire) { fired = f })
		sel.Select(ctx)
		return nil
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	if fired.Kind != "error" || fired.TargetDept != "handler" || fired.ErrorCode != "E1" {
		t.Errorf("addErrorCase fired = %+v, want error/handler/E1", fired)
	}
}

func TestAddErrorCaseFallsThroughToDefaultErrorPath(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	var fired boundaryFire
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		errCh := wf.NewBufferedChannel(ctx, 1)
		errCh.Send(ctx, "unmatched-code")
		sel := wf.NewSelector(ctx)
		addErrorCase(ctx, sel, errCh, []dsl.ErrorPath{{ErrorCode: "", TargetDept: "catch-all"}}, func(f boundaryFire) { fired = f })
		sel.Select(ctx)
		return nil
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	if fired.TargetDept != "catch-all" {
		t.Errorf("addErrorCase should fall through to the empty-ErrorCode catch-all, fired = %+v", fired)
	}
}

func TestAddErrorCaseNilChannelOrEmptyPathsIsANoOp(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.ExecuteWorkflow(func(ctx wf.Context) error {
		sel := wf.NewSelector(ctx)
		addErrorCase(ctx, sel, nil, []dsl.ErrorPath{{ErrorCode: "E1"}}, func(boundaryFire) {})
		ch := wf.NewBufferedChannel(ctx, 1)
		addErrorCase(ctx, sel, ch, nil, func(boundaryFire) {})
		return nil
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
}

func TestCheckPausedBlocksUntilResumed(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	var resumedBeforeReturn bool
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		in := newInterpreter("tenant", "instance", "", nil)
		in.pauseDept(ctx, "deptA")

		var resumed bool
		wf.Go(ctx, func(gctx wf.Context) {
			in.resumeDept(gctx, "deptA")
			resumed = true
		})

		in.checkPaused(ctx, "deptA")
		resumedBeforeReturn = resumed
		return nil
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	if !resumedBeforeReturn {
		t.Error("checkPaused returned before the pause gate was resumed")
	}
}

func TestCheckPausedWithoutAGateIsANoOp(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	env.ExecuteWorkflow(func(ctx wf.Context) error {
		in := newInterpreter("tenant", "instance", "", nil)
		in.checkPaused(ctx, "deptA") // no gate armed — must return immediately
		return nil
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
}
