package workflow_test

import (
	"errors"
	"sync/atomic"
	"testing"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

func TestExecute_ADefinitionOutageLongerThanFiveAttemptsDoesNotFailTheInstance(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	var reads atomic.Int32
	const failures = 9
	registerFakeActivities(env, singleStageCollaboration(dsl.StageDef{Type: "prep", Activity: "step"}), &activityHooks{
		getCompiledPlan: func(port.GetCompiledPlanInput) (port.GetCompiledPlanOutput, error) {
			if reads.Add(1) <= failures {
				return port.GetCompiledPlanOutput{}, errors.New("definition service unavailable")
			}
			return port.GetCompiledPlanOutput{}, nil
		},
		createTask: func(in port.CreateTaskInput) (port.CreateTaskOutput, error) {
			env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
				DeptID: "sales", ToStage: "prep", ResultJSON: "{}", RecordVersion: 1, VisitCount: 1,
			})
			return port.CreateTaskOutput{TaskID: string(in.NodeKey)}, nil
		},
	})

	env.ExecuteWorkflow(wfengine.Execute, wfengine.ExecuteInput{TenantID: "tenant-1", InstanceID: "instance-1", VersionID: "version-1"})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("an outage that ended after %d failed reads failed the instance: %v", failures, err)
	}
	var out wfengine.ExecuteOutput
	if err := env.GetWorkflowResult(&out); err != nil {
		t.Fatal(err)
	}
	if out.Status != domain.InstanceStatusCompleted {
		t.Errorf("Status = %v, want COMPLETED", out.Status)
	}
	if got := reads.Load(); got != failures+1 {
		t.Errorf("plan reads = %d, want %d", got, failures+1)
	}
}

func TestExecute_ARejectedPlanReadFailsTheInstanceWithoutRetrying(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	var reads atomic.Int32
	registerFakeActivities(env, singleStageCollaboration(dsl.StageDef{Type: "prep", Activity: "step"}), &activityHooks{
		getCompiledPlan: func(port.GetCompiledPlanInput) (port.GetCompiledPlanOutput, error) {
			reads.Add(1)
			return port.GetCompiledPlanOutput{}, temporal.NewNonRetryableApplicationError("version is not published", "DefinitionServiceClientError", nil)
		},
	})

	env.ExecuteWorkflow(wfengine.Execute, wfengine.ExecuteInput{TenantID: "tenant-1", InstanceID: "instance-1", VersionID: "version-1"})

	if env.GetWorkflowError() == nil {
		t.Fatal("a rejected read must fail the workflow")
	}
	var out wfengine.ExecuteOutput
	_ = env.GetWorkflowResult(&out)
	if reads.Load() != 1 {
		t.Errorf("plan reads = %d, want exactly 1: a rejection is not retried", reads.Load())
	}
}
