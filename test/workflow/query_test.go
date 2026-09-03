package workflow_test

import (
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

// TestExecute_GetWorkflowStatusQuery drives LLD §3.1's one Query
// (get-workflow-status): while a task is pending, the query must report the
// instance's current status and the pending node's key.
//
// The query's delay is deliberately much larger than this package's usual
// first-checkpoint convention (time.Millisecond): this is the only test in
// the package racing a query against three sequential activity round trips
// (GetCompiledPlan -> CreateTask -> UpdateInstanceNodes) rather than a
// signal, and each round trip crosses a real goroutine boundary the mock
// clock doesn't wait on — 1ms/5ms reproduced a real, occasional failure
// under -race. SetOnActivityCompletedListener was tried as a firmer sync
// point and made it worse (fires before the workflow coroutine processes the
// activity's own result, so it undercounts every time, not occasionally).
func TestExecute_GetWorkflowStatusQuery(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	collab := singleStageCollaboration(dsl.StageDef{Type: "approve", Activity: "approve_order"})
	registerFakeActivities(env, collab, nil)

	env.RegisterDelayedCallback(func() {
		result, err := env.QueryWorkflow(wfengine.QueryGetWorkflowStatus)
		if err != nil {
			t.Errorf("QueryWorkflow error: %v", err)
			return
		}
		var status wfengine.WorkflowStatusQuery
		if err := result.Get(&status); err != nil {
			t.Errorf("decoding query result: %v", err)
			return
		}
		if status.Status != domain.InstanceStatusRunning {
			t.Errorf("Status = %v, want RUNNING", status.Status)
		}
		if len(status.CurrentNodeKeys) != 1 || status.CurrentNodeKeys[0] != "sales/approve" {
			t.Errorf("CurrentNodeKeys = %v, want [sales/approve]", status.CurrentNodeKeys)
		}
	}, 100*time.Millisecond)

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "sales", ToStage: "approve", ResultJSON: `{"decision":"approved"}`, RecordVersion: 1,
		})
	}, 200*time.Millisecond)

	env.ExecuteWorkflow(wfengine.Execute, wfengine.ExecuteInput{
		TenantID: "tenant-1", InstanceID: "instance-1", VersionID: "version-1",
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
}
