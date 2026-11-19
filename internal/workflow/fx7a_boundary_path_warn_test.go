package workflow

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

func fx7aKeyvals(keyvals []any) map[string]any {
	out := map[string]any{}
	for i := 0; i+1 < len(keyvals); i += 2 {
		if k, ok := keyvals[i].(string); ok {
			out[k] = keyvals[i+1]
		}
	}
	return out
}

// A non-interrupting boundary path that fails does not fail its host, but the
// failure must be visible: one warning names the tenant, the instance, the host
// node and the cause.
func TestRunTaskStage_NonInterruptingBoundaryPathThatFails_LogsAWarningNamingTenantInstanceAndHostNode(t *testing.T) {
	cases := []struct {
		name  string
		timer *dsl.BoundaryTimer
	}{
		{name: "a target department", timer: &dsl.BoundaryTimer{Duration: "1h", TargetDept: "escalation"}},
		{name: "a list of steps", timer: &dsl.BoundaryTimer{Duration: "1h", Steps: []dsl.ExecutionStep{{Sequential: []string{"escalation"}}}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			warnings := &w23bWarnings{}
			suite.SetLogger(warnings)
			env := suite.NewTestWorkflowEnvironment()
			env.SetStartTime(w23bStart)
			rec := &w23bRecorder{failCreate: func(in port.CreateTaskInput) error {
				if in.NodeKey == "escalation/prep" {
					return w23bRefusal("create refused")
				}
				return nil
			}}
			w23bRegister(env, rec)

			env.ExecuteWorkflow(func(ctx wf.Context) error {
				in := newInterpreter("tenant", "instance", "", nil, nil)
				runCtx, cancelRun := wf.WithCancel(ctx)
				wf.Go(ctx, func(gctx wf.Context) {
					_ = wf.Sleep(gctx, 2*time.Hour)
					cancelRun()
				})
				plan := &dsl.CompiledPlan{
					Name:        "main",
					Departments: []dsl.DepartmentDef{{ID: "escalation", Stages: []dsl.StageDef{{Type: "prep"}}}},
				}
				stage := &dsl.StageDef{Type: "approve", NodeID: "n1", BoundaryTimer: tc.timer}
				_, _, _ = in.runTaskStage(runCtx, plan, "sales", stage, "sales/n1")
				in.awaitAbandonedStages(ctx)
				return nil
			})
			require.NoError(t, env.GetWorkflowError())

			logged := warnings.with("non-interrupting boundary path failed")
			require.Len(t, logged, 1)
			fields := fx7aKeyvals(logged[0].keyvals)
			assert.Equal(t, "tenant", fields["tenant_id"])
			assert.Equal(t, "instance", fields["instance_id"])
			assert.Equal(t, "sales/n1", fields["node_key"])
			assert.Contains(t, fmt.Sprint(fields["error"]), "create refused")
		})
	}
}
