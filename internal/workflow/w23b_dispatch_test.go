package workflow

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

func w23bTwoStageDepts(ids ...string) []dsl.DepartmentDef {
	depts := make([]dsl.DepartmentDef, 0, len(ids))
	for _, id := range ids {
		depts = append(depts, dsl.DepartmentDef{ID: id, Stages: []dsl.StageDef{{Type: "prep"}}})
	}
	return depts
}

func TestRunSteps_FailingExclusiveOrSubWorkflowStep_StopsTheListAndLaterStepsDoNotRun(t *testing.T) {
	cases := []struct {
		name  string
		steps []dsl.ExecutionStep
	}{
		{
			name: "exclusive branch whose step fails",
			steps: []dsl.ExecutionStep{
				{Exclusive: []dsl.ExclusiveBranch{{Steps: []dsl.ExecutionStep{{Sequential: []string{"bad"}}}}}},
				{Sequential: []string{"later"}},
			},
		},
		{
			name: "sub-workflow whose step fails",
			steps: []dsl.ExecutionStep{
				{SubWorkflow: &dsl.SubWorkflowStep{Name: "call", Plan: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"bad"}}}}}},
				{Sequential: []string{"later"}},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &w23bRecorder{failCreate: func(in port.CreateTaskInput) error {
				if in.NodeKey == "bad/prep" {
					return w23bRefusal("create refused")
				}
				return nil
			}}
			env := w23bNewEnv(rec)

			var out stepOutcome
			var runErr error
			env.ExecuteWorkflow(func(ctx wf.Context) error {
				in := newInterpreter("tenant", "instance", "", nil, nil)
				w23bStartRouter(ctx, in)
				plan := &dsl.CompiledPlan{Name: "main", Departments: w23bTwoStageDepts("bad", "later")}
				out, runErr = in.runSteps(ctx, plan, tc.steps, wf.NewBufferedChannel(ctx, 1))
				return nil
			})
			require.NoError(t, env.GetWorkflowError())

			require.Error(t, runErr)
			var appErr *temporal.ApplicationError
			require.ErrorAs(t, runErr, &appErr)
			assert.Equal(t, "ValidationError", appErr.Type())
			assert.ErrorContains(t, runErr, "create refused")
			assert.False(t, out.Terminated)
			assert.Equal(t, []domain.NodeKey{"bad/prep"}, rec.createdKeys(), "the later step created no task")
		})
	}
}

func TestRunSteps_StepIOMapping_FeedsEachStepTheVariablesItsInputsSelect(t *testing.T) {
	rec := &w23bRecorder{}
	env := w23bNewEnv(rec)
	w23bAnswer(env, stageTransitionSignal{DeptID: "a", ToStage: "prep", UserID: "user-1", ResultJSON: "{}", VisitCount: 1})
	w23bAnswer(env, stageTransitionSignal{DeptID: "b", ToStage: "prep", UserID: "user-1", ResultJSON: "{}", VisitCount: 1})

	var runErr error
	var finalContext string
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		in := newInterpreter("tenant", "instance", `{"clarificationId":"C-9"}`, nil, nil)
		w23bStartRouter(ctx, in)
		plan := &dsl.CompiledPlan{Name: "main", Departments: w23bTwoStageDepts("a", "b")}
		steps := []dsl.ExecutionStep{
			{
				IOMapping: &dsl.IOMapping{Inputs: []dsl.IOVar{
					{Source: "=clarificationId", Target: "id"},
					{Source: "dept_id", Target: "literal"},
				}},
				Sequential: []string{"a"},
			},
			{
				IOMapping:  &dsl.IOMapping{Inputs: []dsl.IOVar{{Source: "=id", Target: "copy"}}},
				Sequential: []string{"b"},
			},
		}
		_, runErr = in.runSteps(ctx, plan, steps, wf.NewBufferedChannel(ctx, 1))
		finalContext = in.contextJSON
		return nil
	})
	require.NoError(t, env.GetWorkflowError())

	require.NoError(t, runErr)
	require.Len(t, rec.created, 2)
	assert.Equal(t, domain.NodeKey("a/prep"), rec.created[0].NodeKey)
	assert.JSONEq(t, `{"clarificationId":"C-9","id":"C-9","literal":"dept_id"}`, rec.created[0].ContextJSON,
		"=name reads the variable; any other source is a literal")
	assert.Equal(t, domain.NodeKey("b/prep"), rec.created[1].NodeKey)
	assert.JSONEq(t, `{"clarificationId":"C-9","id":"C-9","literal":"dept_id","copy":"C-9"}`, rec.created[1].ContextJSON,
		"the second step reads what the first one wrote")
	assert.JSONEq(t, rec.created[1].ContextJSON, finalContext)
}

func TestRunSteps_StepIOMappingWithUnparseableContext_EndsWithTheWrappedErrorAndCreatesNoTask(t *testing.T) {
	for _, raw := range []string{"{", "[1,2]"} {
		t.Run(raw, func(t *testing.T) {
			rec := &w23bRecorder{}
			env := w23bNewEnv(rec)

			var runErr error
			var contextJSON string
			env.ExecuteWorkflow(func(ctx wf.Context) error {
				in := newInterpreter("tenant", "instance", raw, nil, nil)
				plan := &dsl.CompiledPlan{Name: "main", Departments: w23bTwoStageDepts("a")}
				_, runErr = in.runSteps(ctx, plan, []dsl.ExecutionStep{{
					IOMapping:  &dsl.IOMapping{Inputs: []dsl.IOVar{{Source: "=x", Target: "y"}}},
					Sequential: []string{"a"},
				}}, wf.NewBufferedChannel(ctx, 1))
				contextJSON = in.contextJSON
				return nil
			})
			require.NoError(t, env.GetWorkflowError())

			require.Error(t, runErr)
			assert.ErrorContains(t, runErr, "workflow: parsing context_json for io_mapping: ")
			assert.Empty(t, rec.created, "no task is created for the step")
			assert.Equal(t, raw, contextJSON, "the context is left as it was")
		})
	}
}

// w23bServePlan answers the compiled-plan read with collab.
func w23bServePlan(env *testsuite.TestWorkflowEnvironment, rec *w23bRecorder, collab dsl.CompiledCollaboration) {
	env.RegisterActivityWithOptions(
		func(_ context.Context, in port.GetCompiledPlanInput) (port.GetCompiledPlanOutput, error) {
			rec.mu.Lock()
			rec.planReads = append(rec.planReads, in)
			rec.mu.Unlock()
			out := collab
			out.SchemaVersion = dsl.CurrentSchemaVersion
			return port.GetCompiledPlanOutput{Collaboration: out}, nil
		},
		activity.RegisterOptions{Name: port.ActivityGetCompiledPlan},
	)
}

func TestExecute_StepIOMappingWithUnparseableContext_FinalisesTheInstanceFailedWithCompletedAt(t *testing.T) {
	rec := &w23bRecorder{}
	env := w23bNewEnv(rec)
	w23bServePlan(env, rec, dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name:        "main",
			Departments: w23bTwoStageDepts("sales"),
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{
				IOMapping:  &dsl.IOMapping{Inputs: []dsl.IOVar{{Source: "=x", Target: "y"}}},
				Sequential: []string{"sales"},
			}}},
		}},
	})

	env.ExecuteWorkflow(Execute, ExecuteInput{TenantID: "tenant-5", InstanceID: "instance-5", VersionID: "version-5", ContextJSON: "{"})

	require.True(t, env.IsWorkflowCompleted())
	err := env.GetWorkflowError()
	require.Error(t, err)
	assert.ErrorContains(t, err, "workflow: parsing context_json for io_mapping: ")
	require.Len(t, rec.planReads, 1)
	assert.Equal(t, "tenant-5", rec.planReads[0].TenantID)
	assert.Equal(t, "version-5", rec.planReads[0].VersionID)
	assert.Empty(t, rec.created, "no task is created")
	require.Len(t, rec.statuses, 1)
	assert.Equal(t, domain.InstanceStatusFailed, rec.statuses[0].Status)
	assert.Equal(t, "tenant-5", rec.statuses[0].TenantID)
	assert.Equal(t, "instance-5", rec.statuses[0].InstanceID)
	require.NotNil(t, rec.statuses[0].CompletedAt, "a failed instance carries its completion time")
	assert.WithinDuration(t, w23bStart, *rec.statuses[0].CompletedAt, 0, "workflow time, not the wall clock")
}
