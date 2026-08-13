// Package workflow_test exercises internal/workflow's exported Execute
// function end-to-end via testsuite.WorkflowTestSuite — the LLD §7.1 tier
// for genuinely Temporal-shaped behavior (dispatch, boundary Selectors,
// force-back, DEGRADED, SLA timers). Every Activity is mocked/faked here;
// none of the real DB-writing Activity bodies exist yet (a separate sibling
// task's job).
package workflow_test

import (
	"context"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

// singleStageCollaboration is the smallest valid fixture: one pool, one
// department, one prep/review/approve-shaped stage, dispatched Sequential.
func singleStageCollaboration(stage dsl.StageDef) *dsl.CompiledCollaboration {
	return &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{
			{
				Name: "main",
				Departments: []dsl.DepartmentDef{
					{ID: "sales", Stages: []dsl.StageDef{stage}},
				},
				Execution: dsl.ExecutionPlan{
					Steps: []dsl.ExecutionStep{
						{Sequential: []string{"sales"}},
					},
				},
			},
		},
	}
}

// activityHooks lets individual tests observe or override specific fake
// activity bodies without every test needing to know every activity's
// signature. Nil fields fall back to a harmless default.
type activityHooks struct {
	createTask           func(port.CreateTaskInput) (port.CreateTaskOutput, error)
	completeAssignment   func(port.CompleteAssignmentInput) (port.CompleteAssignmentOutput, error)
	updateTaskStatus     func(port.UpdateTaskStatusInput) error
	recordSLAWarn        func(port.RecordSLAWarningInput)
	recordSLABreach      func(port.RecordSLABreachInput)
	recordForceRoute     func(port.RecordForceRouteInput)
	updateInstanceStatus func(port.UpdateInstanceStatusInput)
	pauseInstance        func(port.PauseInstanceInput)
	resumeInstance       func(port.ResumeInstanceInput)
	cancelInstance       func(port.CancelInstanceInput) error
	reassignAssignment   func(port.ReassignAssignmentInput) error
}

// registerFakeActivities registers minimal, real (not testify-mocked)
// activity function bodies under every port.ActivityXxx name this package
// calls, so testsuite.WorkflowTestSuite can dispatch workflow.ExecuteActivity
// calls without any real persistence layer existing yet. hooks may be nil.
func registerFakeActivities(env *testsuite.TestWorkflowEnvironment, collab *dsl.CompiledCollaboration, hooks *activityHooks) {
	if hooks == nil {
		hooks = &activityHooks{}
	}
	createTaskFn := hooks.createTask
	if createTaskFn == nil {
		createTaskFn = func(in port.CreateTaskInput) (port.CreateTaskOutput, error) {
			return port.CreateTaskOutput{TaskID: string(in.NodeKey)}, nil
		}
	}

	env.RegisterActivityWithOptions(
		func(_ context.Context, _ port.GetCompiledPlanInput) (port.GetCompiledPlanOutput, error) {
			out := *collab
			// Fixtures that don't care about schema versioning leave this
			// zero; default it to the current version so they don't need to
			// set it explicitly. A fixture testing the compat check sets its
			// own (possibly unsupported) SchemaVersion, which this never
			// overrides.
			if out.SchemaVersion == 0 {
				out.SchemaVersion = dsl.CurrentSchemaVersion
			}
			return port.GetCompiledPlanOutput{Collaboration: out}, nil
		},
		activity.RegisterOptions{Name: port.ActivityGetCompiledPlan},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, in port.CreateTaskInput) (port.CreateTaskOutput, error) {
			return createTaskFn(in)
		},
		activity.RegisterOptions{Name: port.ActivityCreateTask},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, _ port.UpdateInstanceNodesInput) error { return nil },
		activity.RegisterOptions{Name: port.ActivityUpdateInstanceNodes},
	)
	registerFakeTaskActivities(env, hooks)
	env.RegisterActivityWithOptions(
		func(_ context.Context, in port.UpdateInstanceStatusInput) error {
			if hooks.updateInstanceStatus != nil {
				hooks.updateInstanceStatus(in)
			}
			return nil
		},
		activity.RegisterOptions{Name: port.ActivityUpdateInstanceStatus},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, in port.RecordForceRouteInput) error {
			if hooks.recordForceRoute != nil {
				hooks.recordForceRoute(in)
			}
			return nil
		},
		activity.RegisterOptions{Name: port.ActivityRecordForceRoute},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, in port.RecordSLAWarningInput) error {
			if hooks.recordSLAWarn != nil {
				hooks.recordSLAWarn(in)
			}
			return nil
		},
		activity.RegisterOptions{Name: port.ActivityRecordSLAWarning},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, in port.RecordSLABreachInput) error {
			if hooks.recordSLABreach != nil {
				hooks.recordSLABreach(in)
			}
			return nil
		},
		activity.RegisterOptions{Name: port.ActivityRecordSLABreach},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, _ port.DeferTaskInput) (port.DeferTaskOutput, error) {
			return port.DeferTaskOutput{}, nil
		},
		activity.RegisterOptions{Name: port.ActivityDeferTask},
	)
	registerFakeAdminInstanceActivities(env, hooks)
}

// registerFakeTaskActivities registers the completeAssignment/
// updateTaskStatus Activities — split out of registerFakeActivities to keep
// both under the cognitive-complexity lint budget.
func registerFakeTaskActivities(env *testsuite.TestWorkflowEnvironment, hooks *activityHooks) {
	env.RegisterActivityWithOptions(
		func(_ context.Context, in port.CompleteAssignmentInput) (port.CompleteAssignmentOutput, error) {
			if hooks.completeAssignment != nil {
				return hooks.completeAssignment(in)
			}
			return port.CompleteAssignmentOutput{AllDone: true}, nil
		},
		activity.RegisterOptions{Name: port.ActivityCompleteAssignment},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, in port.UpdateTaskStatusInput) error {
			if hooks.updateTaskStatus != nil {
				return hooks.updateTaskStatus(in)
			}
			return nil
		},
		activity.RegisterOptions{Name: port.ActivityUpdateTaskStatus},
	)
}

// registerFakeAdminInstanceActivities registers the pause/resume/cancel
// Activities — split out of registerFakeActivities to keep both under the
// cognitive-complexity lint budget.
func registerFakeAdminInstanceActivities(env *testsuite.TestWorkflowEnvironment, hooks *activityHooks) {
	env.RegisterActivityWithOptions(
		func(_ context.Context, in port.PauseInstanceInput) error {
			if hooks.pauseInstance != nil {
				hooks.pauseInstance(in)
			}
			return nil
		},
		activity.RegisterOptions{Name: port.ActivityPauseInstance},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, in port.ResumeInstanceInput) error {
			if hooks.resumeInstance != nil {
				hooks.resumeInstance(in)
			}
			return nil
		},
		activity.RegisterOptions{Name: port.ActivityResumeInstance},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, in port.CancelInstanceInput) error {
			if hooks.cancelInstance != nil {
				return hooks.cancelInstance(in)
			}
			return nil
		},
		activity.RegisterOptions{Name: port.ActivityCancelInstance},
	)
	env.RegisterActivityWithOptions(
		func(_ context.Context, in port.ReassignAssignmentInput) error {
			if hooks.reassignAssignment != nil {
				return hooks.reassignAssignment(in)
			}
			return nil
		},
		activity.RegisterOptions{Name: port.ActivityReassignAssignment},
	)
}

// stageTransitionWire mirrors internal/workflow's unexported
// stageTransitionSignal payload shape by field name — JSON matching is by
// name, so this package doesn't need access to the unexported type to send
// a signal the workflow correctly decodes.
type stageTransitionWire struct {
	DeptID        string
	ToStage       string
	NodeID        string
	UserID        string
	ResultJSON    string
	RecordVersion int64
	Failed        bool
	Reason        string
}

// adminSignalWire mirrors internal/workflow's unexported adminSignal payload.
type adminSignalWire struct {
	AdminUserID   string
	Reason        string
	TargetDeptID  string
	TargetNodeKey string
	RecordVersion int64
}

// reassignSignalWire mirrors internal/workflow's unexported reassignSignal
// payload.
type reassignSignalWire struct {
	TaskID        string
	OldUserID     string
	NewUserID     string
	AdminUserID   string
	RecordVersion int64
}
