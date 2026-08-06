package workflow

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// Activity retry-policy classes, per LLD §3.3's retry-policy table.
var (
	dbWriteActivityOptions = wf.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        time.Second,
			BackoffCoefficient:     2.0,
			MaximumInterval:        60 * time.Second,
			MaximumAttempts:        0, // unlimited
			NonRetryableErrorTypes: []string{"ValidationError", "NotFoundError"},
		},
	}

	// externalCallActivityOptions backs GetCompiledPlanActivity. A DSL
	// schema-version major mismatch could also belong in
	// NonRetryableErrorTypes once the Activity's real implementation
	// exists (cmd/worker, not yet built) — see "execution LLD" §2.5 and
	// compat.go for the actual compatibility strategy (a Factory/Strategy
	// layer in Execute), which this would only optimize, not replace.
	externalCallActivityOptions = wf.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:        500 * time.Millisecond,
			BackoffCoefficient:     2.0,
			MaximumAttempts:        5,
			NonRetryableErrorTypes: []string{"DefinitionServiceClientError"},
		},
	}
)

func withDBWriteOptions(ctx wf.Context) wf.Context {
	return wf.WithActivityOptions(ctx, dbWriteActivityOptions)
}

func withExternalCallOptions(ctx wf.Context) wf.Context {
	return wf.WithActivityOptions(ctx, externalCallActivityOptions)
}

// wrapActivityErr labels an Activity failure with the Activity name it came
// from, since every wrapper below otherwise returns the SDK's bare error
// (satisfies wrapcheck: errors crossing out of go.temporal.io/sdk's own
// Future.Get must be wrapped, not passed through raw).
func wrapActivityErr(activityName string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("workflow: %s: %w", activityName, err)
}

func getCompiledPlan(ctx wf.Context, in port.GetCompiledPlanInput) (port.GetCompiledPlanOutput, error) {
	var out port.GetCompiledPlanOutput
	err := wf.ExecuteActivity(withExternalCallOptions(ctx), port.ActivityGetCompiledPlan, in).Get(ctx, &out)
	return out, wrapActivityErr(port.ActivityGetCompiledPlan, err)
}

func createTask(ctx wf.Context, in port.CreateTaskInput) (port.CreateTaskOutput, error) {
	var out port.CreateTaskOutput
	err := wf.ExecuteActivity(withDBWriteOptions(ctx), port.ActivityCreateTask, in).Get(ctx, &out)
	return out, wrapActivityErr(port.ActivityCreateTask, err)
}

func updateInstanceNodes(ctx wf.Context, in port.UpdateInstanceNodesInput) error {
	err := wf.ExecuteActivity(withDBWriteOptions(ctx), port.ActivityUpdateInstanceNodes, in).Get(ctx, nil)
	return wrapActivityErr(port.ActivityUpdateInstanceNodes, err)
}

func completeAssignment(ctx wf.Context, in port.CompleteAssignmentInput) (port.CompleteAssignmentOutput, error) {
	var out port.CompleteAssignmentOutput
	err := wf.ExecuteActivity(withDBWriteOptions(ctx), port.ActivityCompleteAssignment, in).Get(ctx, &out)
	return out, wrapActivityErr(port.ActivityCompleteAssignment, err)
}

func updateInstanceStatus(ctx wf.Context, in port.UpdateInstanceStatusInput) error {
	err := wf.ExecuteActivity(withDBWriteOptions(ctx), port.ActivityUpdateInstanceStatus, in).Get(ctx, nil)
	return wrapActivityErr(port.ActivityUpdateInstanceStatus, err)
}

func deferTask(ctx wf.Context, in port.DeferTaskInput) (port.DeferTaskOutput, error) {
	var out port.DeferTaskOutput
	err := wf.ExecuteActivity(withDBWriteOptions(ctx), port.ActivityDeferTask, in).Get(ctx, &out)
	return out, wrapActivityErr(port.ActivityDeferTask, err)
}

func recordForceRoute(ctx wf.Context, in port.RecordForceRouteInput) error {
	err := wf.ExecuteActivity(withDBWriteOptions(ctx), port.ActivityRecordForceRoute, in).Get(ctx, nil)
	return wrapActivityErr(port.ActivityRecordForceRoute, err)
}

func recordSLAWarning(ctx wf.Context, in port.RecordSLAWarningInput) error {
	err := wf.ExecuteActivity(withDBWriteOptions(ctx), port.ActivityRecordSLAWarning, in).Get(ctx, nil)
	return wrapActivityErr(port.ActivityRecordSLAWarning, err)
}

func recordSLABreach(ctx wf.Context, in port.RecordSLABreachInput) error {
	err := wf.ExecuteActivity(withDBWriteOptions(ctx), port.ActivityRecordSLABreach, in).Get(ctx, nil)
	return wrapActivityErr(port.ActivityRecordSLABreach, err)
}
