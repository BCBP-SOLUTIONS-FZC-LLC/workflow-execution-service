package workflow

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// The create-task call must wait for a cancelled activity to finish, so the
// task it made is returned to the workflow instead of being hidden by the
// cancel error. The test environment ignores this option, so a real server is
// needed to see the wait itself; the options are checked here.
func TestCreateTaskOptions_WaitForCancellationOnlyForThatCall(t *testing.T) {
	opts := createTaskActivityOptions()
	assert.True(t, opts.WaitForCancellation)
	assert.Equal(t, dbWriteActivityOptions.StartToCloseTimeout, opts.StartToCloseTimeout)
	assert.False(t, dbWriteActivityOptions.WaitForCancellation, "other database writes keep the default")
}

type fxdSettleResult struct {
	err      error
	statuses []port.UpdateTaskStatusInput
}

func fxdSettleCreate(t *testing.T, routing bool, cancelled bool, out port.CreateTaskOutput, createErr error) fxdSettleResult {
	t.Helper()
	var res fxdSettleResult
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivityWithOptions(
		func(_ context.Context, in port.UpdateTaskStatusInput) error {
			res.statuses = append(res.statuses, in)
			return nil
		},
		activity.RegisterOptions{Name: port.ActivityUpdateTaskStatus},
	)
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		runCtx, cancel := wf.WithCancel(ctx)
		if cancelled {
			cancel()
		}
		in := newInterpreter("tenant-1", "instance-1", "", nil, nil)
		in.routing = routing
		res.err = in.settleCreatedTask(runCtx, out, createErr)
		return nil
	})
	require.NoError(t, env.GetWorkflowError())
	return res
}

// A create that returns its task after the stage was cancelled leaves a task
// no node lookup finds, so it is superseded here, also during a force-route
// (whose audit only supersedes the tasks it knew about).
func TestSettleCreatedTask_TaskReturnedAfterCancelIsSuperseded(t *testing.T) {
	for name, routing := range map[string]bool{"plain cancel": false, "force-route": true} {
		t.Run(name, func(t *testing.T) {
			res := fxdSettleCreate(t, routing, true, port.CreateTaskOutput{TaskID: "task-9"}, nil)

			assert.ErrorIs(t, res.err, errStageAbandoned)
			require.Len(t, res.statuses, 1)
			assert.Equal(t, "task-9", res.statuses[0].TaskID)
			assert.Equal(t, "tenant-1", res.statuses[0].TenantID)
			assert.Equal(t, domain.TaskStatusSuperseded, res.statuses[0].Status)
		})
	}
}

func TestSettleCreatedTask_CancelledCreateWithoutTaskSupersedesNothing(t *testing.T) {
	res := fxdSettleCreate(t, true, true, port.CreateTaskOutput{}, errors.New("canceled"))

	assert.ErrorIs(t, res.err, errStageAbandoned)
	assert.Empty(t, res.statuses)
}

func TestSettleCreatedTask_LiveContextPassesResultAndErrorThrough(t *testing.T) {
	ok := fxdSettleCreate(t, false, false, port.CreateTaskOutput{TaskID: "task-1"}, nil)
	assert.NoError(t, ok.err)
	assert.Empty(t, ok.statuses)

	boom := errors.New("create failed")
	failed := fxdSettleCreate(t, false, false, port.CreateTaskOutput{}, boom)
	assert.ErrorIs(t, failed.err, boom)
	assert.Empty(t, failed.statuses)
}

// A refused supersede write does not change how the stage ends: it is still
// abandoned, and the write was tried once.
func TestSettleCreatedTask_RefusedSupersedeStillEndsTheStageAbandoned(t *testing.T) {
	var attempts int
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	env.RegisterActivityWithOptions(
		func(_ context.Context, _ port.UpdateTaskStatusInput) error {
			attempts++
			return temporal.NewNonRetryableApplicationError("refused", "ValidationError", nil)
		},
		activity.RegisterOptions{Name: port.ActivityUpdateTaskStatus},
	)
	var got error
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		runCtx, cancel := wf.WithCancel(ctx)
		cancel()
		in := newInterpreter("tenant-1", "instance-1", "", nil, nil)
		got = in.settleCreatedTask(runCtx, port.CreateTaskOutput{TaskID: "task-9"}, nil)
		return nil
	})
	require.NoError(t, env.GetWorkflowError())
	assert.ErrorIs(t, got, errStageAbandoned)
	assert.Equal(t, 1, attempts)
}
