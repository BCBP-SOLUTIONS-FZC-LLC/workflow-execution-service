package workflow

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type w28aSettleResult struct {
	Status         domain.InstanceStatus
	RestorePending bool
	LockCreated    bool
	LockCreatedPre bool
}

type w28aSettleRig struct {
	mu     sync.Mutex
	status []port.UpdateInstanceStatusInput
	pauses []port.PauseInstanceInput
	refuse bool
}

func (r *w28aSettleRig) register(env *testsuite.TestWorkflowEnvironment) {
	env.RegisterActivityWithOptions(func(_ context.Context, in port.UpdateInstanceStatusInput) error {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.status = append(r.status, in)
		if r.refuse {
			return temporal.NewNonRetryableApplicationError("write refused", "ValidationError", nil)
		}
		return nil
	}, activity.RegisterOptions{Name: port.ActivityUpdateInstanceStatus})
	env.RegisterActivityWithOptions(func(_ context.Context, in port.PauseInstanceInput) error {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.pauses = append(r.pauses, in)
		return nil
	}, activity.RegisterOptions{Name: port.ActivityPauseInstance})
}

func w28aOpenPark(dept uuid.UUID, lastNode string) *park {
	return &park{branches: []domain.FailedBranch{{DepartmentID: dept, LastNodeKey: lastNode}}}
}

func TestSettleStatusAfterAbandonedPark_WritesWhatTheOpenParksCallFor(t *testing.T) {
	dept := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	held := adminSignal{AdminUserID: "admin-7", Initiator: domain.InitiatorAdmin, RecordVersion: 9}
	cases := []struct {
		name         string
		status       domain.InstanceStatus
		pending      bool
		parks        []*park
		pauseHeld    bool
		refuse       bool
		wantStatus   domain.InstanceStatus
		wantPending  bool
		wantWrites   []domain.InstanceStatus
		wantBranches []domain.FailedBranch
		wantPauses   int
	}{
		{
			name: "neither degraded nor restore pending writes nothing even with a park open", status: domain.InstanceStatusRunning,
			parks: []*park{w28aOpenPark(dept, "billing/B1")}, wantStatus: domain.InstanceStatusRunning,
		},
		{
			name: "degraded with a park open writes DEGRADED with its branches", status: domain.InstanceStatusDegraded,
			parks: []*park{w28aOpenPark(dept, "billing/B1")}, wantStatus: domain.InstanceStatusDegraded,
			wantWrites:   []domain.InstanceStatus{domain.InstanceStatusDegraded},
			wantBranches: []domain.FailedBranch{{DepartmentID: dept, LastNodeKey: "billing/B1"}},
		},
		{
			name: "degraded with no park left writes RUNNING", status: domain.InstanceStatusDegraded,
			wantStatus: domain.InstanceStatusRunning, wantWrites: []domain.InstanceStatus{domain.InstanceStatusRunning},
		},
		{
			name: "a refused RUNNING write still counts as RUNNING in memory and stays pending", status: domain.InstanceStatusDegraded,
			refuse: true, wantStatus: domain.InstanceStatusRunning, wantPending: true,
			wantWrites: []domain.InstanceStatus{domain.InstanceStatusRunning},
		},
		{
			name: "a refused DEGRADED write keeps DEGRADED in memory and stays pending", status: domain.InstanceStatusDegraded,
			parks: []*park{w28aOpenPark(dept, "billing/B1")}, refuse: true, wantStatus: domain.InstanceStatusDegraded, wantPending: true,
			wantWrites:   []domain.InstanceStatus{domain.InstanceStatusDegraded},
			wantBranches: []domain.FailedBranch{{DepartmentID: dept, LastNodeKey: "billing/B1"}},
		},
		{
			name: "a restore that is pending is written even when the instance is not degraded", status: domain.InstanceStatusRunning,
			pending: true, wantStatus: domain.InstanceStatusRunning, wantWrites: []domain.InstanceStatus{domain.InstanceStatusRunning},
		},
		{
			name: "a held pause with no park left is applied instead of a RUNNING write", status: domain.InstanceStatusDegraded,
			pauseHeld: true, wantStatus: domain.InstanceStatusPaused, wantPauses: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			rig := &w28aSettleRig{refuse: tc.refuse}
			rig.register(env)
			var res w28aSettleResult

			env.ExecuteWorkflow(func(ctx wf.Context) (w28aSettleResult, error) {
				in := newInterpreter("tenant-1", "instance-1", "", nil, nil)
				in.status, in.statusRestorePending = tc.status, tc.pending
				in.parks = tc.parks
				in.pauseHeld, in.pauseSignal, in.pauseInitiator = tc.pauseHeld, held, held.Initiator
				res.LockCreatedPre = in.settleLock != nil
				in.settleStatusAfterAbandonedPark(ctx)
				res.Status, res.RestorePending, res.LockCreated = in.status, in.statusRestorePending, in.settleLock != nil
				return res, nil
			})

			require.NoError(t, env.GetWorkflowError())
			var out w28aSettleResult
			require.NoError(t, env.GetWorkflowResult(&out))
			assert.False(t, out.LockCreatedPre)
			assert.True(t, out.LockCreated, "the one-slot lock is created at first use")
			assert.Equal(t, tc.wantStatus, out.Status)
			assert.Equal(t, tc.wantPending, out.RestorePending)
			rig.mu.Lock()
			defer rig.mu.Unlock()
			var gotWrites []domain.InstanceStatus
			for _, w := range rig.status {
				gotWrites = append(gotWrites, w.Status)
				assert.Equal(t, "tenant-1", w.TenantID)
				assert.Equal(t, "instance-1", w.InstanceID)
			}
			assert.Equal(t, tc.wantWrites, gotWrites)
			if tc.wantBranches != nil {
				assert.Equal(t, tc.wantBranches, rig.status[0].FailedBranches)
			}
			require.Len(t, rig.pauses, tc.wantPauses)
			if tc.wantPauses > 0 {
				assert.Equal(t, port.PauseInstanceInput{
					InstanceID: "instance-1", TenantID: "tenant-1", AdminUserID: "admin-7", Initiator: domain.InitiatorAdmin, RecordVersion: 9,
				}, rig.pauses[0])
			}
		})
	}
}

func TestSettleStatusAfterAbandonedPark_TwoParksAbandonedTogetherSettleOneAtATimeReadingTheParksInsideTheLock(t *testing.T) {
	dept := uuid.MustParse("11111111-1111-4111-8111-111111111111")
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	rig := &w28aSettleRig{}
	rig.register(env)

	env.ExecuteWorkflow(func(ctx wf.Context) (domain.InstanceStatus, error) {
		in := newInterpreter("tenant-1", "instance-1", "", nil, nil)
		in.status = domain.InstanceStatusDegraded
		in.parks = []*park{w28aOpenPark(dept, "billing/B1")}
		done := wf.NewChannel(ctx)
		wf.Go(ctx, func(gctx wf.Context) {
			in.settleStatusAfterAbandonedPark(gctx)
			// The park that was open when this settle started closes before the next one takes the lock.
			in.parks = nil
			done.Send(gctx, struct{}{})
		})
		wf.Go(ctx, func(gctx wf.Context) {
			in.settleStatusAfterAbandonedPark(gctx)
			done.Send(gctx, struct{}{})
		})
		done.Receive(ctx, nil)
		done.Receive(ctx, nil)
		return in.status, nil
	})

	require.NoError(t, env.GetWorkflowError())
	var status domain.InstanceStatus
	require.NoError(t, env.GetWorkflowResult(&status))
	rig.mu.Lock()
	defer rig.mu.Unlock()
	require.Len(t, rig.status, 2, "each abandoned park settled once, one after the other")
	assert.Equal(t, domain.InstanceStatusDegraded, rig.status[0].Status)
	assert.Len(t, rig.status[0].FailedBranches, 1)
	assert.Equal(t, domain.InstanceStatusRunning, rig.status[1].Status, "the second settle read the parks inside the lock, so it saw the first one closed")
	assert.Equal(t, domain.InstanceStatusRunning, status)
}

func TestRetryStatusRestore_RunsAgainAPendingRestoreUnlessTheRunIsFinishing(t *testing.T) {
	cases := []struct {
		name       string
		pending    bool
		finishing  bool
		wantWrites int
	}{
		{name: "pending and running", pending: true, wantWrites: 1},
		{name: "pending but the run is finishing", pending: true, finishing: true, wantWrites: 0},
		{name: "nothing pending", pending: false, wantWrites: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			rig := &w28aSettleRig{}
			rig.register(env)

			env.ExecuteWorkflow(func(ctx wf.Context) (bool, error) {
				in := newInterpreter("tenant-1", "instance-1", "", nil, nil)
				in.statusRestorePending, in.finishing = tc.pending, tc.finishing
				in.retryStatusRestore(ctx)
				return in.statusRestorePending, nil
			})

			require.NoError(t, env.GetWorkflowError())
			var pendingAfter bool
			require.NoError(t, env.GetWorkflowResult(&pendingAfter))
			rig.mu.Lock()
			defer rig.mu.Unlock()
			assert.Len(t, rig.status, tc.wantWrites)
			assert.Equal(t, tc.pending && tc.finishing, pendingAfter, "an accepted retry clears the pending restore; a skipped one leaves it")
			for _, w := range rig.status {
				assert.Equal(t, domain.InstanceStatusRunning, w.Status)
				assert.Equal(t, "instance-1", w.InstanceID)
			}
		})
	}
}

func TestAwaitWindDown_WaitsUpToThirtySecondsForTheChildToEnd(t *testing.T) {
	cases := []struct {
		name        string
		childEnds   bool
		wantElapsed time.Duration
	}{
		{name: "the child never ends", childEnds: false, wantElapsed: 30 * time.Second},
		{name: "the child has already ended", childEnds: true, wantElapsed: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()

			env.ExecuteWorkflow(func(ctx wf.Context) (time.Duration, error) {
				in := newInterpreter("tenant-1", "instance-1", "", nil, nil)
				doneCh := wf.NewBufferedChannel(ctx, 1)
				if tc.childEnds {
					doneCh.Send(ctx, struct{}{})
				}
				start := wf.Now(ctx)
				in.awaitWindDown(ctx, doneCh)
				return wf.Now(ctx).Sub(start), nil
			})

			require.NoError(t, env.GetWorkflowError())
			var elapsed time.Duration
			require.NoError(t, env.GetWorkflowResult(&elapsed))
			assert.Equal(t, tc.wantElapsed, elapsed)
		})
	}
}
