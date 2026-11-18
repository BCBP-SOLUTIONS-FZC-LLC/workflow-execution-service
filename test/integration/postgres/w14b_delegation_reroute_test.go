//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

var errW14bRendezvousTimeout = errors.New("w14b: the two reroutes did not meet before the locks")

type w14bPlanRead struct{ tenantID, versionID uuid.UUID }

// w14bPlanSource serves the one compiled plan and records the tenant and
// version of every read.
type w14bPlanSource struct {
	resp  *port.CompiledWorkflow
	mu    sync.Mutex
	reads []w14bPlanRead
}

func (d *w14bPlanSource) GetCompiledWorkflow(_ context.Context, tenantID, versionID uuid.UUID) (*port.CompiledWorkflow, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.reads = append(d.reads, w14bPlanRead{tenantID: tenantID, versionID: versionID})
	return d.resp, nil
}

func (d *w14bPlanSource) snapshot() []w14bPlanRead {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]w14bPlanRead(nil), d.reads...)
}

// w14bLockSpy decorates the scope locker: it records the keys of each call and
// the most calls that were inside their critical section at the same moment.
type w14bLockSpy struct {
	inner port.ScopeSetLocker

	mu      sync.Mutex
	calls   [][]string
	inside  int
	maxSeen int
}

func (l *w14bLockSpy) enter(keys []string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = append(l.calls, append([]string(nil), keys...))
	l.inside++
	l.maxSeen = max(l.maxSeen, l.inside)
}

func (l *w14bLockSpy) leave() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.inside--
}

func (l *w14bLockSpy) WithLock(ctx context.Context, scopeKey string, fn func(context.Context) error) error {
	return l.inner.WithLock(ctx, scopeKey, func(ctx context.Context) error {
		l.enter([]string{scopeKey})
		defer l.leave()
		return fn(ctx)
	})
}

func (l *w14bLockSpy) WithLocks(ctx context.Context, scopeKeys []string, fn func(context.Context) error) error {
	return l.inner.WithLocks(ctx, scopeKeys, func(ctx context.Context) error {
		l.enter(scopeKeys)
		defer l.leave()
		return fn(ctx)
	})
}

func (l *w14bLockSpy) snapshot() (calls [][]string, maxInside int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([][]string(nil), l.calls...), l.maxSeen
}

// w14bEligibility answers "eligible" to every check and records the tenant of
// each batch. When parties is above one, each batch waits until that many
// callers have arrived, so the callers' reads before the lock overlap.
type w14bEligibility struct {
	mu       sync.Mutex
	tenants  []uuid.UUID
	parties  int
	arrived  int
	released chan struct{}
}

func (e *w14bEligibility) CheckEligibility(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string) (bool, error) {
	return true, nil
}

func (e *w14bEligibility) CheckEligibilityBatch(_ context.Context, tenantID uuid.UUID, requests []port.EligibilityCheckRequest) ([]port.EligibilityResult, error) {
	e.mu.Lock()
	e.tenants = append(e.tenants, tenantID)
	e.arrived++
	if e.parties > 1 && e.arrived == e.parties {
		close(e.released)
	}
	released := e.released
	e.mu.Unlock()
	if e.parties > 1 {
		select {
		case <-released:
		case <-time.After(30 * time.Second):
			return nil, errW14bRendezvousTimeout
		}
	}
	out := make([]port.EligibilityResult, len(requests))
	for i := range out {
		out[i].Eligible = true
	}
	return out, nil
}

type w14bRerouteRig struct {
	superPool   *pgcommon.Pool
	reconciler  *service.DelegationReconciler
	assignments *postgres.TaskAssignmentRepo
	eligibility *w14bEligibility
	plans       *w14bPlanSource
	locks       *w14bLockSpy
	versionID   uuid.UUID
	tenantID    uuid.UUID
	delegator   uuid.UUID
	task        *domain.Task
	held        *domain.TaskAssignment
}

func w14bSorted(keys []string) []string {
	out := append([]string(nil), keys...)
	slices.Sort(out)
	return out
}

func w14bCount(t *testing.T, pool *pgcommon.Pool, query string, args ...any) int {
	t.Helper()
	var n int
	require.NoError(t, pool.WithConn(context.Background(), func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx, query, args...).Scan(&n)
	}))
	return n
}

// w14bNewRerouteRig seeds one instance, one task and one assignment held by the
// delegator, and wires a reconciler to the row-level-security app role.
func w14bNewRerouteRig(t *testing.T, parties int) *w14bRerouteRig {
	t.Helper()
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	taskRepo := postgres.NewTaskRepo(appPool)
	assignmentRepo := postgres.NewTaskAssignmentRepo(appPool)
	outboxRepo := postgres.NewOutboxRepo(appPool)
	validator, err := eventbus.NewSchemaValidator()
	require.NoError(t, err)

	deptID := uuid.New()
	planJSON, err := json.Marshal(dsl.CompiledCollaboration{
		SchemaVersion: dsl.CurrentSchemaVersion,
		MainPlan:      "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main", TaskQueue: "wf-queue-default",
			Departments: []dsl.DepartmentDef{{
				ID: "finance", Label: "Finance", IAMDepartmentID: deptID.String(),
				Stages: []dsl.StageDef{{Type: "userTask", NodeID: "review", Role: "reviewer"}},
			}},
		}},
	})
	require.NoError(t, err)

	eligibility := &w14bEligibility{parties: parties, released: make(chan struct{})}
	ctx := context.Background()
	tenantID, delegator := uuid.New(), uuid.New()
	inst := newInstance(tenantID, time.Now().UTC())
	require.NoError(t, instanceRepo.Create(ctx, inst))
	task := newTask(tenantID, inst.ID)
	task.DepartmentID = deptID
	require.NoError(t, taskRepo.Create(ctx, task))
	held := newTaskAssignment(tenantID, task.ID, delegator)
	require.NoError(t, assignmentRepo.Create(ctx, held))

	plans := &w14bPlanSource{resp: &port.CompiledWorkflow{
		WorkflowID: inst.WorkflowID, VersionID: inst.WorkflowVersionID, Status: "PUBLISHED", IsValid: true, CompiledPlanJSON: string(planJSON),
	}}
	locks := &w14bLockSpy{inner: postgres.NewRecencyGuardRepo(appPool, appPool)}

	return &w14bRerouteRig{
		superPool: superPool,
		reconciler: &service.DelegationReconciler{
			Instances: instanceRepo, Tasks: taskRepo, Assignments: assignmentRepo,
			Outbox: outboxRepo, Signals: outboxRepo, Transactor: postgres.NewTransactor(appPool),
			Definitions: plans,
			Eligibility: eligibility, Validator: validator,
			Availability: postgres.NewUserAvailabilityRepo(appPool),
			Locks:        locks,
			Ended:        postgres.NewEndedDelegationRepo(appPool),
		},
		assignments: assignmentRepo, eligibility: eligibility, plans: plans, locks: locks, versionID: inst.WorkflowVersionID,
		tenantID: tenantID, delegator: delegator, task: task, held: held,
	}
}

func (r *w14bRerouteRig) reroute(delegationID, delegateID uuid.UUID) error {
	return r.reconciler.Reroute(context.Background(), port.DelegationRerouteInput{
		TenantID: r.tenantID, DelegationID: delegationID, DelegatorID: r.delegator, DelegateID: delegateID, Scope: "all",
	})
}

const (
	w14bReassignedEvents = `SELECT count(*) FROM outbox_events WHERE tenant_id = $1 AND event_type = 'WorkflowTaskReassigned'`
	w14bPendingSignals   = `SELECT count(*) FROM outbox_events WHERE tenant_id = $1 AND event_type = 'WorkflowSignalPending'`
)

func TestDelegationReroute_EveryWriteOfTheMoveRunsUnderTheTenant(t *testing.T) {
	rig := w14bNewRerouteRig(t, 1)
	ctx := context.Background()
	delegate, delegationID := uuid.New(), uuid.New()

	require.NoError(t, rig.reroute(delegationID, delegate))

	require.Equal(t, []uuid.UUID{rig.tenantID}, rig.eligibility.tenants)
	reads := rig.plans.snapshot()
	require.NotEmpty(t, reads)
	for _, read := range reads {
		assert.Equal(t, w14bPlanRead{tenantID: rig.tenantID, versionID: rig.versionID}, read, "the plan is read for the instance's own tenant and version")
	}
	vacated, err := rig.assignments.GetByID(ctx, rig.tenantID, rig.held.ID)
	require.NoError(t, err)
	assert.False(t, vacated.IsActive, "the vacate was written")
	active, err := rig.assignments.ListActiveByTask(ctx, rig.tenantID, rig.task.ID)
	require.NoError(t, err)
	require.Len(t, active, 1, "the new assignment was written")
	assert.Equal(t, delegate, active[0].UserID)
	assert.Equal(t, rig.tenantID, active[0].TenantID)
	assert.Equal(t, "delegation:"+delegationID.String()+":"+rig.delegator.String(), active[0].Reason)
	assert.Equal(t, 1, w14bCount(t, rig.superPool, w14bReassignedEvents, rig.tenantID.String()), "the event was written under the tenant")
	assert.Equal(t, 1, w14bCount(t, rig.superPool, w14bPendingSignals, rig.tenantID.String()), "the signal was written under the tenant")

	other := uuid.New()
	seen, err := rig.assignments.ListActiveByTask(ctx, other, rig.task.ID)
	require.NoError(t, err)
	assert.Empty(t, seen, "another tenant sees none of the moved rows")
	_, err = rig.assignments.GetByID(ctx, other, active[0].ID)
	require.ErrorIs(t, err, domain.ErrNotFound)
}

func TestDelegationReroute_TwoRerouteEventsForOneDelegatorMoveTheRowOnce(t *testing.T) {
	rig := w14bNewRerouteRig(t, 2)
	delegates := []uuid.UUID{uuid.New(), uuid.New()}

	delegations := []uuid.UUID{uuid.New(), uuid.New()}

	errs := make([]error, len(delegates))
	var wg sync.WaitGroup
	for i, delegate := range delegates {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = rig.reroute(delegations[i], delegate)
		}()
	}
	wg.Wait()

	for i, err := range errs {
		require.NoError(t, err, "reroute %d", i)
	}
	require.Len(t, rig.eligibility.tenants, 2, "both reroutes read the delegator's row before either took the lock")
	for _, tenant := range rig.eligibility.tenants {
		assert.Equal(t, rig.tenantID, tenant)
	}
	reads := rig.plans.snapshot()
	require.NotEmpty(t, reads)
	for _, read := range reads {
		assert.Equal(t, w14bPlanRead{tenantID: rig.tenantID, versionID: rig.versionID}, read)
	}
	lockCalls, maxInside := rig.locks.snapshot()
	assert.Equal(t, 1, maxInside, "the two reroutes were never inside the lock together")
	require.Len(t, lockCalls, 2)
	wantScopes := make([][]string, len(delegates))
	for i, delegate := range delegates {
		wantScopes[i] = []string{
			port.UserAvailabilityScope(rig.tenantID, rig.delegator),
			port.DelegationScope(rig.tenantID, delegations[i]),
			port.DelegateScope(rig.tenantID, delegate),
		}
	}
	for i, want := range wantScopes {
		matching := 0
		for _, keys := range lockCalls {
			if len(keys) == len(want) && assert.ObjectsAreEqual(w14bSorted(want), w14bSorted(keys)) {
				matching++
			}
		}
		assert.Equal(t, 1, matching, "exactly one call takes the delegator's, delegation %d's and its delegate's scope", i)
	}
	active, err := rig.assignments.ListActiveByTask(context.Background(), rig.tenantID, rig.task.ID)
	require.NoError(t, err)
	require.Len(t, active, 1, "the row is held by exactly one delegate")
	winner := active[0].UserID
	require.Contains(t, delegates, winner)
	for _, delegate := range delegates {
		rows := w14bCount(t, rig.superPool, `SELECT count(*) FROM workflow_task_assignment WHERE task_id = $1 AND user_id = $2`, rig.task.ID, delegate)
		if delegate == winner {
			assert.Equal(t, 1, rows)
		} else {
			assert.Zero(t, rows, "the later reroute found the row already taken and wrote nothing for its delegate")
		}
	}
	assert.Equal(t, 1, w14bCount(t, rig.superPool, w14bReassignedEvents, rig.tenantID.String()), "one event, not one per reroute")
	assert.Equal(t, 1, w14bCount(t, rig.superPool, w14bPendingSignals, rig.tenantID.String()), "one signal, not one per reroute")
}
