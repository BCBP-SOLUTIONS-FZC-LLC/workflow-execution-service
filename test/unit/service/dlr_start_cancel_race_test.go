package service_test

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

// dlrKeyedLocker is a lock per scope key: a second caller of a held key waits for it and
// announces on blocked that it is waiting.
type dlrKeyedLocker struct {
	mu      sync.Mutex
	held    map[string]chan struct{}
	blocked chan string
}

func newDlrKeyedLocker() *dlrKeyedLocker {
	return &dlrKeyedLocker{held: map[string]chan struct{}{}, blocked: make(chan string, 16)}
}

func (l *dlrKeyedLocker) WithLock(ctx context.Context, key string, fn func(context.Context) error) error {
	return l.WithLocks(ctx, []string{key}, fn)
}

func (l *dlrKeyedLocker) WithLocks(ctx context.Context, keys []string, fn func(context.Context) error) error {
	ordered := slices.Clone(keys)
	slices.Sort(ordered)
	for _, key := range ordered {
		l.acquire(key)
	}
	defer func() {
		for _, key := range ordered {
			l.release(key)
		}
	}()
	return fn(ctx)
}

func (l *dlrKeyedLocker) acquire(key string) {
	for {
		l.mu.Lock()
		wait, busy := l.held[key]
		if !busy {
			l.held[key] = make(chan struct{})
			l.mu.Unlock()
			return
		}
		l.mu.Unlock()
		select {
		case l.blocked <- key:
		default:
		}
		<-wait
	}
}

func (l *dlrKeyedLocker) release(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	close(l.held[key])
	delete(l.held, key)
}

// dlrGatedAssignments stops the first read of one user's active assignments after it has
// answered, until release is closed.
type dlrGatedAssignments struct {
	port.TaskAssignmentRepository
	user    uuid.UUID
	reached chan struct{}
	release chan struct{}
	once    sync.Once
}

func (g *dlrGatedAssignments) ListActiveByUser(ctx context.Context, tenantID, userID uuid.UUID) ([]*domain.TaskAssignment, error) {
	rows, err := g.TaskAssignmentRepository.ListActiveByUser(ctx, tenantID, userID)
	if userID == g.user {
		g.once.Do(func() {
			close(g.reached)
			<-g.release
		})
	}
	return rows, err
}

type dlrRace struct {
	w       *endedFirstWorld
	locker  *dlrKeyedLocker
	gate    *dlrGatedAssignments
	client  *service.WorkflowClient
	release func()
}

func dlrNewRace(t *testing.T) *dlrRace {
	t.Helper()
	w := newEndedFirstWorld(t)
	r := &dlrRace{w: w, locker: newDlrKeyedLocker()}
	r.gate = &dlrGatedAssignments{TaskAssignmentRepository: w.svc.Assignments, user: w.delegate, reached: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	r.release = func() { releaseOnce.Do(func() { close(r.gate.release) }) }
	t.Cleanup(r.release)
	w.svc.Assignments = r.gate
	w.svc.Locks = r.locker
	r.client = &service.WorkflowClient{
		Instances: w.svc.Instances, Tasks: w.svc.Tasks, Assignments: r.gate, Transactor: w.svc.Transactor,
		Outbox: w.svc.Outbox, Signals: w.svc.Signals, Validator: w.svc.Validator, Ended: w.ended, Locks: r.locker,
	}
	fx10AllowClientEligibility(r.client, w.task.DepartmentID)
	return r
}

// startEmitted runs a start whose event was emitted at emittedAt; startsAt is the
// delegation's own business start date, which says nothing about when the event was sent.
func (r *dlrRace) startEmitted(delegationID uuid.UUID, startsAt, emittedAt time.Time) error {
	return r.w.svc.Reroute(context.Background(), port.DelegationRerouteInput{
		TenantID: r.w.tenantID, DelegationID: delegationID, DelegatorID: r.w.delegator, DelegateID: r.w.delegate, Scope: "all",
		StartsAt: startsAt, EmittedAt: emittedAt,
	})
}

func (r *dlrRace) cancel(delegationID *uuid.UUID) (int, error) {
	return r.client.CancelByDelegate(context.Background(), port.CancelByDelegateInput{
		TenantID: r.w.tenantID, DelegateUserID: r.w.delegate, DelegationID: delegationID,
	})
}

// overlap runs a cancel that has read the delegate's rows and is held there, then runs a start
// and waits until it either waits for the cancel or finishes on its own. It returns once both
// have finished.
func (r *dlrRace) overlap(t *testing.T, delegationID uuid.UUID, cancelDelegation *uuid.UUID, emittedAt time.Time) {
	t.Helper()
	cancelDone := make(chan error, 1)
	go func() {
		_, err := r.cancel(cancelDelegation)
		cancelDone <- err
	}()
	<-r.gate.reached

	startDone := make(chan error, 1)
	go func() { startDone <- r.startEmitted(delegationID, time.Now().Add(time.Hour), emittedAt) }()
	select {
	case <-r.locker.blocked:
	case err := <-startDone:
		r.release()
		<-cancelDone
		require.Failf(t, "the start did not wait for the cancel", "it finished on its own with %v", err)
	}

	r.release()
	require.NoError(t, <-cancelDone)
	require.NoError(t, <-startDone)
}

func (r *dlrRace) requireNothingMoved(t *testing.T) {
	t.Helper()
	assert.True(t, r.w.held.IsActive, "the delegator keeps the work")
	assert.Nil(t, assignmentOf(r.w.assignments, r.w.task.ID, r.w.delegate), "the delegate was given nothing")
	assert.Empty(t, r.w.outbox.enqueued)
	assert.Empty(t, r.w.outbox.signals)
}

func TestDelegationStart_WaitsForTheCancelOfItsDelegationAndThenMovesNothing(t *testing.T) {
	r := dlrNewRace(t)
	delegationID := uuid.New()

	r.overlap(t, delegationID, &delegationID, time.Now().Add(-time.Hour))

	r.requireNothingMoved(t)
	ended, err := r.w.ended.IsEnded(context.Background(), r.w.tenantID, delegationID)
	require.NoError(t, err)
	assert.True(t, ended, "the cancel marked the delegation ended")
}

func TestDelegationStart_WaitsForACancelOfEveryDelegationToTheDelegateAndThenMovesNothing(t *testing.T) {
	r := dlrNewRace(t)

	r.overlap(t, uuid.New(), nil, time.Now().Add(-time.Hour))

	r.requireNothingMoved(t)
}

func TestDelegationStart_EmittedBeforeACancelOfEveryDelegationToTheDelegateMovesNothingWhateverItsStartDate(t *testing.T) {
	for name, startsAt := range map[string]time.Time{
		"a start date far ahead": time.Now().Add(24 * time.Hour),
		"a start date long past": time.Now().Add(-24 * time.Hour),
	} {
		t.Run(name, func(t *testing.T) {
			r := dlrNewRace(t)
			r.release()
			_, err := r.cancel(nil)
			require.NoError(t, err)

			require.NoError(t, r.startEmitted(uuid.New(), startsAt, time.Now().Add(-time.Hour)))

			r.requireNothingMoved(t)
		})
	}
}

func TestDelegationStart_EmittedAfterACancelOfEveryDelegationToTheDelegateMovesWorkWhateverItsStartDate(t *testing.T) {
	cancelled := time.Now().UTC()
	for name, startsAt := range map[string]time.Time{
		"a start date in the second of the cancel": cancelled.Truncate(time.Second),
		"a start date one second before":           cancelled.Add(-time.Second),
		"a start date a day earlier":               cancelled.Add(-24 * time.Hour),
	} {
		t.Run(name, func(t *testing.T) {
			r := dlrNewRace(t)
			r.release()
			_, err := r.cancel(nil)
			require.NoError(t, err)

			require.NoError(t, r.startEmitted(uuid.New(), startsAt, time.Now().Add(time.Hour)))

			assert.False(t, r.w.held.IsActive, "a delegation created after the cancel takes effect")
			assert.NotNil(t, assignmentOf(r.w.assignments, r.w.task.ID, r.w.delegate))
		})
	}
}

func TestDelegationStart_AfterACancelOfAnOlderDelegationStillMovesWorkForANewerDelegationID(t *testing.T) {
	r := dlrNewRace(t)
	r.release()
	older := uuid.New()
	_, err := r.cancel(&older)
	require.NoError(t, err)

	require.NoError(t, r.startEmitted(uuid.New(), time.Now().Add(-time.Hour), time.Now().Add(-time.Hour)))

	assert.False(t, r.w.held.IsActive, "a cancel of one delegation does not stop another")
	assert.NotNil(t, assignmentOf(r.w.assignments, r.w.task.ID, r.w.delegate))
}
