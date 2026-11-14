package service_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type csaStateCall struct {
	tenantID  uuid.UUID
	status    string
	changedAt time.Time
}

// csaTenantStates records each tenant status it was asked to keep.
type csaTenantStates struct {
	mu    sync.Mutex
	calls []csaStateCall
}

func (s *csaTenantStates) Record(_ context.Context, tenantID uuid.UUID, status string, changedAt time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, csaStateCall{tenantID, status, changedAt})
	return nil
}

func (s *csaTenantStates) Get(context.Context, uuid.UUID) (string, bool, error) {
	return "", false, nil
}

func TestTenantLifecycleReconciler_Apply_AFailedReturnWriteStopsBeforeAnyInstanceIsSignalled(t *testing.T) {
	changedAt := time.Date(2026, 10, 18, 9, 30, 0, 0, time.UTC)
	for _, tc := range []struct {
		name           string
		previous       string
		status         string
		instanceStatus domain.InstanceStatus
	}{
		{"a suspension that would pause", "active", "suspended", domain.InstanceStatusRunning},
		{"a reactivation that would resume", "suspended", "active", domain.InstanceStatusPaused},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc, instances, tasks, _, _, outbox, temporal := newTenantLifecycleHarness()
			retention := &fakeRetention{recordErr: assert.AnError}
			states := &csaTenantStates{}
			svc.Retention, svc.States = retention, states
			tenantID := uuid.New()
			inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: tc.instanceStatus, TemporalWorkflowID: "tenant:one", RecordVersion: 1}
			instances.byID[inst.ID] = inst
			task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: inst.ID, Status: domain.TaskStatusReady, RecordVersion: 1}
			tasks.byID[task.ID] = task

			err := svc.Apply(context.Background(), port.TenantLifecycleInput{TenantID: tenantID, Status: tc.status, PreviousStatus: tc.previous, ChangedAt: changedAt})

			require.ErrorIs(t, err, assert.AnError, "the event is redelivered")
			assert.Equal(t, []csaStateCall{{tenantID, tc.status, changedAt}}, states.calls, "the new status was already kept")
			assert.Equal(t, []recordedReturn{{tenantID, changedAt}}, retention.returned, "the return write named the tenant and the event time")
			assert.Empty(t, temporal.signals, "no instance is signalled")
			assert.Empty(t, temporal.terminate)
			assert.Equal(t, tc.instanceStatus, inst.Status)
			assert.Equal(t, domain.TaskStatusReady, task.Status)
			assert.Empty(t, outbox.enqueued)
		})
	}
}

func TestTenantLifecycleReconciler_Apply_AReturnWriteThatFailsStopsEveryPauseAndResumeStatus(t *testing.T) {
	now := time.Date(2026, 10, 19, 8, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name             string
		previous, status string
	}{
		{"past_due is a resume", "suspended", "past_due"},
		{"trial is a resume", "trial_expired", "trial"},
		{"cancelled is a pause", "active", "cancelled"},
		{"trial_expired is a pause", "trial", "trial_expired"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc, instances, _, _, _, _, temporal := newTenantLifecycleHarness()
			retention := &fakeRetention{recordErr: assert.AnError}
			svc.Retention = retention
			svc.Now = func() time.Time { return now }
			tenantID := uuid.New()
			for _, status := range []domain.InstanceStatus{domain.InstanceStatusRunning, domain.InstanceStatusPaused} {
				inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: status, TemporalWorkflowID: "tenant:" + string(status), RecordVersion: 1}
				instances.byID[inst.ID] = inst
			}

			err := svc.Apply(context.Background(), port.TenantLifecycleInput{TenantID: tenantID, Status: tc.status, PreviousStatus: tc.previous})

			require.ErrorIs(t, err, assert.AnError)
			assert.Equal(t, []recordedReturn{{tenantID, now}}, retention.returned, "without an event time the clock's time is written")
			assert.Zero(t, instances.listByTenantCalls, "the instances are not even listed")
			assert.Empty(t, temporal.signals)
		})
	}
}

// csaStaleInstances lists an instance as running while the transaction reads it
// in its real, terminal state: another writer ended it in between.
type csaStaleInstances struct {
	*fakeInstanceRepo
	mu            sync.Mutex
	stale         *domain.Instance
	reads         []csaIDCall
	statusUpdates []csaIDCall
}

func (r *csaStaleInstances) ListByTenant(_ context.Context, tenantID uuid.UUID, _ port.InstanceListFilter, _ port.PageRequest) ([]*domain.Instance, *port.Cursor, error) {
	if r.stale.TenantID != tenantID {
		return nil, nil, nil
	}
	listed := *r.stale
	listed.Status = domain.InstanceStatusRunning
	return []*domain.Instance{&listed}, nil, nil
}

func (r *csaStaleInstances) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	r.mu.Lock()
	r.reads = append(r.reads, csaIDCall{tenantID, id})
	r.mu.Unlock()
	return r.fakeInstanceRepo.GetByID(ctx, tenantID, id)
}

func (r *csaStaleInstances) UpdateStatus(ctx context.Context, tenantID, id uuid.UUID, status domain.InstanceStatus, recordVersion int64) (*domain.Instance, error) {
	r.mu.Lock()
	r.statusUpdates = append(r.statusUpdates, csaIDCall{tenantID, id})
	r.mu.Unlock()
	return r.fakeInstanceRepo.UpdateStatus(ctx, tenantID, id, status, recordVersion)
}

func TestTenantLifecycleReconciler_Offboard_AnInstanceAlreadyTerminalInTheTransactionIsLeftAlone(t *testing.T) {
	for _, terminal := range []domain.InstanceStatus{domain.InstanceStatusCompleted, domain.InstanceStatusTerminated, domain.InstanceStatusFailed} {
		t.Run(string(terminal), func(t *testing.T) {
			svc, instances, tasks, assignments, _, outbox, temporal := newTenantLifecycleHarness()
			retention := &fakeRetention{}
			svc.Retention = retention
			tenantID := uuid.New()
			inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: terminal, TemporalWorkflowID: "tenant:subject:key", TemporalRunID: "run-9", RecordVersion: 4}
			instances.byID[inst.ID] = inst
			stale := &csaStaleInstances{fakeInstanceRepo: instances, stale: inst}
			svc.Instances = stale
			task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: inst.ID, Status: domain.TaskStatusReady, RecordVersion: 1}
			tasks.byID[task.ID] = task
			a := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: uuid.New(), IsActive: true}
			assignments.byID[a.ID] = a

			err := svc.Apply(context.Background(), port.TenantLifecycleInput{TenantID: tenantID, Status: "offboarded", PreviousStatus: "active"})

			require.NoError(t, err)
			assert.Equal(t, []csaIDCall{{tenantID, inst.ID}}, stale.reads, "the transaction reads the instance in the tenant")
			assert.Empty(t, stale.statusUpdates, "no status write")
			assert.Equal(t, terminal, inst.Status)
			assert.Equal(t, int64(4), inst.RecordVersion)
			assert.Equal(t, domain.TaskStatusReady, task.Status, "no task cascade")
			assert.True(t, a.IsActive, "no assignment is vacated")
			assert.Empty(t, outbox.enqueued, "no terminated event")
			assert.Equal(t, []terminateCall{
				{"tenant:subject:key", "run-9", "tenant offboarded"},
				{"tenant:subject:key", "run-9", "tenant offboarded"},
			}, temporal.terminateCalls(), "the second terminate still runs")
			assert.Len(t, retention.recorded, 1, "the tenant is still recorded as offboarded")
		})
	}
}
