package service_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"
)

// w12aTenantTx runs the callback on the same context and records the tenant it carries and
// whether the callback is running.
type w12aTenantTx struct {
	running bool
	tenants []string
}

func (t *w12aTenantTx) RunInTx(ctx context.Context, fn func(context.Context) error) error {
	return t.run(ctx, fn)
}

func (t *w12aTenantTx) RunInTxWithRetry(ctx context.Context, fn func(context.Context) error) error {
	return t.run(ctx, fn)
}

func (t *w12aTenantTx) run(ctx context.Context, fn func(context.Context) error) error {
	tenant := ""
	if g, ok := pgcommon.GUCSetFromContext(ctx); ok {
		tenant = g.TenantID
	}
	t.tenants = append(t.tenants, tenant)
	t.running = true
	defer func() { t.running = false }()
	return fn(ctx)
}

type w12aInstanceRead struct {
	tenantID, id uuid.UUID
	inTx         bool
}

type w12aStatusWrite struct {
	tenantID, id uuid.UUID
	status       string
	version      int64
	inTx         bool
}

// w12aTenantInstances records every read and status write with its tenant.
type w12aTenantInstances struct {
	*fakeInstanceRepo
	tx      *w12aTenantTx
	reads   []w12aInstanceRead
	updates []w12aStatusWrite
}

func (r *w12aTenantInstances) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	r.reads = append(r.reads, w12aInstanceRead{tenantID: tenantID, id: id, inTx: r.tx.running})
	return r.fakeInstanceRepo.GetByID(ctx, tenantID, id)
}

func (r *w12aTenantInstances) UpdateStatus(ctx context.Context, tenantID, id uuid.UUID, status domain.InstanceStatus, version int64) (*domain.Instance, error) {
	r.updates = append(r.updates, w12aStatusWrite{tenantID: tenantID, id: id, status: string(status), version: version, inTx: r.tx.running})
	return r.fakeInstanceRepo.UpdateStatus(ctx, tenantID, id, status, version)
}

// w12aTenantTasks records every task status write with its tenant.
type w12aTenantTasks struct {
	*fakeTaskRepo
	updates []w12aStatusWrite
}

func (r *w12aTenantTasks) UpdateStatus(ctx context.Context, tenantID, id uuid.UUID, status domain.TaskStatus, version int64) (*domain.Task, error) {
	r.updates = append(r.updates, w12aStatusWrite{tenantID: tenantID, id: id, status: string(status), version: version})
	return r.fakeTaskRepo.UpdateStatus(ctx, tenantID, id, status, version)
}

// w12aTenantAssignments records every vacate with its tenant.
type w12aTenantAssignments struct {
	*fakeAssignmentRepo
	vacated []w12aAssignmentCall
}

func (r *w12aTenantAssignments) Vacate(ctx context.Context, tenantID, id uuid.UUID) (*domain.TaskAssignment, error) {
	r.vacated = append(r.vacated, w12aAssignmentCall{tenantID: tenantID, id: id})
	return r.fakeAssignmentRepo.Vacate(ctx, tenantID, id)
}

type w12aTenantRig struct {
	svc         *service.TenantLifecycleReconciler
	tx          *w12aTenantTx
	instances   *w12aTenantInstances
	tasks       *w12aTenantTasks
	assignments *w12aTenantAssignments
	queues      *fakeActiveTaskQueueRepo
	outbox      *fakeOutbox
	temporal    *fakeTemporalClient
	states      *fakeTenantStates
	retention   *fakeRetention
	tenantID    uuid.UUID
	now         time.Time
}

func w12aNewTenantRig() *w12aTenantRig {
	tx := &w12aTenantTx{}
	rig := &w12aTenantRig{
		tx:          tx,
		instances:   &w12aTenantInstances{fakeInstanceRepo: newFakeInstanceRepo(), tx: tx},
		tasks:       &w12aTenantTasks{fakeTaskRepo: newFakeTaskRepo()},
		assignments: &w12aTenantAssignments{fakeAssignmentRepo: newFakeAssignmentRepo()},
		queues:      newFakeActiveTaskQueueRepo(),
		outbox:      &fakeOutbox{},
		temporal:    &fakeTemporalClient{},
		states:      &fakeTenantStates{},
		retention:   &fakeRetention{},
		tenantID:    uuid.New(),
		now:         time.Date(2026, 10, 18, 9, 0, 0, 0, time.UTC),
	}
	rig.svc = &service.TenantLifecycleReconciler{
		Instances: rig.instances, Tasks: rig.tasks, Assignments: rig.assignments, Queues: rig.queues,
		Outbox: rig.outbox, Transactor: tx, Temporal: rig.temporal, Validator: realValidator(),
		States: rig.states, Retention: rig.retention, HistoryRetention: 48 * time.Hour,
		Now: func() time.Time { return rig.now },
	}
	return rig
}

func (rig *w12aTenantRig) instance(status domain.InstanceStatus) *domain.Instance {
	inst := &domain.Instance{
		ID: uuid.New(), TenantID: rig.tenantID, Status: status, RecordVersion: 3,
		SubjectID: "subject", BusinessKey: uuid.NewString(), WorkflowVersionID: uuid.New(),
		TemporalWorkflowID: rig.tenantID.String() + ":" + uuid.NewString(), TemporalRunID: uuid.NewString(),
	}
	rig.instances.byID[inst.ID] = inst
	return inst
}

// task adds a task of the instance with the given status, and one active assignment per user.
func (rig *w12aTenantRig) task(inst *domain.Instance, status domain.TaskStatus, assignees ...uuid.UUID) (*domain.Task, []*domain.TaskAssignment) {
	task := &domain.Task{
		ID: uuid.New(), TenantID: rig.tenantID, WorkflowInstanceID: inst.ID, NodeKey: "finance/" + uuid.NewString(),
		DepartmentID: uuid.New(), Status: status, RecordVersion: 5, CreatedAt: rig.now,
	}
	rig.tasks.byID[task.ID] = task
	var rows []*domain.TaskAssignment
	for _, user := range assignees {
		a := &domain.TaskAssignment{ID: uuid.New(), TenantID: rig.tenantID, TaskID: task.ID, UserID: user, IsActive: true}
		rig.assignments.add(a)
		rows = append(rows, a)
	}
	return task, rows
}

func (rig *w12aTenantRig) offboard() error {
	return rig.svc.Apply(context.Background(), port.TenantLifecycleInput{
		TenantID: rig.tenantID, Status: domain.TenantStatusOffboarded, PreviousStatus: domain.TenantStatusActive, ChangedAt: rig.now,
	})
}

func w12aEventsOfType(t *testing.T, rig *w12aTenantRig, eventType string) []json.RawMessage {
	t.Helper()
	var out []json.RawMessage
	for _, env := range rig.outbox.enqueued {
		assert.Equal(t, rig.tenantID.String(), env.TenantID)
		if env.Type == eventType {
			out = append(out, env.Payload)
		}
	}
	return out
}

func TestTenantLifecycleReconciler_Offboard_AnInstanceThatEndedAfterItWasListed_IsLeftAsItIsWithNoEvent(t *testing.T) {
	for _, ended := range []domain.InstanceStatus{domain.InstanceStatusCompleted, domain.InstanceStatusTerminated, domain.InstanceStatusFailed} {
		t.Run(string(ended), func(t *testing.T) {
			rig := w12aNewTenantRig()
			racing := rig.instance(domain.InstanceStatusRunning)
			racingTask, racingRows := rig.task(racing, domain.TaskStatusReady, uuid.New())
			control := rig.instance(domain.InstanceStatusRunning)
			endedVersion := racing.RecordVersion + 1
			rig.temporal.terminateFunc = func(_ context.Context, workflowID, _, _ string) error {
				if workflowID == racing.TemporalWorkflowID {
					racing.Status, racing.RecordVersion = ended, endedVersion
				}
				return nil
			}

			require.NoError(t, rig.offboard())

			assert.Equal(t, ended, racing.Status, "the status it reached on its own stands")
			assert.Equal(t, endedVersion, racing.RecordVersion, "nothing was written to it")
			assert.Equal(t, domain.TaskStatusReady, racingTask.Status, "its tasks are not failed")
			assert.True(t, racingRows[0].IsActive, "its assignments are not vacated")
			assert.Empty(t, rig.tasks.updates, "no task is written")
			require.Len(t, rig.instances.updates, 1, "only the instance that was still open is written")
			assert.Equal(t, w12aStatusWrite{tenantID: rig.tenantID, id: control.ID, status: "TERMINATED", version: 3, inTx: true}, rig.instances.updates[0])
			terminated := w12aEventsOfType(t, rig, "WorkflowInstanceTerminated")
			require.Len(t, terminated, 1, "an event for the instance that was terminated, none for the one that had ended")
			assert.Contains(t, string(terminated[0]), control.ID.String())
			assert.NotContains(t, string(terminated[0]), racing.ID.String())
			assert.Len(t, rig.outbox.enqueued, 1)
			var readInTx []uuid.UUID
			for _, r := range rig.instances.reads {
				assert.Equal(t, rig.tenantID, r.tenantID)
				if r.inTx {
					readInTx = append(readInTx, r.id)
				}
			}
			assert.ElementsMatch(t, []uuid.UUID{racing.ID, control.ID}, readInTx, "each instance is read again inside its own transaction")
			assert.Equal(t, []string{rig.tenantID.String(), rig.tenantID.String()}, rig.tx.tenants)
			require.Len(t, rig.retention.recorded, 1, "the offboarding is recorded: the event succeeded")
		})
	}
}

func TestTenantLifecycleReconciler_Offboard_OnlyReadyAndInProgressTasksFail_AndTheirAssigneesAreVacated(t *testing.T) {
	rig := w12aNewTenantRig()
	inst := rig.instance(domain.InstanceStatusRunning)
	leadUser, helperUser, workerUser := uuid.New(), uuid.New(), uuid.New()
	ready, readyRows := rig.task(inst, domain.TaskStatusReady, leadUser, helperUser)
	inProgress, inProgressRows := rig.task(inst, domain.TaskStatusInProgress, workerUser)
	completed, _ := rig.task(inst, domain.TaskStatusCompleted)
	completedRow := &domain.TaskAssignment{ID: uuid.New(), TenantID: rig.tenantID, TaskID: completed.ID, UserID: uuid.New(), IsActive: false}
	rig.assignments.add(completedRow)
	superseded, _ := rig.task(inst, domain.TaskStatusSuperseded)
	alreadyFailed, _ := rig.task(inst, domain.TaskStatusFailed)

	require.NoError(t, rig.offboard())

	assert.Equal(t, domain.TaskStatusFailed, ready.Status)
	assert.Equal(t, domain.TaskStatusFailed, inProgress.Status)
	assert.Equal(t, domain.TaskStatusCompleted, completed.Status, "a completed task is not rewritten")
	assert.Equal(t, domain.TaskStatusSuperseded, superseded.Status)
	assert.Equal(t, domain.TaskStatusFailed, alreadyFailed.Status)

	require.Len(t, rig.tasks.updates, 2, "only the two open tasks are written")
	written := map[uuid.UUID]w12aStatusWrite{}
	for _, u := range rig.tasks.updates {
		written[u.id] = u
	}
	assert.Equal(t, w12aStatusWrite{tenantID: rig.tenantID, id: ready.ID, status: "FAILED", version: 5}, written[ready.ID])
	assert.Equal(t, w12aStatusWrite{tenantID: rig.tenantID, id: inProgress.ID, status: "FAILED", version: 5}, written[inProgress.ID])

	var vacated []uuid.UUID
	for _, v := range rig.assignments.vacated {
		assert.Equal(t, rig.tenantID, v.tenantID)
		vacated = append(vacated, v.id)
	}
	assert.ElementsMatch(t, []uuid.UUID{readyRows[0].ID, readyRows[1].ID, inProgressRows[0].ID}, vacated, "every active assignment of the two open tasks, and nothing else")
	for _, a := range append(append([]*domain.TaskAssignment{}, readyRows...), inProgressRows...) {
		assert.False(t, a.IsActive)
	}
	assert.False(t, completedRow.IsActive)

	failed := w12aEventsOfType(t, rig, "WorkflowTaskFailed")
	require.Len(t, failed, 2, "one failed event per open task, none for the others")
	byTask := map[uuid.UUID]domain.WorkflowTaskFailedPayload{}
	for _, raw := range failed {
		var p domain.WorkflowTaskFailedPayload
		require.NoError(t, json.Unmarshal(raw, &p))
		byTask[p.TaskID] = p
	}
	assert.ElementsMatch(t, []uuid.UUID{leadUser, helperUser}, byTask[ready.ID].AssigneeUserIDs)
	assert.Equal(t, []uuid.UUID{workerUser}, byTask[inProgress.ID].AssigneeUserIDs)
	for _, p := range byTask {
		assert.Equal(t, inst.ID, p.WorkflowInstanceID)
		assert.Equal(t, "instance_terminate", p.CascadeSource)
	}
	assert.Len(t, w12aEventsOfType(t, rig, "WorkflowInstanceTerminated"), 1)
	assert.Len(t, rig.outbox.enqueued, 3)
}

func TestTenantLifecycleReconciler_Apply_StatusAndPlanUnchanged_RecordsTheStatusAndDoesNothingElse(t *testing.T) {
	changedAt := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	for _, status := range []string{
		domain.TenantStatusTrial, domain.TenantStatusActive, domain.TenantStatusPastDue, domain.TenantStatusCancelled,
		domain.TenantStatusSuspended, domain.TenantStatusTrialExpired, domain.TenantStatusOffboarded,
	} {
		t.Run(status, func(t *testing.T) {
			rig := w12aNewTenantRig()
			inst := rig.instance(domain.InstanceStatusRunning)
			rig.task(inst, domain.TaskStatusReady, uuid.New())

			err := rig.svc.Apply(context.Background(), port.TenantLifecycleInput{
				TenantID: rig.tenantID, Status: status, PreviousStatus: status, Plan: "pro", PreviousPlan: "pro", ChangedAt: changedAt,
			})

			require.NoError(t, err)
			assert.Equal(t, []recordedState{{tenantID: rig.tenantID, status: status, changedAt: changedAt}}, rig.states.recorded)
			assert.Empty(t, rig.temporal.signals, "no pause or resume")
			assert.Empty(t, rig.temporal.terminate, "no termination")
			assert.Empty(t, rig.retention.recorded, "no offboarding is recorded")
			assert.Empty(t, rig.retention.returned, "no return is recorded")
			assert.Empty(t, rig.queues.registered)
			assert.Empty(t, rig.queues.stopped)
			assert.Empty(t, rig.outbox.enqueued)
			assert.Empty(t, rig.instances.updates)
			assert.Empty(t, rig.tasks.updates)
			assert.Equal(t, domain.InstanceStatusRunning, inst.Status)
		})
	}

	t.Run("a status that is not recognised is not recorded", func(t *testing.T) {
		rig := w12aNewTenantRig()

		err := rig.svc.Apply(context.Background(), port.TenantLifecycleInput{
			TenantID: rig.tenantID, Status: "", PreviousStatus: "", Plan: "pro", PreviousPlan: "pro", ChangedAt: changedAt,
		})

		require.NoError(t, err)
		assert.Empty(t, rig.states.recorded)
		assert.Empty(t, rig.temporal.signals)
		assert.Empty(t, rig.retention.recorded)
		assert.Empty(t, rig.retention.returned)
	})

	t.Run("a changed plan, with the same status, registers the tenant's queue", func(t *testing.T) {
		rig := w12aNewTenantRig()

		err := rig.svc.Apply(context.Background(), port.TenantLifecycleInput{
			TenantID: rig.tenantID, Status: domain.TenantStatusActive, PreviousStatus: domain.TenantStatusActive, Plan: domain.PlanEnterprise, PreviousPlan: "pro", ChangedAt: changedAt,
		})

		require.NoError(t, err)
		require.Len(t, rig.queues.registered, 1)
		assert.Equal(t, rig.tenantID, rig.queues.registered[0].TenantID)
		assert.Contains(t, rig.queues.registered[0].QueueName, rig.tenantID.String())
		assert.Empty(t, rig.queues.stopped)
		assert.Empty(t, rig.temporal.signals, "the status did not change")
	})

	t.Run("a different status, with the same input shape, does act", func(t *testing.T) {
		rig := w12aNewTenantRig()
		inst := rig.instance(domain.InstanceStatusRunning)

		err := rig.svc.Apply(context.Background(), port.TenantLifecycleInput{
			TenantID: rig.tenantID, Status: domain.TenantStatusSuspended, PreviousStatus: domain.TenantStatusActive, Plan: "pro", PreviousPlan: "pro", ChangedAt: changedAt,
		})

		require.NoError(t, err)
		require.Len(t, rig.temporal.signals, 1)
		assert.Equal(t, port.SignalInstancePause, rig.temporal.signals[0].SignalName)
		assert.Equal(t, inst.ID, rig.temporal.signals[0].InstanceID)
		assert.Equal(t, []recordedReturn{{tenantID: rig.tenantID, at: changedAt}}, rig.retention.returned)
	})
}

func TestTenantLifecycleReconciler_Apply_ZeroChangedAt_UsesTheProcessingTimeForTheStatusAndTheRetentionDates(t *testing.T) {
	supplied := time.Date(2026, 10, 17, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name           string
		status, before string
		changedAt      time.Time
		wantAt         func(rig *w12aTenantRig) time.Time
	}{
		{name: "offboarded", status: domain.TenantStatusOffboarded, before: domain.TenantStatusActive, wantAt: func(rig *w12aTenantRig) time.Time { return rig.now }},
		{name: "suspended", status: domain.TenantStatusSuspended, before: domain.TenantStatusActive, wantAt: func(rig *w12aTenantRig) time.Time { return rig.now }},
		{name: "unchanged status", status: domain.TenantStatusActive, before: domain.TenantStatusActive, wantAt: func(rig *w12aTenantRig) time.Time { return rig.now }},
		{name: "a supplied time is kept", status: domain.TenantStatusOffboarded, before: domain.TenantStatusActive, changedAt: supplied, wantAt: func(*w12aTenantRig) time.Time { return supplied }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w12aNewTenantRig()
			want := tc.wantAt(rig)

			err := rig.svc.Apply(context.Background(), port.TenantLifecycleInput{
				TenantID: rig.tenantID, Status: tc.status, PreviousStatus: tc.before, ChangedAt: tc.changedAt,
			})

			require.NoError(t, err)
			assert.Equal(t, []recordedState{{tenantID: rig.tenantID, status: tc.status, changedAt: want}}, rig.states.recorded, "the stored status carries a real time, never the zero time")
			switch tc.status {
			case tc.before:
				assert.Empty(t, rig.retention.recorded)
				assert.Empty(t, rig.retention.returned)
			case domain.TenantStatusOffboarded:
				assert.Equal(t, []recordedOffboard{{tenantID: rig.tenantID, offboardedAt: want, purgeAfter: want.Add(48 * time.Hour)}}, rig.retention.recorded)
			default:
				assert.Equal(t, []recordedReturn{{tenantID: rig.tenantID, at: want}}, rig.retention.returned)
			}
		})
	}
}
