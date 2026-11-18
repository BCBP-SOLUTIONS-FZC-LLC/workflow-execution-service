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
)

type w12aRecordCall struct {
	tenantID  uuid.UUID
	userID    uuid.UUID
	status    string
	changedAt time.Time
}

// w12aAvailability records every status it is asked to store, with the tenant and user.
type w12aAvailability struct {
	*fakeUserAvailability
	records []w12aRecordCall
}

func (a *w12aAvailability) Record(ctx context.Context, tenantID, userID uuid.UUID, status string, changedAt time.Time) (bool, error) {
	a.records = append(a.records, w12aRecordCall{tenantID: tenantID, userID: userID, status: status, changedAt: changedAt})
	return a.fakeUserAvailability.Record(ctx, tenantID, userID, status, changedAt)
}

// w12aOOOAssignments records the tenant and user of every lookup of a user's active assignments.
type w12aOOOAssignments struct {
	*fakeAssignmentRepo
	lookups []w12aLookup
}

type w12aLookup struct{ tenantID, id uuid.UUID }

func (r *w12aOOOAssignments) ListActiveByUser(ctx context.Context, tenantID, userID uuid.UUID) ([]*domain.TaskAssignment, error) {
	r.lookups = append(r.lookups, w12aLookup{tenantID: tenantID, id: userID})
	return r.fakeAssignmentRepo.ListActiveByUser(ctx, tenantID, userID)
}

// w12aOOOTasks records the tenant and id of every task read.
type w12aOOOTasks struct {
	*fakeTaskRepo
	reads []w12aLookup
}

func (r *w12aOOOTasks) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Task, error) {
	r.reads = append(r.reads, w12aLookup{tenantID: tenantID, id: id})
	return r.fakeTaskRepo.GetByID(ctx, tenantID, id)
}

// w12aOOOInstances records the tenant and id of every instance read.
type w12aOOOInstances struct {
	*fakeInstanceRepo
	reads []w12aLookup
}

func (r *w12aOOOInstances) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	r.reads = append(r.reads, w12aLookup{tenantID: tenantID, id: id})
	return r.fakeInstanceRepo.GetByID(ctx, tenantID, id)
}

type w12aOOOWorld struct {
	svc          *service.OOOAvailabilityReconciler
	availability *w12aAvailability
	assignments  *w12aOOOAssignments
	tasks        *w12aOOOTasks
	instances    *w12aOOOInstances
	temporal     *fakeTemporalClient
	tenantID     uuid.UUID
	userID       uuid.UUID
}

func w12aNewOOOWorld() *w12aOOOWorld {
	w := &w12aOOOWorld{
		availability: &w12aAvailability{fakeUserAvailability: newFakeUserAvailability()},
		assignments:  &w12aOOOAssignments{fakeAssignmentRepo: newFakeAssignmentRepo()},
		tasks:        &w12aOOOTasks{fakeTaskRepo: newFakeTaskRepo()},
		instances:    &w12aOOOInstances{fakeInstanceRepo: newFakeInstanceRepo()},
		temporal:     &fakeTemporalClient{},
		tenantID:     uuid.New(), userID: uuid.New(),
	}
	w.svc = &service.OOOAvailabilityReconciler{
		Instances: w.instances, Assignments: w.assignments, Tasks: w.tasks,
		Temporal: w.temporal, Availability: w.availability,
	}
	return w
}

// instance adds an instance with the given status and version.
func (w *w12aOOOWorld) instance(status domain.InstanceStatus, version int64) *domain.Instance {
	inst := &domain.Instance{
		ID: uuid.New(), TenantID: w.tenantID, Status: status, RecordVersion: version,
		TemporalWorkflowID: w.tenantID.String() + ":" + uuid.NewString(),
	}
	w.instances.byID[inst.ID] = inst
	return inst
}

// hold gives user an active assignment on a new task of inst.
func (w *w12aOOOWorld) hold(inst *domain.Instance, user uuid.UUID) *domain.Task {
	task := &domain.Task{ID: uuid.New(), TenantID: w.tenantID, WorkflowInstanceID: inst.ID}
	w.tasks.byID[task.ID] = task
	w.assignments.add(&domain.TaskAssignment{ID: uuid.New(), TenantID: w.tenantID, TaskID: task.ID, UserID: user, IsActive: true})
	return task
}

func (w *w12aOOOWorld) apply(status string, delegate *uuid.UUID, changedAt time.Time) error {
	return w.svc.Apply(context.Background(), port.UserAvailabilityInput{
		TenantID: w.tenantID, UserID: w.userID, Status: status, DelegateUserID: delegate, ChangedAt: changedAt,
	})
}

// requireStored checks the one status the guard was asked to store, and that it is now the current one.
func (w *w12aOOOWorld) requireStored(t *testing.T, status string, changedAt time.Time) {
	t.Helper()
	require.Equal(t, []w12aRecordCall{{tenantID: w.tenantID, userID: w.userID, status: status, changedAt: changedAt}}, w.availability.records[len(w.availability.records)-1:])
	got, found, err := w.availability.Get(context.Background(), w.tenantID, w.userID)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, status, got)
}

// requireNoReads checks that neither an assignment, a task nor an instance was read.
func (w *w12aOOOWorld) requireNoReads(t *testing.T) {
	t.Helper()
	assert.Empty(t, w.assignments.lookups, "the user's assignments are not looked up")
	assert.Empty(t, w.tasks.reads, "no task is read")
	assert.Empty(t, w.instances.reads, "no instance is read")
}

type w12aSignalWire struct {
	Initiator     string
	RecordVersion int64
}

func w12aDecodeWire(t *testing.T, payload any) w12aSignalWire {
	t.Helper()
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	var wire w12aSignalWire
	require.NoError(t, json.Unmarshal(raw, &wire))
	return wire
}

func TestOOOAvailability_OutOfOfficeWithADelegate_RecordsTheStatusAndPausesNothing(t *testing.T) {
	changedAt := time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)
	delegate := uuid.New()
	cases := []struct {
		name       string
		delegate   *uuid.UUID
		wantSignal int
	}{
		{name: "with a delegate", delegate: &delegate, wantSignal: 0},
		{name: "without a delegate, the same user and work is paused", delegate: nil, wantSignal: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := w12aNewOOOWorld()
			inst := w.instance(domain.InstanceStatusRunning, 4)
			w.hold(inst, w.userID)

			require.NoError(t, w.apply(port.AvailabilityOOO, tc.delegate, changedAt))

			w.requireStored(t, port.AvailabilityOOO, changedAt)
			assert.Len(t, w.temporal.signals, tc.wantSignal)
			if tc.delegate != nil {
				w.requireNoReads(t)
				return
			}
			assert.Equal(t, []w12aLookup{{tenantID: w.tenantID, id: w.userID}}, w.assignments.lookups)
		})
	}
}

func TestOOOAvailability_BusyOnlyRecordsTheStatusWhileAvailableSignalsTheUsersInstances(t *testing.T) {
	changedAt := time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)
	cases := []struct {
		name       string
		status     string
		wantSignal string
	}{
		{name: "busy", status: port.AvailabilityBusy},
		{name: "available, the same user and work, is signalled", status: port.AvailabilityAvailable, wantSignal: port.SignalInstanceResume},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := w12aNewOOOWorld()
			paused := w.instance(domain.InstanceStatusPaused, 2)
			running := w.instance(domain.InstanceStatusRunning, 5)
			w.hold(paused, w.userID)
			w.hold(running, w.userID)

			require.NoError(t, w.apply(tc.status, nil, changedAt))

			w.requireStored(t, tc.status, changedAt)
			if tc.wantSignal == "" {
				assert.Empty(t, w.temporal.signals, "no pause and no resume")
				w.requireNoReads(t)
				return
			}
			assert.Len(t, w.temporal.signalsNamed(tc.wantSignal), 2)
			assert.Len(t, w.assignments.lookups, 1)
		})
	}
}

func TestOOOAvailability_OutOfOfficeForAUserWithNoAssignments_RecordsTheStatusAndReadsNoTaskOrInstance(t *testing.T) {
	changedAt := time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)
	cases := []struct {
		name          string
		otherUserWork bool
		inactiveWork  bool
	}{
		{name: "the user holds nothing at all"},
		{name: "only another user holds work", otherUserWork: true},
		{name: "the user's own assignment already ended", inactiveWork: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := w12aNewOOOWorld()
			inst := w.instance(domain.InstanceStatusRunning, 1)
			if tc.otherUserWork {
				w.hold(inst, uuid.New())
			}
			if tc.inactiveWork {
				w.hold(inst, w.userID)
				for _, a := range w.assignments.byID {
					a.IsActive = false
				}
			}

			require.NoError(t, w.apply(port.AvailabilityOOO, nil, changedAt))

			w.requireStored(t, port.AvailabilityOOO, changedAt)
			assert.Equal(t, []w12aLookup{{tenantID: w.tenantID, id: w.userID}}, w.assignments.lookups, "the assignments are looked up once, for this user")
			assert.Empty(t, w.tasks.reads)
			assert.Empty(t, w.instances.reads)
			assert.Empty(t, w.temporal.signals)
		})
	}
}

func TestOOOAvailability_OutOfOfficeWithoutADelegate_PausesEachDistinctInstanceOnce(t *testing.T) {
	changedAt := time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)
	w := w12aNewOOOWorld()
	first := w.instance(domain.InstanceStatusRunning, 7)
	second := w.instance(domain.InstanceStatusRunning, 11)
	untouched := w.instance(domain.InstanceStatusRunning, 13)
	w.hold(first, w.userID)
	w.hold(first, w.userID)
	w.hold(first, w.userID)
	w.hold(second, w.userID)
	w.hold(untouched, uuid.New())

	require.NoError(t, w.apply(port.AvailabilityOOO, nil, changedAt))

	w.requireStored(t, port.AvailabilityOOO, changedAt)
	require.Len(t, w.temporal.signals, 2, "three tasks of one instance and one of another are two signals")
	byInstance := map[uuid.UUID]signalCall{}
	for _, s := range w.temporal.signals {
		assert.Equal(t, port.SignalInstancePause, s.SignalName)
		byInstance[s.InstanceID] = s
	}
	require.Contains(t, byInstance, first.ID)
	require.Contains(t, byInstance, second.ID)
	assert.NotContains(t, byInstance, untouched.ID, "an instance only another user holds work on is left alone")
	for _, inst := range []*domain.Instance{first, second} {
		call := byInstance[inst.ID]
		assert.Equal(t, inst.TemporalWorkflowID, call.TemporalWorkflowID)
		wire := w12aDecodeWire(t, call.Payload)
		assert.Equal(t, domain.InitiatorOOO, wire.Initiator)
		assert.Equal(t, inst.RecordVersion, wire.RecordVersion, "the signal carries the version of the instance as read")
	}
	assert.Len(t, w.tasks.reads, 4, "every task of the user is read")
	assert.Len(t, w.instances.reads, 2, "each distinct instance is read once")
	for _, read := range append(append([]w12aLookup{}, w.tasks.reads...), w.instances.reads...) {
		assert.Equal(t, w.tenantID, read.tenantID)
	}
}

func TestOOOAvailability_OutOfOfficeForAnInstanceAlreadyPaused_StillSendsThePauseAndLeavesOwnershipToTheWorkflow(t *testing.T) {
	changedAt := time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)
	cases := []struct {
		name   string
		status domain.InstanceStatus
	}{
		{name: "running", status: domain.InstanceStatusRunning},
		{name: "already paused", status: domain.InstanceStatusPaused},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := w12aNewOOOWorld()
			inst := w.instance(tc.status, 9)
			w.hold(inst, w.userID)

			require.NoError(t, w.apply(port.AvailabilityOOO, nil, changedAt))

			w.requireStored(t, port.AvailabilityOOO, changedAt)
			require.Len(t, w.temporal.signals, 1, "the status of the instance does not gate the signal")
			call := w.temporal.signals[0]
			assert.Equal(t, port.SignalInstancePause, call.SignalName)
			assert.Equal(t, inst.ID, call.InstanceID)
			assert.Equal(t, inst.TemporalWorkflowID, call.TemporalWorkflowID)
			wire := w12aDecodeWire(t, call.Payload)
			assert.Equal(t, domain.InitiatorOOO, wire.Initiator, "the workflow decides who owns the pause from the initiator")
			assert.Equal(t, int64(9), wire.RecordVersion)
		})
	}
}

func TestOOOAvailability_BusyAfterOutOfOffice_IsRecordedAndSendsNoResume(t *testing.T) {
	oooAt := time.Date(2026, 3, 1, 9, 0, 0, 0, time.UTC)
	busyAt := oooAt.Add(time.Hour)
	w := w12aNewOOOWorld()
	inst := w.instance(domain.InstanceStatusRunning, 3)
	w.hold(inst, w.userID)

	require.NoError(t, w.apply(port.AvailabilityOOO, nil, oooAt))
	require.Len(t, w.temporal.signals, 1)
	require.Equal(t, port.SignalInstancePause, w.temporal.signals[0].SignalName)
	lookupsAfterOOO, readsAfterOOO := len(w.assignments.lookups), len(w.instances.reads)

	require.NoError(t, w.apply(port.AvailabilityBusy, nil, busyAt))

	w.requireStored(t, port.AvailabilityBusy, busyAt)
	assert.Len(t, w.availability.records, 2, "the out-of-office status and then the busy one")
	assert.Len(t, w.temporal.signals, 1, "nothing is sent for busy: the instance stays paused until an available event or an admin resume")
	assert.Empty(t, w.temporal.signalsNamed(port.SignalInstanceResume))
	assert.Equal(t, lookupsAfterOOO, len(w.assignments.lookups), "busy looks up no assignments")
	assert.Equal(t, readsAfterOOO, len(w.instances.reads))
}
