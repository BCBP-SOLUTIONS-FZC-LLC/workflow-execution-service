package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

var (
	errW11bPauseList   = errors.New("w11b: listing the user's assignments failed")
	errW11bPauseSignal = errors.New("w11b: pause signal failed")
)

const w11bPauseFailuresMetric = "workflow_execution_service_user_pause_failures_total"

type w11bPauseHarness struct {
	pauser      *service.UserTaskPauser
	instances   *w11bReadFailInstances
	tasks       *w11bRecordingTasks
	assignments *w11bListingAssignments
	temporal    *fakeTemporalClient
	log         *fakeLogger
	tenantID    uuid.UUID
	userID      uuid.UUID
}

func newW11bPauseHarness() *w11bPauseHarness {
	h := &w11bPauseHarness{
		instances:   &w11bReadFailInstances{fakeInstanceRepo: newFakeInstanceRepo(), failing: map[uuid.UUID]error{}},
		tasks:       &w11bRecordingTasks{fakeTaskRepo: newFakeTaskRepo()},
		assignments: &w11bListingAssignments{fakeAssignmentRepo: newFakeAssignmentRepo()},
		temporal:    &fakeTemporalClient{},
		log:         &fakeLogger{},
		tenantID:    uuid.New(),
		userID:      uuid.New(),
	}
	h.pauser = &service.UserTaskPauser{
		Instances: h.instances, Assignments: h.assignments, Tasks: h.tasks, Temporal: h.temporal, Log: h.log,
	}
	return h
}

// hold gives the user an active task on a new instance with the given status.
func (h *w11bPauseHarness) hold(status domain.InstanceStatus) *domain.Instance {
	inst := &domain.Instance{
		ID: uuid.New(), TenantID: h.tenantID, Status: status, RecordVersion: 5,
		TemporalWorkflowID: "tenant:" + h.tenantID.String() + ":" + uuid.NewString(),
	}
	h.instances.byID[inst.ID] = inst
	task := &domain.Task{ID: uuid.New(), TenantID: h.tenantID, WorkflowInstanceID: inst.ID}
	h.tasks.byID[task.ID] = task
	h.assignments.add(&domain.TaskAssignment{ID: uuid.New(), TenantID: h.tenantID, TaskID: task.ID, UserID: h.userID, IsActive: true})
	return inst
}

func (h *w11bPauseHarness) pause() error {
	return h.pauser.PauseUserTasks(context.Background(), h.tenantID, h.userID, nil)
}

func (h *w11bPauseHarness) signalledInstances() []uuid.UUID {
	var out []uuid.UUID
	for _, s := range h.temporal.signals {
		out = append(out, s.InstanceID)
	}
	return out
}

// requireReadsInTenant fails unless exactly the given instances were read, each
// after its own task, and every read carried the request's tenant.
func (h *w11bPauseHarness) requireReadsInTenant(t *testing.T, instanceIDs ...uuid.UUID) {
	t.Helper()
	require.Len(t, h.tasks.gets, len(instanceIDs), "one task read per assignment")
	for _, get := range h.tasks.gets {
		assert.Equal(t, h.tenantID, get.tenantID, "a task is read in the request's tenant")
	}
	var read []uuid.UUID
	for _, r := range h.instances.reads {
		assert.Equal(t, h.tenantID, r.tenantID, "an instance is read in the request's tenant")
		read = append(read, r.id)
	}
	assert.Equal(t, instanceIDs, read)
}

func w11bPauseSignalWire(t *testing.T, payload any) (initiator string, recordVersion float64) {
	t.Helper()
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	var wire map[string]any
	require.NoError(t, json.Unmarshal(raw, &wire))
	initiator, _ = wire["Initiator"].(string)
	recordVersion, _ = wire["RecordVersion"].(float64)
	return initiator, recordVersion
}

func TestUserTaskPauser_UnreadableInstance_FailsTheCallAsIncompleteAndStillSignalsTheOthers(t *testing.T) {
	cases := []struct {
		name     string
		sabotage func(h *w11bPauseHarness, bad *domain.Instance)
		cause    error
	}{
		{
			name:     "the instance does not exist",
			sabotage: func(h *w11bPauseHarness, bad *domain.Instance) { delete(h.instances.byID, bad.ID) },
			cause:    domain.ErrNotFound,
		},
		{
			name:     "the instance read fails",
			sabotage: func(h *w11bPauseHarness, bad *domain.Instance) { h.instances.failing[bad.ID] = errW11bInstanceRead },
			cause:    errW11bInstanceRead,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obstest.Init(t)
			before := obstest.Sum(t, w11bPauseFailuresMetric, map[string]string{"stage": "read_instance"})
			h := newW11bPauseHarness()
			first, bad, last := h.hold(domain.InstanceStatusRunning), h.hold(domain.InstanceStatusRunning), h.hold(domain.InstanceStatusDegraded)
			tc.sabotage(h, bad)

			err := h.pause()

			require.ErrorIs(t, err, port.ErrUserPauseIncomplete, "the caller is told to retry, which the handler answers 503")
			require.ErrorIs(t, err, tc.cause)
			assert.Equal(t, []uuid.UUID{first.ID, last.ID}, h.signalledInstances(), "the instances on both sides of the unreadable one are still paused")
			for _, s := range h.temporal.signals {
				assert.Equal(t, port.SignalInstancePause, s.SignalName)
			}
			h.requireReadsInTenant(t, first.ID, bad.ID, last.ID)
			require.Len(t, h.log.warnFields, 1)
			assert.Equal(t, bad.ID, h.log.warnFields[0]["instance_id"])
			assert.Equal(t, before+1, obstest.Sum(t, w11bPauseFailuresMetric, map[string]string{"stage": "read_instance"}))
		})
	}
}

func TestUserTaskPauser_ListingTheAssignmentsFails_ReturnsTheRawErrorAndSignalsNothing(t *testing.T) {
	cases := []struct {
		name  string
		cause error
	}{
		{name: "a database error", cause: errW11bPauseList},
		{name: "a deadline that ran out", cause: context.DeadlineExceeded},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obstest.Init(t)
			before := obstest.Sum(t, w11bPauseFailuresMetric, nil)
			h := newW11bPauseHarness()
			h.hold(domain.InstanceStatusRunning)
			h.assignments.err = tc.cause

			err := h.pause()

			require.ErrorIs(t, err, tc.cause)
			assert.NotErrorIs(t, err, port.ErrUserPauseIncomplete, "a failed listing is not a partial pause, so the handler does not report it as one")
			assert.Equal(t, []w11bUserListCall{{tenantID: h.tenantID, userID: h.userID}}, h.assignments.calls)
			assert.Empty(t, h.temporal.signals, "nothing is signalled")
			assert.Empty(t, h.tasks.gets)
			assert.Empty(t, h.instances.reads)
			assert.Equal(t, before, obstest.Sum(t, w11bPauseFailuresMetric, nil), "no per-instance failure is counted")
		})
	}
}

func TestUserTaskPauser_RepeatAfterAPartialFailure_SignalsEveryInstanceAgain(t *testing.T) {
	h := newW11bPauseHarness()
	running := h.hold(domain.InstanceStatusRunning)
	paused := h.hold(domain.InstanceStatusPaused)
	degraded := h.hold(domain.InstanceStatusDegraded)
	all := []uuid.UUID{running.ID, paused.ID, degraded.ID}
	h.temporal.signalFunc = func(_ context.Context, _ string, instanceID uuid.UUID, _ string, _ any) error {
		if instanceID == paused.ID {
			return errW11bPauseSignal
		}
		return nil
	}

	first := h.pause()

	require.ErrorIs(t, first, port.ErrUserPauseIncomplete)
	require.ErrorIs(t, first, errW11bPauseSignal)
	assert.Equal(t, all, h.signalledInstances(), "every instance is attempted, one failure does not stop the others")

	running.Status, degraded.Status = domain.InstanceStatusPaused, domain.InstanceStatusPaused
	h.temporal.signalFunc = nil
	h.temporal.signals = nil

	require.NoError(t, h.pause(), "a repeat that succeeds everywhere is a plain success")

	assert.Equal(t, all, h.signalledInstances(), "the instances that are already paused are signalled again")
	h.requireReadsInTenant(t, append(append([]uuid.UUID{}, all...), all...)...)
	for i, s := range h.temporal.signals {
		assert.Equal(t, port.SignalInstancePause, s.SignalName)
		assert.Equal(t, all[i], s.InstanceID)
		initiator, version := w11bPauseSignalWire(t, s.Payload)
		assert.Equal(t, "safety_net", initiator)
		assert.Equal(t, float64(5), version)
	}
	assert.Equal(t, []string{
		h.instances.byID[running.ID].TemporalWorkflowID, h.instances.byID[paused.ID].TemporalWorkflowID, h.instances.byID[degraded.ID].TemporalWorkflowID,
	}, []string{h.temporal.signals[0].TemporalWorkflowID, h.temporal.signals[1].TemporalWorkflowID, h.temporal.signals[2].TemporalWorkflowID})
}
