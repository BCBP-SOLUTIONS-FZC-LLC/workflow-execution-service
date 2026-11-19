package temporal_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

const w25bDegradedCounter = "workflow_execution_service_instance_degraded_total"

func TestInstanceEnd_OpenTaskWithAssignees_IsFailedVacatedAndAnnouncedWithTheCascadeSource(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		run         func(s *w25bScene) error
		wantCascade string
	}{
		{name: "cancel", run: (*w25bScene).cancelInstance, wantCascade: "instance_cancel"},
		{name: "instance failed", run: func(s *w25bScene) error { return s.setInstanceStatus(domain.InstanceStatusFailed) }, wantCascade: "instance_failed"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := w25bNewScene(t, true)
			closed := s.rig.addTask(s.tenant, s.inst, "sales/done", domain.TaskStatusCompleted)
			s.rig.addAssignment(s.tenant, closed, uuid.New(), true, false)

			require.NoError(t, tc.run(s))

			assert.Equal(t, domain.TaskStatusFailed, s.rig.tasks[s.task.ID].Status)
			assert.Equal(t, domain.TaskStatusCompleted, s.rig.tasks[closed.ID].Status, "a task that already ended is left alone")
			assert.False(t, s.rig.assignments[s.lead.ID].IsActive)
			assert.False(t, s.rig.assignments[s.helper.ID].IsActive)
			assert.ElementsMatch(t, []uuid.UUID{s.lead.ID, s.helper.ID}, s.rig.idsOf("Assignments.Vacate"))
			failed := s.rig.eventsOfType(domain.EventWorkflowTaskFailed)
			require.Len(t, failed, 1, "one task.failed event, for the open task only")
			payload := w25bDecode[domain.WorkflowTaskFailedPayload](t, failed[0])
			assert.Equal(t, s.task.ID, payload.TaskID)
			assert.Equal(t, tc.wantCascade, payload.CascadeSource)
			assert.Empty(t, payload.ErrorClass)
			assert.ElementsMatch(t, []uuid.UUID{s.leadUser, s.helperUser}, payload.AssigneeUserIDs)
			fields := map[string]json.RawMessage{}
			require.NoError(t, json.Unmarshal(failed[0].Payload, &fields))
			assert.NotContains(t, fields, "error_class", "a cascade failure names no error class")
			s.rig.requireEveryCallScopedTo(t, s.tenant)

			before := s.rig.snapshot()
			require.NoError(t, tc.run(s), "a retry of the committed call succeeds")
			w25bRequireUntouched(t, s.rig, before)
		})
	}
}

func TestInstanceEnd_InstanceCannotBeReadForTheFailedEvent_RollsBackTheTaskAndTheInstance(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		run  func(s *w25bScene) error
	}{
		{name: "cancel", run: (*w25bScene).cancelInstance},
		{name: "instance failed", run: func(s *w25bScene) error { return s.setInstanceStatus(domain.InstanceStatusFailed) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := w25bNewScene(t, true)
			before := s.rig.snapshot()
			// The first read opens the activity; the second is the one for the task's event.
			s.rig.failOn("Instances.GetByID", errW25bStorage, 2)

			err := tc.run(s)

			w25bRequireRetryable(t, err)
			require.ErrorIs(t, err, errW25bStorage)
			assert.Contains(t, err.Error(), "get instance for event core")
			assert.Equal(t, 1, s.rig.rolledBack)
			assert.Empty(t, s.rig.callsTo("Outbox.Enqueue"))
			w25bRequireUntouched(t, s.rig, before)
			assert.Equal(t, domain.TaskStatusInProgress, s.rig.tasks[s.task.ID].Status)
			assert.True(t, s.rig.assignments[s.lead.ID].IsActive, "the vacated assignments come back with the rollback")

			require.NoError(t, tc.run(s), "the retry finishes the cascade")
			assert.Equal(t, domain.TaskStatusFailed, s.rig.tasks[s.task.ID].Status)
			assert.Len(t, s.rig.eventsOfType(domain.EventWorkflowTaskFailed), 1)
		})
	}
}

func TestEnqueueInstanceEvent_SuccessNamesTheInstanceSubjectAndTenantWithoutAnActor(t *testing.T) {
	t.Parallel()
	s := w25bNewScene(t, true)

	require.NoError(t, s.pauseInstance())

	paused := s.rig.eventsOfType(domain.EventWorkflowInstancePaused)
	require.Len(t, paused, 1)
	assert.Equal(t, "instances/"+s.inst.ID.String(), paused[0].Subject)
	assert.Equal(t, s.tenant.String(), paused[0].TenantID)
	assert.Empty(t, paused[0].Actor, "the admin is named in the payload, not as the envelope's actor")
	enqueued := s.rig.callsTo("Outbox.Enqueue")
	require.Len(t, enqueued, 1)
	assert.Equal(t, s.tenant, enqueued[0].Tenant)
	s.rig.requireEveryCallScopedTo(t, s.tenant)

	before := s.rig.snapshot()
	require.NoError(t, s.pauseInstance())
	w25bRequireUntouched(t, s.rig, before)
}

func TestEnqueueInstanceEvent_RefusedOrNotStored_ReturnsTheWrappedCauseAndUndoesTheStatusChange(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		fault       func(s *w25bScene)
		heal        func(s *w25bScene)
		messagePart string
		pinRetry    bool
	}{
		{
			name:        "the schema check refuses the payload",
			fault:       func(s *w25bScene) { w25bRejectEvents(s.deps, errW25bStorage) },
			heal:        func(s *w25bScene) { w25bRejectEvents(s.deps, nil) },
			messagePart: "build " + domain.EventWorkflowInstancePaused + " envelope",
		},
		{
			name:        "the outbox insert fails",
			fault:       func(s *w25bScene) { s.rig.failOn("Outbox.Enqueue", errW25bStorage, 0) },
			heal:        func(s *w25bScene) { delete(s.rig.failures, "Outbox.Enqueue") },
			messagePart: "enqueue " + domain.EventWorkflowInstancePaused,
			pinRetry:    true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := w25bNewScene(t, true)
			before := s.rig.snapshot()
			tc.fault(s)

			err := s.pauseInstance()

			if tc.pinRetry {
				w25bRequireRetryable(t, err)
			} else {
				// Whether a payload the schema refuses should be retried at all is an open question: only the rollback is pinned for it.
				require.Error(t, err)
			}
			require.ErrorIs(t, err, errW25bStorage)
			assert.Contains(t, err.Error(), tc.messagePart)
			assert.Equal(t, 1, s.rig.rolledBack)
			assert.Equal(t, domain.InstanceStatusRunning, s.rig.instances[s.inst.ID].Status, "the status change goes with the failed event")
			w25bRequireUntouched(t, s.rig, before)

			tc.heal(s)
			require.NoError(t, s.pauseInstance())
			assert.Equal(t, domain.InstanceStatusPaused, s.rig.instances[s.inst.ID].Status)
			assert.Len(t, s.rig.outbox, 1)
			s.rig.requireEveryCallScopedTo(t, s.tenant)
		})
	}
}

func TestInstanceStatusWrites_UpdateStatusFails_IsAWrappedRetryableErrorWithNoEventAndNoCount(t *testing.T) {
	obstest.Init(t)

	causes := []struct {
		name  string
		cause error
	}{
		{name: "storage failure", cause: errW25bStorage},
		{name: "record version conflict", cause: domain.ErrRecordVersionConflict},
	}
	entries := []struct {
		name         string
		start        domain.InstanceStatus
		run          func(s *w25bScene) error
		wantFinished bool
	}{
		{name: "status completed", start: domain.InstanceStatusRunning, wantFinished: true, run: func(s *w25bScene) error { return s.setInstanceStatus(domain.InstanceStatusCompleted) }},
		{name: "status failed", start: domain.InstanceStatusRunning, run: func(s *w25bScene) error { return s.setInstanceStatus(domain.InstanceStatusFailed) }},
		{name: "status degraded", start: domain.InstanceStatusRunning, run: func(s *w25bScene) error {
			return s.deps.UpdateInstanceStatus(context.Background(), port.UpdateInstanceStatusInput{
				TenantID: s.tenant.String(), InstanceID: s.inst.ID.String(), Status: domain.InstanceStatusDegraded,
				FailedBranches: []domain.FailedBranch{{DepartmentID: uuid.New(), LastNodeKey: "legal/review"}},
			})
		}},
		{name: "status running again", start: domain.InstanceStatusDegraded, run: func(s *w25bScene) error { return s.setInstanceStatus(domain.InstanceStatusRunning) }},
		{name: "pause", start: domain.InstanceStatusRunning, run: (*w25bScene).pauseInstance},
		{name: "resume", start: domain.InstanceStatusPaused, run: (*w25bScene).resumeInstance},
		{name: "cancel", start: domain.InstanceStatusRunning, run: (*w25bScene).cancelInstance},
	}
	for _, entry := range entries {
		for _, c := range causes {
			t.Run(fmt.Sprintf("%s: %s", entry.name, c.name), func(t *testing.T) {
				s := w25bNewScene(t, true)
				s.rig.instances[s.inst.ID].Status = entry.start
				before := s.rig.snapshot()
				degradedBefore := obstest.Sum(t, w25bDegradedCounter, nil)
				s.rig.failOn("Instances.UpdateStatus", c.cause, 0)

				err := entry.run(s)

				w25bRequireRetryable(t, err)
				require.ErrorIs(t, err, c.cause)
				assert.Contains(t, err.Error(), "update instance status")
				assert.Equal(t, 1, s.rig.rolledBack)
				assert.Empty(t, s.rig.outbox, "no event outlives the rolled-back transaction")
				assert.Equal(t, degradedBefore, obstest.Sum(t, w25bDegradedCounter, nil), "an entry that did not commit is not counted")
				w25bRequireUntouched(t, s.rig, before)

				delete(s.rig.failures, "Instances.UpdateStatus")
				require.NoError(t, entry.run(s), "the retry goes through")
				s.rig.requireEveryCallScopedTo(t, s.tenant)
				if entry.wantFinished {
					finished := s.rig.eventsOfType(domain.EventWorkflowInstanceFinished)
					require.Len(t, finished, 1)
					assert.True(t, w25bDecode[domain.WorkflowInstanceFinishedPayload](t, finished[0]).CompletedAt.Equal(w25bClock),
						"the finished event carries the time the row was updated")
				}
				afterRetry := s.rig.snapshot()
				require.NoError(t, entry.run(s))
				w25bRequireUntouched(t, s.rig, afterRetry)
			})
		}
	}
}

func TestCancelInstance_IdsThatAreNotUUIDs_AreRefusedBeforeAnyRepositoryCall(t *testing.T) {
	t.Parallel()
	tenant, instance, admin := uuid.NewString(), uuid.NewString(), uuid.NewString()

	cases := []struct {
		name        string
		in          port.CancelInstanceInput
		messagePart string
	}{
		{name: "tenant id", in: port.CancelInstanceInput{TenantID: "bad", InstanceID: instance, AdminUserID: admin}, messagePart: "parse tenant_id"},
		{name: "instance id", in: port.CancelInstanceInput{TenantID: tenant, InstanceID: "bad", AdminUserID: admin}, messagePart: "parse instance_id"},
		{name: "admin user id", in: port.CancelInstanceInput{TenantID: tenant, InstanceID: instance, AdminUserID: "bad"}, messagePart: "parse admin_user_id"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rig := w25bNewRig()
			deps := w25bNewDeps(t, rig)

			err := deps.CancelInstance(context.Background(), tc.in)

			_ = w25bRequireNonRetryable(t, err, "ValidationError", tc.messagePart)
			assert.Empty(t, rig.txOpened)
			assert.Empty(t, rig.calls)
		})
	}
}

func TestCancelInstance_InstanceThatIsNotThere_EndsAsANonRetryableNotFound(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		setup func(rig *w25bRig) uuid.UUID
	}{
		{name: "no such instance", setup: func(_ *w25bRig) uuid.UUID { return uuid.New() }},
		{name: "instance of another tenant", setup: func(rig *w25bRig) uuid.UUID {
			return rig.addInstance(uuid.New(), domain.InstanceStatusRunning).ID
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rig := w25bNewRig()
			deps := w25bNewDeps(t, rig)
			tenant := uuid.New()
			instanceID := tc.setup(rig)
			before := rig.snapshot()

			err := deps.CancelInstance(context.Background(), port.CancelInstanceInput{
				TenantID: tenant.String(), InstanceID: instanceID.String(), AdminUserID: uuid.NewString(),
			})

			appErr := w25bRequireNonRetryable(t, err, "NotFoundError", "get instance")
			require.ErrorIs(t, err, domain.ErrNotFound)
			assert.NotNil(t, appErr.Unwrap())
			assert.Empty(t, rig.callsTo("Instances.UpdateStatus"))
			assert.Empty(t, rig.outbox)
			w25bRequireUntouched(t, rig, before)
		})
	}
}

func TestRecordForceRoute_LookupOfItsOwnEventFails_ChangesNothingAndTheRetryRoutesOnce(t *testing.T) {
	t.Parallel()
	s := w25bNewScene(t, true)
	before := s.rig.snapshot()
	s.rig.failOn("Outbox.ExistsByID", errW25bStorage, 1)

	err := s.forceRoute()

	w25bRequireRetryable(t, err)
	require.ErrorIs(t, err, errW25bStorage)
	assert.Contains(t, err.Error(), "look up event ")
	assert.Equal(t, 1, s.rig.rolledBack)
	assert.Empty(t, s.rig.callsTo("Tasks.ListByInstance"), "no task is looked at before the lookup answered")
	assert.Empty(t, s.rig.callsTo("Tasks.UpdateStatus"), "no task is superseded before the lookup answered")
	assert.Empty(t, s.rig.callsTo("Outbox.Enqueue"))
	w25bRequireUntouched(t, s.rig, before)

	require.NoError(t, s.forceRoute())
	assert.Equal(t, domain.TaskStatusSuperseded, s.rig.tasks[s.task.ID].Status)
	announced := s.rig.eventsOfType(domain.EventWorkflowInstanceForceRouted)
	require.Len(t, announced, 1)
	s.rig.requireEveryCallScopedTo(t, s.tenant)

	afterCommit := s.rig.snapshot()
	require.NoError(t, s.forceRoute(), "an event found by id means the earlier attempt committed")
	w25bRequireUntouched(t, s.rig, afterCommit)
	lookups := s.rig.callsTo("Outbox.ExistsByID")
	require.Len(t, lookups, 3)
	assert.Equal(t, lookups[1].ID, lookups[2].ID, "every attempt asks for the same event")
	assert.Equal(t, announced[0].ID, lookups[2].ID.String())
	assert.Len(t, s.rig.callsTo("Tasks.UpdateStatus"), 1, "the found event stops the retry before it supersedes anything")
}
