package temporal_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func w29aPause(s *w25bScene, adminID string) error {
	return s.deps.PauseInstance(context.Background(), port.PauseInstanceInput{
		TenantID: s.tenant.String(), InstanceID: s.inst.ID.String(), AdminUserID: adminID,
		Initiator: domain.InitiatorAdmin, RecordVersion: s.inst.RecordVersion,
	})
}

func w29aEventTypes(s *w25bScene) []string {
	types := make([]string, 0, len(s.rig.outbox))
	for _, env := range s.rig.outbox {
		types = append(types, env.Type)
	}
	return types
}

func TestPauseInstance_RunningRow_BecomesPausedAndTheEventIsWrittenInTheSameTransaction(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		adminID func(s *w25bScene) string
	}{
		{
			name:    "an admin pause names the admin",
			adminID: func(s *w25bScene) string { return s.admin.String() },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := w25bNewScene(t, true)
			startVersion := s.rig.instances[s.inst.ID].RecordVersion

			require.NoError(t, w29aPause(s, tc.adminID(s)))

			stored := s.rig.instances[s.inst.ID]
			assert.Equal(t, domain.InstanceStatusPaused, stored.Status)
			assert.Equal(t, startVersion+1, stored.RecordVersion, "the status write bumps the record version once")
			writes := s.rig.callsTo("Instances.UpdateStatus")
			require.Len(t, writes, 1)
			assert.Equal(t, startVersion, writes[0].Version, "written at the version read in the transaction")
			assert.Equal(t, s.inst.ID, writes[0].ID)
			assert.Equal(t, s.tenant, writes[0].Tenant)
			assert.True(t, writes[0].InTx)
			enqueues := s.rig.callsTo("Outbox.Enqueue")
			require.Len(t, enqueues, 1)
			assert.True(t, enqueues[0].InTx, "the event is written inside the status transaction")
			assert.Len(t, s.rig.txOpened, 1, "one transaction holds the status write and the event")
			assert.Equal(t, 1, s.rig.committed)
			assert.Equal(t, 0, s.rig.rolledBack)

			require.Len(t, s.rig.outbox, 1)
			env := s.rig.outbox[0]
			assert.Equal(t, domain.EventWorkflowInstancePaused, env.Type)
			assert.Equal(t, s.tenant.String(), env.TenantID)
			assert.Equal(t, "instances/"+s.inst.ID.String(), env.Subject)
			_, idErr := uuid.Parse(env.ID)
			require.NoError(t, idErr)
			payload := w25bDecode[domain.WorkflowInstancePausedPayload](t, env)
			assert.Equal(t, s.inst.ID, payload.WorkflowInstanceID)
			assert.Equal(t, s.inst.SubjectID, payload.SubjectID)
			assert.Equal(t, s.inst.BusinessKey, payload.BusinessKey)
			assert.Equal(t, s.inst.WorkflowVersionID, payload.WorkflowVersionID)
			assert.Equal(t, s.inst.StartedByUserID, payload.StartedByUserID)
			assert.Equal(t, domain.InitiatorAdmin, payload.Initiator)
			require.NotNil(t, payload.ActorUserID)
			assert.Equal(t, s.admin, *payload.ActorUserID)
			fields := map[string]json.RawMessage{}
			require.NoError(t, json.Unmarshal(env.Payload, &fields))
			assert.Contains(t, fields, "actor_user_id")
			s.rig.requireEveryCallScopedTo(t, s.tenant)
		})
	}
}

func TestPauseInstance_ReadOrStatusWriteFails_IsAWrappedRetryableErrorAndTheInstanceKeepsRunning(t *testing.T) {
	t.Parallel()

	ops := []struct {
		name        string
		op          string
		messagePart string
	}{
		{name: "the instance read", op: "Instances.GetByID", messagePart: "get instance"},
		{name: "the status update", op: "Instances.UpdateStatus", messagePart: "update instance status"},
	}
	causes := []struct {
		name  string
		cause error
	}{
		{name: "database error", cause: errW25bStorage},
		{name: "version conflict", cause: domain.ErrRecordVersionConflict},
	}
	for _, op := range ops {
		for _, c := range causes {
			t.Run(op.name+" fails with a "+c.name, func(t *testing.T) {
				t.Parallel()
				s := w25bNewScene(t, true)
				before := s.rig.snapshot()
				s.rig.failOn(op.op, c.cause, 0)

				err := w29aPause(s, s.admin.String())

				w25bRequireRetryable(t, err)
				require.ErrorIs(t, err, c.cause)
				assert.Contains(t, err.Error(), op.messagePart)
				assert.Equal(t, domain.InstanceStatusRunning, s.rig.instances[s.inst.ID].Status)
				assert.Equal(t, 1, s.rig.rolledBack)
				assert.Equal(t, 0, s.rig.committed)
				assert.Empty(t, s.rig.outbox, "no event for a pause that did not happen")
				w25bRequireUntouched(t, s.rig, before)

				delete(s.rig.failures, op.op)
				require.NoError(t, w29aPause(s, s.admin.String()), "the retry pauses the instance")
				assert.Equal(t, domain.InstanceStatusPaused, s.rig.instances[s.inst.ID].Status)
				assert.Len(t, s.rig.eventsOfType(domain.EventWorkflowInstancePaused), 1)
			})
		}
	}
}

func TestPauseInstance_EventCannotBeBuiltOrStored_RollsBackTheStatusAndKeepsTheVersion(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name         string
		fault        func(s *w25bScene)
		heal         func(s *w25bScene)
		messagePart  string
		nonRetryable bool
	}{
		{
			name:         "the schema check refuses the payload",
			fault:        func(s *w25bScene) { w25bRejectEvents(s.deps, errW25bStorage) },
			heal:         func(s *w25bScene) { w25bRejectEvents(s.deps, nil) },
			messagePart:  "build " + domain.EventWorkflowInstancePaused + " envelope",
			nonRetryable: true,
		},
		{
			name:        "the outbox insert fails",
			fault:       func(s *w25bScene) { s.rig.failOn("Outbox.Enqueue", errW25bStorage, 0) },
			heal:        func(s *w25bScene) { delete(s.rig.failures, "Outbox.Enqueue") },
			messagePart: "enqueue " + domain.EventWorkflowInstancePaused,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := w25bNewScene(t, true)
			startVersion := s.rig.instances[s.inst.ID].RecordVersion
			tc.fault(s)

			err := w29aPause(s, s.admin.String())

			if tc.nonRetryable {
				_ = w25bRequireNonRetryable(t, err, "ValidationError", tc.messagePart)
			} else {
				w25bRequireRetryable(t, err)
			}
			require.ErrorIs(t, err, errW25bStorage)
			assert.Contains(t, err.Error(), tc.messagePart)
			assert.Len(t, s.rig.callsTo("Instances.UpdateStatus"), 1, "the status was written before the event failed")
			assert.Equal(t, 1, s.rig.rolledBack, "the whole transaction is rolled back")
			assert.Equal(t, 0, s.rig.committed)
			assert.Equal(t, domain.InstanceStatusRunning, s.rig.instances[s.inst.ID].Status)
			assert.Equal(t, startVersion, s.rig.instances[s.inst.ID].RecordVersion)
			assert.Empty(t, s.rig.outbox)

			tc.heal(s)
			require.NoError(t, w29aPause(s, s.admin.String()))
			assert.Equal(t, domain.InstanceStatusPaused, s.rig.instances[s.inst.ID].Status)
			assert.Equal(t, startVersion+1, s.rig.instances[s.inst.ID].RecordVersion)
			assert.Len(t, s.rig.outbox, 1)
		})
	}
}

// w29aCancelScene is the scene's in-progress task (a lead and a co-assignee)
// plus a second, ready task with one assignee, on one running instance.
type w29aCancelScene struct {
	*w25bScene
	second         *domain.Task
	secondUser     uuid.UUID
	secondAssigned *domain.TaskAssignment
}

func w29aNewCancelScene(t *testing.T) *w29aCancelScene {
	t.Helper()
	s := w25bNewScene(t, true)
	second := s.rig.addTask(s.tenant, s.inst, "legal/approve", domain.TaskStatusReady)
	user := uuid.New()
	return &w29aCancelScene{
		w25bScene: s, second: second, secondUser: user,
		secondAssigned: s.rig.addAssignment(s.tenant, second, user, true, true),
	}
}

func TestCancelInstance_ReadyAndInProgressTasks_EachEndFailedAndTheirAssignmentsAreVacated(t *testing.T) {
	t.Parallel()
	s := w29aNewCancelScene(t)
	done := s.rig.addTask(s.tenant, s.inst, "legal/done", domain.TaskStatusCompleted)
	doneAssignment := s.rig.addAssignment(s.tenant, done, uuid.New(), true, false)

	require.NoError(t, s.cancelInstance())

	assert.Equal(t, domain.TaskStatusFailed, s.rig.tasks[s.task.ID].Status)
	assert.Equal(t, domain.TaskStatusFailed, s.rig.tasks[s.second.ID].Status)
	assert.Equal(t, domain.TaskStatusCompleted, s.rig.tasks[done.ID].Status)
	for _, a := range []*domain.TaskAssignment{s.lead, s.helper, s.secondAssigned} {
		got := s.rig.assignments[a.ID]
		assert.False(t, got.IsActive, "assignment %s stays active", a.ID)
		assert.NotNil(t, got.VacatedAt, "assignment %s was not vacated", a.ID)
		assert.Nil(t, got.CompletedAt, "a vacated assignment is not completed")
		assert.Nil(t, got.ResultJSON)
	}
	assert.Nil(t, s.rig.assignments[doneAssignment.ID].VacatedAt, "an assignment of a finished task is not touched")
	assert.ElementsMatch(t, []uuid.UUID{s.lead.ID, s.helper.ID, s.secondAssigned.ID}, s.rig.idsOf("Assignments.Vacate"))
	assert.ElementsMatch(t, []uuid.UUID{s.task.ID, s.second.ID}, s.rig.idsOf("Tasks.UpdateStatus"))
	for _, c := range s.rig.callsTo("Tasks.UpdateStatus") {
		assert.Equal(t, int64(7), c.Version, "each task is failed at the version the listing returned")
	}
	assert.Equal(t, domain.InstanceStatusTerminated, s.rig.instances[s.inst.ID].Status)
	s.rig.requireEveryCallScopedTo(t, s.tenant)
}

func TestCancelInstance_OpenTasks_WriteOneTaskFailedEventEachBeforeTheCancelledEvent(t *testing.T) {
	t.Parallel()
	s := w29aNewCancelScene(t)
	reason := "wrong template"

	require.NoError(t, s.deps.CancelInstance(context.Background(), port.CancelInstanceInput{
		TenantID: s.tenant.String(), InstanceID: s.inst.ID.String(), AdminUserID: s.admin.String(), Reason: &reason,
	}))

	assert.Equal(t, []string{
		domain.EventWorkflowTaskFailed, domain.EventWorkflowTaskFailed, domain.EventWorkflowInstanceCancelled,
	}, w29aEventTypes(s.w25bScene))
	failed := s.rig.eventsOfType(domain.EventWorkflowTaskFailed)
	require.Len(t, failed, 2)
	want := []struct {
		task      *domain.Task
		assignees []uuid.UUID
	}{
		{task: s.task, assignees: []uuid.UUID{s.leadUser, s.helperUser}},
		{task: s.second, assignees: []uuid.UUID{s.secondUser}},
	}
	for i, w := range want {
		env := failed[i]
		assert.Equal(t, s.tenant.String(), env.TenantID)
		assert.Equal(t, "instances/"+s.inst.ID.String(), env.Subject)
		payload := w25bDecode[domain.WorkflowTaskFailedPayload](t, env)
		assert.Equal(t, s.inst.ID, payload.WorkflowInstanceID)
		assert.Equal(t, w.task.ID, payload.TaskID)
		assert.Equal(t, w.task.NodeKey, payload.NodeKey)
		assert.Equal(t, w.task.DepartmentID, payload.DepartmentID)
		assert.Equal(t, "instance_cancel", payload.CascadeSource)
		assert.Empty(t, payload.ErrorClass)
		assert.Equal(t, w.assignees, payload.AssigneeUserIDs, "the vacated assignees, in the order they were vacated")
		fields := map[string]json.RawMessage{}
		require.NoError(t, json.Unmarshal(env.Payload, &fields))
		assert.NotContains(t, fields, "error_class")
	}
	cancelled := w25bDecode[domain.WorkflowInstanceCancelledPayload](t, s.rig.eventsOfType(domain.EventWorkflowInstanceCancelled)[0])
	assert.Equal(t, s.admin, cancelled.ActorUserID)
	require.NotNil(t, cancelled.Reason)
	assert.Equal(t, reason, *cancelled.Reason)
	assert.Equal(t, 1, s.rig.committed, "every event is written in the one transaction")
	assert.Len(t, s.rig.txOpened, 1)
}

func TestCancelInstance_ListVacateOrEventCoreReadFails_ReturnsTheWrappedErrorAndPersistsNothing(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		op          string
		onCall      int
		messagePart string
	}{
		{name: "the task listing", op: "Tasks.ListByInstance", onCall: 1, messagePart: "list tasks by instance"},
		{name: "the first task's vacate", op: "Assignments.Vacate", onCall: 1, messagePart: "vacate assignment "},
		{name: "the second task's vacate, after the first task was written", op: "Assignments.Vacate", onCall: 3, messagePart: "vacate assignment "},
		{name: "the instance read for the first task's event", op: "Instances.GetByID", onCall: 2, messagePart: "get instance for event core"},
		{name: "the instance read for the second task's event, after the first was written", op: "Instances.GetByID", onCall: 3, messagePart: "get instance for event core"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := w29aNewCancelScene(t)
			before := s.rig.snapshot()
			s.rig.failOn(tc.op, errW25bStorage, tc.onCall)

			err := s.cancelInstance()

			w25bRequireRetryable(t, err)
			require.ErrorIs(t, err, errW25bStorage)
			assert.Contains(t, err.Error(), tc.messagePart)
			assert.Equal(t, 1, s.rig.rolledBack)
			assert.Equal(t, 0, s.rig.committed)
			assert.Equal(t, domain.InstanceStatusRunning, s.rig.instances[s.inst.ID].Status)
			assert.Equal(t, domain.TaskStatusInProgress, s.rig.tasks[s.task.ID].Status)
			assert.Equal(t, domain.TaskStatusReady, s.rig.tasks[s.second.ID].Status)
			assert.True(t, s.rig.assignments[s.lead.ID].IsActive)
			assert.Empty(t, s.rig.outbox, "no event outlives the rollback")
			w25bRequireUntouched(t, s.rig, before)

			delete(s.rig.failures, tc.op)
			require.NoError(t, s.cancelInstance(), "the retry cancels the instance")
			assert.Equal(t, domain.InstanceStatusTerminated, s.rig.instances[s.inst.ID].Status)
			assert.Len(t, s.rig.eventsOfType(domain.EventWorkflowTaskFailed), 2, "each task is announced once")
		})
	}
}

func TestCancelInstance_AWriteFailsMidway_RollsBackEveryEarlierWriteAndTheRetryDoesItOnce(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		op          string
		onCall      int
		messagePart string
	}{
		{name: "the second task's status update", op: "Tasks.UpdateStatus", onCall: 2, messagePart: "fail task "},
		{name: "the instance status update after both tasks failed", op: "Instances.UpdateStatus", onCall: 1, messagePart: "update instance status"},
		{name: "the first task's event", op: "Outbox.Enqueue", onCall: 1, messagePart: "enqueue " + domain.EventWorkflowTaskFailed},
		{name: "the second task's event", op: "Outbox.Enqueue", onCall: 2, messagePart: "enqueue " + domain.EventWorkflowTaskFailed},
		{name: "the cancelled event after both task events", op: "Outbox.Enqueue", onCall: 3, messagePart: "enqueue " + domain.EventWorkflowInstanceCancelled},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := w29aNewCancelScene(t)
			before := s.rig.snapshot()
			s.rig.failOn(tc.op, errW25bStorage, tc.onCall)

			err := s.cancelInstance()

			w25bRequireRetryable(t, err)
			require.ErrorIs(t, err, errW25bStorage)
			assert.Contains(t, err.Error(), tc.messagePart)
			assert.Equal(t, 1, s.rig.rolledBack)
			assert.Equal(t, 0, s.rig.committed)
			assert.Equal(t, domain.TaskStatusInProgress, s.rig.tasks[s.task.ID].Status, "a task failed before the error is restored")
			assert.Equal(t, domain.TaskStatusReady, s.rig.tasks[s.second.ID].Status)
			assert.Equal(t, domain.InstanceStatusRunning, s.rig.instances[s.inst.ID].Status)
			assert.True(t, s.rig.assignments[s.lead.ID].IsActive, "a vacated assignment is restored")
			assert.True(t, s.rig.assignments[s.secondAssigned.ID].IsActive)
			assert.Empty(t, s.rig.outbox)
			w25bRequireUntouched(t, s.rig, before)

			delete(s.rig.failures, tc.op)
			require.NoError(t, s.cancelInstance())
			assert.Equal(t, []string{
				domain.EventWorkflowTaskFailed, domain.EventWorkflowTaskFailed, domain.EventWorkflowInstanceCancelled,
			}, w29aEventTypes(s.w25bScene))
			assert.Equal(t, 1, s.rig.committed)
		})
	}
}

func TestRecordForceRoute_BypassedTasks_AreSupersededThenOneForceRoutedEventIsWrittenInOneTransaction(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		direction string
	}{
		{name: "direction named forward", direction: domain.ForceRouteDirectionForward},
		{name: "direction left out", direction: ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := w29aNewCancelScene(t)
			other := s.rig.addTask(s.tenant, s.inst, "ops/untouched", domain.TaskStatusReady)

			err := s.deps.RecordForceRoute(context.Background(), port.RecordForceRouteInput{
				TenantID: s.tenant.String(), InstanceID: s.inst.ID.String(),
				OldNodeKeys:  []domain.NodeKey{domain.NodeKey(s.task.NodeKey), domain.NodeKey(s.second.NodeKey)},
				TargetNodeID: "sales/approve", Direction: tc.direction, AdminUserID: s.admin.String(), RecordVersion: 5,
			})

			require.NoError(t, err)
			assert.Equal(t, []string{
				domain.EventWorkflowTaskSuperseded, domain.EventWorkflowTaskSuperseded, domain.EventWorkflowInstanceForceRouted,
			}, w29aEventTypes(s.w25bScene))
			assert.Equal(t, 1, s.rig.committed, "the supersedes and the audit event share one transaction")
			assert.Len(t, s.rig.txOpened, 1)
			assert.Equal(t, 0, s.rig.rolledBack)
			assert.Equal(t, domain.TaskStatusSuperseded, s.rig.tasks[s.task.ID].Status)
			assert.Equal(t, domain.TaskStatusSuperseded, s.rig.tasks[s.second.ID].Status)
			assert.Equal(t, domain.TaskStatusReady, s.rig.tasks[other.ID].Status, "a task of a node that was not bypassed stays")

			superseded := s.rig.eventsOfType(domain.EventWorkflowTaskSuperseded)
			wantAssignees := [][]uuid.UUID{{s.leadUser, s.helperUser}, {s.secondUser}}
			for i, task := range []*domain.Task{s.task, s.second} {
				payload := w25bDecode[domain.WorkflowTaskSupersededPayload](t, superseded[i])
				assert.Equal(t, task.ID, payload.TaskID)
				assert.Equal(t, task.NodeKey, payload.NodeKey)
				assert.Equal(t, wantAssignees[i], payload.AssigneeUserIDs)
				require.NotNil(t, payload.ActorUserID)
				assert.Equal(t, s.admin, *payload.ActorUserID, "the admin who routed is the actor")
			}
			routed := s.rig.eventsOfType(domain.EventWorkflowInstanceForceRouted)
			require.Len(t, routed, 1)
			assert.Equal(t, s.tenant.String(), routed[0].TenantID)
			assert.Equal(t, "instances/"+s.inst.ID.String(), routed[0].Subject)
			payload := w25bDecode[domain.WorkflowInstanceForceRoutedPayload](t, routed[0])
			assert.Equal(t, s.inst.ID, payload.WorkflowInstanceID)
			assert.Equal(t, s.inst.SubjectID, payload.SubjectID)
			assert.Equal(t, "forward", payload.Direction)
			assert.Equal(t, "sales/approve", payload.ToNodeKey)
			assert.Equal(t, []string{s.task.NodeKey, s.second.NodeKey}, payload.FromNodeKeys)
			assert.Equal(t, s.admin, payload.ActorUserID)
			s.rig.requireEveryCallScopedTo(t, s.tenant)
		})
	}
}
