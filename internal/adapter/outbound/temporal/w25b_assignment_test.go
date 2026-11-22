package temporal_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestInstanceCore_EventsCarryTheInstancesSubjectBusinessKeyAndVersion_AndAMissingInstanceEndsAsNotFound(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		eventType string
		run       func(s *w25bScene) error
	}{
		{name: "complete", eventType: domain.EventWorkflowTaskCompleted, run: (*w25bScene).complete},
		{name: "defer", eventType: domain.EventWorkflowTaskDeferred, run: (*w25bScene).deferTask},
		{name: "reassign to a new user", eventType: domain.EventWorkflowTaskReassigned, run: (*w25bScene).reassignToNewUser},
		{name: "reassign to a co-assignee", eventType: domain.EventWorkflowTaskReassigned, run: (*w25bScene).reassignToHolder},
		{name: "sla warning", eventType: domain.EventWorkflowTaskSLAWarning, run: (*w25bScene).slaWarning},
		{name: "sla breach", eventType: domain.EventWorkflowTaskSLABreached, run: (*w25bScene).slaBreach},
		{name: "fail the task", eventType: domain.EventWorkflowTaskFailed, run: func(s *w25bScene) error { return s.setStatus(domain.TaskStatusFailed) }},
		{name: "supersede the task", eventType: domain.EventWorkflowTaskSuperseded, run: func(s *w25bScene) error { return s.setStatus(domain.TaskStatusSuperseded) }},
	}
	for _, tc := range cases {
		t.Run(tc.name+" with the instance", func(t *testing.T) {
			t.Parallel()
			s := w25bNewScene(t, true)

			require.NoError(t, tc.run(s))

			announced := s.rig.eventsOfType(tc.eventType)
			require.Len(t, announced, 1)
			core := w25bDecode[domain.CommonCore](t, announced[0])
			assert.Equal(t, s.inst.ID, core.WorkflowInstanceID)
			assert.Equal(t, s.inst.SubjectID, core.SubjectID)
			assert.Equal(t, s.inst.BusinessKey, core.BusinessKey)
			assert.Equal(t, s.inst.WorkflowVersionID, core.WorkflowVersionID)
			s.rig.requireEveryCallScopedTo(t, s.tenant)

			before := s.rig.snapshot()
			require.NoError(t, tc.run(s), "a retry of the committed call succeeds")
			w25bRequireUntouched(t, s.rig, before)
		})
		t.Run(tc.name+" without the instance", func(t *testing.T) {
			t.Parallel()
			s := w25bNewScene(t, false)
			before := s.rig.snapshot()

			err := tc.run(s)

			appErr := w25bRequireNonRetryable(t, err, "NotFoundError", "get instance for event core")
			require.ErrorIs(t, err, domain.ErrNotFound)
			assert.NotNil(t, appErr.Unwrap())
			assert.Equal(t, 1, s.rig.rolledBack, "whatever was written before the lookup is undone")
			assert.Empty(t, s.rig.outbox)
			w25bRequireUntouched(t, s.rig, before)
		})
	}
}

func TestCompleteAssignment_TheLeadCompletes_OnlyTheLeadsAssignmentRecordsTheResultAndTheEventNamesOnlyTheLead(t *testing.T) {
	t.Parallel()
	s := w25bNewScene(t, true)
	extra := s.rig.addAssignment(s.tenant, s.task, uuid.New(), false, true)

	require.NoError(t, s.complete())

	assert.False(t, s.rig.assignments[s.lead.ID].IsActive)
	require.NotNil(t, s.rig.assignments[s.lead.ID].CompletedAt)
	assert.JSONEq(t, `{"ok":true}`, string(s.rig.assignments[s.lead.ID].ResultJSON))
	for _, helper := range []*domain.TaskAssignment{s.helper, extra} {
		got := s.rig.assignments[helper.ID]
		assert.False(t, got.IsActive, "the co-assignee's open assignment is vacated")
		assert.Nil(t, got.CompletedAt, "a co-assignee did not complete anything")
		assert.Nil(t, got.ResultJSON, "a co-assignee gets no result recorded")
	}
	assert.ElementsMatch(t, []uuid.UUID{s.helper.ID, extra.ID}, s.rig.idsOf("Assignments.Vacate"))
	completed := s.rig.eventsOfType(domain.EventWorkflowTaskCompleted)
	require.Len(t, completed, 1)
	payload := w25bDecode[domain.WorkflowTaskCompletedPayload](t, completed[0])
	assert.Equal(t, s.leadUser, payload.CompletedByUserID)
	assert.Equal(t, []uuid.UUID{s.leadUser}, payload.AssigneeUserIDs)
	versions := s.rig.callsTo("Assignments.Complete")
	require.Len(t, versions, 1)
	assert.Equal(t, s.version, versions[0].Version, "the completion is written at the task version the activity read")
}

func TestCompleteAssignment_VacatingACoAssigneeFails_RollsBackTheWholeCompletionAndTheRetryFinishesIt(t *testing.T) {
	t.Parallel()
	s := w25bNewScene(t, true)
	before := s.rig.snapshot()
	s.rig.failOn("Assignments.Vacate", errW25bStorage, 0)

	out, err := s.deps.CompleteAssignment(context.Background(), port.CompleteAssignmentInput{
		TenantID: s.tenant.String(), TaskID: s.task.ID.String(), UserID: s.leadUser.String(), ResultJSON: `{"ok":true}`,
	})

	w25bRequireRetryable(t, err)
	require.ErrorIs(t, err, errW25bStorage)
	assert.Contains(t, err.Error(), "vacate co-assignee assignment "+s.helper.ID.String())
	assert.False(t, out.AllDone)
	assert.Equal(t, 1, s.rig.rolledBack)
	assert.Empty(t, s.rig.callsTo("Outbox.Enqueue"))
	w25bRequireUntouched(t, s.rig, before)
	assert.True(t, s.rig.assignments[s.lead.ID].IsActive, "the lead's completion is undone with it")
	assert.Nil(t, s.rig.assignments[s.lead.ID].CompletedAt)

	delete(s.rig.failures, "Assignments.Vacate")
	require.NoError(t, s.complete())
	assert.False(t, s.rig.assignments[s.helper.ID].IsActive)
	assert.Len(t, s.rig.eventsOfType(domain.EventWorkflowTaskCompleted), 1)
}

func TestReassignAssignment_ToACoAssigneeWhoIsNotLead_MovesTheLeadAtTheInputVersionWithoutANewRow(t *testing.T) {
	t.Parallel()
	s := w25bNewScene(t, true)

	require.NoError(t, s.reassignToHolder())

	assert.False(t, s.rig.assignments[s.lead.ID].IsActive, "the replaced lead is vacated")
	assert.True(t, s.rig.assignments[s.helper.ID].IsActive)
	assert.True(t, s.rig.assignments[s.helper.ID].IsLead)
	assert.False(t, s.rig.assignments[s.lead.ID].IsLead)
	assert.Empty(t, s.rig.callsTo("Assignments.Create"), "the co-assignee already holds the task: no new row")
	assert.Len(t, s.rig.assignments, 2)
	setLead := s.rig.callsTo("Assignments.SetLead")
	require.Len(t, setLead, 1)
	assert.Equal(t, s.helper.ID, setLead[0].ID)
	assert.Equal(t, s.version, setLead[0].Version, "the lead moves at the record version the caller sent")
	assert.Equal(t, s.version+1, s.rig.tasks[s.task.ID].RecordVersion)
	moved := s.rig.eventsOfType(domain.EventWorkflowTaskReassigned)
	require.Len(t, moved, 1)
	payload := w25bDecode[domain.WorkflowTaskReassignedPayload](t, moved[0])
	require.NotNil(t, payload.OldUserID)
	assert.Equal(t, s.leadUser, *payload.OldUserID)
	assert.Equal(t, s.helperUser, payload.NewUserID)
	assert.Equal(t, []uuid.UUID{s.helperUser}, payload.AssigneeUserIDs)
	assert.Equal(t, domain.ReassignInitiatorAdmin, payload.Initiator)

	before := s.rig.snapshot()
	require.NoError(t, s.reassignToHolder(), "a retry finds the new lead and changes nothing")
	w25bRequireUntouched(t, s.rig, before)
}

func TestReassignAssignment_ToACoAssigneeWhoIsNotLead_AFailureUndoesTheMove(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		prepare     func(s *w25bScene)
		wantType    string
		messagePart string
		cause       error
	}{
		{
			name:        "vacating the replaced lead fails",
			prepare:     func(s *w25bScene) { s.rig.failOn("Assignments.Vacate", errW25bStorage, 0) },
			messagePart: "vacate old assignment", cause: errW25bStorage,
		},
		{
			name:        "setting the new lead fails",
			prepare:     func(s *w25bScene) { s.rig.failOn("Assignments.SetLead", errW25bStorage, 0) },
			messagePart: "set lead", cause: errW25bStorage,
		},
		{
			name:        "the instance cannot be read for the event",
			prepare:     func(s *w25bScene) { s.rig.failOn("Instances.GetByID", errW25bStorage, 0) },
			messagePart: "get instance for event core", cause: errW25bStorage,
		},
		{
			name:        "the task moved since the caller read it",
			prepare:     func(s *w25bScene) { s.rig.tasks[s.task.ID].RecordVersion++ },
			wantType:    "VersionConflict",
			messagePart: "reassign: lost the record_version race", cause: domain.ErrRecordVersionConflict,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := w25bNewScene(t, true)
			tc.prepare(s)
			before := s.rig.snapshot()

			err := s.reassignToHolder()

			require.ErrorIs(t, err, tc.cause)
			if tc.wantType != "" {
				_ = w25bRequireNonRetryable(t, err, tc.wantType, tc.messagePart)
			} else {
				w25bRequireRetryable(t, err)
				assert.Contains(t, err.Error(), tc.messagePart)
			}
			assert.Equal(t, 1, s.rig.rolledBack)
			assert.Empty(t, s.rig.callsTo("Outbox.Enqueue"))
			w25bRequireUntouched(t, s.rig, before)
			assert.True(t, s.rig.assignments[s.lead.ID].IsActive, "the replaced lead is not left vacated")
			assert.True(t, s.rig.assignments[s.lead.ID].IsLead)
			assert.False(t, s.rig.assignments[s.helper.ID].IsLead)
		})
	}
}

func TestReassignAssignment_LookUpOfTheNewAssignmentFailsWithADatabaseError_WritesNothingAndIsRetried(t *testing.T) {
	t.Parallel()
	s := w25bNewScene(t, true)
	before := s.rig.snapshot()
	s.rig.failOn("Assignments.GetByID", errW25bStorage, 0)

	err := s.reassignToNewUser()

	w25bRequireRetryable(t, err)
	require.ErrorIs(t, err, errW25bStorage)
	assert.Contains(t, err.Error(), "look up assignment before creating it")
	assert.Equal(t, 1, s.rig.rolledBack)
	assert.Empty(t, s.rig.callsTo("Tasks.BumpRecordVersion"), "nothing is written after a look-up that failed")
	assert.Empty(t, s.rig.callsTo("Assignments.Vacate"))
	assert.Empty(t, s.rig.callsTo("Assignments.Create"))
	assert.Empty(t, s.rig.callsTo("Outbox.Enqueue"))
	w25bRequireUntouched(t, s.rig, before)

	delete(s.rig.failures, "Assignments.GetByID")
	require.NoError(t, s.reassignToNewUser())
	assert.Len(t, s.rig.assignments, 3, "the retry creates the new assignment")
	assert.Len(t, s.rig.eventsOfType(domain.EventWorkflowTaskReassigned), 1)
}

func TestAssignmentActivities_ADatabaseErrorOnAReadOrWrite_IsAWrappedRetryableErrorAndRollsBack(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		failingOp   string
		messagePart string
		run         func(s *w25bScene) error
	}{
		{name: "complete: task lookup", failingOp: "Tasks.GetByID", messagePart: "get task", run: (*w25bScene).complete},
		{name: "complete: assignment listing", failingOp: "Assignments.ListActiveByTask", messagePart: "list active assignments", run: (*w25bScene).complete},
		{name: "complete: assignment write", failingOp: "Assignments.Complete", messagePart: "complete assignment", run: (*w25bScene).complete},
		{name: "reassign: task lookup", failingOp: "Tasks.GetByID", messagePart: "get task", run: (*w25bScene).reassignToNewUser},
		{name: "reassign: assignment listing", failingOp: "Assignments.ListActiveByTask", messagePart: "list active assignments", run: (*w25bScene).reassignToNewUser},
		{name: "reassign: version bump", failingOp: "Tasks.BumpRecordVersion", messagePart: "bump task record version", run: (*w25bScene).reassignToNewUser},
		{name: "reassign: vacate", failingOp: "Assignments.Vacate", messagePart: "vacate old assignment", run: (*w25bScene).reassignToNewUser},
		{name: "reassign: assignment write", failingOp: "Assignments.Create", messagePart: "create new assignment", run: (*w25bScene).reassignToNewUser},
		{name: "status: task lookup", failingOp: "Tasks.GetByID", messagePart: "get task", run: func(s *w25bScene) error { return s.setStatus(domain.TaskStatusFailed) }},
		{name: "status: status write", failingOp: "Tasks.UpdateStatus", messagePart: "update task status", run: func(s *w25bScene) error { return s.setStatus(domain.TaskStatusFailed) }},
		{name: "status: assignment listing", failingOp: "Assignments.ListActiveByTask", messagePart: "list active assignments for task", run: func(s *w25bScene) error { return s.setStatus(domain.TaskStatusFailed) }},
		{name: "status: vacate", failingOp: "Assignments.Vacate", messagePart: "vacate assignment", run: func(s *w25bScene) error { return s.setStatus(domain.TaskStatusFailed) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := w25bNewScene(t, true)
			before := s.rig.snapshot()
			s.rig.failOn(tc.failingOp, errW25bStorage, 0)

			err := tc.run(s)

			w25bRequireRetryable(t, err)
			require.ErrorIs(t, err, errW25bStorage)
			assert.Contains(t, err.Error(), tc.messagePart)
			assert.Equal(t, 1, s.rig.rolledBack)
			assert.Equal(t, 0, s.rig.committed)
			assert.Empty(t, s.rig.callsTo("Outbox.Enqueue"), "no event is announced for a transaction that failed")
			w25bRequireUntouched(t, s.rig, before)

			delete(s.rig.failures, tc.failingOp)
			require.NoError(t, tc.run(s), "the retry goes through")
			assert.Equal(t, 1, s.rig.committed)
			afterRetry := s.rig.snapshot()
			require.NoError(t, tc.run(s))
			w25bRequireUntouched(t, s.rig, afterRetry)
		})
	}
}
