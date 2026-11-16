package service_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type vacatedWire struct {
	TaskID        string
	VacatedUserID string
	RecordedAt    time.Time
}

func decodeVacated(t *testing.T, payload any) vacatedWire {
	t.Helper()
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	var wire vacatedWire
	require.NoError(t, json.Unmarshal(raw, &wire))
	return wire
}

func TestDelegationCancel_ATaskLeftWithoutAnAssigneeRecordsASignalInTheSameTransaction(t *testing.T) {
	h := dlgfixNewClient(t)
	held := h.seed(t, "reviewer")
	var runningAtRecord []bool
	var commitsAtRecord []int
	h.outbox.onSignal = func() {
		runningAtRecord = append(runningAtRecord, h.tx.running)
		commitsAtRecord = append(commitsAtRecord, h.tx.commits)
	}

	count, err := h.cancel()

	require.NoError(t, err)
	assert.Equal(t, 1, count)
	require.Len(t, h.outbox.signals, 1, "the workflow is told that the task lost its only assignee")
	sent := h.outbox.signals[0]
	assert.Equal(t, port.SignalInstanceTaskVacated, sent.SignalName)
	assert.Equal(t, held.instance.ID, sent.InstanceID)
	assert.Equal(t, held.instance.TemporalWorkflowID, sent.TemporalWorkflowID)
	wire := decodeVacated(t, sent.Payload)
	assert.False(t, wire.RecordedAt.IsZero(), "the signal carries the time it was recorded, because delivery is not ordered")
	wire.RecordedAt = time.Time{}
	assert.Equal(t, vacatedWire{TaskID: held.task.ID.String(), VacatedUserID: h.oldDelegate.String()}, wire)
	assert.Equal(t, []uuid.UUID{h.tenantID}, h.outbox.signalTenants)
	assert.Equal(t, []bool{true}, runningAtRecord, "recorded inside the transaction")
	assert.Equal(t, []int{0}, commitsAtRecord, "before the commit")
}

func TestDelegationCancel_ATaskThatStillHasAnAssigneeIsNotSignalled(t *testing.T) {
	h := dlgfixNewClient(t)
	shared, alone := h.seed(t, "reviewer"), h.seed(t, "reviewer")
	h.assignments.add(&domain.TaskAssignment{ID: uuid.New(), TenantID: h.tenantID, TaskID: shared.task.ID, UserID: uuid.New(), IsActive: true})

	count, err := h.cancel()

	require.NoError(t, err)
	assert.Equal(t, 2, count)
	require.Len(t, h.outbox.signals, 1, "a co-assignee keeps the task going, so only the task with nobody left is reported")
	assert.Equal(t, alone.instance.ID, h.outbox.signals[0].InstanceID)
	assert.Equal(t, alone.task.ID.String(), decodeVacated(t, h.outbox.signals[0].Payload).TaskID)
}

func TestDelegationCancel_OneSignalForEachTaskLeftWithoutAnAssignee(t *testing.T) {
	h := dlgfixNewClient(t)
	first, second := h.seed(t, "reviewer"), h.seed(t, "reviewer")

	count, err := h.cancel()

	require.NoError(t, err)
	assert.Equal(t, 2, count)
	tasks := map[string]uuid.UUID{}
	for _, s := range h.outbox.signalsNamed(port.SignalInstanceTaskVacated) {
		tasks[decodeVacated(t, s.Payload).TaskID] = s.InstanceID
	}
	assert.Equal(t, map[string]uuid.UUID{first.task.ID.String(): first.instance.ID, second.task.ID.String(): second.instance.ID}, tasks)
}

func TestDelegationCancel_AFinishedInstanceIsNotSignalled(t *testing.T) {
	for _, status := range []domain.InstanceStatus{domain.InstanceStatusCompleted, domain.InstanceStatusTerminated, domain.InstanceStatusFailed} {
		t.Run(string(status), func(t *testing.T) {
			h := dlgfixNewClient(t)
			finished, live := h.seed(t, "reviewer"), h.seed(t, "reviewer")
			finished.instance.Status = status

			count, err := h.cancel()

			require.NoError(t, err)
			assert.Equal(t, 2, count, "both assignments are vacated")
			require.Len(t, h.outbox.signals, 1, "there is no workflow left to tell about the finished instance")
			assert.Equal(t, live.instance.ID, h.outbox.signals[0].InstanceID)
		})
	}
}

func TestDelegationCancel_AFailedSignalRecordingChangesNothingAndFailsTheCall(t *testing.T) {
	h := dlgfixNewClient(t)
	first, second := h.seed(t, "reviewer"), h.seed(t, "reviewer")
	h.outbox.signalErr = assert.AnError

	count, err := h.cancel()

	require.ErrorIs(t, err, assert.AnError, "a cancel whose signal cannot be recorded is not committed, so the caller retries it")
	assert.Zero(t, count)
	assert.True(t, first.assignment.IsActive)
	assert.True(t, second.assignment.IsActive)
	assert.Empty(t, h.outbox.signals)
	assert.Equal(t, 1, h.tx.rollbacks)
}

func TestDelegationCancel_ATaskThatCannotBeReadFailsTheCallBeforeAnythingChanges(t *testing.T) {
	h := dlgfixNewClient(t)
	first := h.seed(t, "reviewer")
	h.assignments.add(&domain.TaskAssignment{ID: uuid.New(), TenantID: h.tenantID, TaskID: uuid.New(), UserID: h.oldDelegate, IsActive: true})

	count, err := h.cancel()

	require.Error(t, err)
	assert.Zero(t, count)
	assert.True(t, first.assignment.IsActive)
	assert.Zero(t, h.tx.commits)
	assert.Empty(t, h.outbox.signals)
}

func recordedAtOf(t *testing.T, payload any) time.Time {
	t.Helper()
	return decodeVacated(t, payload).RecordedAt
}

func TestDelegationReassign_TheSignalCarriesTheTimeItWasRecorded(t *testing.T) {
	h := dlgfixNewClient(t)
	h.seed(t, "reviewer")
	before := time.Now().UTC()

	count, err := h.reassign()

	require.NoError(t, err)
	assert.Equal(t, 1, count)
	require.Len(t, h.outbox.signals, 1)
	recorded := recordedAtOf(t, h.outbox.signals[0].Payload)
	assert.False(t, recorded.Before(before), "stamped when the move is recorded")
	assert.False(t, recorded.After(time.Now().UTC()))
}
