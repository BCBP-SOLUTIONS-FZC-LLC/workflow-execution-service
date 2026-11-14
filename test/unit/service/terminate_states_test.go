package service_test

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

func TestInstanceService_Terminate_FromPausedOrDegraded_TerminatesCascadesAndAnnounces(t *testing.T) {
	for _, status := range []domain.InstanceStatus{domain.InstanceStatusPaused, domain.InstanceStatusDegraded} {
		t.Run(string(status), func(t *testing.T) {
			tenantID, actorID := uuid.New(), uuid.New()
			inst := newCscInstance(tenantID, status, 7)
			h := newCscHarness(inst)
			open := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: inst.ID, Status: domain.TaskStatusReady, RecordVersion: 2}
			done := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: inst.ID, Status: domain.TaskStatusCompleted, RecordVersion: 5}
			h.tasks.byID[open.ID], h.tasks.byID[done.ID] = open, done
			assignment := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: open.ID, IsActive: true}
			h.assignments.byID[assignment.ID] = assignment

			err := h.svc.Terminate(context.Background(), tenantID, inst.ID, actorID, "admin cleanup")

			require.NoError(t, err)
			require.NotEmpty(t, h.temporal.terminates)
			assert.Equal(t, cscTerminate{WorkflowID: inst.TemporalWorkflowID, RunID: inst.TemporalRunID, Reason: "admin cleanup"}, h.temporal.terminates[0])

			assert.Equal(t, []cscUpdateCall{{
				TenantID: tenantID, InstanceID: inst.ID, Status: domain.InstanceStatusTerminated, RecordVersion: 7, InTx: true,
			}}, h.instances.updates)
			assert.Equal(t, domain.InstanceStatusTerminated, h.instances.byID[inst.ID].Status)

			assert.Equal(t, []cscTaskUpdate{{
				TenantID: tenantID, TaskID: open.ID, Status: domain.TaskStatusFailed, RecordVersion: 2, InTx: true,
			}}, h.tasks.updates, "only the open task is failed")
			assert.Equal(t, domain.TaskStatusCompleted, h.tasks.byID[done.ID].Status)
			assert.Equal(t, []cscVacateCall{{TenantID: tenantID, AssignmentID: assignment.ID}}, h.assignments.vacated)
			assert.False(t, assignment.IsActive)

			require.Len(t, h.outbox.attempts, 1)
			sent := h.outbox.attempts[0]
			assert.True(t, sent.InTx, "the event is written in the same transaction as the status change")
			assert.Equal(t, domain.EventWorkflowInstanceTerminated, sent.Envelope.Type)
			assert.Equal(t, tenantID.String(), sent.Envelope.TenantID)
			assert.Equal(t, "instances/"+inst.ID.String(), sent.Envelope.Subject)
			assert.Equal(t, actorID.String(), sent.Envelope.Actor)
			var data struct {
				WorkflowInstanceID string `json:"workflow_instance_id"`
				Initiator          string `json:"initiator"`
				ActorUserID        string `json:"actor_user_id"`
			}
			require.NoError(t, json.Unmarshal(sent.Envelope.Payload, &data))
			assert.Equal(t, inst.ID.String(), data.WorkflowInstanceID)
			assert.Equal(t, "admin", data.Initiator)
			assert.Equal(t, actorID.String(), data.ActorUserID)
		})
	}
}

func TestInstanceService_Terminate_ReReadInsideTransaction_MapsTheOutcome(t *testing.T) {
	cases := []struct {
		name    string
		readErr error
		wantErr error
		notErr  error
	}{
		{"row is gone", domain.ErrNotFound, port.ErrInstanceNotFound, nil},
		{"database failure", errCscDatabase, errCscDatabase, port.ErrInstanceNotFound},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tenantID := uuid.New()
			inst := newCscInstance(tenantID, domain.InstanceStatusRunning, 3)
			h := newCscHarness(inst)
			h.instances.getErrOnCall = map[int]error{2: tc.readErr}

			err := h.svc.Terminate(context.Background(), tenantID, inst.ID, uuid.New(), "reason")

			require.ErrorIs(t, err, tc.wantErr)
			if tc.notErr != nil {
				assert.NotErrorIs(t, err, tc.notErr)
			}
			require.Len(t, h.instances.gets, 2)
			assert.False(t, h.instances.gets[0].InTx, "the first read is outside the transaction")
			assert.True(t, h.instances.gets[1].InTx, "the second read is inside the transaction")
			assert.Equal(t, tenantID, h.instances.gets[1].TenantID)
			assert.Equal(t, inst.ID, h.instances.gets[1].InstanceID)
			assert.Empty(t, h.instances.updates)
			assert.Empty(t, h.outbox.attempts)
			assert.Equal(t, domain.InstanceStatusRunning, h.instances.byID[inst.ID].Status)
			require.Equal(t, 1, h.tx.runs)
			assert.ErrorIs(t, h.tx.lastErr, tc.wantErr, "the callback's error is what rolls the transaction back")
			assert.Equal(t, []string{tenantID.String()}, h.tx.guTenants, "the transaction is opened on the caller's tenant")
		})
	}
}

func TestInstanceService_Terminate_OutboxFailure_ReturnsTheErrorFromInsideTheTransaction(t *testing.T) {
	tenantID, actorID := uuid.New(), uuid.New()
	inst := newCscInstance(tenantID, domain.InstanceStatusRunning, 3)
	h := newCscHarness(inst)
	open := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: inst.ID, Status: domain.TaskStatusInProgress, RecordVersion: 1}
	h.tasks.byID[open.ID] = open
	h.outbox.enqueueErr = errCscOutbox

	err := h.svc.Terminate(context.Background(), tenantID, inst.ID, actorID, "reason")

	require.ErrorIs(t, err, errCscOutbox)
	require.Len(t, h.outbox.attempts, 1)
	assert.Equal(t, domain.EventWorkflowInstanceTerminated, h.outbox.attempts[0].Envelope.Type)

	// The status change, the task change and the outbox write all ran inside the
	// one transaction, and the callback ended in the outbox error, so the database
	// discards all three together.
	require.Equal(t, 1, h.tx.runs)
	require.ErrorIs(t, h.tx.lastErr, errCscOutbox)
	require.Len(t, h.instances.updates, 1)
	assert.True(t, h.instances.updates[0].InTx)
	require.Len(t, h.tasks.updates, 1)
	assert.True(t, h.tasks.updates[0].InTx)
	assert.True(t, h.outbox.attempts[0].InTx)
}
