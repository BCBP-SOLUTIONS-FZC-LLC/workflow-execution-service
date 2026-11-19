package handler_test

import (
	"errors"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
)

func TestTerminateRoute_TemporalReportsTheWorkflowGone_StillMarksTheInstanceTerminated(t *testing.T) {
	rig := w19bNewRig(t)
	inst := rig.seed(domain.InstanceStatusRunning, 3)
	task := rig.addTask(inst, domain.TaskStatusReady)
	rig.sdk.terminateErr = func(int) error { return serviceerror.NewNotFound("workflow execution not found") }

	w := rig.terminate(inst.ID)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	assert.Equal(t, []w19bStatusWrite{{testTenantID, inst.ID, domain.InstanceStatusTerminated, 3}}, rig.instances.writes)
	assert.Equal(t, domain.InstanceStatusTerminated, rig.instances.rows[inst.ID].Status)
	assert.Equal(t, []w19bTaskWrite{{testTenantID, task.ID, domain.TaskStatusFailed, 1}}, rig.tasks.statusWrites)
	assert.Equal(t, []w19bIDs{{testTenantID, task.ID}}, rig.assignments.lookups, "the task's assignments are read for the caller's tenant")
	assert.Equal(t, 1, rig.transactor.plain, "the writes run in one transaction")
	assert.Zero(t, rig.transactor.retried, "a version conflict is answered, not retried")
	assert.Equal(t, []w19bEvent{
		{string(domain.EventWorkflowTaskFailed), testTenantID.String(), "instances/" + inst.ID.String()},
		{string(domain.EventWorkflowInstanceTerminated), testTenantID.String(), "instances/" + inst.ID.String()},
	}, rig.outbox.events, "both events carry the caller's tenant and the instance as their subject")
	require.Len(t, rig.sdk.terminates, 2, "the execution is closed before the write and again after it, and a gone workflow fails neither")
	for _, call := range rig.sdk.terminates {
		assert.Equal(t, w19bTerminateCall{workflowID: inst.TemporalWorkflowID, runID: "", reason: "stuck"}, call)
	}
}

func TestTerminateRoute_AnExecutionStartedBetweenTheTwoCloses_IsClosedByItsRunID(t *testing.T) {
	rig := w19bNewRig(t)
	inst := rig.seed(domain.InstanceStatusRunning, 3)
	require.Empty(t, inst.TemporalRunID, "the instance is still being started: no run id is recorded")
	const lateRunID = "run-recorded-by-the-start"
	rig.sdk.onTerminate = func(call int) {
		if call == 1 {
			rig.instances.rows[inst.ID].TemporalRunID = lateRunID
		}
	}

	w := rig.terminate(inst.ID)

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	assert.Equal(t, domain.InstanceStatusTerminated, rig.instances.rows[inst.ID].Status)
	assert.Equal(t, []w19bTerminateCall{
		{workflowID: inst.TemporalWorkflowID, runID: "", reason: "stuck"},
		{workflowID: inst.TemporalWorkflowID, runID: lateRunID, reason: "stuck"},
	}, rig.sdk.terminates, "the first close names no run, the second names the run the start recorded in between")
}

func TestTerminateRoute_TemporalFailsToCloseTheExecution_Returns503AndLeavesTheInstanceAsItWas(t *testing.T) {
	cases := []struct {
		name  string
		cause error
	}{
		{"the cluster is unavailable", serviceerror.NewUnavailable("frontend down: cause-marker-7f3")},
		{"the cluster call times out", serviceerror.NewDeadlineExceeded("took too long: cause-marker-7f3")},
		{"the connection fails", errors.New("dial tcp: connection refused: cause-marker-7f3")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w19bNewRig(t)
			inst := rig.seed(domain.InstanceStatusRunning, 3)
			task := rig.addTask(inst, domain.TaskStatusReady)
			rig.sdk.terminateErr = func(int) error { return tc.cause }

			w := rig.terminate(inst.ID)

			require.Equal(t, http.StatusServiceUnavailable, w.Code, w.Body.String())
			p := w19bProblemOf(t, w)
			assert.Equal(t, "UPSTREAM_UNAVAILABLE", p.Code)
			assert.NotEmpty(t, p.Detail)
			assert.NotContains(t, w.Body.String(), "cause-marker-7f3", "the cause is logged, not returned")
			assert.Equal(t, domain.InstanceStatusRunning, rig.instances.rows[inst.ID].Status)
			assert.Equal(t, domain.TaskStatusReady, task.Status)
			assert.Empty(t, rig.instances.writes)
			assert.Empty(t, rig.tasks.statusWrites)
			assert.Empty(t, rig.outbox.types, "no event is announced")
			assert.Len(t, rig.sdk.terminates, 1, "the failed close is not followed by a second one")
		})
	}
}

func TestTerminateRoute_AnInstanceTheCallersTenantDoesNotHave_Returns404AndClosesNothing(t *testing.T) {
	foreignTenant := uuid.New()
	cases := []struct {
		name string
		seed func(rig *w19bRig) uuid.UUID
	}{
		{"an id no tenant has", func(*w19bRig) uuid.UUID { return uuid.New() }},
		{"an instance of another tenant", func(rig *w19bRig) uuid.UUID {
			inst := rig.seed(domain.InstanceStatusRunning, 3)
			inst.TenantID = foreignTenant
			return inst.ID
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w19bNewRig(t)
			id := tc.seed(rig)

			w := rig.terminate(id)

			require.Equal(t, http.StatusNotFound, w.Code, w.Body.String())
			assert.Equal(t, "INSTANCE_NOT_FOUND", w19bProblemOf(t, w).Code)
			assert.Equal(t, []w19bIDs{{testTenantID, id}}, rig.instances.gets, "the lookup is scoped to the caller's tenant")
			assert.Empty(t, rig.sdk.terminates, "no workflow is closed")
			assert.Empty(t, rig.instances.writes)
			assert.Empty(t, rig.outbox.types)
			if row, ok := rig.instances.rows[id]; ok {
				assert.Equal(t, domain.InstanceStatusRunning, row.Status, "the other tenant's instance is untouched")
			}
		})
	}
}
