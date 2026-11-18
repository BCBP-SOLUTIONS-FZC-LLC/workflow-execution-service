package service_test

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
)

type w8aTerminateCall struct {
	WorkflowID string
	RunID      string
	Reason     string
}

// w8aFailingTerminateTemporal fails every TerminateWorkflow and records what it was asked to close.
type w8aFailingTerminateTemporal struct {
	port.TemporalClient
	err   error
	calls []w8aTerminateCall
}

func (c *w8aFailingTerminateTemporal) TerminateWorkflow(_ context.Context, workflowID, runID, reason string) error {
	c.calls = append(c.calls, w8aTerminateCall{WorkflowID: workflowID, RunID: runID, Reason: reason})
	return c.err
}

func TestInstanceTerminate_TemporalDeadlineExceeded_ReturnsTheDeadlineErrorAndLeavesTheInstanceRunning(t *testing.T) {
	tenantID, actorID := uuid.New(), uuid.New()
	inst := newCscInstance(tenantID, domain.InstanceStatusRunning, 3)
	h := newCscHarness(inst)
	task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: inst.ID, Status: domain.TaskStatusReady, RecordVersion: 1}
	h.tasks.byID[task.ID] = task
	temporal := &w8aFailingTerminateTemporal{err: fmt.Errorf("terminate execution: %w", context.DeadlineExceeded)}
	h.svc.Temporal = temporal
	logger := &fakeLogger{}
	h.svc.Log = logger

	err := h.svc.Terminate(context.Background(), tenantID, inst.ID, actorID, "no longer needed")

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.NotErrorIs(t, err, port.ErrInstanceAlreadyTerminal)
	assert.Equal(t, []w8aTerminateCall{{WorkflowID: inst.TemporalWorkflowID, RunID: inst.TemporalRunID, Reason: "no longer needed"}}, temporal.calls,
		"one close was attempted, for this instance's execution, and it is not retried")
	assert.Equal(t, []cscGetCall{{TenantID: tenantID, InstanceID: inst.ID}}, h.instances.gets, "the instance was read once, for the caller's tenant")
	assert.Zero(t, h.tx.runs, "no transaction is opened for a close that failed")
	assert.Empty(t, h.instances.updates)
	assert.Equal(t, domain.InstanceStatusRunning, h.instances.byID[inst.ID].Status, "a live execution is never left under a TERMINATED row")
	assert.Equal(t, domain.TaskStatusReady, h.tasks.byID[task.ID].Status)
	assert.Empty(t, h.tasks.updates)
	assert.Empty(t, h.outbox.attempts, "no event is recorded for a terminate that did not happen")
	assert.Len(t, logger.errorCalls, 1, "the failure is logged once")
}

func TestInstanceForceForward_CompiledPlanIsNotReadableJSON_ReturnsAnUnmappedErrorAndSendsNoSignal(t *testing.T) {
	cases := []struct {
		name string
		plan string
		as   func(t *testing.T, err error)
	}{
		{"truncated JSON", `{"main_plan":"main","plans":[`, func(t *testing.T, err error) {
			var syntax *json.SyntaxError
			require.ErrorAs(t, err, &syntax)
		}},
		{"JSON of the wrong shape", `"not a collaboration"`, func(t *testing.T, err error) {
			var shape *json.UnmarshalTypeError
			require.ErrorAs(t, err, &shape)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tenantID := uuid.New()
			inst := newCscInstance(tenantID, domain.InstanceStatusRunning, 4)
			h := newCscHarness(inst)
			h.definitions.resp = publishedCompiledWorkflow(uuid.New(), inst.WorkflowVersionID, tc.plan)

			err := h.svc.ForceForward(context.Background(), tenantID, inst.ID, uuid.New(), "finance/review", 4)

			require.Error(t, err)
			tc.as(t, err)
			for _, mapped := range []error{port.ErrTargetNodeNotFound, port.ErrVersionInvalid, port.ErrRecordVersionConflict, port.ErrInvalidInstanceState, port.ErrInstanceNotFound} {
				assert.NotErrorIs(t, err, mapped, "a stored plan that cannot be read is a server fault, not a refusal the caller can fix")
			}
			assert.Equal(t, []cscPlanRead{{TenantID: tenantID, VersionID: inst.WorkflowVersionID}}, h.definitions.reads)
			assert.Empty(t, h.temporal.signals)
			assert.Empty(t, h.tasks.lists, "the open tasks are not read once the plan is unusable")
		})
	}
}
