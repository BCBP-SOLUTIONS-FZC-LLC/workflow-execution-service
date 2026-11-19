package service_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporalclient"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
)

type w29bTerminateCall struct{ workflowID, runID, reason string }

// w29bGoneSDK is the Temporal SDK under the real adapter: every terminate answers that the execution is
// gone, and each call is recorded.
type w29bGoneSDK struct {
	client.Client
	calls []w29bTerminateCall
}

func (s *w29bGoneSDK) TerminateWorkflow(_ context.Context, workflowID, runID, reason string, _ ...any) error {
	s.calls = append(s.calls, w29bTerminateCall{workflowID, runID, reason})
	return serviceerror.NewNotFound("workflow execution not found")
}

func TestTenantLifecycleReconciler_Offboard_WorkflowAlreadyGone_StillMarksTheInstanceTerminated(t *testing.T) {
	rig := w12aNewTenantRig()
	sdk := &w29bGoneSDK{}
	rig.svc.Temporal = temporalclient.New(sdk)
	gone := rig.instance(domain.InstanceStatusRunning)
	task, rows := rig.task(gone, domain.TaskStatusReady, uuid.New())

	require.NoError(t, rig.offboard(), "a workflow Temporal no longer knows does not fail the offboarding event")

	assert.Equal(t, domain.InstanceStatusTerminated, gone.Status)
	require.Len(t, rig.instances.updates, 1)
	assert.Equal(t, w12aStatusWrite{tenantID: rig.tenantID, id: gone.ID, status: "TERMINATED", version: 3, inTx: true}, rig.instances.updates[0])
	assert.Equal(t, domain.TaskStatusFailed, task.Status, "its open task is failed with the instance")
	assert.False(t, rows[0].IsActive, "its assignment is vacated")
	terminated := w12aEventsOfType(t, rig, "WorkflowInstanceTerminated")
	require.Len(t, terminated, 1)
	assert.Contains(t, string(terminated[0]), gone.ID.String())
	want := w29bTerminateCall{workflowID: gone.TemporalWorkflowID, runID: gone.TemporalRunID, reason: "tenant offboarded"}
	assert.Equal(t, []w29bTerminateCall{want, want}, sdk.calls, "the execution is stopped before the commit and once more after it")
	require.Len(t, rig.retention.recorded, 1, "the offboarding is recorded: the event succeeded")
}
