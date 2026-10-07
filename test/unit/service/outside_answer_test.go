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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

const outsideKey = "finance/Receive_Bid"

// outsideAnswerHarness has a running instance whose plan has one outside wait,
// the receive task finance/Receive_Bid, and a Temporal query that reports the
// keys the instance is waiting in.
func outsideAnswerHarness(t *testing.T, status domain.InstanceStatus, waiting ...string) (*service.InstanceService, *domain.Instance, *fakeTemporalClient) {
	t.Helper()
	svc, instances, _, _, _, temporal, definitions, _ := newInstanceServiceHarness()
	versionID := uuid.New()
	definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t,
		dsl.StageDef{Type: "receive_task", NodeID: "Receive_Bid", Extras: map[string]string{"message": "bid-received"}},
		dsl.StageDef{Type: "prep", NodeID: "Prepare", BoundaryMessage: &dsl.MessagePath{MessageName: "withdraw", BoundaryID: "Boundary_Withdraw", Interrupting: true, Terminates: true}},
	))
	inst := &domain.Instance{
		ID: uuid.New(), TenantID: uuid.New(), WorkflowVersionID: versionID, Status: status,
		RecordVersion: 1, TemporalWorkflowID: "tenant:subject:key",
	}
	instances.byID[inst.ID] = inst
	temporal.queryFunc = func(_ context.Context, _, queryType string, out any) error {
		assert.Equal(t, port.QueryWaitingExternal, queryType)
		raw, err := json.Marshal(waiting)
		require.NoError(t, err)
		return json.Unmarshal(raw, out)
	}
	return svc, inst, temporal
}

// TestAnswerOutsideWait_SignalsTheWaitingInstance pins the success path: the
// key is an outside wait of the plan and the instance waits in it, so the
// answer's variables are signalled to the workflow.
func TestAnswerOutsideWait_SignalsTheWaitingInstance(t *testing.T) {
	svc, inst, temporal := outsideAnswerHarness(t, domain.InstanceStatusRunning, outsideKey)

	err := svc.AnswerOutsideWait(context.Background(), inst.TenantID, inst.ID, uuid.New(), outsideKey, map[string]any{"bid": "42"})
	require.NoError(t, err)

	require.Len(t, temporal.signals, 1)
	assert.Equal(t, port.SignalInstanceExternalAnswer, temporal.signals[0].SignalName)
	assert.Equal(t, inst.TemporalWorkflowID, temporal.signals[0].TemporalWorkflowID)
	b, err := json.Marshal(temporal.signals[0].Payload)
	require.NoError(t, err)
	assert.JSONEq(t, `{"Key":"finance/Receive_Bid","Variables":{"bid":"42"}}`, string(b))
}

func TestAnswerOutsideWait_AMessageBoundaryIsAddressedByHostAndBoundary(t *testing.T) {
	key := dsl.BoundaryWaitKey("finance/Prepare", "Boundary_Withdraw")
	svc, inst, temporal := outsideAnswerHarness(t, domain.InstanceStatusRunning, key)

	require.NoError(t, svc.AnswerOutsideWait(context.Background(), inst.TenantID, inst.ID, uuid.New(), key, nil))
	require.Len(t, temporal.signals, 1)
}

func TestAnswerOutsideWait_Refusals(t *testing.T) {
	for _, tc := range []struct {
		name    string
		status  domain.InstanceStatus
		waiting []string
		key     string
		want    error
	}{
		{"a key that is no outside wait of the plan", domain.InstanceStatusRunning, []string{"finance/Prepare"}, "finance/Prepare", port.ErrWaitNotFound},
		{"an outside wait the instance is not in", domain.InstanceStatusRunning, nil, outsideKey, port.ErrNotWaiting},
		{"a finished instance", domain.InstanceStatusCompleted, []string{outsideKey}, outsideKey, port.ErrInstanceAlreadyTerminal},
		{"a paused instance", domain.InstanceStatusPaused, []string{outsideKey}, outsideKey, port.ErrInvalidInstanceState},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc, inst, temporal := outsideAnswerHarness(t, tc.status, tc.waiting...)

			err := svc.AnswerOutsideWait(context.Background(), inst.TenantID, inst.ID, uuid.New(), tc.key, nil)

			assert.ErrorIs(t, err, tc.want)
			assert.Empty(t, temporal.signals, "a refused answer must not be signalled")
		})
	}
}

func TestAnswerOutsideWait_UnknownInstance(t *testing.T) {
	svc, _, _ := outsideAnswerHarness(t, domain.InstanceStatusRunning, outsideKey)
	err := svc.AnswerOutsideWait(context.Background(), uuid.New(), uuid.New(), uuid.New(), outsideKey, nil)
	assert.ErrorIs(t, err, port.ErrInstanceNotFound)
}
