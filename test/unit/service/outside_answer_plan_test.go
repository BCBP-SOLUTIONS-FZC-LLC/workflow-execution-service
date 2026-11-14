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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// csbOutsideSetup has an instance of the given status whose plan is planJSON.
// The workflow reports the waiting keys it is given, and the query counter
// shows whether the workflow was asked at all.
func csbOutsideSetup(t *testing.T, planJSON string, status domain.InstanceStatus, waiting ...string) (*service.InstanceService, *domain.Instance, *fakeTemporalClient, *int) {
	t.Helper()
	svc, instances, _, _, _, temporal, definitions, _ := newInstanceServiceHarness()
	versionID := uuid.New()
	definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, planJSON)
	inst := &domain.Instance{
		ID: uuid.New(), TenantID: uuid.New(), WorkflowVersionID: versionID, Status: status,
		RecordVersion: 1, TemporalWorkflowID: "tenant:subject:key",
	}
	instances.byID[inst.ID] = inst
	queries := 0
	temporal.queryFunc = func(_ context.Context, workflowID, queryType string, out any) error {
		queries++
		assert.Equal(t, inst.TemporalWorkflowID, workflowID)
		assert.Equal(t, port.QueryWaitingExternal, queryType)
		raw, err := json.Marshal(waiting)
		require.NoError(t, err)
		return json.Unmarshal(raw, out)
	}
	return svc, inst, temporal, &queries
}

func csbSendAndBoundaryPlan(t *testing.T, sentMessage string) string {
	t.Helper()
	stages := []dsl.StageDef{
		{Type: "prep", NodeID: "Prepare", BoundaryMessage: &dsl.MessagePath{MessageName: "withdraw", BoundaryID: "Boundary_Withdraw", Interrupting: true, Terminates: true}},
	}
	if sentMessage != "" {
		stages = append(stages, dsl.StageDef{Type: "send_task", NodeID: "Send_Withdraw", Extras: map[string]string{"message": sentMessage}})
	}
	return compiledPlanJSON(t, stages...)
}

func TestAnswerOutsideWait_AMessageBoundaryTheSamePlanSendsIsNotAnOutsideWait(t *testing.T) {
	boundaryKey := dsl.BoundaryWaitKey("finance/Prepare", "Boundary_Withdraw")

	t.Run("a send task of the plan sends the boundary's message", func(t *testing.T) {
		svc, inst, temporal, queries := csbOutsideSetup(t, csbSendAndBoundaryPlan(t, "withdraw"), domain.InstanceStatusRunning, boundaryKey)

		err := svc.AnswerOutsideWait(context.Background(), inst.TenantID, inst.ID, adminScope(), boundaryKey, nil)

		require.ErrorIs(t, err, port.ErrWaitNotFound)
		assert.Empty(t, temporal.signals, "the refused answer is not signalled")
		assert.Zero(t, *queries, "the workflow is not asked before the plan refuses the key")
	})

	t.Run("a send task of another message leaves the boundary answerable", func(t *testing.T) {
		svc, inst, temporal, _ := csbOutsideSetup(t, csbSendAndBoundaryPlan(t, "something-else"), domain.InstanceStatusRunning, boundaryKey)

		require.NoError(t, svc.AnswerOutsideWait(context.Background(), inst.TenantID, inst.ID, adminScope(), boundaryKey, nil))
		require.Len(t, temporal.signals, 1)
	})
}

// csbCalledPlanJSON is a plan whose only message boundary sits on a call to
// another plan: the wait exists only once the call is expanded.
func csbCalledPlanJSON(t *testing.T, schemaVersion int) string {
	t.Helper()
	collab := dsl.CompiledCollaboration{
		MainPlan:      "main",
		SchemaVersion: schemaVersion,
		Plans: []*dsl.CompiledPlan{
			{
				Name:        "main",
				TaskQueue:   "wf-queue-default",
				Departments: []dsl.DepartmentDef{{ID: "finance", Label: "Finance", Stages: []dsl.StageDef{assignedStage("review")}}},
				Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{CallPlan: &dsl.CallPlanStep{
					NodeID: "CA_1", Name: "CA_1", Plan: "lib",
					MessagePaths: []dsl.MessagePath{{MessageName: "cancel", BoundaryID: "Boundary_Cancel", Interrupting: true, Terminates: true}},
				}}}},
			},
			{
				Name:        "lib",
				Departments: []dsl.DepartmentDef{{ID: "ops", Label: "Ops", Stages: []dsl.StageDef{assignedStage("check")}}},
				Execution:   dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"ops"}}}},
			},
		},
	}
	b, err := json.Marshal(collab)
	require.NoError(t, err)
	return string(b)
}

func TestAnswerOutsideWait_AnOlderPlanSchemaIsReadWithoutExpandingItsCalls(t *testing.T) {
	var current dsl.CompiledCollaboration
	require.NoError(t, json.Unmarshal([]byte(csbCalledPlanJSON(t, dsl.CurrentSchemaVersion)), &current))
	expanded, err := dsl.ExpandCalls(&current, domain.MaxPlanStages)
	require.NoError(t, err)
	waits := dsl.ExternalWaits(expanded)
	require.Len(t, waits, 1, "the call's boundary is an outside wait once the plan is expanded")
	callKey := waits[0].Key

	t.Run("the current schema expands the call, so its boundary can be answered", func(t *testing.T) {
		svc, inst, temporal, _ := csbOutsideSetup(t, csbCalledPlanJSON(t, dsl.CurrentSchemaVersion), domain.InstanceStatusRunning, callKey)

		require.NoError(t, svc.AnswerOutsideWait(context.Background(), inst.TenantID, inst.ID, adminScope(), callKey, nil))
		require.Len(t, temporal.signals, 1)
	})

	t.Run("an older schema is not expanded, so the same key is not an outside wait", func(t *testing.T) {
		svc, inst, temporal, queries := csbOutsideSetup(t, csbCalledPlanJSON(t, dsl.CurrentSchemaVersion-1), domain.InstanceStatusRunning, callKey)

		err := svc.AnswerOutsideWait(context.Background(), inst.TenantID, inst.ID, adminScope(), callKey, nil)

		require.ErrorIs(t, err, port.ErrWaitNotFound)
		assert.Empty(t, temporal.signals)
		assert.Zero(t, *queries)
	})
}

func TestAnswerOutsideWait_AStoredPlanThatIsNotJSONIsAnInternalFailure(t *testing.T) {
	svc, inst, temporal, queries := csbOutsideSetup(t, "{not json", domain.InstanceStatusRunning, outsideKey)

	err := svc.AnswerOutsideWait(context.Background(), inst.TenantID, inst.ID, adminScope(), outsideKey, nil)

	require.Error(t, err)
	assert.ErrorContains(t, err, "unmarshal compiled plan")
	for _, refusal := range []error{port.ErrWaitNotFound, port.ErrNotWaiting, port.ErrVersionInvalid, port.ErrInstanceNotFound} {
		assert.NotErrorIs(t, err, refusal, "an unreadable stored plan is not a refusal the caller can fix")
	}
	assert.Empty(t, temporal.signals)
	assert.Zero(t, *queries)
}

func TestAnswerOutsideWait_ADegradedInstanceCanBeAnswered(t *testing.T) {
	svc, inst, temporal, _ := csbOutsideSetup(t, compiledPlanJSON(t,
		dsl.StageDef{Type: "receive_task", NodeID: "Receive_Bid", Extras: map[string]string{"message": "bid-received"}}),
		domain.InstanceStatusDegraded, outsideKey)

	err := svc.AnswerOutsideWait(context.Background(), inst.TenantID, inst.ID, adminScope(), outsideKey, map[string]any{"bid": "42"})

	require.NoError(t, err)
	require.Len(t, temporal.signals, 1)
	assert.Equal(t, port.SignalInstanceExternalAnswer, temporal.signals[0].SignalName)
	assert.Equal(t, inst.TemporalWorkflowID, temporal.signals[0].TemporalWorkflowID)
	assert.Equal(t, inst.ID, temporal.signals[0].InstanceID)
	b, err := json.Marshal(temporal.signals[0].Payload)
	require.NoError(t, err)
	assert.JSONEq(t, `{"Key":"finance/Receive_Bid","Variables":{"bid":"42"}}`, string(b))
}
