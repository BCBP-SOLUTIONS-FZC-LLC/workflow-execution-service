package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

const outsideKey = "finance/Receive_Bid"

func adminScope() port.ReadScope { return port.ReadScope{CallerUserID: uuid.New(), IsAdmin: true} }

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

	err := svc.AnswerOutsideWait(context.Background(), inst.TenantID, inst.ID, adminScope(), outsideKey, map[string]any{"bid": "42"})
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

	require.NoError(t, svc.AnswerOutsideWait(context.Background(), inst.TenantID, inst.ID, adminScope(), key, nil))
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

			err := svc.AnswerOutsideWait(context.Background(), inst.TenantID, inst.ID, adminScope(), tc.key, nil)

			assert.ErrorIs(t, err, tc.want)
			assert.Empty(t, temporal.signals, "a refused answer must not be signalled")
		})
	}
}

func TestAnswerOutsideWait_UnknownInstance(t *testing.T) {
	svc, _, _ := outsideAnswerHarness(t, domain.InstanceStatusRunning, outsideKey)
	err := svc.AnswerOutsideWait(context.Background(), uuid.New(), uuid.New(), adminScope(), outsideKey, nil)
	assert.ErrorIs(t, err, port.ErrInstanceNotFound)
}

// TestAnswerOutsideWait_AnAssigneeOfTheInstancesTasksMayAnswer pins who may
// answer besides an admin: a user with an active assignment on a task of the
// instance. Another user, even in the task's department, may not.
func TestAnswerOutsideWait_AnAssigneeOfTheInstancesTasksMayAnswer(t *testing.T) {
	svc, instances, tasks, assignments, _, temporal, definitions, _ := newInstanceServiceHarness()
	versionID := uuid.New()
	definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t,
		dsl.StageDef{Type: "receive_task", NodeID: "Receive_Bid", Extras: map[string]string{"message": "bid-received"}}))
	inst := &domain.Instance{ID: uuid.New(), TenantID: uuid.New(), WorkflowVersionID: versionID, Status: domain.InstanceStatusRunning, TemporalWorkflowID: "t:s:k"}
	instances.byID[inst.ID] = inst
	deptID, assignee := uuid.New(), uuid.New()
	task := &domain.Task{ID: uuid.New(), TenantID: inst.TenantID, WorkflowInstanceID: inst.ID, DepartmentID: deptID, Status: domain.TaskStatusReady}
	tasks.byID[task.ID] = task
	assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: inst.TenantID, TaskID: task.ID, UserID: assignee, IsActive: true}
	temporal.queryFunc = func(_ context.Context, _, _ string, out any) error {
		return json.Unmarshal([]byte(`["`+outsideKey+`"]`), out)
	}

	err := svc.AnswerOutsideWait(context.Background(), inst.TenantID, inst.ID, port.ReadScope{CallerUserID: assignee}, outsideKey, nil)
	require.NoError(t, err)
	require.Len(t, temporal.signals, 1)

	other := port.ReadScope{CallerUserID: uuid.New(), Departments: []port.DepartmentRole{{DepartmentID: deptID}}}
	err = svc.AnswerOutsideWait(context.Background(), inst.TenantID, inst.ID, other, outsideKey, nil)
	assert.ErrorIs(t, err, port.ErrNotAuthorizedForRead)
	assert.Len(t, temporal.signals, 1, "an unauthorised answer must not be signalled")
}

// An answer reads the instance's plan through the compiled-plan cache: the
// body of a version never changes, so a running instance does not need the
// Definition Service's live answer that a start needs.
func TestAnswerOutsideWait_ReadsThePlanThroughTheCache(t *testing.T) {
	cachedHarness := func(t *testing.T, entry func(raw string) string, getErr error) (*service.InstanceService, *domain.Instance, *fakeCacheStore) {
		t.Helper()
		svc, inst, _ := outsideAnswerHarness(t, domain.InstanceStatusRunning, outsideKey)
		plan, err := svc.Definitions.GetCompiledWorkflow(context.Background(), inst.TenantID, inst.WorkflowVersionID)
		require.NoError(t, err)
		raw, err := json.Marshal(plan)
		require.NoError(t, err)
		cache := newFakeCacheStore()
		cache.values["compiled_plan:"+inst.TenantID.String()+":"+inst.WorkflowVersionID.String()] = entry(string(raw))
		cache.getErr = getErr
		svc.Cache = cache
		return svc, inst, cache
	}

	t.Run("a cached plan answers without the Definition Service", func(t *testing.T) {
		svc, inst, _ := cachedHarness(t, func(raw string) string { return raw }, nil)
		svc.Definitions = &fakeDefinitionClient{err: errors.New("the Definition Service must not be read on a cache hit")}
		require.NoError(t, svc.AnswerOutsideWait(context.Background(), inst.TenantID, inst.ID, adminScope(), outsideKey, nil))
	})
	t.Run("an unreadable cache falls back to the Definition Service", func(t *testing.T) {
		svc, inst, _ := cachedHarness(t, func(raw string) string { return raw }, errors.New("valkey unavailable"))
		log := &fakeLogger{}
		svc.Log = log
		require.NoError(t, svc.AnswerOutsideWait(context.Background(), inst.TenantID, inst.ID, adminScope(), outsideKey, nil))
		assert.NotEmpty(t, log.warnCalls)
	})
	t.Run("a corrupt cache entry falls back to the Definition Service", func(t *testing.T) {
		svc, inst, _ := cachedHarness(t, func(string) string { return "{not json" }, nil)
		log := &fakeLogger{}
		svc.Log = log
		require.NoError(t, svc.AnswerOutsideWait(context.Background(), inst.TenantID, inst.ID, adminScope(), outsideKey, nil))
		assert.NotEmpty(t, log.warnCalls)
	})
}
