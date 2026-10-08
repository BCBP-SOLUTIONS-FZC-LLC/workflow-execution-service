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
)

func connectorTask(connectorType string, status domain.TaskStatus, extras string) *domain.Task {
	return &domain.Task{
		ID:                 uuid.New(),
		TenantID:           uuid.New(),
		WorkflowInstanceID: uuid.New(),
		NodeKey:            "dept-1/nodeA",
		Status:             status,
		RecordVersion:      3,
		ConnectorType:      &connectorType,
		ExtrasJSON:         json.RawMessage(extras),
	}
}

func newConnectorTaskService(task *domain.Task, inst *domain.Instance, temporal *fakeTemporalClient, cache *fakeCacheStore) *service.ConnectorTaskService {
	return newConnectorTaskServiceWith(task, inst, temporal, cache, newFakeAssignmentRepo())
}

func newConnectorTaskServiceWith(
	task *domain.Task, inst *domain.Instance, temporal *fakeTemporalClient,
	cache *fakeCacheStore, assignments *fakeAssignmentRepo,
) *service.ConnectorTaskService {
	tasks := newFakeTaskRepo()
	if task != nil {
		tasks.byID[task.ID] = task
	}
	instances := newFakeInstanceRepo()
	if inst != nil {
		instances.byID[inst.ID] = inst
	}
	return &service.ConnectorTaskService{
		Instances:   instances,
		Tasks:       tasks,
		Assignments: assignments,
		Temporal:    temporal,
		Cache:       cache,
		Principals:  fakePrincipals{principal: testConnectorActorID},
	}
}

var testConnectorActorID = uuid.MustParse("55555555-5555-5555-5555-555555555555")

func TestConnectorTaskService_Complete_AssignsTheActingPrincipal(t *testing.T) {
	t.Parallel()

	extras := `{"resolved_inputs":{},"output_mapping":[]}`
	task := connectorTask("storage", domain.TaskStatusReady, extras)
	inst := &domain.Instance{ID: task.WorkflowInstanceID, TemporalWorkflowID: "tenant:subj:biz"}
	temporal := &fakeTemporalClient{}
	assignments := newFakeAssignmentRepo()
	svc := newConnectorTaskServiceWith(task, inst, temporal, newFakeCacheStore(), assignments)

	require.NoError(t, svc.Complete(context.Background(), task.TenantID, task.ID, map[string]any{}))

	active, err := assignments.ListActiveByTask(context.Background(), task.TenantID, task.ID)
	require.NoError(t, err)
	require.Len(t, active, 1, "the principal holds a real assignment, so completion is not a special case")
	assert.Equal(t, testConnectorActorID, active[0].UserID)

	require.Len(t, temporal.signals, 1)
	payload, err := json.Marshal(temporal.signals[0].Payload)
	require.NoError(t, err)
	var wire map[string]any
	require.NoError(t, json.Unmarshal(payload, &wire))
	assert.Equal(t, testConnectorActorID.String(), wire["UserID"],
		"the signal names the actor in the field the completion activity reads")
}

// Fail creates the same row Complete does: the failed task's event reads its
// assignees off the task, so without it the actor is retained nowhere.
func TestConnectorTaskService_Fail_AssignsTheActingPrincipal(t *testing.T) {
	t.Parallel()

	task := connectorTask("storage", domain.TaskStatusReady, `{}`)
	inst := &domain.Instance{ID: task.WorkflowInstanceID, TemporalWorkflowID: "tenant:subj:biz"}
	assignments := newFakeAssignmentRepo()
	svc := newConnectorTaskServiceWith(task, inst, &fakeTemporalClient{}, newFakeCacheStore(), assignments)

	require.NoError(t, svc.Fail(context.Background(), task.TenantID, task.ID, "upstream_error"))

	active, err := assignments.ListActiveByTask(context.Background(), task.TenantID, task.ID)
	require.NoError(t, err)
	require.Len(t, active, 1)
	assert.Equal(t, testConnectorActorID, active[0].UserID)
}

// A retried callback must not leave a second assignment behind: the row's
// identifier derives from the task and user. Each pass gets a fresh cache, so
// the Valkey dedup guard cannot be what swallows the second attempt.
func TestConnectorTaskService_Complete_RetriedCallbackReusesTheSameRow(t *testing.T) {
	t.Parallel()

	extras := `{"resolved_inputs":{},"output_mapping":[]}`
	task := connectorTask("storage", domain.TaskStatusReady, extras)
	inst := &domain.Instance{ID: task.WorkflowInstanceID, TemporalWorkflowID: "tenant:subj:biz"}
	assignments := newFakeAssignmentRepo()

	for range 2 {
		svc := newConnectorTaskServiceWith(task, inst, &fakeTemporalClient{}, newFakeCacheStore(), assignments)
		require.NoError(t, svc.Complete(context.Background(), task.TenantID, task.ID, map[string]any{}))
	}

	active, err := assignments.ListActiveByTask(context.Background(), task.TenantID, task.ID)
	require.NoError(t, err)
	assert.Len(t, active, 1)
}

// No row means the completion activity has nothing to resolve the actor
// against, so the callback fails and its dedup claim is released for the retry.
func TestConnectorTaskService_Complete_AssignmentFailureReleasesReservation(t *testing.T) {
	t.Parallel()

	extras := `{"resolved_inputs":{},"output_mapping":[]}`
	task := connectorTask("storage", domain.TaskStatusReady, extras)
	inst := &domain.Instance{ID: task.WorkflowInstanceID, TemporalWorkflowID: "tenant:subj:biz"}
	assignments := newFakeAssignmentRepo()
	assignments.createErr = errors.New("transient database error")
	temporal := &fakeTemporalClient{}
	cache := newFakeCacheStore()
	svc := newConnectorTaskServiceWith(task, inst, temporal, cache, assignments)

	err := svc.Complete(context.Background(), task.TenantID, task.ID, map[string]any{})
	require.Error(t, err)
	assert.Empty(t, temporal.signals, "an unattributable completion must not be signalled")
	assert.Empty(t, cache.values, "the dedup claim must not outlive the failed attempt")

	failCache := newFakeCacheStore()
	failTemporal := &fakeTemporalClient{}
	failSvc := newConnectorTaskServiceWith(task, inst, failTemporal, failCache, assignments)
	require.Error(t, failSvc.Fail(context.Background(), task.TenantID, task.ID, "upstream_error"))
	assert.Empty(t, failTemporal.signals, "an unattributable failure must not be signalled either")
	assert.Empty(t, failCache.values)
}

func TestConnectorTaskService_Complete_HappyPath(t *testing.T) {
	t.Parallel()

	extras := `{"resolved_inputs":{},"output_mapping":[{"source":"resultUrl","target":"docUrl"}]}`
	task := connectorTask("storage", domain.TaskStatusReady, extras)
	inst := &domain.Instance{ID: task.WorkflowInstanceID, TemporalWorkflowID: "tenant:biz"}
	temporal := &fakeTemporalClient{}
	svc := newConnectorTaskService(task, inst, temporal, newFakeCacheStore())

	err := svc.Complete(context.Background(), task.TenantID, task.ID, map[string]any{"resultUrl": "s3://x", "ignored": "y"})
	require.NoError(t, err)

	require.Len(t, temporal.signals, 1)
	assert.Equal(t, "stage-transition", temporal.signals[0].SignalName)
	assert.Equal(t, "tenant:biz", temporal.signals[0].TemporalWorkflowID)

	b, err := json.Marshal(temporal.signals[0].Payload)
	require.NoError(t, err)
	var wire struct {
		ResultJSON    string
		RecordVersion int64
	}
	require.NoError(t, json.Unmarshal(b, &wire))
	assert.Equal(t, int64(3), wire.RecordVersion)

	var result map[string]any
	require.NoError(t, json.Unmarshal([]byte(wire.ResultJSON), &result))
	assert.Equal(t, "s3://x", result["docUrl"])
	_, hasIgnored := result["ignored"]
	assert.False(t, hasIgnored)
	_, hasSource := result["resultUrl"]
	assert.False(t, hasSource)
}

// An author writes a connector output as `source="=contentRef"`; both spellings
// name the key of the connector's result (workflow-models IOVar.OutputKey).
func TestConnectorTaskService_Complete_OutputSourceMayCarryTheEqualsSign(t *testing.T) {
	t.Parallel()

	extras := `{"resolved_inputs":{},"output_mapping":[{"source":"=contentRef","target":"packet_ref"}]}`
	task := connectorTask("storage", domain.TaskStatusReady, extras)
	inst := &domain.Instance{ID: task.WorkflowInstanceID, TemporalWorkflowID: "tenant:biz"}
	temporal := &fakeTemporalClient{}
	svc := newConnectorTaskService(task, inst, temporal, newFakeCacheStore())

	require.NoError(t, svc.Complete(context.Background(), task.TenantID, task.ID, map[string]any{"contentRef": "doc-1"}))

	require.Len(t, temporal.signals, 1)
	b, err := json.Marshal(temporal.signals[0].Payload)
	require.NoError(t, err)
	var wire struct{ ResultJSON string }
	require.NoError(t, json.Unmarshal(b, &wire))
	var result map[string]any
	require.NoError(t, json.Unmarshal([]byte(wire.ResultJSON), &result))
	assert.Equal(t, map[string]any{"packet_ref": "doc-1"}, result)
}

func TestConnectorTaskService_Complete_NoOutputMapping_PassesThrough(t *testing.T) {
	t.Parallel()

	task := connectorTask("rest-call", domain.TaskStatusReady, "")
	inst := &domain.Instance{ID: task.WorkflowInstanceID, TemporalWorkflowID: "tenant:biz"}
	temporal := &fakeTemporalClient{}
	svc := newConnectorTaskService(task, inst, temporal, newFakeCacheStore())

	err := svc.Complete(context.Background(), task.TenantID, task.ID, map[string]any{"status": 200})
	require.NoError(t, err)

	b, err := json.Marshal(temporal.signals[0].Payload)
	require.NoError(t, err)
	var wire struct{ ResultJSON string }
	require.NoError(t, json.Unmarshal(b, &wire))
	var result map[string]any
	require.NoError(t, json.Unmarshal([]byte(wire.ResultJSON), &result))
	assert.EqualValues(t, 200, result["status"])
}

func TestConnectorTaskService_Complete_TaskNotFound(t *testing.T) {
	t.Parallel()

	svc := newConnectorTaskService(nil, nil, &fakeTemporalClient{}, newFakeCacheStore())
	err := svc.Complete(context.Background(), uuid.New(), uuid.New(), nil)
	require.Error(t, err)
	assert.True(t, errors.Is(err, port.ErrTaskNotFound))
}

func TestConnectorTaskService_Complete_NotConnectorTyped(t *testing.T) {
	t.Parallel()

	task := &domain.Task{ID: uuid.New(), TenantID: uuid.New(), Status: domain.TaskStatusReady}
	svc := newConnectorTaskService(task, nil, &fakeTemporalClient{}, newFakeCacheStore())

	err := svc.Complete(context.Background(), task.TenantID, task.ID, nil)
	require.Error(t, err)
	assert.True(t, errors.Is(err, port.ErrTaskNotConnectorTyped))
}

func TestConnectorTaskService_Complete_AlreadyTerminal_IsNoOp(t *testing.T) {
	t.Parallel()

	task := connectorTask("storage", domain.TaskStatusCompleted, "")
	temporal := &fakeTemporalClient{}
	svc := newConnectorTaskService(task, &domain.Instance{ID: task.WorkflowInstanceID}, temporal, newFakeCacheStore())

	err := svc.Complete(context.Background(), task.TenantID, task.ID, map[string]any{"x": 1})
	require.NoError(t, err)
	assert.Empty(t, temporal.signals)
}

func TestConnectorTaskService_Complete_DedupPreventsSecondSignal(t *testing.T) {
	t.Parallel()

	task := connectorTask("storage", domain.TaskStatusReady, "")
	inst := &domain.Instance{ID: task.WorkflowInstanceID, TemporalWorkflowID: "tenant:biz"}
	temporal := &fakeTemporalClient{}
	cache := newFakeCacheStore()
	svc := newConnectorTaskService(task, inst, temporal, cache)

	require.NoError(t, svc.Complete(context.Background(), task.TenantID, task.ID, map[string]any{"x": 1}))
	// Simulate a Stream-redelivery-driven retry arriving before the DB write
	// that would flip task.Status to COMPLETED has committed — task is still
	// READY in this fake, so only the SETNX dedup guard can prevent a second
	// signal.
	require.NoError(t, svc.Complete(context.Background(), task.TenantID, task.ID, map[string]any{"x": 1}))

	assert.Len(t, temporal.signals, 1)
}

func TestConnectorTaskService_Complete_SignalFails_ReleasesReservation(t *testing.T) {
	t.Parallel()

	task := connectorTask("storage", domain.TaskStatusReady, "")
	inst := &domain.Instance{ID: task.WorkflowInstanceID, TemporalWorkflowID: "tenant:biz"}
	temporal := &fakeTemporalClient{signalFunc: func(context.Context, string, uuid.UUID, string, any) error {
		return errors.New("transient temporal error")
	}}
	cache := newFakeCacheStore()
	svc := newConnectorTaskService(task, inst, temporal, cache)

	err := svc.Complete(context.Background(), task.TenantID, task.ID, map[string]any{"x": 1})
	require.Error(t, err)
	assert.Empty(t, cache.values, "the dedup key must be released after a failed signal, not left standing for the full TTL")

	// A retry after the transient failure clears must actually re-signal, not
	// see a stale claim and silently no-op.
	temporal.signalFunc = nil
	require.NoError(t, svc.Complete(context.Background(), task.TenantID, task.ID, map[string]any{"x": 1}))
	assert.Len(t, temporal.signals, 2, "the retry must re-attempt the signal, not be swallowed by a stale dedup claim")
}

func TestConnectorTaskService_Fail_SignalFails_ReleasesReservation(t *testing.T) {
	t.Parallel()

	task := connectorTask("send-email", domain.TaskStatusReady, "")
	inst := &domain.Instance{ID: task.WorkflowInstanceID, TemporalWorkflowID: "tenant:biz"}
	temporal := &fakeTemporalClient{signalFunc: func(context.Context, string, uuid.UUID, string, any) error {
		return errors.New("transient temporal error")
	}}
	cache := newFakeCacheStore()
	svc := newConnectorTaskService(task, inst, temporal, cache)

	err := svc.Fail(context.Background(), task.TenantID, task.ID, "upstream_error")
	require.Error(t, err)
	assert.Empty(t, cache.values, "the dedup key must be released after a failed signal, not left standing for the full TTL")

	temporal.signalFunc = nil
	require.NoError(t, svc.Fail(context.Background(), task.TenantID, task.ID, "upstream_error"))
	assert.Len(t, temporal.signals, 2, "the retry must re-attempt the signal, not be swallowed by a stale dedup claim")
}

func TestConnectorTaskService_Fail_HappyPath(t *testing.T) {
	t.Parallel()

	task := connectorTask("send-email", domain.TaskStatusReady, "")
	inst := &domain.Instance{ID: task.WorkflowInstanceID, TemporalWorkflowID: "tenant:biz"}
	temporal := &fakeTemporalClient{}
	svc := newConnectorTaskService(task, inst, temporal, newFakeCacheStore())

	err := svc.Fail(context.Background(), task.TenantID, task.ID, "upstream_error")
	require.NoError(t, err)

	require.Len(t, temporal.signals, 1)
	assert.Equal(t, "stage-fail", temporal.signals[0].SignalName)

	b, err := json.Marshal(temporal.signals[0].Payload)
	require.NoError(t, err)
	var wire struct {
		ConnectorType string `json:"connector_type"`
		ErrorClass    string `json:"error_class"`
	}
	require.NoError(t, json.Unmarshal(b, &wire))
	assert.Equal(t, "send-email", wire.ConnectorType)
	assert.Equal(t, "upstream_error", wire.ErrorClass)
}

func TestConnectorTaskService_Fail_AlreadyTerminal_IsNoOp(t *testing.T) {
	t.Parallel()

	task := connectorTask("send-email", domain.TaskStatusFailed, "")
	temporal := &fakeTemporalClient{}
	svc := newConnectorTaskService(task, &domain.Instance{ID: task.WorkflowInstanceID}, temporal, newFakeCacheStore())

	err := svc.Fail(context.Background(), task.TenantID, task.ID, "timeout")
	require.NoError(t, err)
	assert.Empty(t, temporal.signals)
}

func TestConnectorTaskService_NoAutomationPrincipal_ResolvesNothing(t *testing.T) {
	t.Parallel()

	task := connectorTask("storage", domain.TaskStatusReady, `{"resolved_inputs":{},"output_mapping":[]}`)
	inst := &domain.Instance{ID: task.WorkflowInstanceID, TemporalWorkflowID: "tenant:subj:biz"}
	temporal := &fakeTemporalClient{}
	assignments := newFakeAssignmentRepo()
	svc := newConnectorTaskServiceWith(task, inst, temporal, newFakeCacheStore(), assignments)
	svc.Principals = fakePrincipals{err: port.ErrNoAutomationPrincipal}

	require.ErrorIs(t, svc.Complete(context.Background(), task.TenantID, task.ID, map[string]any{}), port.ErrNoAutomationPrincipal)
	require.ErrorIs(t, svc.Fail(context.Background(), task.TenantID, task.ID, "upstream_error"), port.ErrNoAutomationPrincipal)

	active, err := assignments.ListActiveByTask(context.Background(), task.TenantID, task.ID)
	require.NoError(t, err)
	assert.Empty(t, active, "no actor, so no assignment row")
	assert.Empty(t, temporal.signals, "no actor, so the workflow is not signalled")
}

func TestConnectorTaskService_PrincipalLookupFailure_IsReturned(t *testing.T) {
	t.Parallel()

	task := connectorTask("storage", domain.TaskStatusReady, `{"resolved_inputs":{},"output_mapping":[]}`)
	inst := &domain.Instance{ID: task.WorkflowInstanceID, TemporalWorkflowID: "tenant:subj:biz"}
	temporal := &fakeTemporalClient{}
	svc := newConnectorTaskServiceWith(task, inst, temporal, newFakeCacheStore(), newFakeAssignmentRepo())
	boom := errors.New("token service down")
	svc.Principals = fakePrincipals{err: boom}

	require.ErrorIs(t, svc.Complete(context.Background(), task.TenantID, task.ID, map[string]any{}), boom)
	assert.Empty(t, temporal.signals)
}

func TestConnectorTaskService_NoResolverWired_FailsClosed(t *testing.T) {
	t.Parallel()

	task := connectorTask("storage", domain.TaskStatusReady, `{"resolved_inputs":{},"output_mapping":[]}`)
	inst := &domain.Instance{ID: task.WorkflowInstanceID, TemporalWorkflowID: "tenant:subj:biz"}
	temporal := &fakeTemporalClient{}
	svc := newConnectorTaskServiceWith(task, inst, temporal, newFakeCacheStore(), newFakeAssignmentRepo())
	svc.Principals = nil

	require.ErrorIs(t, svc.Complete(context.Background(), task.TenantID, task.ID, map[string]any{}), port.ErrNoAutomationPrincipal)
	assert.Empty(t, temporal.signals)
}
