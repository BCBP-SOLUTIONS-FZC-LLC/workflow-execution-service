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
	tasks := newFakeTaskRepo()
	if task != nil {
		tasks.byID[task.ID] = task
	}
	instances := newFakeInstanceRepo()
	if inst != nil {
		instances.byID[inst.ID] = inst
	}
	return &service.ConnectorTaskService{
		Instances: instances,
		Tasks:     tasks,
		Temporal:  temporal,
		Cache:     cache,
	}
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
