package handler_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

type csfReassignTasks struct {
	port.TaskRepository
	task       *domain.Task
	gotTenants []uuid.UUID
	gotTaskIDs []uuid.UUID
}

func (r *csfReassignTasks) GetByID(_ context.Context, tenantID, id uuid.UUID) (*domain.Task, error) {
	r.gotTenants = append(r.gotTenants, tenantID)
	r.gotTaskIDs = append(r.gotTaskIDs, id)
	if id != r.task.ID {
		return nil, domain.ErrNotFound
	}
	return r.task, nil
}

type csfReassignTemporal struct {
	port.TemporalClient
	signals []string
	updates []string
	gotIDs  []uuid.UUID
}

func (c *csfReassignTemporal) SignalWorkflow(_ context.Context, _ string, instanceID uuid.UUID, signalName string, _ any) error {
	c.gotIDs = append(c.gotIDs, instanceID)
	c.signals = append(c.signals, signalName)
	return nil
}

func (c *csfReassignTemporal) UpdateWorkflow(_ context.Context, _ string, instanceID uuid.UUID, updateName string, _ any) error {
	c.gotIDs = append(c.gotIDs, instanceID)
	c.updates = append(c.updates, updateName)
	return nil
}

func TestReassignTask_AStaleVersion_IsAConflictAndSignalsNothing(t *testing.T) {
	tasks := &csfReassignTasks{task: &domain.Task{
		ID: testTaskID, TenantID: testTenantID, WorkflowInstanceID: testInstID,
		Status: domain.TaskStatusReady, RecordVersion: 3, AssigneeMode: "single",
	}}
	temporal := &csfReassignTemporal{}
	svc := &service.TaskService{Tasks: tasks, Temporal: temporal}
	router := newRouter(handler.New(handler.Services{Tasks: svc}))

	w := do(router, asAdmin(req(http.MethodPost, "/api/v1/tasks/"+testTaskID.String()+"/reassign", map[string]any{
		"new_user_id": uuid.New(), "record_version": 5,
	})))

	require.Equal(t, http.StatusConflict, w.Code, w.Body.String())
	var body problemBody
	decodeJSON(t, w.Body, &body)
	assert.Equal(t, "RECORD_VERSION_CONFLICT", body.Code)
	assert.Equal(t, []uuid.UUID{testTenantID}, tasks.gotTenants)
	assert.Equal(t, []uuid.UUID{testTaskID}, tasks.gotTaskIDs)
	assert.Empty(t, temporal.signals, "a refused reassignment must not signal the workflow")
	assert.Empty(t, temporal.updates, "a refused reassignment must not update the workflow")
	assert.Empty(t, temporal.gotIDs, "no instance was addressed")
	assert.EqualValues(t, 3, tasks.task.RecordVersion)
}
