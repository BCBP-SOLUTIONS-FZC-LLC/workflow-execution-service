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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

type ocmTaskRepo struct {
	port.TaskRepository
	task          *domain.Task
	nodeLookups   int
	tenantsSeen   []uuid.UUID
	instancesSeen []uuid.UUID
}

func (r *ocmTaskRepo) GetByInstanceAndNode(_ context.Context, tenantID, instanceID uuid.UUID, nodeKey string) (*domain.Task, error) {
	r.nodeLookups++
	r.tenantsSeen = append(r.tenantsSeen, tenantID)
	r.instancesSeen = append(r.instancesSeen, instanceID)
	if nodeKey != r.task.NodeKey {
		return nil, domain.ErrNotFound
	}
	return r.task, nil
}

func TestOverrideNodeAssignee_StaleRecordVersionThroughHTTP_CountsOneTaskConflictAndReadsTheTaskOnce(t *testing.T) {
	obstest.Init(t)
	const name = "workflow_execution_service_record_version_conflict_total"
	repo := &ocmTaskRepo{task: &domain.Task{
		ID: testTaskID, TenantID: testTenantID, WorkflowInstanceID: testInstID, NodeKey: "review_finance",
		Status: domain.TaskStatusReady, RecordVersion: 9,
	}}
	svc := &service.TaskService{Tasks: repo}
	router := newRouter(handler.New(handler.Services{Tasks: svc, Eligibility: &fakeEligibilityChecker{}}))
	before := obstest.Sum(t, name, map[string]string{"table": "workflow_task"})
	beforeInstance := obstest.Sum(t, name, map[string]string{"table": "workflow_instance"})

	w := do(router, asAdmin(req(http.MethodPost, overridePath(testInstID, "review_finance"), map[string]any{
		"new_user_id": uuid.New(), "record_version": 4,
	})))

	require.Equal(t, http.StatusConflict, w.Code)
	var body struct {
		Code string `json:"code"`
	}
	decodeJSON(t, w.Body, &body)
	assert.Equal(t, "RECORD_VERSION_CONFLICT", body.Code)
	assert.Equal(t, before+1, obstest.Sum(t, name, map[string]string{"table": "workflow_task"}), "the stale version is counted once, by the service")
	assert.Equal(t, beforeInstance, obstest.Sum(t, name, map[string]string{"table": "workflow_instance"}))
	assert.Equal(t, 1, repo.nodeLookups, "the task is read once, by the service")
	assert.Equal(t, []uuid.UUID{testTenantID}, repo.tenantsSeen)
	assert.Equal(t, []uuid.UUID{testInstID}, repo.instancesSeen)
}
