package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

var _ port.ConnectorTaskService = (*ConnectorTaskService)(nil)

const connectorSignalDedupTTL = 5 * time.Minute

type ConnectorTaskService struct {
	Instances   port.InstanceRepository
	Tasks       port.TaskRepository
	Assignments port.TaskAssignmentRepository
	Temporal    port.TemporalClient
	Cache       port.CacheStore
	Principals  port.AutomationPrincipalResolver
	Log         port.Logger
}

func (s *ConnectorTaskService) logger() port.Logger {
	if s.Log != nil {
		return s.Log
	}
	return noopLogger{}
}

type stageFailWire struct {
	DeptID        string `json:"dept_id"`
	NodeID        string `json:"node_id"`
	ConnectorType string `json:"connector_type"`
	ErrorClass    string `json:"error_class"`
	RecordVersion int64  `json:"record_version"`
	VisitCount    int64  `json:"visit_count"`
}

func (s *ConnectorTaskService) Complete(ctx context.Context, tenantID, taskID uuid.UUID, output map[string]any) error {
	task, done, err := s.loadConnectorTask(ctx, tenantID, taskID)
	if err != nil || done {
		return err
	}
	actorUserID, err := s.automationPrincipal(ctx, tenantID)
	if err != nil {
		return err
	}

	resultJSON, err := applyOutputMapping(task.ExtrasJSON, output)
	if err != nil {
		return fmt.Errorf("apply output mapping: %w", err)
	}

	inst, err := s.Instances.GetByID(ctx, tenantID, task.WorkflowInstanceID)
	if err != nil {
		return wrapInstanceErr(err)
	}
	if !s.reserveSignal(ctx, taskID) {
		return nil
	}

	if err := s.assignActor(ctx, tenantID, taskID, actorUserID); err != nil {
		s.releaseSignalReservation(ctx, taskID)
		return err
	}

	deptID, nodeID := deptAndSuffix(task.NodeKey)
	if err := s.Temporal.SignalWorkflow(ctx, inst.TemporalWorkflowID, inst.ID, "stage-transition", stageTransitionWire{
		DeptID: deptID, NodeID: nodeID, UserID: actorUserID.String(),
		ResultJSON: string(resultJSON), RecordVersion: task.RecordVersion,
		VisitCount: task.VisitCount,
	}); err != nil {
		s.releaseSignalReservation(ctx, taskID)
		return fmt.Errorf("signal stage-transition: %w", err)
	}
	return nil
}

func (s *ConnectorTaskService) Fail(ctx context.Context, tenantID, taskID uuid.UUID, errorClass string, output map[string]any) error {
	task, done, err := s.loadConnectorTask(ctx, tenantID, taskID)
	if err != nil || done {
		return err
	}
	actorUserID, err := s.automationPrincipal(ctx, tenantID)
	if err != nil {
		return err
	}

	inst, err := s.Instances.GetByID(ctx, tenantID, task.WorkflowInstanceID)
	if err != nil {
		return wrapInstanceErr(err)
	}
	if !s.reserveSignal(ctx, taskID) {
		return nil
	}

	if err := s.assignActor(ctx, tenantID, taskID, actorUserID); err != nil {
		s.releaseSignalReservation(ctx, taskID)
		return err
	}

	s.logger().Warn("connector task failed", map[string]any{
		"task_id": taskID, "tenant_id": tenantID, "connector_type": *task.ConnectorType, "error_class": errorClass, "output": output,
	})

	deptID, nodeID := deptAndSuffix(task.NodeKey)
	if err := s.Temporal.SignalWorkflow(ctx, inst.TemporalWorkflowID, inst.ID, "stage-fail", stageFailWire{
		DeptID: deptID, NodeID: nodeID, ConnectorType: *task.ConnectorType, ErrorClass: errorClass, RecordVersion: task.RecordVersion,
		VisitCount: task.VisitCount,
	}); err != nil {
		s.releaseSignalReservation(ctx, taskID)
		return fmt.Errorf("signal stage-fail: %w", err)
	}
	return nil
}

func (s *ConnectorTaskService) automationPrincipal(ctx context.Context, tenantID uuid.UUID) (uuid.UUID, error) {
	if s.Principals == nil {
		return uuid.Nil, port.ErrNoAutomationPrincipal
	}
	return s.Principals.AutomationPrincipal(ctx, tenantID)
}

const automationReasonTag = "automation"

func (s *ConnectorTaskService) assignActor(ctx context.Context, tenantID, taskID, actorUserID uuid.UUID) error {
	assignment := &domain.TaskAssignment{
		ID:       domain.DeterministicAssignmentID(taskID, actorUserID),
		TenantID: tenantID,
		TaskID:   taskID,
		UserID:   actorUserID,
		Reason:   automationReasonTag,
	}
	err := s.Assignments.Create(ctx, assignment)
	switch {
	case err == nil,
		errors.Is(err, domain.ErrAlreadyExists),
		errors.Is(err, domain.ErrDuplicateActiveAssignment):
		return nil
	default:
		return fmt.Errorf("assign automation principal: %w", err)
	}
}

func (s *ConnectorTaskService) loadConnectorTask(ctx context.Context, tenantID, taskID uuid.UUID) (*domain.Task, bool, error) {
	task, err := s.Tasks.GetByID(ctx, tenantID, taskID)
	if err != nil {
		return nil, false, wrapTaskErr(err)
	}
	if task.ConnectorType == nil || *task.ConnectorType == "" {
		return nil, false, port.ErrTaskNotConnectorTyped
	}
	if task.Status == domain.TaskStatusCompleted || task.Status == domain.TaskStatusFailed {
		return nil, true, nil
	}
	return task, false, nil
}

func (s *ConnectorTaskService) reserveSignal(ctx context.Context, taskID uuid.UUID) bool {
	if s.Cache == nil {
		return true
	}
	ok, err := s.Cache.SetNX(ctx, connectorSignalKey(taskID), "1", connectorSignalDedupTTL)
	if err != nil {
		s.logger().Warn("connector-task signal dedup check failed, proceeding fail-open", map[string]any{"task_id": taskID, "error": err.Error()})
		return true
	}
	return ok
}

func (s *ConnectorTaskService) releaseSignalReservation(ctx context.Context, taskID uuid.UUID) {
	if s.Cache == nil {
		return
	}
	if err := s.Cache.Del(ctx, connectorSignalKey(taskID)); err != nil {
		s.logger().Warn("connector-task signal dedup release failed after a failed signal", map[string]any{"task_id": taskID, "error": err.Error()})
	}
}

func connectorSignalKey(taskID uuid.UUID) string {
	return "connector-signal:" + taskID.String()
}

type connectorTaskExtras struct {
	OutputMapping []dsl.IOVar `json:"output_mapping"`
}

func applyOutputMapping(extrasJSON json.RawMessage, output map[string]any) (json.RawMessage, error) {
	var extras connectorTaskExtras
	if len(extrasJSON) == 0 {
		return marshalResult(output)
	}
	if err := json.Unmarshal(extrasJSON, &extras); err != nil || len(extras.OutputMapping) == 0 {
		return marshalResult(output)
	}

	mapped := make(map[string]any, len(extras.OutputMapping))
	for _, ref := range extras.OutputMapping {
		if v, ok := output[ref.OutputKey()]; ok {
			mapped[ref.Target] = v
		}
	}
	return marshalResult(mapped)
}

func marshalResult(v map[string]any) (json.RawMessage, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshal connector result: %w", err)
	}
	return b, nil
}
