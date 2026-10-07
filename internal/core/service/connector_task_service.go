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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

var _ port.ConnectorTaskService = (*ConnectorTaskService)(nil)

// connectorSignalDedupTTL only needs to outlast real Temporal
// signal-delivery + activity-commit latency (seconds)
const connectorSignalDedupTTL = 5 * time.Minute

type ConnectorTaskService struct {
	Instances   port.InstanceRepository
	Tasks       port.TaskRepository
	Assignments port.TaskAssignmentRepository
	Temporal    port.TemporalClient
	Cache       port.CacheStore
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
	// VisitCount mirrors stageTransitionWire's own field (task_service.go) —
	// see domain.Task.VisitCount's doc comment.
	VisitCount int64 `json:"visit_count"`
}

func (s *ConnectorTaskService) Complete(ctx context.Context, tenantID, taskID, actorUserID uuid.UUID, output map[string]any) error {
	task, done, err := s.loadConnectorTask(ctx, tenantID, taskID)
	if err != nil || done {
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

func (s *ConnectorTaskService) Fail(ctx context.Context, tenantID, taskID, actorUserID uuid.UUID, errorClass string) error {
	task, done, err := s.loadConnectorTask(ctx, tenantID, taskID)
	if err != nil || done {
		return err
	}

	inst, err := s.Instances.GetByID(ctx, tenantID, task.WorkflowInstanceID)
	if err != nil {
		return wrapInstanceErr(err)
	}
	if !s.reserveSignal(ctx, taskID) {
		return nil
	}

	// The same row a completion creates: the failed task's event reads its
	// assignees off the task, so without it a connector failure names nobody
	// and the actor the callback required is retained nowhere.
	if err := s.assignActor(ctx, tenantID, taskID, actorUserID); err != nil {
		s.releaseSignalReservation(ctx, taskID)
		return err
	}

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

// A second identical signal can arrive in the short window before the
// task's own status has advanced past the first one's write — the status
// check above doesn't catch that, so a dedup claim guards it directly.
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

// Releases reserveSignal's claim after a failed send, so a retry isn't
// blocked by a stale claim for the rest of its TTL.
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

// renames output's top-level keys into the workflow context variable names
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
