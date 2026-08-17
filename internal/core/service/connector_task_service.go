package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var _ port.ConnectorTaskService = (*ConnectorTaskService)(nil)

// connectorSignalDedupTTL only needs to outlast real Temporal
// signal-delivery + activity-commit latency (seconds) — the state check in
// loadConnectorTask protects the long tail (a retry arriving well after the
// task already reached a terminal status); this TTL only needs to cover the
// short window between "signal delivered" and "the resulting DB write
// commits," where the task's own status hasn't advanced yet.
const connectorSignalDedupTTL = 5 * time.Minute

// ConnectorTaskService implements port.ConnectorTaskService — the
// completion/fail-signal path cmd/connector-worker's new internal HTTP
// endpoint calls into (LLD workflow_connectors.md §6.5 step 3). Deliberately
// a separate struct from TaskService, not new methods on it: TaskService's
// checkHumanActionable exists specifically to keep the human path from
// touching connector tasks, and a parallel, clearly-named service keeps that
// boundary legible rather than growing TaskService a second, inverse gate.
type ConnectorTaskService struct {
	Instances port.InstanceRepository
	Tasks     port.TaskRepository
	Temporal  port.TemporalClient
	Cache     port.CacheStore
	Log       port.Logger
}

func (s *ConnectorTaskService) logger() port.Logger {
	if s.Log != nil {
		return s.Log
	}
	return noopLogger{}
}

// stageFailWire mirrors internal/workflow/signals.go's own unexported
// stageFailSignal field-for-field, snake_case tags included — the same
// duplication convention task_service.go's stageTransitionWire/
// stageDeferWire already use (arch-lint forbids internal/core/service
// depending on internal/workflow).
type stageFailWire struct {
	DeptID        string `json:"dept_id"`
	NodeID        string `json:"node_id"`
	ConnectorType string `json:"connector_type"`
	ErrorClass    string `json:"error_class"`
	RecordVersion int64  `json:"record_version"`
}

// Complete signals stage-transition on behalf of a connector task that
// finished successfully. Unlike the human Complete path, this trusts no
// caller-supplied record_version: a connector task has exactly one
// legitimate resolver (the worker that dispatched it), never a concurrent
// human racing it, so the service reads the task's current RecordVersion
// itself.
func (s *ConnectorTaskService) Complete(ctx context.Context, tenantID, taskID uuid.UUID, output map[string]any) error {
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

	deptID, nodeID := deptAndSuffix(task.NodeKey)
	if err := s.Temporal.SignalWorkflow(ctx, inst.TemporalWorkflowID, inst.ID, "stage-transition", stageTransitionWire{
		DeptID: deptID, NodeID: nodeID, ResultJSON: string(resultJSON), RecordVersion: task.RecordVersion,
	}); err != nil {
		return fmt.Errorf("signal stage-transition: %w", err)
	}
	return nil
}

// Fail signals stage-fail on behalf of a connector task whose dispatch
// exhausted its retries or its internal execution timeout — never a human
// fallback (workflow_connectors.md: connector tasks are fully
// automation-only).
func (s *ConnectorTaskService) Fail(ctx context.Context, tenantID, taskID uuid.UUID, errorClass string) error {
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

	deptID, nodeID := deptAndSuffix(task.NodeKey)
	if err := s.Temporal.SignalWorkflow(ctx, inst.TemporalWorkflowID, inst.ID, "stage-fail", stageFailWire{
		DeptID: deptID, NodeID: nodeID, ConnectorType: *task.ConnectorType, ErrorClass: errorClass, RecordVersion: task.RecordVersion,
	}); err != nil {
		return fmt.Errorf("signal stage-fail: %w", err)
	}
	return nil
}

// loadConnectorTask fetches task and validates it's connector-typed and
// still open. done is true when the task has already reached a terminal
// status — a retried completion/fail call from connector-worker's own
// Stream-redelivery-driven retries is a safe, silent no-op, not an error.
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

// reserveSignal closes the narrow race loadConnectorTask's own state check
// can't: internal/workflow/signals.go's handleStageTransition/handleStageFail
// delete their pending-map entry the instant a signal resolves, so a second
// identical signal arriving in the handful-of-seconds window before the
// resulting DB write commits would not be safely absorbed downstream — it
// must be prevented from being sent at all. A SetNX failure (cache
// unreachable) fails open (returns true, proceeds to signal) rather than
// blocking a legitimate completion on a cache hiccup — the state check above
// still protects the long tail.
func (s *ConnectorTaskService) reserveSignal(ctx context.Context, taskID uuid.UUID) bool {
	if s.Cache == nil {
		return true
	}
	ok, err := s.Cache.SetNX(ctx, "connector-signal:"+taskID.String(), "1", connectorSignalDedupTTL)
	if err != nil {
		s.logger().Warn("connector-task signal dedup check failed, proceeding fail-open", map[string]any{"task_id": taskID, "error": err.Error()})
		return true
	}
	return ok
}

// connectorTaskExtras is the shape CreateTaskActivity persists into a
// connector task's ExtrasJSON (internal/adapter/outbound/temporal/task.go).
type connectorTaskExtras struct {
	OutputMapping []struct {
		Source string `json:"source"`
		Target string `json:"target"`
	} `json:"output_mapping"`
}

// applyOutputMapping renames output's top-level keys (Source, the
// connector's own result field) into the workflow context variable names
// (Target) the compiled plan's IOMapping.Outputs declared, per Decision #9
// ("top-level output keys only, no nested/dot-path in v1"). A Source key
// declared but absent from output is dropped silently, matching
// resolveConnectorInputs' own "missing var -> zero value" leniency. Absent
// or unparsable extras falls back to passing output through unmapped.
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
		if v, ok := output[ref.Source]; ok {
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
