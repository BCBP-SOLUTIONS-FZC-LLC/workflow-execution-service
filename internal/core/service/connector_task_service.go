package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

var _ port.ConnectorTaskService = (*ConnectorTaskService)(nil)

const (
	connectorDeliveredTTL = 5 * time.Minute
	connectorReservedTTL  = 30 * time.Second

	signalReserved  = "reserved"
	signalDelivered = "delivered"

	outcomeComplete = "complete"
	outcomeFail     = "fail"

	errorClassOutputMappingInvalid = "output_mapping_invalid"
)

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
	inst, accepted, err := s.instanceAcceptingCallback(ctx, tenantID, task)
	if err != nil || !accepted {
		return err
	}
	actorUserID, err := s.automationPrincipal(ctx, tenantID)
	if err != nil {
		return err
	}

	resultJSON, err := applyOutputMapping(task.ExtrasJSON, output)
	if errors.Is(err, errUnreadableOutputMapping) {
		s.logger().Error("connector task output mapping is unreadable; failing the task", map[string]any{
			"task_id": taskID, "tenant_id": tenantID, "error": err.Error(),
		})
		return s.failTask(ctx, tenantID, task, inst, actorUserID, errorClassOutputMappingInvalid, nil)
	}
	if err != nil {
		return fmt.Errorf("apply output mapping: %w", err)
	}

	if proceed, err := s.reserveSignal(ctx, outcomeComplete, taskID); err != nil || !proceed {
		return err
	}

	if err := s.assignActor(ctx, tenantID, taskID, actorUserID); err != nil {
		s.releaseSignalReservation(ctx, outcomeComplete, taskID)
		return err
	}

	deptID, nodeID := deptAndSuffix(task.NodeKey)
	if err := s.Temporal.SignalWorkflow(ctx, inst.TemporalWorkflowID, inst.ID, "stage-transition", stageTransitionWire{
		DeptID: deptID, NodeID: nodeID, UserID: actorUserID.String(),
		ResultJSON: string(resultJSON), RecordVersion: task.RecordVersion,
		VisitCount: task.VisitCount,
	}); err != nil {
		s.releaseSignalReservation(ctx, outcomeComplete, taskID)
		return fmt.Errorf("signal stage-transition: %w", err)
	}
	s.markSignalDelivered(ctx, outcomeComplete, taskID)
	return nil
}

func (s *ConnectorTaskService) Fail(ctx context.Context, tenantID, taskID uuid.UUID, errorClass string, output map[string]any) error {
	task, done, err := s.loadConnectorTask(ctx, tenantID, taskID)
	if err != nil || done {
		return err
	}
	inst, accepted, err := s.instanceAcceptingCallback(ctx, tenantID, task)
	if err != nil || !accepted {
		return err
	}
	actorUserID, err := s.automationPrincipal(ctx, tenantID)
	if err != nil {
		return err
	}

	return s.failTask(ctx, tenantID, task, inst, actorUserID, errorClass, output)
}

// failTask records the acting principal and signals stage-fail; both the fail
// callback and a task whose stored output mapping cannot be read end here.
func (s *ConnectorTaskService) failTask(
	ctx context.Context, tenantID uuid.UUID, task *domain.Task, inst *domain.Instance,
	actorUserID uuid.UUID, errorClass string, output map[string]any,
) error {
	if proceed, err := s.reserveSignal(ctx, outcomeFail, task.ID); err != nil || !proceed {
		return err
	}

	if err := s.assignActor(ctx, tenantID, task.ID, actorUserID); err != nil {
		s.releaseSignalReservation(ctx, outcomeFail, task.ID)
		return err
	}

	s.logger().Warn("connector task failed", map[string]any{
		"task_id": task.ID, "tenant_id": tenantID, "connector_type": *task.ConnectorType, "error_class": errorClass, "output_keys": sortedKeys(output),
	})

	deptID, nodeID := deptAndSuffix(task.NodeKey)
	if err := s.Temporal.SignalWorkflow(ctx, inst.TemporalWorkflowID, inst.ID, "stage-fail", stageFailWire{
		DeptID: deptID, NodeID: nodeID, ConnectorType: *task.ConnectorType, ErrorClass: errorClass, RecordVersion: task.RecordVersion,
		VisitCount: task.VisitCount,
	}); err != nil {
		s.releaseSignalReservation(ctx, outcomeFail, task.ID)
		return fmt.Errorf("signal stage-fail: %w", err)
	}
	s.markSignalDelivered(ctx, outcomeFail, task.ID)
	return nil
}

func (s *ConnectorTaskService) instanceAcceptingCallback(ctx context.Context, tenantID uuid.UUID, task *domain.Task) (*domain.Instance, bool, error) {
	inst, err := s.Instances.GetByID(ctx, tenantID, task.WorkflowInstanceID)
	if err != nil {
		return nil, false, wrapInstanceErr(err)
	}
	switch inst.Status {
	case domain.InstanceStatusRunning, domain.InstanceStatusDegraded, domain.InstanceStatusPaused:
		return inst, true, nil
	case domain.InstanceStatusCompleted, domain.InstanceStatusTerminated, domain.InstanceStatusFailed:
		return nil, false, nil
	default:
		return nil, false, fmt.Errorf("instance %s has status %q: %w", inst.ID, inst.Status, port.ErrInvalidInstanceState)
	}
}

func (s *ConnectorTaskService) automationPrincipal(ctx context.Context, tenantID uuid.UUID) (uuid.UUID, error) {
	if s.Principals == nil {
		return uuid.Nil, port.ErrNoAutomationPrincipal
	}
	p, err := s.Principals.AutomationPrincipal(ctx, tenantID)
	return p.ID, err
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
	if task.Status != domain.TaskStatusReady && task.Status != domain.TaskStatusInProgress {
		return nil, true, nil
	}
	return task, false, nil
}

// errCallbackInFlight asks the caller to retry later; the handlers answer it with 503.
var errCallbackInFlight = port.ErrCallbackInFlight

func otherOutcome(outcome string) string {
	if outcome == outcomeComplete {
		return outcomeFail
	}
	return outcomeComplete
}

// reserveSignal decides whether the caller signals now (true), is acknowledged
// without a signal (false, nil) or must retry later (an error). A reservation
// not marked delivered never makes a retry look finished; a task has one
// outcome, so a delivered signal of the other outcome acknowledges this one
// (execution LLD §5.2). Cache failures fail open.
func (s *ConnectorTaskService) reserveSignal(ctx context.Context, outcome string, taskID uuid.UUID) (bool, error) {
	if s.Cache == nil {
		return true, nil
	}
	key := connectorSignalKey(outcome, taskID)
	ok, err := s.Cache.SetNX(ctx, key, signalReserved, connectorReservedTTL)
	if err != nil {
		s.logger().Warn("connector-task signal dedup check failed, proceeding fail-open", map[string]any{"task_id": taskID, "error": err.Error()})
		return true, nil //nolint:nilerr // fail-open by design: a cache outage must not block callbacks
	}
	if !ok {
		return s.verdictFor(ctx, key, taskID)
	}
	proceed, err := s.verdictFor(ctx, connectorSignalKey(otherOutcome(outcome), taskID), taskID)
	if err != nil || !proceed {
		s.releaseSignalReservation(ctx, outcome, taskID)
	}
	return proceed, err
}

// verdictFor reads a reservation another attempt may hold: delivered
// acknowledges, still reserved means retry later, absent or unreadable proceeds.
func (s *ConnectorTaskService) verdictFor(ctx context.Context, key string, taskID uuid.UUID) (bool, error) {
	state, err := s.Cache.Get(ctx, key)
	if err != nil {
		s.logger().Warn("connector-task signal dedup read failed, proceeding fail-open", map[string]any{"task_id": taskID, "error": err.Error()})
		return true, nil //nolint:nilerr // fail-open by design: a cache outage must not block callbacks
	}
	switch state {
	case signalDelivered:
		return false, nil
	case signalReserved:
		return false, errCallbackInFlight
	default:
		return true, nil
	}
}

func (s *ConnectorTaskService) markSignalDelivered(ctx context.Context, outcome string, taskID uuid.UUID) {
	if s.Cache == nil {
		return
	}
	// The signal is already sent: record it even when the caller has given up on its request, or a
	// retry after the reservation expires would signal a second time.
	ctx = context.WithoutCancel(ctx)
	if err := s.Cache.Set(ctx, connectorSignalKey(outcome, taskID), signalDelivered, connectorDeliveredTTL); err != nil {
		s.logger().Warn("connector-task signal dedup could not be marked delivered", map[string]any{"task_id": taskID, "error": err.Error()})
	}
}

func (s *ConnectorTaskService) releaseSignalReservation(ctx context.Context, outcome string, taskID uuid.UUID) {
	if s.Cache == nil {
		return
	}
	if err := s.Cache.Del(ctx, connectorSignalKey(outcome, taskID)); err != nil {
		s.logger().Warn("connector-task signal dedup release failed after a failed signal", map[string]any{"task_id": taskID, "error": err.Error()})
	}
}

func connectorSignalKey(outcome string, taskID uuid.UUID) string {
	return "connector-signal:" + outcome + ":" + taskID.String()
}

var errUnreadableOutputMapping = errors.New("stored output mapping is unreadable")

type connectorTaskExtras struct {
	OutputMapping []dsl.IOVar `json:"output_mapping"`
}

func applyOutputMapping(extrasJSON json.RawMessage, output map[string]any) (json.RawMessage, error) {
	if len(extrasJSON) == 0 {
		return marshalResult(output)
	}
	var extras connectorTaskExtras
	if err := json.Unmarshal(extrasJSON, &extras); err != nil {
		return nil, fmt.Errorf("%w: %w", errUnreadableOutputMapping, err)
	}
	if len(extras.OutputMapping) == 0 {
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
	if v == nil {
		v = map[string]any{}
	}
	b, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("marshal connector result: %w", err)
	}
	return b, nil
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	return keys
}
