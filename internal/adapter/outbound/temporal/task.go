package temporal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

// Idempotent under at-least-once retry: task.ID is deterministic, so a
// retried attempt hits its own primary key and is treated as already done.
func (d *Deps) CreateTask(ctx context.Context, in port.CreateTaskInput) (port.CreateTaskOutput, error) {
	tenantID, err := uuid.Parse(in.TenantID)
	if err != nil {
		return port.CreateTaskOutput{}, nonRetryable("ValidationError", fmt.Errorf("parse tenant_id: %w", err))
	}
	instanceID, err := uuid.Parse(in.InstanceID)
	if err != nil {
		return port.CreateTaskOutput{}, nonRetryable("ValidationError", fmt.Errorf("parse instance_id: %w", err))
	}

	var stage dsl.StageDef
	if err := json.Unmarshal(in.CompiledNode, &stage); err != nil {
		return port.CreateTaskOutput{}, nonRetryable("ValidationError", fmt.Errorf("unmarshal compiled node: %w", err))
	}

	taskID := deterministicTaskID(instanceID, string(in.NodeKey), in.VisitCount)
	task := &domain.Task{
		ID:                 taskID,
		TenantID:           tenantID,
		WorkflowInstanceID: instanceID,
		NodeKey:            string(in.NodeKey),
		DepartmentID:       deptUUID(in.IAMDepartmentID),
		Status:             domain.TaskStatusReady,
		DueAt:              parseCompiledDate(stage.DueDate),
		FollowUpAt:         parseCompiledDate(stage.FollowUpDate),
	}

	var connectorType *string
	var resolvedInputs map[string]any
	var outputMapping []dsl.IOVar
	var assigneeUserIDs []uuid.UUID
	if stage.ConnectorType != "" {
		connectorType = &stage.ConnectorType
		task.ConnectorType = &stage.ConnectorType
		task.AssigneeMode = "single"
		resolvedInputs, err = resolveConnectorInputs(in.ContextJSON, stage.IOMapping)
		if err != nil {
			return port.CreateTaskOutput{}, nonRetryable("ValidationError", err)
		}
		if stage.IOMapping != nil {
			outputMapping = stage.IOMapping.Outputs
		}
		extras, err := json.Marshal(map[string]any{"resolved_inputs": resolvedInputs, "output_mapping": outputMapping})
		if err != nil {
			return port.CreateTaskOutput{}, nonRetryable("ValidationError", fmt.Errorf("marshal extras_json: %w", err))
		}
		task.ExtrasJSON = extras
	} else {
		assigneeUserIDs, err = resolveAssignees(stage.DefaultAssignees, in.OverrideMap[string(in.NodeKey)])
		if err != nil {
			return port.CreateTaskOutput{}, nonRetryable("ValidationError", err)
		}
		task.AssigneeMode = "single"
		if len(assigneeUserIDs) > 1 {
			task.AssigneeMode = "all"
		}
	}

	ctx = withTenantGUC(ctx, tenantID)
	err = d.Transactor.RunInTx(ctx, func(ctx context.Context) error {
		if err := d.Tasks.Create(ctx, task); err != nil {
			if errors.Is(err, domain.ErrAlreadyExists) {
				return nil
			}
			return fmt.Errorf("create workflow_task: %w", err)
		}
		for _, userID := range assigneeUserIDs {
			assignment := &domain.TaskAssignment{ID: deterministicAssignmentID(task.ID, userID), TenantID: tenantID, TaskID: task.ID, UserID: userID}
			if err := d.Assignments.Create(ctx, assignment); err != nil {
				return fmt.Errorf("create workflow_task_assignment: %w", err)
			}
		}

		core := domain.CommonCore{WorkflowInstanceID: instanceID}
		taskCore := domain.TaskScopedCore{
			TaskID: task.ID, NodeKey: task.NodeKey, DepartmentID: task.DepartmentID, AssigneeUserIDs: assigneeUserIDs,
		}
		payload := domain.NewWorkflowTaskCreatedPayload(core, taskCore, stage.Type, task.DueAt, task.FollowUpAt, connectorType, resolvedInputs, outputMapping)
		env, err := service.BuildEnvelope(ctx, d.Validator, domain.EventWorkflowTaskCreated, tenantID.String(), "tasks/"+task.ID.String(), "", payload)
		if err != nil {
			return fmt.Errorf("build workflow.task.created envelope: %w", err)
		}
		return d.Outbox.Enqueue(ctx, env)
	})
	if err != nil {
		return port.CreateTaskOutput{}, fmt.Errorf("create task: %w", err)
	}
	return port.CreateTaskOutput{TaskID: task.ID.String()}, nil
}

func resolveConnectorInputs(contextJSON string, m *dsl.IOMapping) (map[string]any, error) {
	vars := map[string]any{}
	if contextJSON != "" {
		if err := json.Unmarshal([]byte(contextJSON), &vars); err != nil {
			return nil, fmt.Errorf("parsing context_json for connector io_mapping: %w", err)
		}
	}
	resolved := map[string]any{}
	if m != nil {
		for _, iv := range m.Inputs {
			resolved[iv.Target] = vars[iv.Source]
		}
	}
	return resolved, nil
}

// An override, if present for this node, replaces DefaultAssignees entirely.
func resolveAssignees(defaultAssignees []string, override string) ([]uuid.UUID, error) {
	raw := defaultAssignees
	if override != "" {
		raw = []string{override}
	}
	ids := make([]uuid.UUID, 0, len(raw))
	for _, s := range raw {
		id, err := uuid.Parse(s)
		if err != nil {
			return nil, fmt.Errorf("parse assignee user_id %q: %w", s, err)
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func parseCompiledDate(s string) *time.Time {
	if s == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return nil
	}
	return &t
}

func (d *Deps) UpdateInstanceNodes(ctx context.Context, in port.UpdateInstanceNodesInput) error {
	tenantID, err := uuid.Parse(in.TenantID)
	if err != nil {
		return nonRetryable("ValidationError", fmt.Errorf("parse tenant_id: %w", err))
	}
	instanceID, err := uuid.Parse(in.InstanceID)
	if err != nil {
		return nonRetryable("ValidationError", fmt.Errorf("parse instance_id: %w", err))
	}
	inst, err := d.Instances.GetByID(ctx, tenantID, instanceID)
	if err != nil {
		return fmt.Errorf("get instance: %w", err)
	}
	keys := make([]string, len(in.NodeKeys))
	for i, k := range in.NodeKeys {
		keys[i] = string(k)
	}
	if _, err := d.Instances.UpdateCurrentNodeKeys(ctx, tenantID, instanceID, keys, inst.RecordVersion); err != nil {
		return fmt.Errorf("update current_node_keys: %w", err)
	}
	return nil
}
