package temporal

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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

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

	departmentID, err := deptUUID(in.IAMDepartmentID)
	if err != nil {
		return port.CreateTaskOutput{}, nonRetryable("ValidationError", err)
	}
	dueAt, err := parseCompiledDate("due date", stage.DueDate)
	if err != nil {
		return port.CreateTaskOutput{}, nonRetryable("ValidationError", err)
	}
	followUpAt, err := parseCompiledDate("follow-up date", stage.FollowUpDate)
	if err != nil {
		return port.CreateTaskOutput{}, nonRetryable("ValidationError", err)
	}

	taskID := deterministicTaskID(instanceID, string(in.NodeKey), in.VisitCount)
	task := &domain.Task{
		ID:                 taskID,
		TenantID:           tenantID,
		WorkflowInstanceID: instanceID,
		NodeKey:            string(in.NodeKey),
		DepartmentID:       departmentID,
		Status:             domain.TaskStatusReady,
		VisitCount:         in.VisitCount,
		DueAt:              dueAt,
		FollowUpAt:         followUpAt,
	}

	var connectorType *string
	var resolvedInputs map[string]any
	var outputMapping []dsl.IOVar
	assigneeUserIDs := []uuid.UUID{}
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
		replayed, err := d.taskAlreadyCreated(ctx, tenantID, taskID)
		if err != nil || replayed {
			return err
		}
		inst, err := d.Instances.GetByID(ctx, tenantID, instanceID)
		if err != nil {
			return fmt.Errorf("get instance: %w", err)
		}
		if instanceEnded(inst.Status) {
			return nonRetryable("ValidationError", fmt.Errorf("instance %s is %s", instanceID, inst.Status))
		}
		if err := d.Tasks.Create(ctx, task); err != nil {
			return fmt.Errorf("create workflow_task: %w", err)
		}
		for _, userID := range assigneeUserIDs {
			assignment := &domain.TaskAssignment{ID: deterministicAssignmentID(task.ID, userID), TenantID: tenantID, TaskID: task.ID, UserID: userID}
			if err := d.Assignments.Create(ctx, assignment); err != nil {
				return fmt.Errorf("create workflow_task_assignment: %w", err)
			}
		}

		core := domain.CommonCore{WorkflowInstanceID: instanceID, SubjectID: inst.SubjectID, BusinessKey: inst.BusinessKey, WorkflowVersionID: inst.WorkflowVersionID}
		taskCore := domain.TaskScopedCore{
			TaskID: task.ID, NodeKey: task.NodeKey, DepartmentID: task.DepartmentID, AssigneeUserIDs: assigneeUserIDs,
		}
		payload := domain.NewWorkflowTaskCreatedPayload(core, taskCore, stage.Type, task.DueAt, task.FollowUpAt, connectorType, resolvedInputs, outputMapping)
		env, err := service.BuildEnvelope(ctx, d.Validator, domain.EventWorkflowTaskCreated, tenantID.String(), "instances/"+instanceID.String()+"/tasks/"+task.ID.String(), "", payload)
		if err != nil {
			return fmt.Errorf("build workflow.task.created envelope: %w", err)
		}
		return d.Outbox.Enqueue(ctx, env)
	})
	if err != nil {
		return port.CreateTaskOutput{}, retryableOrNotFound(fmt.Errorf("create task: %w", err))
	}
	return port.CreateTaskOutput{TaskID: task.ID.String()}, nil
}

// taskAlreadyCreated reports whether an earlier attempt of this activity already
// committed the task. Inserting the same id again would abort the transaction.
func (d *Deps) taskAlreadyCreated(ctx context.Context, tenantID, taskID uuid.UUID) (bool, error) {
	_, err := d.Tasks.GetByID(ctx, tenantID, taskID)
	if errors.Is(err, domain.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("look up task before creating it: %w", err)
	}
	return true, nil
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
			resolved[iv.Target] = iv.InputValue(vars)
		}
	}
	return resolved, nil
}

func resolveAssignees(defaultAssignees []string, override string) ([]uuid.UUID, error) {
	raw := defaultAssignees
	if override != "" {
		raw = []string{override}
	}
	ids := make([]uuid.UUID, 0, len(raw))
	seen := make(map[uuid.UUID]bool, len(raw))
	for _, s := range raw {
		id, err := uuid.Parse(s)
		if err != nil {
			return nil, fmt.Errorf("parse assignee user_id %q: %w", s, err)
		}
		if seen[id] {
			continue
		}
		seen[id] = true
		ids = append(ids, id)
	}
	return ids, nil
}

func parseCompiledDate(what, s string) (*time.Time, error) {
	if s == "" {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return nil, fmt.Errorf("parse %s %q: %w", what, s, err)
	}
	return &t, nil
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
	keys := make([]string, len(in.NodeKeys))
	for i, k := range in.NodeKeys {
		keys[i] = string(k)
	}
	ctx = withTenantGUC(ctx, tenantID)
	return retryableOrNotFound(d.Transactor.RunInTx(ctx, func(ctx context.Context) error {
		inst, err := d.Instances.GetByID(ctx, tenantID, instanceID)
		if err != nil {
			return fmt.Errorf("get instance: %w", err)
		}
		if instanceEnded(inst.Status) || slices.Equal(inst.CurrentNodeKeys, keys) {
			return nil
		}
		if _, err := d.Instances.UpdateCurrentNodeKeys(ctx, tenantID, instanceID, keys, inst.RecordVersion); err != nil {
			return fmt.Errorf("update current_node_keys: %w", err)
		}
		return nil
	}))
}

func instanceEnded(status domain.InstanceStatus) bool {
	return status == domain.InstanceStatusCompleted || status == domain.InstanceStatusFailed || status == domain.InstanceStatusTerminated
}
