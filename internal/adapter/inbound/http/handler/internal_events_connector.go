package handler

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

type workflowTaskCreatedPayload struct {
	WorkflowInstanceID string         `json:"workflow_instance_id"`
	TaskID             string         `json:"task_id"`
	NodeKey            string         `json:"node_key"`
	DepartmentID       string         `json:"department_id"`
	ConnectorType      *string        `json:"connector_type"`
	ResolvedInputs     map[string]any `json:"resolved_inputs"`
	OutputMapping      []dsl.IOVar    `json:"output_mapping"`
}

func (h *Handler) handleWorkflowTaskCreated(c *gin.Context, env events.Envelope[json.RawMessage]) {
	const eventType = eventTypeWorkflowTaskCreated
	var p workflowTaskCreatedPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		h.badPayload(c, eventType, "invalid WorkflowTaskCreated payload")
		return
	}
	eventID, ok := h.parseEventID(c, eventType, env.ID)
	if !ok {
		return
	}
	tenantID, ok := h.parseTenantID(c, eventType, env.TenantID)
	if !ok {
		return
	}

	if p.ConnectorType == nil || *p.ConnectorType == "" {
		h.markOK(eventType)
		c.Status(http.StatusOK)
		return
	}

	instanceID, err := uuid.Parse(p.WorkflowInstanceID)
	if err != nil {
		h.badPayload(c, eventType, "invalid workflow_instance_id in payload")
		return
	}
	taskID, err := uuid.Parse(p.TaskID)
	if err != nil {
		h.badPayload(c, eventType, "invalid task_id in payload")
		return
	}
	departmentID, err := uuid.Parse(p.DepartmentID)
	if err != nil {
		h.badPayload(c, eventType, "invalid department_id in payload")
		return
	}
	if departmentID == uuid.Nil {
		h.badPayload(c, eventType, "department_id must not be the nil uuid")
		return
	}

	h.processOnce(c, env, eventID, port.ConsumerConnector, eventType, func(ctx context.Context) (string, error) {
		if h.connectorEvents == nil {
			h.logWarn("internal events: connector event publisher not configured, dropping", map[string]any{"task_id": taskID})
			return resultOK, nil
		}
		ctx = guCtx(ctx, tenantID)
		if running, err := h.connectorStillRuns(ctx, tenantID, taskID); !running || err != nil {
			return resultOK, err
		}
		var principal port.AutomationPrincipal
		if h.principals != nil {
			resolved, err := h.principals.AutomationPrincipal(ctx, tenantID)
			if err != nil {
				return "", err
			}
			principal = resolved
		}
		return resultOK, h.connectorEvents.PublishTaskCreated(ctx, port.ConnectorTaskCreatedEvent{
			EventID: eventID, TenantID: tenantID, InstanceID: instanceID, TaskID: taskID,
			NodeKey: p.NodeKey, DepartmentID: departmentID, ConnectorType: *p.ConnectorType,
			ResolvedInputs: orEmptyObject(p.ResolvedInputs), OutputMapping: orEmptyList(p.OutputMapping),
			PrincipalID: principal.ID, PrincipalSub: principal.Subject,
		})
	})
}

// connectorStillRuns answers false, without an error, when the task is finished or its instance has
// ended: the event is spent, not retried (execution LLD §6.2).
func (h *Handler) connectorStillRuns(ctx context.Context, tenantID, taskID uuid.UUID) (bool, error) {
	if h.connectorGate == nil {
		return true, nil
	}
	dispatchable, err := h.connectorGate.Dispatchable(ctx, tenantID, taskID)
	if err != nil {
		return false, err
	}
	if !dispatchable {
		h.logInfo("internal events: connector task finished or instance ended, not queuing", map[string]any{"task_id": taskID, "tenant_id": tenantID})
	}
	return dispatchable, nil
}

func orEmptyObject(m map[string]any) map[string]any {
	if m == nil {
		return map[string]any{}
	}
	return m
}

func orEmptyList(l []dsl.IOVar) []dsl.IOVar {
	if l == nil {
		return []dsl.IOVar{}
	}
	return l
}
