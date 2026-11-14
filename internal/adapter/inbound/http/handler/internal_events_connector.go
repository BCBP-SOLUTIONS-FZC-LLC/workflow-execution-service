package handler

import (
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

	if h.alreadyProcessed(c, eventID, port.ConsumerConnector, eventType) {
		return
	}

	if h.connectorEvents == nil {
		h.logWarn("internal events: connector event publisher not configured, dropping", map[string]any{"task_id": taskID})
		h.respondOK(c, eventID, port.ConsumerConnector, eventType)
		return
	}

	ctx := guCtx(c.Request.Context(), tenantID)
	if !h.connectorStillRuns(c, eventID, tenantID, taskID) {
		return
	}
	var principal port.AutomationPrincipal
	if h.principals != nil {
		resolved, err := h.principals.AutomationPrincipal(ctx, tenantID)
		if err != nil {
			h.reconcilerError(c, eventType, err)
			return
		}
		principal = resolved
	}
	if err := h.connectorEvents.PublishTaskCreated(ctx, port.ConnectorTaskCreatedEvent{
		EventID: eventID, TenantID: tenantID, InstanceID: instanceID, TaskID: taskID,
		NodeKey: p.NodeKey, DepartmentID: departmentID, ConnectorType: *p.ConnectorType,
		ResolvedInputs: orEmptyObject(p.ResolvedInputs), OutputMapping: orEmptyList(p.OutputMapping),
		PrincipalID: principal.ID, PrincipalSub: principal.Subject,
	}); err != nil {
		h.reconcilerError(c, eventType, err)
		return
	}

	h.respondOK(c, eventID, port.ConsumerConnector, eventType)
}

// connectorStillRuns answers false once it has replied: with 200 when the task
// is finished or its instance has ended (the event is spent, not retried), with
// 500 when the state could not be read (execution LLD §6.2).
func (h *Handler) connectorStillRuns(c *gin.Context, eventID, tenantID, taskID uuid.UUID) bool {
	if h.connectorGate == nil {
		return true
	}
	dispatchable, err := h.connectorGate.Dispatchable(guCtx(c.Request.Context(), tenantID), tenantID, taskID)
	if err != nil {
		h.reconcilerError(c, eventTypeWorkflowTaskCreated, err)
		return false
	}
	if dispatchable {
		return true
	}
	h.logInfo("internal events: connector task finished or instance ended, not queuing", map[string]any{"task_id": taskID, "tenant_id": tenantID})
	h.respondOK(c, eventID, port.ConsumerConnector, eventTypeWorkflowTaskCreated)
	return false
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
