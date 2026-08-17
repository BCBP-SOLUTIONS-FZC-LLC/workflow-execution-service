package handler

import (
	"encoding/json"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

// --- workflow.task.created (connector dispatch) ---

// workflowTaskCreatedPayload is a narrow view of domain.WorkflowTaskCreatedPayload
// (internal/core/domain/events.go) — this handler only needs the fields a
// connector-typed task's Stream push requires, not the full event shape.
// Unlike this file's sibling handlers, workflow.task.created is emitted BY
// execution_service itself (CreateTaskActivity), not by another service —
// this only fires if execution_service is subscribed to its own
// workflow.task.created topic via the platform event bus's subscription
// topology; confirm that with whoever owns it before relying on this path.
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
	const eventType = "workflow.task.created"
	var p workflowTaskCreatedPayload
	if err := json.Unmarshal(env.Payload, &p); err != nil {
		h.badPayload(c, eventType, "invalid workflow.task.created payload")
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
		// Non-connector task creation — nothing to push onto the Stream,
		// but still a recognized, successfully handled event type.
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

	if h.alreadyProcessed(c, eventID, consumerConnector, eventType) {
		return
	}

	if h.connectorEvents == nil {
		h.logWarn("internal events: connector event publisher not configured, dropping", map[string]any{"task_id": taskID})
		h.respondOK(c, eventID, consumerConnector, eventType)
		return
	}

	ctx := guCtx(c, env.TenantID)
	if err := h.connectorEvents.PublishTaskCreated(ctx, port.ConnectorTaskCreatedEvent{
		EventID: eventID, TenantID: tenantID, InstanceID: instanceID, TaskID: taskID,
		NodeKey: p.NodeKey, DepartmentID: departmentID, ConnectorType: *p.ConnectorType,
		ResolvedInputs: p.ResolvedInputs, OutputMapping: p.OutputMapping,
	}); err != nil {
		h.reconcilerError(c, eventType, err)
		return
	}

	h.respondOK(c, eventID, consumerConnector, eventType)
}
