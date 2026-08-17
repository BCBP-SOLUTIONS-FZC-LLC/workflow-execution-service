package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// completeConnectorTaskReq/failConnectorTaskReq are cmd/connector-worker's
// only way to resolve a connector-typed task — the human /tasks/:id/complete
// path explicitly rejects these (checkHumanActionable), and
// cmd/connector-worker itself never touches the Temporal SDK (LLD
// workflow_connectors.md §6.1 Decision #2), so this is the one place that
// signal gets sent on its behalf. No record_version in the request body:
// unlike the human path, a connector task has exactly one legitimate
// resolver (the worker that dispatched it), never a concurrent human racing
// it, so the service reads the task's own current RecordVersion itself.
type completeConnectorTaskReq struct {
	TenantID uuid.UUID      `json:"tenant_id" binding:"required"`
	Output   map[string]any `json:"output"`
}

type failConnectorTaskReq struct {
	TenantID   uuid.UUID `json:"tenant_id" binding:"required"`
	ErrorClass string    `json:"error_class" binding:"required"`
}

// CompleteConnectorTask is POST /internal/connector-tasks/:id/complete.
func (h *Handler) CompleteConnectorTask(c *gin.Context) {
	taskID, ok := parseIDParam(c)
	if !ok {
		return
	}
	var req completeConnectorTaskReq
	if !bindJSON(c, &req) {
		return
	}

	if err := h.connectorTasks.Complete(c.Request.Context(), req.TenantID, taskID, req.Output); err != nil {
		errResponse(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

// FailConnectorTask is POST /internal/connector-tasks/:id/fail.
func (h *Handler) FailConnectorTask(c *gin.Context) {
	taskID, ok := parseIDParam(c)
	if !ok {
		return
	}
	var req failConnectorTaskReq
	if !bindJSON(c, &req) {
		return
	}

	if err := h.connectorTasks.Fail(c.Request.Context(), req.TenantID, taskID, req.ErrorClass); err != nil {
		errResponse(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}
