package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type completeConnectorTaskReq struct {
	TenantID uuid.UUID      `json:"tenant_id" binding:"required"`
	Output   map[string]any `json:"output"`
}

type failConnectorTaskReq struct {
	TenantID   uuid.UUID `json:"tenant_id" binding:"required"`
	ErrorClass string    `json:"error_class" binding:"required"`
}

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
