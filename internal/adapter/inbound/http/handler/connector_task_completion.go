package handler

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type completeConnectorTaskReq struct {
	TenantID uuid.UUID      `json:"tenant_id" binding:"required"`
	Output   map[string]any `json:"output"`
}

type failConnectorTaskReq struct {
	TenantID   uuid.UUID      `json:"tenant_id" binding:"required"`
	ErrorClass string         `json:"error_class" binding:"required,notblank,errorclass"`
	Output     map[string]any `json:"output"`
}

func (h *Handler) CompleteConnectorTask(c *gin.Context) {
	taskID, ok := parseIDParam(c)
	if !ok {
		return
	}
	var req completeConnectorTaskReq
	if !bindInternalJSON(c, &req) {
		return
	}

	if err := h.connectorTasks.Complete(c.Request.Context(), req.TenantID, taskID, req.Output); err != nil {
		connectorCallbackError(c, err)
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
	if !bindInternalJSON(c, &req) {
		return
	}

	if err := h.connectorTasks.Fail(c.Request.Context(), req.TenantID, taskID, req.ErrorClass, req.Output); err != nil {
		connectorCallbackError(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}

func connectorCallbackError(c *gin.Context, err error) {
	if errors.Is(err, port.ErrNoAutomationPrincipal) {
		writeProblem(c, http.StatusServiceUnavailable, CodeUpstreamUnavailable,
			"the tenant's automation principal is not available; retry later", nil)
		return
	}
	errResponse(c, err)
}
