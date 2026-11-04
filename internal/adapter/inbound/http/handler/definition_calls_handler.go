package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

type activeInstancesResp struct {
	HasActive bool  `json:"has_active"`
	Count     int32 `json:"count"`
}

func (h *Handler) ActiveInstances(c *gin.Context) {
	workflowID, err := uuid.Parse(c.Param("workflow_id"))
	if err != nil {
		writeProblem(c, http.StatusBadRequest, CodeBadRequest, "invalid workflow_id path parameter", nil)
		return
	}
	tenantID, err := uuid.Parse(c.Query("tenant_id"))
	if err != nil {
		writeProblem(c, http.StatusBadRequest, CodeBadRequest, "tenant_id must be a UUID", nil)
		return
	}
	hasActive, count, err := h.archiveGuard.CheckActiveInstances(c.Request.Context(), tenantID, workflowID)
	if err != nil {
		errResponse(c, err)
		return
	}
	c.JSON(http.StatusOK, activeInstancesResp{HasActive: hasActive, Count: count})
}

type pauseUserTasksReq struct {
	TenantID uuid.UUID `json:"tenant_id" binding:"required"`
}

func (h *Handler) PauseUserTasks(c *gin.Context) {
	userID, err := uuid.Parse(c.Param("user_id"))
	if err != nil {
		writeProblem(c, http.StatusBadRequest, CodeBadRequest, "invalid user_id path parameter", nil)
		return
	}
	var req pauseUserTasksReq
	if !bindInternalJSON(c, &req) {
		return
	}
	if err := h.userTaskPauser.PauseUserTasks(c.Request.Context(), req.TenantID, userID); err != nil {
		errResponse(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}
