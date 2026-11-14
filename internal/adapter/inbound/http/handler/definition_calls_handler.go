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
	if err != nil || workflowID == uuid.Nil {
		writeInvalidParam(c, "workflow_id", "invalid workflow_id path parameter")
		return
	}
	tenantID, err := uuid.Parse(c.Query("tenant_id"))
	if err != nil {
		writeInvalidParam(c, "tenant_id", "tenant_id must be a UUID")
		return
	}
	if tenantID == uuid.Nil {
		writeInvalidParam(c, "tenant_id", "tenant_id must be a non-zero UUID")
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
	TenantID     uuid.UUID  `json:"tenant_id" binding:"required"`
	DepartmentID *uuid.UUID `json:"department_id"`
}

func (h *Handler) PauseUserTasks(c *gin.Context) {
	userID, err := uuid.Parse(c.Param("user_id"))
	if err != nil {
		writeInvalidParam(c, "user_id", "invalid user_id path parameter")
		return
	}
	var req pauseUserTasksReq
	if !bindInternalJSON(c, &req) {
		return
	}
	if req.DepartmentID != nil && *req.DepartmentID == uuid.Nil {
		writeProblem(c, http.StatusBadRequest, CodeBadRequest, "department_id must be a non-zero UUID", nil)
		return
	}
	if err := h.userTaskPauser.PauseUserTasks(c.Request.Context(), req.TenantID, userID, req.DepartmentID); err != nil {
		errResponse(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}
