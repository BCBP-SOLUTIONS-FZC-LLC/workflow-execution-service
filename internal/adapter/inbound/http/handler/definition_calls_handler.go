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
	tenantID, ok := internalIDQuery(c, "tenant_id")
	if !ok {
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
	TenantID     *uuid.UUID `json:"tenant_id" binding:"required"`
	DepartmentID *uuid.UUID `json:"department_id"`
}

func (r pauseUserTasksReq) allZeroFields() []string {
	var names []string
	if *r.TenantID == uuid.Nil {
		names = append(names, "tenant_id")
	}
	if r.DepartmentID != nil && *r.DepartmentID == uuid.Nil {
		names = append(names, "department_id")
	}
	return names
}

func (h *Handler) PauseUserTasks(c *gin.Context) {
	userID, ok := parseNonZeroUUID(c.Param("user_id"))
	if !ok {
		writeInvalidParam(c, "user_id", "invalid user_id path parameter")
		return
	}
	var req pauseUserTasksReq
	if !bindInternalJSON(c, &req) {
		return
	}
	if names := req.allZeroFields(); len(names) > 0 {
		writeInvalidFields(c, names)
		return
	}
	if err := h.userTaskPauser.PauseUserTasks(c.Request.Context(), *req.TenantID, userID, req.DepartmentID); err != nil {
		errResponse(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}
