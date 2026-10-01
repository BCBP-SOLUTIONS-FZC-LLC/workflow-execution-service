package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
)

// The two handlers below serve the Definition Service (execution LLD §5.3).
// Like every internal route they carry no gateway identity: the tenant comes
// from the query or the body.

type activeInstancesResp struct {
	HasActive bool  `json:"has_active"`
	Count     int32 `json:"count"`
}

// ActiveInstances backs the Definition Service's archive guard.
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

// PauseUserTasks pauses every running instance holding a task of a member
// the Definition Service has seen removed.
func (h *Handler) PauseUserTasks(c *gin.Context) {
	userID, err := uuid.Parse(c.Param("user_id"))
	if err != nil {
		writeProblem(c, http.StatusBadRequest, CodeBadRequest, "invalid user_id path parameter", nil)
		return
	}
	var req pauseUserTasksReq
	if !bindJSON(c, &req) {
		return
	}
	if err := h.userTaskPauser.PauseUserTasks(c.Request.Context(), req.TenantID, userID); err != nil {
		errResponse(c, err)
		return
	}
	c.Status(http.StatusNoContent)
}
