package handler

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type taskResp struct {
	ID                 uuid.UUID  `json:"id"`
	WorkflowInstanceID uuid.UUID  `json:"workflow_instance_id"`
	NodeKey            string     `json:"node_key"`
	TaskType           string     `json:"task_type"`
	DepartmentID       uuid.UUID  `json:"department_id"`
	Status             string     `json:"status"`
	RecordVersion      int64      `json:"record_version"`
	AssigneeMode       string     `json:"assignee_mode"`
	AssigneeCount      int        `json:"assignee_count"`
	CreatedAt          time.Time  `json:"created_at"`
	CompletedAt        *time.Time `json:"completed_at,omitempty"`
}

type taskAssignmentResp struct {
	ID        uuid.UUID  `json:"id"`
	UserID    uuid.UUID  `json:"user_id"`
	IsLead    bool       `json:"is_lead"`
	IsActive  bool       `json:"is_active"`
	VacatedAt *time.Time `json:"vacated_at,omitempty"`
}

type taskDetailResp struct {
	taskResp
	Assignments []taskAssignmentResp `json:"assignments"`
}

type activeUserTaskResp struct {
	TaskID             uuid.UUID `json:"task_id"`
	WorkflowInstanceID uuid.UUID `json:"workflow_instance_id"`
	NodeKey            string    `json:"node_key"`
	UserID             uuid.UUID `json:"user_id"`
	DepartmentID       uuid.UUID `json:"department_id"`
	Status             string    `json:"status"`
	RecordVersion      int64     `json:"record_version"`
}

type tasksListResp struct {
	Items      []taskResp `json:"items"`
	NextCursor string     `json:"next_cursor,omitempty"`
}

type activeUserTasksListResp struct {
	Items      []activeUserTaskResp `json:"items"`
	NextCursor string               `json:"next_cursor,omitempty"`
}

func toTaskResp(t *port.Task) taskResp {
	return taskResp{
		ID:                 t.ID,
		WorkflowInstanceID: t.WorkflowInstanceID,
		NodeKey:            t.NodeKey,
		TaskType:           t.TaskType,
		DepartmentID:       t.DepartmentID,
		Status:             string(t.Status),
		RecordVersion:      t.RecordVersion,
		AssigneeMode:       t.AssigneeMode,
		AssigneeCount:      t.AssigneeCount,
		CreatedAt:          t.CreatedAt,
		CompletedAt:        t.CompletedAt,
	}
}

func toTaskAssignmentResp(a *port.TaskAssignment) taskAssignmentResp {
	return taskAssignmentResp{ID: a.ID, UserID: a.UserID, IsLead: a.IsLead, IsActive: a.IsActive, VacatedAt: a.VacatedAt}
}

func toActiveUserTaskResp(t *port.ActiveUserTask) activeUserTaskResp {
	return activeUserTaskResp{
		TaskID:             t.TaskID,
		WorkflowInstanceID: t.WorkflowInstanceID,
		NodeKey:            t.NodeKey,
		UserID:             t.UserID,
		DepartmentID:       t.DepartmentID,
		Status:             string(t.Status),
		RecordVersion:      t.RecordVersion,
	}
}

func readScope(c *gin.Context, callerUserID uuid.UUID) port.ReadScope {
	return port.ReadScope{
		CallerUserID: callerUserID,
		Departments:  callerDepartments(c),
		IsAdmin:      isAdmin(c),
	}
}

func (h *Handler) ListTasks(c *gin.Context) {
	tenantID, userID, ok := callerIdentity(c)
	if !ok {
		return
	}

	var filter port.TaskFilter
	if v := c.Query("status"); v != "" {
		s := port.TaskStatus(v)
		filter.Status = &s
	}
	if v := c.Query("instance_id"); v != "" {
		id, err := uuid.Parse(v)
		if err != nil {
			writeProblem(c, http.StatusBadRequest, CodeBadRequest, "invalid instance_id query parameter", nil)
			return
		}
		filter.WorkflowInstanceID = &id
	}
	if v := c.Query("department_id"); v != "" {
		id, err := uuid.Parse(v)
		if err != nil {
			writeProblem(c, http.StatusBadRequest, CodeBadRequest, "invalid department_id query parameter", nil)
			return
		}
		filter.DepartmentID = &id
	}

	page, ok := pageParams(c)
	if !ok {
		return
	}
	result, err := h.tasks.List(c.Request.Context(), tenantID, readScope(c, userID), filter, page)
	if err != nil {
		errResponse(c, err)
		return
	}
	items := make([]taskResp, len(result.Items))
	for i, t := range result.Items {
		items[i] = toTaskResp(t)
	}
	c.JSON(http.StatusOK, tasksListResp{Items: items, NextCursor: result.NextCursor})
}

func (h *Handler) GetTask(c *gin.Context) {
	tenantID, userID, ok := callerIdentity(c)
	if !ok {
		return
	}
	taskID, ok := parseIDParam(c)
	if !ok {
		return
	}

	task, assignments, err := h.tasks.Get(c.Request.Context(), tenantID, taskID, readScope(c, userID))
	if err != nil {
		errResponse(c, err)
		return
	}

	assignmentResps := make([]taskAssignmentResp, len(assignments))
	for i, a := range assignments {
		assignmentResps[i] = toTaskAssignmentResp(a)
	}
	c.JSON(http.StatusOK, taskDetailResp{taskResp: toTaskResp(task), Assignments: assignmentResps})
}

type claimReq struct {
	RecordVersion int64 `json:"record_version" binding:"required"`
}

func (h *Handler) ClaimTask(c *gin.Context) {
	tenantID, userID, ok := callerIdentity(c)
	if !ok {
		return
	}
	taskID, ok := parseIDParam(c)
	if !ok {
		return
	}
	var req claimReq
	if !bindJSON(c, &req) {
		return
	}

	task, err := h.tasks.Claim(c.Request.Context(), tenantID, taskID, userID, req.RecordVersion)
	if err != nil {
		errResponse(c, err)
		return
	}
	c.JSON(http.StatusAccepted, toTaskResp(task))
}

type completeReq struct {
	ResultJSON    json.RawMessage `json:"result_json"`
	RecordVersion int64           `json:"record_version" binding:"required"`
}

// CompleteTask forwards the caller's own x-user-id as-is (§5.6)
// against the task's assignee downstream, 403 NOT_ASSIGNEE on mismatch.
func (h *Handler) CompleteTask(c *gin.Context) {
	tenantID, userID, ok := callerIdentity(c)
	if !ok {
		return
	}
	taskID, ok := parseIDParam(c)
	if !ok {
		return
	}
	var req completeReq
	if !bindJSON(c, &req) {
		return
	}

	task, err := h.tasks.Complete(c.Request.Context(), tenantID, taskID, userID, req.ResultJSON, req.RecordVersion)
	if err != nil {
		errResponse(c, err)
		return
	}
	c.JSON(http.StatusAccepted, toTaskResp(task))
}

type deferReq struct {
	Reason        string `json:"reason"`
	RecordVersion int64  `json:"record_version" binding:"required"`
}

func (h *Handler) DeferTask(c *gin.Context) {
	tenantID, userID, ok := callerIdentity(c)
	if !ok {
		return
	}
	taskID, ok := parseIDParam(c)
	if !ok {
		return
	}
	var req deferReq
	if !bindJSON(c, &req) {
		return
	}

	task, err := h.tasks.Defer(c.Request.Context(), tenantID, taskID, userID, req.Reason, req.RecordVersion)
	if err != nil {
		errResponse(c, err)
		return
	}
	c.JSON(http.StatusAccepted, toTaskResp(task))
}

type reassignReq struct {
	NewUserID     uuid.UUID `json:"new_user_id" binding:"required"`
	RecordVersion int64     `json:"record_version" binding:"required"`
}

func (h *Handler) ReassignTask(c *gin.Context) {
	tenantID, userID, ok := callerIdentity(c)
	if !ok {
		return
	}
	if !requireAdmin(c) {
		return
	}
	taskID, ok := parseIDParam(c)
	if !ok {
		return
	}
	var req reassignReq
	if !bindJSON(c, &req) {
		return
	}

	task, err := h.tasks.Reassign(c.Request.Context(), tenantID, taskID, userID, req.NewUserID, req.RecordVersion)
	if err != nil {
		errResponse(c, err)
		return
	}
	c.JSON(http.StatusAccepted, toTaskResp(task))
}

func (h *Handler) ListActiveByUser(c *gin.Context) {
	tenantID, _, ok := callerIdentity(c)
	if !ok {
		return
	}
	if !requireAdmin(c) {
		return
	}
	userID, ok := parseUUIDQuery(c, "user_id")
	if !ok {
		return
	}

	page, ok := pageParams(c)
	if !ok {
		return
	}
	result, err := h.tasks.ActiveByUser(c.Request.Context(), tenantID, userID, page)
	if err != nil {
		errResponse(c, err)
		return
	}
	items := make([]activeUserTaskResp, len(result.Items))
	for i, t := range result.Items {
		items[i] = toActiveUserTaskResp(t)
	}
	c.JSON(http.StatusOK, activeUserTasksListResp{Items: items, NextCursor: result.NextCursor})
}

func parseUUIDQuery(c *gin.Context, name string) (uuid.UUID, bool) {
	id, err := uuid.Parse(c.Query(name))
	if err != nil {
		writeProblem(c, http.StatusBadRequest, CodeBadRequest, "invalid "+name+" query parameter", nil)
		return uuid.Nil, false
	}
	return id, true
}
