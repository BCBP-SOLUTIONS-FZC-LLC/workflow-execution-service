package handler

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type signalAcceptedResp struct {
	Message string `json:"message"`
}

func signalAccepted(c *gin.Context) {
	c.JSON(http.StatusAccepted, signalAcceptedResp{Message: "signal accepted"})
}

func derefTime(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}

type instanceSummaryResp struct {
	ID                 uuid.UUID  `json:"id"`
	TenantID           uuid.UUID  `json:"tenant_id"`
	WorkflowID         uuid.UUID  `json:"workflow_id"`
	WorkflowVersionID  uuid.UUID  `json:"workflow_version_id"`
	BusinessKey        string     `json:"business_key"`
	TemporalWorkflowID string     `json:"temporal_workflow_id"`
	CurrentNodeKeys    []string   `json:"current_node_keys"`
	SavedNodeKeys      []string   `json:"saved_node_keys"`
	Status             string     `json:"status"`
	StartedByUserID    uuid.UUID  `json:"started_by_user_id"`
	StartedAt          time.Time  `json:"started_at"`
	CompletedAt        *time.Time `json:"completed_at,omitempty"`
	RecordVersion      int64      `json:"record_version"`
	CreatedAt          time.Time  `json:"created_at"`
	UpdatedAt          time.Time  `json:"updated_at"`
}

func toInstanceSummaryResp(i *port.Instance) instanceSummaryResp {
	return instanceSummaryResp{
		ID:                 i.ID,
		TenantID:           i.TenantID,
		WorkflowID:         i.WorkflowID,
		WorkflowVersionID:  i.WorkflowVersionID,
		BusinessKey:        i.BusinessKey,
		TemporalWorkflowID: i.TemporalWorkflowID,
		CurrentNodeKeys:    i.CurrentNodeKeys,
		SavedNodeKeys:      i.SavedNodeKeys,
		Status:             string(i.Status),
		StartedByUserID:    i.StartedByUserID,
		StartedAt:          derefTime(i.StartedAt),
		CompletedAt:        i.CompletedAt,
		RecordVersion:      i.RecordVersion,
		CreatedAt:          i.CreatedAt,
		UpdatedAt:          i.UpdatedAt,
	}
}

type instancesListResp struct {
	Items      []instanceSummaryResp `json:"items"`
	NextCursor string                `json:"next_cursor,omitempty"`
}

type taskSummaryResp struct {
	ID                 uuid.UUID  `json:"id"`
	WorkflowInstanceID uuid.UUID  `json:"workflow_instance_id"`
	TenantID           uuid.UUID  `json:"tenant_id"`
	NodeKey            string     `json:"node_key"`
	TaskType           string     `json:"task_type"`
	DepartmentID       uuid.UUID  `json:"department_id"`
	Status             string     `json:"status"`
	RecordVersion      int64      `json:"record_version"`
	AssigneeMode       string     `json:"assignee_mode"`
	AssigneeCount      int        `json:"assignee_count"`
	DueAt              *time.Time `json:"due_at,omitempty"`
	DeferredFromTaskID *uuid.UUID `json:"deferred_from_task_id,omitempty"`
	CreatedAt          time.Time  `json:"created_at"`
	CompletedAt        *time.Time `json:"completed_at,omitempty"`
}

func toTaskSummaryResp(t *port.Task) taskSummaryResp {
	return taskSummaryResp{
		ID:                 t.ID,
		WorkflowInstanceID: t.WorkflowInstanceID,
		TenantID:           t.TenantID,
		NodeKey:            t.NodeKey,
		TaskType:           t.TaskType,
		DepartmentID:       t.DepartmentID,
		Status:             string(t.Status),
		RecordVersion:      t.RecordVersion,
		AssigneeMode:       t.AssigneeMode,
		AssigneeCount:      t.AssigneeCount,
		DueAt:              t.DueAt,
		DeferredFromTaskID: t.DeferredFromTaskID,
		CreatedAt:          t.CreatedAt,
		CompletedAt:        t.CompletedAt,
	}
}

type instanceDetailResp struct {
	instanceSummaryResp
	Tasks       []taskSummaryResp `json:"tasks"`
	HasContext  bool              `json:"has_context"`
	OverrideMap json.RawMessage   `json:"override_map"`
}

type workflowEventResp struct {
	ID                 uuid.UUID       `json:"id"`
	WorkflowInstanceID uuid.UUID       `json:"workflow_instance_id"`
	TaskID             *uuid.UUID      `json:"task_id,omitempty"`
	TenantID           uuid.UUID       `json:"tenant_id"`
	EventType          string          `json:"event_type"`
	ActorUserID        *uuid.UUID      `json:"actor_user_id,omitempty"`
	NodeKey            *string         `json:"node_key,omitempty"`
	PayloadJSON        json.RawMessage `json:"payload_json"`
	OccurredAt         time.Time       `json:"occurred_at"`
}

func toWorkflowEventResp(e *port.WorkflowEvent) workflowEventResp {
	return workflowEventResp{
		ID:                 e.ID,
		WorkflowInstanceID: e.WorkflowInstanceID,
		TaskID:             e.TaskID,
		TenantID:           e.TenantID,
		EventType:          string(e.EventType),
		ActorUserID:        e.ActorUserID,
		NodeKey:            e.NodeKey,
		PayloadJSON:        e.PayloadJSON,
		OccurredAt:         e.OccurredAt,
	}
}

type workflowEventsListResp struct {
	Items      []workflowEventResp `json:"items"`
	NextCursor string              `json:"next_cursor,omitempty"`
}

// parseTimeQuery decodes an RFC3339 query parameter, writing 400 BAD_REQUEST
// on a malformed value. An absent parameter is not an error: (nil, true).
func parseTimeQuery(c *gin.Context, name string) (*time.Time, bool) {
	raw := c.Query(name)
	if raw == "" {
		return nil, true
	}
	t, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		writeProblem(c, http.StatusBadRequest, CodeBadRequest, "invalid "+name+" query parameter", nil)
		return nil, false
	}
	return &t, true
}

type startInstanceReq struct {
	BusinessKey       string               `json:"business_key" binding:"required,min=2,max=64"`
	WorkflowVersionID uuid.UUID            `json:"workflow_version_id" binding:"required"`
	ContextJSON       json.RawMessage      `json:"context_json"`
	OverrideMap       map[string]uuid.UUID `json:"override_map"`
}

func (h *Handler) StartInstance(c *gin.Context) {
	tenantID, userID, ok := callerIdentity(c)
	if !ok {
		return
	}
	var req startInstanceReq
	if !bindJSON(c, &req) {
		return
	}

	instance, err := h.instances.Start(c.Request.Context(), port.StartInstanceInput{
		TenantID:          tenantID,
		WorkflowVersionID: req.WorkflowVersionID,
		BusinessKey:       req.BusinessKey,
		ContextJSON:       req.ContextJSON,
		OverrideMap:       req.OverrideMap,
		StartedByUserID:   userID,
	})
	if err != nil {
		errResponse(c, err)
		return
	}
	c.JSON(http.StatusCreated, toInstanceSummaryResp(instance))
}

func (h *Handler) ListInstances(c *gin.Context) {
	tenantID, userID, ok := callerIdentity(c)
	if !ok {
		return
	}

	var filter port.InstanceFilter
	if v := c.Query("status"); v != "" {
		s := port.InstanceStatus(v)
		filter.Status = &s
	}
	if v := c.Query("workflow_version_id"); v != "" {
		id, err := uuid.Parse(v)
		if err != nil {
			writeProblem(c, http.StatusBadRequest, CodeBadRequest, "invalid workflow_version_id query parameter", nil)
			return
		}
		filter.WorkflowVersionID = &id
	}
	startedAfter, ok := parseTimeQuery(c, "started_after")
	if !ok {
		return
	}
	filter.StartedAfter = startedAfter
	startedBefore, ok := parseTimeQuery(c, "started_before")
	if !ok {
		return
	}
	filter.StartedBefore = startedBefore

	page, ok := pageParams(c)
	if !ok {
		return
	}
	result, err := h.instances.List(c.Request.Context(), tenantID, readScope(c, userID), filter, page)
	if err != nil {
		errResponse(c, err)
		return
	}
	items := make([]instanceSummaryResp, len(result.Items))
	for i, inst := range result.Items {
		items[i] = toInstanceSummaryResp(inst)
	}
	c.JSON(http.StatusOK, instancesListResp{Items: items, NextCursor: result.NextCursor})
}

func (h *Handler) GetInstance(c *gin.Context) {
	tenantID, userID, ok := callerIdentity(c)
	if !ok {
		return
	}
	instanceID, ok := parseIDParam(c)
	if !ok {
		return
	}

	instance, tasks, err := h.instances.Get(c.Request.Context(), tenantID, instanceID, readScope(c, userID))
	if err != nil {
		errResponse(c, err)
		return
	}
	taskResps := make([]taskSummaryResp, len(tasks))
	for i, t := range tasks {
		taskResps[i] = toTaskSummaryResp(t)
	}
	c.JSON(http.StatusOK, instanceDetailResp{
		instanceSummaryResp: toInstanceSummaryResp(instance),
		Tasks:               taskResps,
		HasContext:          len(instance.ContextJSON) > 0,
		OverrideMap:         instance.OverrideMap,
	})
}

func (h *Handler) ListInstanceEvents(c *gin.Context) {
	tenantID, userID, ok := callerIdentity(c)
	if !ok {
		return
	}
	instanceID, ok := parseIDParam(c)
	if !ok {
		return
	}
	page, ok := pageParams(c)
	if !ok {
		return
	}

	result, err := h.instances.ListEvents(c.Request.Context(), tenantID, instanceID, readScope(c, userID), page)
	if err != nil {
		errResponse(c, err)
		return
	}
	items := make([]workflowEventResp, len(result.Items))
	for i, e := range result.Items {
		items[i] = toWorkflowEventResp(e)
	}
	c.JSON(http.StatusOK, workflowEventsListResp{Items: items, NextCursor: result.NextCursor})
}

type pauseInstanceReq struct {
	Reason        string `json:"reason" binding:"max=500"`
	RecordVersion int64  `json:"record_version" binding:"required"`
}

func (h *Handler) PauseInstance(c *gin.Context) {
	tenantID, actorUserID, ok := callerIdentity(c)
	if !ok {
		return
	}
	if !requireAdmin(c) {
		return
	}
	instanceID, ok := parseIDParam(c)
	if !ok {
		return
	}
	var req pauseInstanceReq
	if !bindJSON(c, &req) {
		return
	}

	if err := h.instances.Pause(c.Request.Context(), tenantID, instanceID, actorUserID, req.Reason, req.RecordVersion); err != nil {
		errResponse(c, err)
		return
	}
	signalAccepted(c)
}

type resumeInstanceReq struct {
	RecordVersion int64 `json:"record_version" binding:"required"`
}

func (h *Handler) ResumeInstance(c *gin.Context) {
	tenantID, actorUserID, ok := callerIdentity(c)
	if !ok {
		return
	}
	if !requireAdmin(c) {
		return
	}
	instanceID, ok := parseIDParam(c)
	if !ok {
		return
	}
	var req resumeInstanceReq
	if !bindJSON(c, &req) {
		return
	}

	if err := h.instances.Resume(c.Request.Context(), tenantID, instanceID, actorUserID, req.RecordVersion); err != nil {
		errResponse(c, err)
		return
	}
	signalAccepted(c)
}

type cancelInstanceReq struct {
	Reason        string `json:"reason" binding:"required,max=500"`
	RecordVersion int64  `json:"record_version" binding:"required"`
}

func (h *Handler) CancelInstance(c *gin.Context) {
	tenantID, actorUserID, ok := callerIdentity(c)
	if !ok {
		return
	}
	if !requireAdmin(c) {
		return
	}
	instanceID, ok := parseIDParam(c)
	if !ok {
		return
	}
	var req cancelInstanceReq
	if !bindJSON(c, &req) {
		return
	}

	if err := h.instances.Cancel(c.Request.Context(), tenantID, instanceID, actorUserID, req.Reason, req.RecordVersion); err != nil {
		errResponse(c, err)
		return
	}
	signalAccepted(c)
}

type terminateInstanceReq struct {
	Reason string `json:"reason" binding:"required,max=500"`
}

// TerminateInstance carries no record_version: Terminate is a direct
// TerminateWorkflow call, not signal-validated like the other five (LLD §3.1).
func (h *Handler) TerminateInstance(c *gin.Context) {
	tenantID, actorUserID, ok := callerIdentity(c)
	if !ok {
		return
	}
	if !requireAdmin(c) {
		return
	}
	instanceID, ok := parseIDParam(c)
	if !ok {
		return
	}
	var req terminateInstanceReq
	if !bindJSON(c, &req) {
		return
	}

	if err := h.instances.Terminate(c.Request.Context(), tenantID, instanceID, actorUserID, req.Reason); err != nil {
		errResponse(c, err)
		return
	}
	signalAccepted(c)
}

type forceForwardInstanceReq struct {
	TargetNodeKey string `json:"target_node_key" binding:"required"`
	RecordVersion int64  `json:"record_version" binding:"required"`
}

func (h *Handler) ForceForwardInstance(c *gin.Context) {
	tenantID, actorUserID, ok := callerIdentity(c)
	if !ok {
		return
	}
	if !requireAdmin(c) {
		return
	}
	instanceID, ok := parseIDParam(c)
	if !ok {
		return
	}
	var req forceForwardInstanceReq
	if !bindJSON(c, &req) {
		return
	}

	if err := h.instances.ForceForward(c.Request.Context(), tenantID, instanceID, actorUserID, req.TargetNodeKey, req.RecordVersion); err != nil {
		errResponse(c, err)
		return
	}
	signalAccepted(c)
}

type forceBackInstanceReq struct {
	RecordVersion int64 `json:"record_version" binding:"required"`
}

func (h *Handler) ForceBackInstance(c *gin.Context) {
	tenantID, actorUserID, ok := callerIdentity(c)
	if !ok {
		return
	}
	if !requireAdmin(c) {
		return
	}
	instanceID, ok := parseIDParam(c)
	if !ok {
		return
	}
	var req forceBackInstanceReq
	if !bindJSON(c, &req) {
		return
	}

	if err := h.instances.ForceBack(c.Request.Context(), tenantID, instanceID, actorUserID, req.RecordVersion); err != nil {
		errResponse(c, err)
		return
	}
	signalAccepted(c)
}
