package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type overrideReq struct {
	NewUserID     uuid.UUID `json:"new_user_id" binding:"required"`
	Reason        string    `json:"reason"`
	RecordVersion int64     `json:"record_version" binding:"required"`
}

type overrideResp struct {
	WorkflowInstanceID uuid.UUID `json:"workflow_instance_id"`
	NodeKey            string    `json:"node_key"`
	PreviousUserID     uuid.UUID `json:"previous_user_id"`
	NewUserID          uuid.UUID `json:"new_user_id"`
	Reason             string    `json:"reason,omitempty"`
	ActorUserID        uuid.UUID `json:"actor_user_id"`
	RecordVersion      int64     `json:"record_version"`
}

func toOverrideResp(o *port.AssigneeOverride) overrideResp {
	return overrideResp{
		WorkflowInstanceID: o.WorkflowInstanceID,
		NodeKey:            o.NodeKey,
		PreviousUserID:     o.PreviousUserID,
		NewUserID:          o.NewUserID,
		Reason:             o.Reason,
		ActorUserID:        o.ActorUserID,
		RecordVersion:      o.RecordVersion,
	}
}

var resolvedStatuses = map[port.TaskStatus]bool{
	port.TaskStatusCompleted:  true,
	port.TaskStatusDeferred:   true,
	port.TaskStatusFailed:     true,
	port.TaskStatusSuperseded: true,
}

// OverrideNodeAssignee implements LLD §5.4's ordered contract: validate ->
// eligibility -> persist -> signal. Do not reorder — no override row may
// exist for a request later rejected on eligibility, and a signal failure
// after persist is an accepted residual (Appendix B), not to be "fixed" here.
func (h *Handler) OverrideNodeAssignee(c *gin.Context) {
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
	nodeKey := c.Param("node")

	var req overrideReq
	if !bindJSON(c, &req) {
		return
	}

	ctx := c.Request.Context()

	task, err := h.tasks.GetByNode(ctx, tenantID, instanceID, nodeKey)
	if err != nil {
		errResponse(c, err)
		return
	}
	if resolvedStatuses[task.Status] {
		errResponse(c, port.ErrNodeAlreadyResolved)
		return
	}
	if task.RecordVersion != req.RecordVersion {
		errResponse(c, port.ErrRecordVersionConflict)
		return
	}

	eligible, err := h.eligibility.CheckEligibility(ctx, req.NewUserID, task.DepartmentID, task.RequiredLevel, actorUserID)
	if err != nil {
		errResponse(c, err)
		return
	}
	if !eligible {
		writeProblem(c, http.StatusUnprocessableEntity, CodeAssigneeIneligible, "the new assignee is not eligible for this node", nil)
		return
	}

	// TaskService re-checks the version at persist time — the race-safe
	// authoritative gate when two concurrent overrides both pass the checks
	// above and both call eligibility.
	override, err := h.tasks.OverrideAssignee(ctx, port.AssigneeOverrideInput{
		TenantID:      tenantID,
		InstanceID:    instanceID,
		NodeKey:       nodeKey,
		NewUserID:     req.NewUserID,
		Reason:        req.Reason,
		ActorUserID:   actorUserID,
		RecordVersion: req.RecordVersion,
	})
	if err != nil {
		errResponse(c, err)
		return
	}

	// Carries the pre-persist record_version (not override.RecordVersion) so
	// the in-workflow check stays consistent with the one validated above.
	if err := h.tasks.SignalReassign(ctx, tenantID, task.ID, actorUserID, override.PreviousUserID, req.NewUserID, req.RecordVersion); err != nil {
		errResponse(c, err)
		return
	}

	c.JSON(http.StatusOK, toOverrideResp(override))
}
