package handler

import (
	"net/http"
	"strings"
	"unicode/utf8"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type overrideReq struct {
	NewUserID     uuid.UUID `json:"new_user_id" binding:"required"`
	Reason        *string   `json:"reason" binding:"omitempty,notblank,max=500"`
	RecordVersion int64     `json:"record_version" binding:"required,gte=1"`
}

type overrideResp struct {
	WorkflowInstanceID uuid.UUID  `json:"workflow_instance_id"`
	NodeKey            string     `json:"node_key"`
	PreviousUserID     *uuid.UUID `json:"previous_user_id"`
	NewUserID          uuid.UUID  `json:"new_user_id"`
	Reason             string     `json:"reason,omitempty"`
	ActorUserID        uuid.UUID  `json:"actor_user_id"`
	RecordVersion      int64      `json:"record_version"`
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

func validNodeKey(key string) bool {
	return utf8.ValidString(key) && !strings.ContainsRune(key, 0)
}

func deref[T any](p *T) T {
	var zero T
	if p == nil {
		return zero
	}
	return *p
}

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
	if !validNodeKey(nodeKey) {
		writeInvalidParam(c, "node_key", "invalid node_key path parameter")
		return
	}

	var req overrideReq
	if !bindJSON(c, &req) {
		return
	}

	ctx := c.Request.Context()

	override, err := h.tasks.OverrideAssignee(ctx, port.AssigneeOverrideInput{
		TenantID:      tenantID,
		InstanceID:    instanceID,
		NodeKey:       nodeKey,
		NewUserID:     req.NewUserID,
		Reason:        deref(req.Reason),
		ActorUserID:   actorUserID,
		RecordVersion: req.RecordVersion,
	})
	if err != nil {
		errResponse(c, err)
		return
	}

	c.JSON(http.StatusOK, toOverrideResp(override))
}
