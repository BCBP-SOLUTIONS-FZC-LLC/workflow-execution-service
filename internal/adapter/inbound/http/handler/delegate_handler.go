package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// The three /internal/workflows/* handlers below carry no gateway identity
// headers at all (LLD §5.7/§9.2) — tenant_id comes from the request
// body/query, never from gincommon.RequestContext/callerIdentity, which does
// not apply to this route family.

type reassignDelegateReq struct {
	TenantID      uuid.UUID  `json:"tenant_id" binding:"required"`
	OldDelegateID uuid.UUID  `json:"old_delegate_id" binding:"required"`
	NewDelegateID uuid.UUID  `json:"new_delegate_id" binding:"required"`
	DelegationID  *uuid.UUID `json:"delegation_id"`
}

type reassignDelegateResp struct {
	Reassigned int `json:"reassigned"`
}

// ReassignDelegate is not stubbed for Idempotency-Key here — see
// RegisterInternalRoutes, which wraps this handler with WithIdempotency.
func (h *Handler) ReassignDelegate(c *gin.Context) {
	var req reassignDelegateReq
	if !bindJSON(c, &req) {
		return
	}

	reassigned, err := h.workflowClient.ReassignDelegate(c.Request.Context(), port.ReassignDelegateInput{
		TenantID:      req.TenantID,
		OldDelegateID: req.OldDelegateID,
		NewDelegateID: req.NewDelegateID,
		DelegationID:  req.DelegationID,
	})
	if err != nil {
		errResponse(c, err)
		return
	}
	c.JSON(http.StatusOK, reassignDelegateResp{Reassigned: reassigned})
}

type cancelByDelegateReq struct {
	TenantID       uuid.UUID  `json:"tenant_id" binding:"required"`
	DelegateUserID uuid.UUID  `json:"delegate_user_id" binding:"required"`
	DelegationID   *uuid.UUID `json:"delegation_id"`
}

type cancelByDelegateResp struct {
	Cancelled int `json:"cancelled"`
}

func (h *Handler) CancelByDelegate(c *gin.Context) {
	var req cancelByDelegateReq
	if !bindJSON(c, &req) {
		return
	}

	cancelled, err := h.workflowClient.CancelByDelegate(c.Request.Context(), port.CancelByDelegateInput{
		TenantID:       req.TenantID,
		DelegateUserID: req.DelegateUserID,
		DelegationID:   req.DelegationID,
	})
	if err != nil {
		errResponse(c, err)
		return
	}
	c.JSON(http.StatusOK, cancelByDelegateResp{Cancelled: cancelled})
}

type delegateImpactResp struct {
	ReassignedCount int         `json:"reassigned_count"`
	WorkflowIDs     []uuid.UUID `json:"workflow_ids"`
	NextCursor      string      `json:"next_cursor,omitempty"`
}

func toDelegateImpactResp(r port.DelegateImpactResult) delegateImpactResp {
	ids := make([]uuid.UUID, len(r.WorkflowIDs.Items))
	copy(ids, r.WorkflowIDs.Items)
	return delegateImpactResp{
		ReassignedCount: r.ReassignedCount,
		WorkflowIDs:     ids,
		NextCursor:      r.WorkflowIDs.NextCursor,
	}
}

func (h *Handler) DelegateImpact(c *gin.Context) {
	tenantID, ok := parseUUIDQuery(c, "tenant_id")
	if !ok {
		return
	}
	delegateUserID, ok := parseUUIDQuery(c, "delegate_user_id")
	if !ok {
		return
	}
	var delegationID *uuid.UUID
	if raw := c.Query("delegation_id"); raw != "" {
		id, err := uuid.Parse(raw)
		if err != nil {
			writeProblem(c, http.StatusBadRequest, CodeBadRequest, "invalid delegation_id query parameter", nil)
			return
		}
		delegationID = &id
	}

	page, ok := pageParams(c)
	if !ok {
		return
	}
	result, err := h.workflowClient.DelegateImpact(c.Request.Context(), port.DelegateImpactInput{
		TenantID:       tenantID,
		DelegateUserID: delegateUserID,
		DelegationID:   delegationID,
		Page:           page,
	})
	if err != nil {
		errResponse(c, err)
		return
	}
	c.JSON(http.StatusOK, toDelegateImpactResp(result))
}
