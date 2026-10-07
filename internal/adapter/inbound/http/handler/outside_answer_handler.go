package handler

import (
	"github.com/gin-gonic/gin"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

type outsideAnswerReq struct {
	Variables map[string]any `json:"variables"`
}

// CompleteOutsideWait delivers an outside party's answer to a receive task
// (execution LLD §5.11).
func (h *Handler) CompleteOutsideWait(c *gin.Context) {
	h.answerOutsideWait(c, c.Param("node"))
}

// FireOutsideBoundary delivers an outside party's message to a message
// boundary of the host :node (execution LLD §5.11).
func (h *Handler) FireOutsideBoundary(c *gin.Context) {
	h.answerOutsideWait(c, dsl.BoundaryWaitKey(c.Param("node"), c.Param("boundary")))
}

func (h *Handler) answerOutsideWait(c *gin.Context, key string) {
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
	var req outsideAnswerReq
	if c.Request.ContentLength != 0 && !bindJSON(c, &req) {
		return
	}
	if err := h.instances.AnswerOutsideWait(c.Request.Context(), tenantID, instanceID, actorUserID, key, req.Variables); err != nil {
		errResponse(c, err)
		return
	}
	signalAccepted(c)
}
