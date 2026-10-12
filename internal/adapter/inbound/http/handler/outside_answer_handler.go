package handler

import (
	"github.com/gin-gonic/gin"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

type outsideAnswerReq struct {
	Variables map[string]any `json:"variables"`
}

func (h *Handler) CompleteOutsideWait(c *gin.Context) {
	h.answerOutsideWait(c, c.Param("node"))
}

func (h *Handler) FireOutsideBoundary(c *gin.Context) {
	h.answerOutsideWait(c, dsl.BoundaryWaitKey(c.Param("node"), c.Param("boundary")))
}

func (h *Handler) answerOutsideWait(c *gin.Context, key string) {
	tenantID, userID, ok := callerIdentity(c)
	if !ok {
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
	if err := h.instances.AnswerOutsideWait(c.Request.Context(), tenantID, instanceID, readScope(c, userID), key, req.Variables); err != nil {
		errResponse(c, err)
		return
	}
	signalAccepted(c)
}
