package handler

import "github.com/gin-gonic/gin"

// RegisterRoutes mounts this package's routes onto rg (the /api/v1 group).
// cmd/server's gin.Engine and middleware chain are wired elsewhere.
func RegisterRoutes(rg *gin.RouterGroup, h *Handler) {
	tasks := rg.Group("/tasks")
	tasks.GET("", h.ListTasks)
	tasks.GET("/:id", h.GetTask)
	tasks.POST("/:id/claim", h.ClaimTask)
	tasks.POST("/:id/complete", h.CompleteTask)
	tasks.POST("/:id/defer", h.DeferTask)
	tasks.POST("/:id/reassign", h.ReassignTask)

	rg.GET("/workflows/active-by-user", h.ListActiveByUser)
	rg.POST("/instances/:id/nodes/:node/override", h.OverrideNodeAssignee)
}

// RegisterInternalRoutes mounts the /internal/workflows/* routes (LLD §5.8)
// onto rg. rg must be an independent group off the raw gin.Engine — never a
// descendant of RegisterRoutes's own group — since gin subgroups inherit
// every parent .Use() call, and these routes must never see the
// gateway-identity-assuming middleware ordinary /api/v1 routes carry. The
// caller (cmd/server, T2.1) is responsible for layering
// middleware.RequireInternalToken on rg itself before calling this.
func RegisterInternalRoutes(rg *gin.RouterGroup, h *Handler) {
	workflows := rg.Group("/workflows")
	workflows.POST("/reassign-delegate", WithIdempotency(h.cache, h.idempotencyTTL, h.log, h.ReassignDelegate))
	workflows.POST("/cancel-by-delegate", WithIdempotency(h.cache, h.idempotencyTTL, h.log, h.CancelByDelegate))
	workflows.GET("/delegate-impact", h.DelegateImpact)
}

// RegisterInternalEventsRoutes mounts POST /internal/events (LLD §6.1) onto
// rg — the same /internal-rooted group RegisterInternalRoutes uses. Kept as
// a sibling function rather than folded into RegisterInternalRoutes: that
// function's own doc comment specifically scopes it to /internal/workflows/*,
// and this repo already splits registration by feature area (RegisterRoutes
// vs. RegisterInternalRoutes) — T2.1's future composition root calls both on
// the same real /internal group.
func RegisterInternalEventsRoutes(rg *gin.RouterGroup, h *Handler) {
	rg.POST("/events", h.HandleInternalEvent)
}
