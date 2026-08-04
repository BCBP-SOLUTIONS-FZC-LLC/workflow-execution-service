package handler

import "github.com/gin-gonic/gin"

// RegisterRoutes mounts this package's routes onto rg (the /api/v1 group).
// cmd/server's gin.Engine and middleware chain are wired elsewhere.
func RegisterRoutes(rg *gin.RouterGroup, h *Handler) {
	tasks := rg.Group("/tasks")
	tasks.GET("", h.ListTasks)
	tasks.GET("/:id", h.GetTask)
	tasks.POST("/:id/claim", WithIdempotency(h.cache, h.idempotencyTTL, h.log, h.ClaimTask))
	tasks.POST("/:id/complete", WithIdempotency(h.cache, h.idempotencyTTL, h.log, h.CompleteTask))
	tasks.POST("/:id/defer", WithIdempotency(h.cache, h.idempotencyTTL, h.log, h.DeferTask))
	tasks.POST("/:id/reassign", WithIdempotency(h.cache, h.idempotencyTTL, h.log, h.ReassignTask))

	rg.GET("/workflows/active-by-user", h.ListActiveByUser)
	rg.POST("/instances/:id/nodes/:node/override", WithIdempotency(h.cache, h.idempotencyTTL, h.log, h.OverrideNodeAssignee))

	instances := rg.Group("/instances")
	instances.POST("", WithIdempotency(h.cache, h.idempotencyTTL, h.log, h.StartInstance))
	instances.GET("", h.ListInstances)
	instances.GET("/:id", h.GetInstance)
	instances.GET("/:id/events", h.ListInstanceEvents)
	instances.POST("/:id/pause", WithIdempotency(h.cache, h.idempotencyTTL, h.log, h.PauseInstance))
	instances.POST("/:id/resume", WithIdempotency(h.cache, h.idempotencyTTL, h.log, h.ResumeInstance))
	instances.POST("/:id/cancel", WithIdempotency(h.cache, h.idempotencyTTL, h.log, h.CancelInstance))
	instances.POST("/:id/terminate", WithIdempotency(h.cache, h.idempotencyTTL, h.log, h.TerminateInstance))
	instances.POST("/:id/force-forward", WithIdempotency(h.cache, h.idempotencyTTL, h.log, h.ForceForwardInstance))
	instances.POST("/:id/force-back", WithIdempotency(h.cache, h.idempotencyTTL, h.log, h.ForceBackInstance))
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
