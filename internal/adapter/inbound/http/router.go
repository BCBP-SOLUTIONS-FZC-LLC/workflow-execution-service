// Package http's router.go is the single source of truth for this
// service's HTTP surface: every path, method, middleware, and route group.
package http

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/middleware"
)

// Pinger is satisfied by any /readyz dependency that only needs a
// healthy/unhealthy verdict — the Valkey cache and Temporal frontend today.
type Pinger interface {
	Ping(ctx context.Context) error
}

type DBHealth struct {
	Healthy       bool
	Utilization   float64
	AcquiredConns int32
	MaxConns      int32
}

type DBPinger interface {
	Health(ctx context.Context) DBHealth
}

type RouterConfig struct {
	GinConfig        gincommon.Config
	AppEnv           string
	InternalAPIToken string

	Handler *handler.Handler

	DB       DBPinger
	Cache    Pinger
	Temporal Pinger
}

type Router struct {
	engine *gin.Engine
}

func (r *Router) Handler() http.Handler { return r.engine }

func NewRouter(cfg RouterConfig) *Router {
	if cfg.AppEnv != "dev" {
		gin.SetMode(gin.ReleaseMode)
	}
	r := gin.New()
	// A compiled NodeKey is always "<deptID>/<stageID>" (dsl's own
	// stageNodeKey convention) — every real call to POST
	// /instances/:id/nodes/:node/override needs that literal "/" preserved
	// inside :node's single path segment, which gin's default routing can't
	// do (it 404s: an unescaped "/" splits into an extra segment, and a raw
	// path match without UseRawPath decodes %2F back to "/" before ever
	// reaching the router tree). UseRawPath makes gin match on the
	// still-encoded path and only unescape the matched param value
	// afterward — callers must percent-encode the node key's "/" as %2F.
	r.UseRawPath = true
	h := cfg.Handler

	r.Use(gincommon.TimeoutMiddleware(30 * time.Second))

	if cfg.AppEnv == "dev" {
		r.GET("/asyncapi", AsyncAPIHandler)
	}

	for _, mw := range gincommon.ObservabilityMiddlewares(cfg.GinConfig) {
		r.Use(mw)
	}

	r.GET("/healthz", gincommon.HealthHandler())
	r.GET("/readyz", readyzHandler(cfg.DB, cfg.Cache, cfg.Temporal))

	// /api/v1/internal is service-to-service only.
	internal := r.Group("/api/v1/internal")
	internal.Use(middleware.RequireInternalToken(cfg.InternalAPIToken))

	internalWorkflows := internal.Group("/workflows")
	internalWorkflows.POST("/reassign-delegate", h.Idempotent(h.ReassignDelegate))
	internalWorkflows.POST("/cancel-by-delegate", h.Idempotent(h.CancelByDelegate))
	internalWorkflows.GET("/delegate-impact", h.DelegateImpact)

	internal.POST("/events/delegation", h.HandleDelegationEvents)
	internal.POST("/events/user-profile", h.HandleUserProfileEvents)
	internal.POST("/events/tenant", h.HandleTenantEvents)
	internal.POST("/events/workflow-task", h.HandleWorkflowTaskEvents)

	connectorTasks := internal.Group("/connector-tasks")
	connectorTasks.POST("/:id/complete", h.CompleteConnectorTask)
	connectorTasks.POST("/:id/fail", h.FailConnectorTask)

	api := r.Group("/api/v1")
	for _, mw := range gincommon.ProtectedMiddlewares(cfg.GinConfig) {
		api.Use(mw)
	}

	tasks := api.Group("/tasks")
	tasks.GET("", h.ListTasks)
	tasks.GET("/:id", h.GetTask)
	tasks.POST("/:id/claim", h.Idempotent(h.ClaimTask))
	tasks.POST("/:id/complete", h.Idempotent(h.CompleteTask))
	tasks.POST("/:id/defer", h.Idempotent(h.DeferTask))
	tasks.POST("/:id/reassign", h.Idempotent(h.ReassignTask))

	api.GET("/workflows/active-by-user", h.ListActiveByUser)
	api.POST("/instances/:id/nodes/:node/override", h.Idempotent(h.OverrideNodeAssignee))

	instances := api.Group("/instances")
	instances.POST("", h.Idempotent(h.StartInstance))
	instances.GET("", h.ListInstances)
	instances.GET("/:id", h.GetInstance)
	instances.GET("/:id/events", h.ListInstanceEvents)
	instances.POST("/:id/pause", h.Idempotent(h.PauseInstance))
	instances.POST("/:id/resume", h.Idempotent(h.ResumeInstance))
	instances.POST("/:id/cancel", h.Idempotent(h.CancelInstance))
	instances.POST("/:id/terminate", h.Idempotent(h.TerminateInstance))
	instances.POST("/:id/force-forward", h.Idempotent(h.ForceForwardInstance))
	instances.POST("/:id/force-back", h.Idempotent(h.ForceBackInstance))

	return &Router{engine: r}
}

// readyzHandler checks the app pool, cache, and Temporal frontend, each
// under its own bounded timeout. Failing readiness when Temporal is
// unreachable is standard readiness-probe behavior: it removes the pod from
// load-balancer rotation until the next successful check adds it back —
// there's no real alternative to serving traffic from a pod that can't
// reach a hard dependency.
func readyzHandler(db DBPinger, cache, temporal Pinger) gin.HandlerFunc {
	return func(c *gin.Context) {
		dbCtx, dbCancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
		defer dbCancel()
		hs := db.Health(dbCtx)
		if !hs.Healthy {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "unavailable", "db": "unreachable"})
			return
		}

		cacheCtx, cacheCancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
		defer cacheCancel()
		if err := cache.Ping(cacheCtx); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "unavailable", "cache": "unreachable"})
			return
		}

		temporalCtx, temporalCancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
		defer temporalCancel()
		if err := temporal.Ping(temporalCtx); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "unavailable", "temporal": "unreachable"})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"status":         "OK",
			"db_utilization": hs.Utilization,
			"db_conns":       hs.AcquiredConns,
			"db_max_conns":   hs.MaxConns,
		})
	}
}
