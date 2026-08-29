// Package http's router.go is the single source of truth for this
// service's HTTP surface: every path, method, middleware, and route group.
// cmd/server's job is to construct dependencies and call NewRouter, not to
// encode routing decisions itself.
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
// The composition root (cmd/server) wraps each concrete client to implement
// it, so this package never imports platform-pgcommon or the Temporal SDK
// directly.
type Pinger interface {
	Ping(ctx context.Context) error
}

// DBHealth is a decoupled copy of pgcommon.HealthStatus's fields /readyz
// actually reports. Kept separate rather than importing pgcommon.HealthStatus
// itself, for the same reason Pinger exists.
type DBHealth struct {
	Healthy       bool
	Utilization   float64
	AcquiredConns int32
	MaxConns      int32
}

// DBPinger is Pinger's richer counterpart for the one /readyz dependency
// (Postgres) whose response carries diagnostic numbers beyond a bare
// healthy/unhealthy bit.
type DBPinger interface {
	Health(ctx context.Context) DBHealth
}

// RouterConfig bundles every dependency NewRouter needs: the already-built
// handler (composition root's job to construct), the /readyz pingers, and
// the shared gincommon/internal-token configuration.
type RouterConfig struct {
	GinConfig        gincommon.Config
	AppEnv           string
	InternalAPIToken string

	Handler *handler.Handler

	DB       DBPinger
	Cache    Pinger
	Temporal Pinger
}

// Router owns the Gin engine for this service.
type Router struct {
	engine *gin.Engine
}

// Handler returns the http.Handler to serve.
func (r *Router) Handler() http.Handler { return r.engine }

// NewRouter builds and wires every route this service exposes: the dev-only
// AsyncAPI doc route, the unauthenticated infra probes, and the protected
// /internal + /api/v1 route groups. TimeoutMiddleware is applied first, ahead
// of every other middleware and route, per its own doc comment's ordering
// requirement.
func NewRouter(cfg RouterConfig) *Router {
	if cfg.AppEnv != "dev" {
		gin.SetMode(gin.ReleaseMode)
	}
	r := gin.New()
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

	// /internal is service-to-service only, never a descendant of /api/v1's
	// group — gin subgroups inherit every parent .Use(), and these routes
	// must never see the gateway-identity-assuming ProtectedMiddlewares chain.
	internal := r.Group("/internal")
	internal.Use(middleware.RequireInternalToken(cfg.InternalAPIToken))

	// /internal/workflows/* (LLD §5.8).
	internalWorkflows := internal.Group("/workflows")
	internalWorkflows.POST("/reassign-delegate", h.Idempotent(h.ReassignDelegate))
	internalWorkflows.POST("/cancel-by-delegate", h.Idempotent(h.CancelByDelegate))
	internalWorkflows.GET("/delegate-impact", h.DelegateImpact)

	// POST /internal/events (LLD §6.1) stays registered as the legacy
	// catch-all alongside the 3 category-scoped subpaths event_consumer
	// forwards to directly (internal/forwarder/category.go) — workflow.task.created
	// and any future/unrecognized type still arrive here, never dropped.
	internal.POST("/events", h.HandleInternalEvent)
	internal.POST("/events/delegation", h.HandleDelegationEvents)
	internal.POST("/events/user-profile", h.HandleUserProfileEvents)
	internal.POST("/events/tenant", h.HandleTenantEvents)

	// POST /internal/connector-tasks/:id/{complete,fail} — cmd/connector-worker
	// calls these instead of touching the Temporal SDK directly (the
	// workflow-connectors LLD §6.1 Decision #2); the human /tasks/:id/complete
	// path explicitly rejects connector-typed tasks (checkHumanActionable), so
	// this is their only completion path. No idempotency wrapper:
	// connector-worker's own retry model is Stream-redelivery-driven, not
	// header-driven, and ConnectorTaskService already carries its own
	// state+dedup idempotency guard.
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
