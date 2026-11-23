package http

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/middleware"
)

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
	GinConfig gincommon.Config
	AppEnv    string

	Handler *handler.Handler

	DB       DBPinger
	Cache    Pinger
	Temporal Pinger

	ShuttingDown func() bool
}

const RequestTimeout = 30 * time.Second

const eventRoutePrefix = "/api/v1/internal/events/"

// requestTimeout leaves the event routes out: they run detached from the request and must answer
// with their own result once effects have committed, not with a late 503 (execution LLD §6.2).
func requestTimeout(d time.Duration) gin.HandlerFunc {
	timeout := gincommon.TimeoutMiddleware(d)
	return func(c *gin.Context) {
		if strings.HasPrefix(c.FullPath(), eventRoutePrefix) {
			c.Next()
			return
		}
		timeout(c)
	}
}

type Router struct {
	engine *gin.Engine
}

func (r *Router) Handler() http.Handler { return r.engine }

func NewRouter(cfg RouterConfig) *Router {
	dev := cfg.AppEnv == "dev" || cfg.AppEnv == "local"
	if !dev {
		gin.SetMode(gin.ReleaseMode)
	}
	r := gin.New()
	r.UseRawPath = true
	r.RedirectTrailingSlash = false
	r.RedirectFixedPath = false
	r.HandleMethodNotAllowed = true
	h := cfg.Handler
	r.NoRoute(handler.NotFound)
	r.NoMethod(handler.MethodNotAllowed)

	if dev {
		r.GET("/asyncapi", AsyncAPIHandler(cfg.AppEnv))
		r.HEAD("/asyncapi", AsyncAPIHandler(cfg.AppEnv))
	}

	cfg.GinConfig.RequestTimeout = 0
	for _, mw := range gincommon.ObservabilityMiddlewares(cfg.GinConfig) {
		r.Use(mw)
	}
	r.Use(requestTimeout(RequestTimeout))
	r.Use(middleware.LogRequestErrors(cfg.GinConfig.Logger))
	r.Use(handler.RejectNulInRequestStrings())

	ready := readyzHandler(cfg.DB, cfg.Cache, cfg.Temporal, cfg.ShuttingDown)
	r.GET("/healthz", gincommon.HealthHandler())
	r.HEAD("/healthz", gincommon.HealthHandler())
	r.GET("/readyz", ready)
	r.HEAD("/readyz", ready)

	internal := r.Group("/api/v1/internal")
	internal.Use(middleware.RequireSystemCaller())

	internalWorkflows := internal.Group("/workflows")
	internalWorkflows.POST("/reassign-delegate", h.Idempotent(h.ReassignDelegate))
	internalWorkflows.POST("/cancel-by-delegate", h.Idempotent(h.CancelByDelegate))
	internalWorkflows.GET("/delegate-impact", h.DelegateImpact)
	internalWorkflows.GET("/:workflow_id/active-instances", h.ActiveInstances)
	internal.POST("/users/:user_id/pause-tasks", h.PauseUserTasks)

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
	api.Use(middleware.RequireUUIDIdentity(cfg.GinConfig.Logger))

	tasks := api.Group("/tasks")
	tasks.GET("", h.ListTasks)
	tasks.GET("/:id", h.GetTask)
	tasks.POST("/:id/claim", h.Idempotent(h.ClaimTask))
	tasks.POST("/:id/complete", h.Idempotent(h.CompleteTask))
	tasks.POST("/:id/defer", h.Idempotent(h.DeferTask))
	tasks.POST("/:id/reassign", h.Idempotent(h.ReassignTask))

	api.GET("/workflows/active-by-user", h.ListActiveByUser)
	api.POST("/instances/:id/nodes/:node/override", h.Idempotent(h.OverrideNodeAssignee))
	api.POST("/instances/:id/nodes/:node/complete", h.Idempotent(h.CompleteOutsideWait))
	api.POST("/instances/:id/nodes/:node/boundaries/:boundary/fire", h.Idempotent(h.FireOutsideBoundary))

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

func readyzHandler(db DBPinger, cache, temporal Pinger, shuttingDown func() bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		if shuttingDown != nil && shuttingDown() {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "unavailable", "shutting_down": true})
			return
		}

		dbCtx, dbCancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
		defer dbCancel()
		hs := db.Health(dbCtx)
		if !hs.Healthy {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "unavailable", "db": "unreachable"})
			return
		}

		temporalCtx, temporalCancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
		defer temporalCancel()
		if err := temporal.Ping(temporalCtx); err != nil {
			c.JSON(http.StatusServiceUnavailable, gin.H{"status": "unavailable", "temporal": "unreachable"})
			return
		}

		body := gin.H{
			"status":         "OK",
			"db_utilization": hs.Utilization,
			"db_conns":       hs.AcquiredConns,
			"db_max_conns":   hs.MaxConns,
		}
		cacheCtx, cacheCancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
		defer cacheCancel()
		if err := cache.Ping(cacheCtx); err != nil {
			body["cache"] = "degraded"
		}
		c.JSON(http.StatusOK, body)
	}
}
