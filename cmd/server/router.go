package main

import (
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.temporal.io/sdk/client"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/middleware"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func newRouter(cfg *config.Config, pool *pgcommon.Pool, cache port.CacheStore, sdk client.Client, log port.Logger, h *handler.Handler) *gin.Engine {
	if cfg.AppEnv != "dev" {
		gin.SetMode(gin.ReleaseMode)
	}
	r := gin.New()

	mwCfg := gincommon.Config{Logger: log, ServiceName: cfg.OTELServiceName, BuildVersion: cfg.BuildVersion}
	for _, mw := range gincommon.ObservabilityMiddlewares(mwCfg) {
		r.Use(mw)
	}

	r.GET("/healthz", gincommon.HealthHandler())
	r.GET("/readyz", readyzHandler(pool, cache, sdk))
	r.GET("/metrics", gin.WrapH(promhttp.Handler()))

	// /internal is service-to-service only, never a descendant of /api/v1's
	// group — gin subgroups inherit every parent .Use(), and these routes
	// must never see the gateway-identity-assuming ProtectedMiddlewares chain.
	internal := r.Group("/internal")
	internal.Use(middleware.RequireInternalToken(cfg.InternalAPIToken))
	handler.RegisterInternalRoutes(internal, h)
	handler.RegisterInternalEventsRoutes(internal, h)
	handler.RegisterInternalConnectorRoutes(internal, h)

	api := r.Group("/api/v1")
	for _, mw := range gincommon.ProtectedMiddlewares(mwCfg) {
		api.Use(mw)
	}
	handler.RegisterRoutes(api, h)

	return r
}
