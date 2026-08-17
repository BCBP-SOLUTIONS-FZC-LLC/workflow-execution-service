package main

import (
	"context"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"go.temporal.io/sdk/client"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// readyzHandler checks the app pool, cache, and Temporal frontend, each
// under its own bounded timeout. Failing readiness when Temporal is
// unreachable is standard readiness-probe behavior: it removes the pod from
// load-balancer rotation until the next successful check adds it back —
// there's no real alternative to serving traffic from a pod that can't
// reach a hard dependency.
func readyzHandler(pool *pgcommon.Pool, cache port.CacheStore, sdk client.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		dbCtx, dbCancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
		defer dbCancel()
		hs := pool.Health(dbCtx)
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
		if _, err := sdk.CheckHealth(temporalCtx, &client.CheckHealthRequest{}); err != nil {
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
