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
