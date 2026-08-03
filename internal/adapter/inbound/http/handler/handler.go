// Package handler is the HTTP surface for /tasks, GET /workflows/active-by-user,
// and POST /instances/:id/nodes/:node/override. Route registration lives in
// router.go's RegisterRoutes — this package never starts a gin.Engine or
// touches cmd/server.
package handler

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"
)

// adminRoles are the x-tenant-roles values granting AdminRole (LLD §9.2).
var adminRoles = map[string]bool{
	"tenant_admin": true,
	"tenant_owner": true,
}

const (
	defaultPageLimit  = 25
	maxPageLimit      = 100
	departmentsHeader = "x-departments"

	// maxBodyBytes bounds every request body this package binds (LLD §5.10:
	// 413 is a floor on every operation with a body).
	maxBodyBytes = 10 << 20
)

// defaultIdempotencyTTL is used when Services.IdempotencyTTL is left zero.
const defaultIdempotencyTTL = 24 * time.Hour

// Handler codes against port interfaces, not a concrete internal/core/service
// implementation; tests inject hand-rolled fakes.
type Handler struct {
	tasks          port.TaskService
	eligibility    port.EligibilityChecker
	workflowClient port.WorkflowClient
	cache          port.CacheStore
	idempotencyTTL time.Duration

	processedEvents port.ProcessedEventRepository
	recency         port.RecencyGuard
	delegation      port.DelegationReconciler
	tenantLifecycle port.TenantLifecycleReconciler
	userSafetyNet   port.UserSafetyNetReconciler
	oooAvailability port.OOOAvailabilityReconciler
	templateCache   port.TemplateCachePrewarmer
	log             port.Logger
}

type Services struct {
	Tasks          port.TaskService
	Eligibility    port.EligibilityChecker
	WorkflowClient port.WorkflowClient
	// Cache is nil-safe: WithIdempotency no-ops when it's nil, which is the
	// expected state until T2.1's composition root constructs a real
	// valkey.Cache and passes it in.
	Cache          port.CacheStore
	IdempotencyTTL time.Duration

	ProcessedEvents port.ProcessedEventRepository
	Recency         port.RecencyGuard
	Delegation      port.DelegationReconciler
	TenantLifecycle port.TenantLifecycleReconciler
	UserSafetyNet   port.UserSafetyNetReconciler
	OOOAvailability port.OOOAvailabilityReconciler
	TemplateCache   port.TemplateCachePrewarmer
	// Log is nil-safe throughout this package — every call site guards it.
	Log port.Logger
}

func New(s Services) *Handler {
	ttl := s.IdempotencyTTL
	if ttl == 0 {
		ttl = defaultIdempotencyTTL
	}
	return &Handler{
		tasks:          s.Tasks,
		eligibility:    s.Eligibility,
		workflowClient: s.WorkflowClient,
		cache:          s.Cache,
		idempotencyTTL: ttl,

		processedEvents: s.ProcessedEvents,
		recency:         s.Recency,
		delegation:      s.Delegation,
		tenantLifecycle: s.TenantLifecycle,
		userSafetyNet:   s.UserSafetyNet,
		oooAvailability: s.OOOAvailability,
		templateCache:   s.TemplateCache,
		log:             s.Log,
	}
}

func callerIdentity(c *gin.Context) (tenantID, userID uuid.UUID, ok bool) {
	rc, exists := gincommon.RequestContext(c)
	if !exists {
		writeProblem(c, http.StatusInternalServerError, CodeInternal, "missing request context", nil)
		return uuid.Nil, uuid.Nil, false
	}
	tenantID, err := uuid.Parse(rc.TenantID)
	if err != nil {
		writeProblem(c, http.StatusInternalServerError, CodeInternal, "malformed tenant id in request context", nil)
		return uuid.Nil, uuid.Nil, false
	}
	userID, err = uuid.Parse(rc.UserID)
	if err != nil {
		writeProblem(c, http.StatusInternalServerError, CodeInternal, "malformed user id in request context", nil)
		return uuid.Nil, uuid.Nil, false
	}
	return tenantID, userID, true
}

// isAdmin bypasses the intra-tenant read-scope check entirely (LLD §9.2).
func isAdmin(c *gin.Context) bool {
	rc, ok := gincommon.RequestContext(c)
	if !ok {
		return false
	}
	for _, role := range rc.Roles {
		if adminRoles[role] {
			return true
		}
	}
	return false
}

func callerDepartments(c *gin.Context) []string {
	raw := c.GetHeader(departmentsHeader)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	departments := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			departments = append(departments, p)
		}
	}
	return departments
}

// bindJSON enforces the request-body floor every mutating endpoint documents
// (LLD §5.10): 415 on a non-JSON Content-Type, 413 on a body over
// maxBodyBytes, 400 on anything else ShouldBindJSON rejects.
func bindJSON(c *gin.Context, req any) bool {
	if ct := c.GetHeader("Content-Type"); ct != "" && !strings.HasPrefix(ct, "application/json") {
		writeProblem(c, http.StatusUnsupportedMediaType, CodeUnsupportedMediaType, "request body must be application/json", nil)
		return false
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxBodyBytes)
	if err := c.ShouldBindJSON(req); err != nil {
		bindErrResponse(c, err)
		return false
	}
	return true
}

func parseIDParam(c *gin.Context) (uuid.UUID, bool) {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		writeProblem(c, http.StatusBadRequest, CodeBadRequest, "invalid id path parameter", nil)
		return uuid.Nil, false
	}
	return id, true
}

func (h *Handler) logInfo(msg string, fields map[string]any) {
	if h.log != nil {
		h.log.Info(msg, fields)
	}
}

func (h *Handler) logWarn(msg string, fields map[string]any) {
	if h.log != nil {
		h.log.Warn(msg, fields)
	}
}

func (h *Handler) logError(msg string, fields map[string]any) {
	if h.log != nil {
		h.log.Error(msg, fields)
	}
}

// pageParams round-trips the opaque cursor (LLD §5.9); decoding happens
// wherever it's actually interpreted, not in this handler.
func pageParams(c *gin.Context) port.Page {
	limit := defaultPageLimit
	if raw := c.Query("limit"); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v > 0 {
			limit = v
		}
	}
	if limit > maxPageLimit {
		limit = maxPageLimit
	}
	return port.Page{Cursor: c.Query("cursor"), Limit: limit}
}
