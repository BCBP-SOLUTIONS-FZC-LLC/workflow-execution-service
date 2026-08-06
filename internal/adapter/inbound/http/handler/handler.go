// Package handler is the HTTP surface for /tasks, /instances,
// GET /workflows/active-by-user, and POST /instances/:id/nodes/:node/override.
// Route registration lives in router.go's RegisterRoutes — this package
// never starts a gin.Engine or touches cmd/server.
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
	instances      port.InstanceService
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
	eventDecoder    port.EventDecoder
	log             port.Logger
}

type Services struct {
	Tasks          port.TaskService
	Instances      port.InstanceService
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
	// EventDecoder is nil-safe: HandleInternalEvent treats a nil decoder as
	// a decode failure only for envelopes that actually carry a SchemaID -
	// until a real events.Codec is wired up in cmd/server, no producer sets
	// SchemaID, so this stays unused in practice today.
	EventDecoder port.EventDecoder
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
		instances:      s.Instances,
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
		eventDecoder:    s.EventDecoder,
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

// requireAdmin writes 403 FORBIDDEN and returns false if the caller lacks
// AdminRole (LLD §9.2) — a hard gate for admin-only endpoints, unlike
// isAdmin's use inside readScope() as a visibility-check bypass.
func requireAdmin(c *gin.Context) bool {
	if !isAdmin(c) {
		writeProblem(c, http.StatusForbidden, CodeForbidden, "caller lacks tenant_admin/tenant_owner role", nil)
		return false
	}
	return true
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

// pageParams decodes and validates both query params (LLD §5.9); the false
// return means the caller must return immediately, the response is already
// written. LimitQuery: values above 100 are clamped, not rejected; values
// <= 0 (or a non-integer) are rejected with 400. CursorQuery: an unparseable
// or tampered cursor is rejected with 400 rather than silently treated as
// "no cursor", which would mask a client-side bug as a quietly-restarted
// page — decoding happens here, once, so the port boundary only ever sees an
// already-validated CursorPosition, never a raw client string.
func pageParams(c *gin.Context) (port.Page, bool) {
	limit := defaultPageLimit
	if raw := c.Query("limit"); raw != "" {
		v, err := strconv.Atoi(raw)
		if err != nil || v <= 0 {
			writeProblem(c, http.StatusBadRequest, CodeBadRequest, "limit must be a positive integer", nil)
			return port.Page{}, false
		}
		limit = v
	}
	if limit > maxPageLimit {
		limit = maxPageLimit
	}
	var cursor *port.CursorPosition
	if raw := c.Query("cursor"); raw != "" {
		pos, err := port.DecodeCursor(raw)
		if err != nil {
			writeProblem(c, http.StatusBadRequest, CodeBadRequest, "invalid cursor", nil)
			return port.Page{}, false
		}
		cursor = &pos
	}
	return port.Page{Cursor: cursor, Limit: limit}, true
}
