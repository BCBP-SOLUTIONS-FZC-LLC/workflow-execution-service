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

var adminRoles = map[string]bool{
	"tenant_admin": true,
	"tenant_owner": true,
}

const (
	defaultPageLimit  = 25
	maxPageLimit      = 100
	departmentsHeader = "x-departments"

	maxBodyBytes = 10 << 20
)

const defaultIdempotencyTTL = 24 * time.Hour

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
	eventDecoder    port.EventDecoder
	connectorTasks  port.ConnectorTaskService
	connectorEvents port.ConnectorEventPublisher
	principals      port.AutomationPrincipalResolver
	archiveGuard    port.ArchiveGuard
	userTaskPauser  port.UserTaskPauser
	log             port.Logger
}

type Services struct {
	Tasks          port.TaskService
	Instances      port.InstanceService
	Eligibility    port.EligibilityChecker
	WorkflowClient port.WorkflowClient
	Cache          port.CacheStore
	IdempotencyTTL time.Duration

	ProcessedEvents      port.ProcessedEventRepository
	Recency              port.RecencyGuard
	Delegation           port.DelegationReconciler
	TenantLifecycle      port.TenantLifecycleReconciler
	UserSafetyNet        port.UserSafetyNetReconciler
	OOOAvailability      port.OOOAvailabilityReconciler
	EventDecoder         port.EventDecoder
	ConnectorTasks       port.ConnectorTaskService
	ConnectorEvents      port.ConnectorEventPublisher
	AutomationPrincipals port.AutomationPrincipalResolver
	ArchiveGuard         port.ArchiveGuard
	UserTaskPauser       port.UserTaskPauser
	Log                  port.Logger
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
		eventDecoder:    s.EventDecoder,
		connectorTasks:  s.ConnectorTasks,
		connectorEvents: s.ConnectorEvents,
		principals:      s.AutomationPrincipals,
		archiveGuard:    s.ArchiveGuard,
		userTaskPauser:  s.UserTaskPauser,
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

func requireAdmin(c *gin.Context) bool {
	if !isAdmin(c) {
		writeProblem(c, http.StatusForbidden, CodeForbidden, "caller lacks tenant_admin/tenant_owner role", nil)
		return false
	}
	return true
}

func callerDepartments(c *gin.Context) []port.DepartmentRole {
	raw := c.GetHeader(departmentsHeader)
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	departments := make([]port.DepartmentRole, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		idPart, role, found := strings.Cut(p, ":")
		if !found {
			continue
		}
		deptID, err := uuid.Parse(idPart)
		if err != nil {
			continue
		}
		departments = append(departments, port.DepartmentRole{DepartmentID: deptID, Role: role})
	}
	return departments
}

func bindJSON(c *gin.Context, req any) bool {
	if ct := c.GetHeader("Content-Type"); ct != "" && !strings.HasPrefix(ct, "application/json") {
		writeProblem(c, http.StatusUnsupportedMediaType, CodeUnsupportedMediaType, "request body must be application/json", nil)
		return false
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxBodyBytes)
	if err := bindStrictJSON(c, req); err != nil {
		bindErrResponse(c, err)
		return false
	}
	return true
}

func bindInternalJSON(c *gin.Context, req any) bool {
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
