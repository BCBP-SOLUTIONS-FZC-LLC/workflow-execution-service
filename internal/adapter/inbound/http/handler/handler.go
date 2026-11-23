package handler

import (
	"bytes"
	"errors"
	"io"
	"mime"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/gin-gonic/gin"
	"github.com/gin-gonic/gin/binding"
	"github.com/go-playground/validator/v10"
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

	maxErrorClassBytes = 100

	reasonNotValid = "is not valid"
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
	connectorTasks  port.ConnectorTaskService
	connectorEvents port.ConnectorEventPublisher
	connectorGate   port.ConnectorDispatchGate
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
	ConnectorTasks       port.ConnectorTaskService
	ConnectorEvents      port.ConnectorEventPublisher
	ConnectorDispatch    port.ConnectorDispatchGate
	AutomationPrincipals port.AutomationPrincipalResolver
	ArchiveGuard         port.ArchiveGuard
	UserTaskPauser       port.UserTaskPauser
	Log                  port.Logger
}

func New(s Services) *Handler {
	nameFieldsByJSONTag.Do(registerJSONFieldNames)
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
		connectorTasks:  s.ConnectorTasks,
		connectorEvents: s.ConnectorEvents,
		connectorGate:   s.ConnectorDispatch,
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
	// Both ids are UUIDs here: RequireUUIDIdentity refuses anything else before a /api/v1 handler runs.
	tenantID, _ = uuid.Parse(rc.TenantID)
	userID, _ = uuid.Parse(rc.UserID)
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
	return bindBody(c, req, bindStrictJSON)
}

func bindInternalJSON(c *gin.Context, req any) bool {
	return bindBody(c, req, bindInternalStrictJSON)
}

func isJSONContentType(header string) bool {
	if header == "" {
		return true
	}
	mediaType, _, err := mime.ParseMediaType(header)
	return err == nil && mediaType == "application/json"
}

func bindBody(c *gin.Context, req any, bind func(*gin.Context, any) error) bool {
	if !isJSONContentType(c.GetHeader("Content-Type")) {
		writeProblem(c, http.StatusUnsupportedMediaType, CodeUnsupportedMediaType, "request body must be application/json", nil)
		return false
	}
	if c.Request.Body == nil {
		c.Request.Body = http.NoBody
	}
	body, err := io.ReadAll(http.MaxBytesReader(c.Writer, c.Request.Body, maxBodyBytes))
	if err != nil {
		bindErrResponse(c, err)
		return false
	}
	if jsonHasNulEscape(body) {
		writeProblem(c, http.StatusBadRequest, CodeBadRequest, "request body has a NUL character", nil)
		return false
	}
	c.Request.Body = io.NopCloser(bytes.NewReader(body))
	err = bind(c, req)
	if err == nil {
		return true
	}
	if _, unknown := errors.AsType[*unknownFieldsError](err); !unknown {
		if names := invalidUUIDFields(body, req); len(names) > 0 {
			writeInvalidFields(c, names)
			return false
		}
	}
	bindErrResponse(c, err)
	return false
}

func jsonHasNulEscape(body []byte) bool {
	for i := 0; i < len(body); i++ {
		if body[i] != '\\' {
			continue
		}
		if bytes.HasPrefix(body[i+1:], []byte(`u0000`)) {
			return true
		}
		i++
	}
	return false
}

func writeInvalidFields(c *gin.Context, names []string) {
	params := make([]invalidParam, len(names))
	for i, name := range names {
		params[i] = invalidParam{Name: name, Reason: reasonNotValid}
	}
	writeProblem(c, http.StatusBadRequest, CodeBadRequest, "request body has missing or invalid fields", params)
}

func writeInvalidParam(c *gin.Context, name, detail string) {
	writeProblem(c, http.StatusBadRequest, CodeBadRequest, detail, []invalidParam{{Name: name, Reason: reasonNotValid}})
}

// requestBodyPresent reports whether the request carries any body at all,
// whatever its framing: an empty or whitespace-only body counts as absent.
func requestBodyPresent(c *gin.Context) (present, ok bool) {
	if c.Request.Body == nil || c.Request.Body == http.NoBody {
		return false, true
	}
	body, err := io.ReadAll(http.MaxBytesReader(c.Writer, c.Request.Body, maxBodyBytes))
	if err != nil {
		bindErrResponse(c, err)
		return false, false
	}
	c.Request.Body = io.NopCloser(bytes.NewReader(body))
	return len(bytes.TrimSpace(body)) > 0, true
}

func parseIDParam(c *gin.Context) (uuid.UUID, bool) {
	id, err := uuid.Parse(c.Param("id"))
	if err != nil {
		writeInvalidParam(c, "id", "invalid id path parameter")
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

func isUnprintableErrorClassRune(r rune) bool {
	return unicode.IsControl(r) || unicode.In(r, unicode.Cf, unicode.Zl, unicode.Zp)
}

func pageParams(c *gin.Context) (port.Page, bool) {
	limit := defaultPageLimit
	if raw := c.Query("limit"); raw != "" {
		v, err := strconv.Atoi(raw)
		if errors.Is(err, strconv.ErrRange) && !strings.HasPrefix(raw, "-") {
			v, err = maxPageLimit, nil
		}
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

var nameFieldsByJSONTag sync.Once

func registerJSONFieldNames() {
	engine, ok := binding.Validator.Engine().(*validator.Validate)
	if !ok {
		return
	}
	engine.RegisterTagNameFunc(func(f reflect.StructField) string {
		name, _, _ := strings.Cut(f.Tag.Get("json"), ",")
		if name == "" || name == "-" {
			return f.Name
		}
		return name
	})
	_ = engine.RegisterValidation("notblank", func(fl validator.FieldLevel) bool {
		return strings.TrimSpace(fl.Field().String()) != ""
	})
	_ = engine.RegisterValidation("errorclass", func(fl validator.FieldLevel) bool {
		value := fl.Field().String()
		return len(value) <= maxErrorClassBytes &&
			strings.IndexFunc(value, isUnprintableErrorClassRune) < 0
	})
}
