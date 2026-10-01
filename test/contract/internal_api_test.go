package contract_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	httpadapter "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/contract"
)

const internalSpec = "../../api/internal-openapi.yaml"

type upPinger struct{}

func (upPinger) Ping(context.Context) error { return nil }

type upDB struct{}

func (upDB) Health(context.Context) httpadapter.DBHealth { return httpadapter.DBHealth{Healthy: true} }

type guard struct {
	count int32
	err   error
	got   [2]uuid.UUID
}

func (g *guard) CheckActiveInstances(_ context.Context, tenantID, workflowID uuid.UUID) (bool, int32, error) {
	g.got = [2]uuid.UUID{tenantID, workflowID}
	return g.count > 0, g.count, g.err
}

type pauser struct {
	err error
	got [2]uuid.UUID
}

func (p *pauser) PauseUserTasks(_ context.Context, tenantID, userID uuid.UUID) error {
	p.got = [2]uuid.UUID{tenantID, userID}
	return p.err
}

func internalRouter(g *guard, p *pauser) http.Handler {
	gin.SetMode(gin.TestMode)
	return httpadapter.NewRouter(httpadapter.RouterConfig{
		GinConfig: gincommon.Config{ServiceName: "execution-contract-test"}, AppEnv: "test", InternalAPIToken: "internal-secret",
		Handler: handler.New(handler.Services{ArchiveGuard: g, UserTaskPauser: p}),
		DB:      upDB{}, Cache: upPinger{}, Temporal: upPinger{},
	}).Handler()
}

type exchange struct {
	name, method, path, body, token string
	want                            int
	validReq                        bool
}

func run(t *testing.T, spec *contract.Spec, router http.Handler, tc exchange) {
	t.Helper()
	var body *strings.Reader
	if tc.body != "" {
		body = strings.NewReader(tc.body)
	}
	var req *http.Request
	if body != nil {
		req = httptest.NewRequest(tc.method, tc.path, body)
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(tc.method, tc.path, nil)
	}
	if tc.token != "" {
		req.Header.Set("x-internal-token", tc.token)
	}
	if tc.validReq {
		spec.Request(t, req)
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != tc.want {
		t.Fatalf("status %d, want %d: %s", w.Code, tc.want, w.Body.String())
	}
	spec.Response(t, req, w.Code, w.Header(), w.Body.Bytes())
}

func TestInternalContract_ActiveInstances(t *testing.T) {
	spec := contract.Load(t, internalSpec)
	tenantID, workflowID := uuid.New(), uuid.New()
	path := "/api/v1/internal/workflows/" + workflowID.String() + "/active-instances?tenant_id=" + tenantID.String()
	g := &guard{count: 2}
	router := internalRouter(g, &pauser{})

	for _, tc := range []exchange{
		{name: "active", method: http.MethodGet, path: path, token: "internal-secret", want: http.StatusOK, validReq: true},
		{name: "bad workflow id", method: http.MethodGet, path: "/api/v1/internal/workflows/nope/active-instances?tenant_id=" + tenantID.String(), token: "internal-secret", want: http.StatusBadRequest},
		{name: "no tenant", method: http.MethodGet, path: "/api/v1/internal/workflows/" + workflowID.String() + "/active-instances", token: "internal-secret", want: http.StatusBadRequest},
		{name: "no token", method: http.MethodGet, path: path, want: http.StatusUnauthorized, validReq: true},
	} {
		t.Run(tc.name, func(t *testing.T) { run(t, spec, router, tc) })
	}
	if g.got != [2]uuid.UUID{tenantID, workflowID} {
		t.Errorf("guard asked about %v, want tenant %s workflow %s", g.got, tenantID, workflowID)
	}

	t.Run("guard failure", func(t *testing.T) {
		run(t, spec, internalRouter(&guard{err: errors.New("db down")}, &pauser{}),
			exchange{method: http.MethodGet, path: path, token: "internal-secret", want: http.StatusInternalServerError, validReq: true})
	})
}

func TestInternalContract_PauseTasks(t *testing.T) {
	spec := contract.Load(t, internalSpec)
	tenantID, userID := uuid.New(), uuid.New()
	path := "/api/v1/internal/users/" + userID.String() + "/pause-tasks"
	body := `{"tenant_id":"` + tenantID.String() + `"}`
	p := &pauser{}
	router := internalRouter(&guard{}, p)

	for _, tc := range []exchange{
		{name: "paused", method: http.MethodPost, path: path, body: body, token: "internal-secret", want: http.StatusNoContent, validReq: true},
		{name: "bad user id", method: http.MethodPost, path: "/api/v1/internal/users/nope/pause-tasks", body: body, token: "internal-secret", want: http.StatusBadRequest},
		{name: "no tenant", method: http.MethodPost, path: path, body: `{}`, token: "internal-secret", want: http.StatusBadRequest},
		{name: "no token", method: http.MethodPost, path: path, body: body, want: http.StatusUnauthorized, validReq: true},
	} {
		t.Run(tc.name, func(t *testing.T) { run(t, spec, router, tc) })
	}
	if p.got != [2]uuid.UUID{tenantID, userID} {
		t.Errorf("paused %v, want tenant %s user %s", p.got, tenantID, userID)
	}

	t.Run("pauser failure", func(t *testing.T) {
		run(t, spec, internalRouter(&guard{}, &pauser{err: errors.New("db down")}),
			exchange{method: http.MethodPost, path: path, body: body, token: "internal-secret", want: http.StatusInternalServerError, validReq: true})
	})
}

// TestSpecs_AreValid holds both of this service's documents to OpenAPI 3:
// Swagger UI mounts them side by side.
func TestSpecs_AreValid(t *testing.T) {
	contract.Load(t, internalSpec)
	contract.Load(t, "../../docs/swagger/openapi.yaml")
}
