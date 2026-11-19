package handler_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// The force-forward and force-back routes run the real instance service behind the real handler. The instance
// repository, the Definition Service client and the Temporal client are the only fakes; each records what it got.

const w26bProblemBase = "https://errors.bcbp.io/execution/"

type w26bProblem struct {
	Type          string `json:"type"`
	Status        int    `json:"status"`
	Detail        string `json:"detail"`
	Code          string `json:"code"`
	InvalidParams []struct {
		Name   string `json:"name"`
		Reason string `json:"reason"`
	} `json:"invalid_params"`
}

func w26bProblemOf(t *testing.T, rec *httptest.ResponseRecorder, status int) w26bProblem {
	t.Helper()
	require.Equal(t, status, rec.Code, rec.Body.String())
	assert.Equal(t, "application/problem+json", strings.Split(rec.Header().Get("Content-Type"), ";")[0])
	var p w26bProblem
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &p), rec.Body.String())
	assert.Equal(t, status, p.Status)
	return p
}

type w26bLookup struct{ tenant, id uuid.UUID }

type w26bInstanceRepo struct {
	port.InstanceRepository
	rows  []*domain.Instance
	reads []w26bLookup
}

func (r *w26bInstanceRepo) GetByID(_ context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	r.reads = append(r.reads, w26bLookup{tenantID, id})
	for _, i := range r.rows {
		if i.ID == id && i.TenantID == tenantID {
			return i, nil
		}
	}
	return nil, domain.ErrNotFound
}

type w26bDefinitionClient struct {
	port.DefinitionServiceClient
	resp  *port.CompiledWorkflow
	calls []w26bLookup
}

func (d *w26bDefinitionClient) GetCompiledWorkflow(_ context.Context, tenantID, versionID uuid.UUID) (*port.CompiledWorkflow, error) {
	d.calls = append(d.calls, w26bLookup{tenantID, versionID})
	return d.resp, nil
}

type w26bSignal struct {
	workflowID string
	instanceID uuid.UUID
	name       string
	payload    any
}

type w26bTemporalClient struct {
	port.TemporalClient
	signals []w26bSignal
}

func (c *w26bTemporalClient) SignalWorkflow(_ context.Context, workflowID string, instanceID uuid.UUID, name string, payload any) error {
	c.signals = append(c.signals, w26bSignal{workflowID, instanceID, name, payload})
	return nil
}

type w26bAdminWire struct {
	AdminUserID   string
	TargetNodeKey string
	RecordVersion int64
}

// w26bRouteWorld is the caller's tenant with one DEGRADED instance (a degraded instance needs no open parallel
// step to check, so a node of the plan is accepted) whose plan holds one node, finance/review.
type w26bRouteWorld struct {
	repo        *w26bInstanceRepo
	definitions *w26bDefinitionClient
	temporal    *w26bTemporalClient
	versionID   uuid.UUID
	router      *gin.Engine
}

func w26bNewRouteWorld(t *testing.T) *w26bRouteWorld {
	t.Helper()
	w := &w26bRouteWorld{versionID: uuid.New(), temporal: &w26bTemporalClient{}}
	inst := &domain.Instance{
		ID: testInstID, TenantID: testTenantID, WorkflowVersionID: w.versionID, Status: domain.InstanceStatusDegraded,
		RecordVersion: 5, TemporalWorkflowID: "tenant:subject:w26b",
	}
	w.repo = &w26bInstanceRepo{rows: []*domain.Instance{inst}}
	raw, err := json.Marshal(dsl.CompiledCollaboration{
		SchemaVersion: dsl.CurrentSchemaVersion, MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main", TaskQueue: "wf-queue-default",
			Departments: []dsl.DepartmentDef{{
				ID: "finance", IAMDepartmentID: uuid.New().String(),
				Stages: []dsl.StageDef{{Type: "userTask", NodeID: "review", Role: "approver"}},
			}},
		}},
	})
	require.NoError(t, err)
	w.definitions = &w26bDefinitionClient{resp: &port.CompiledWorkflow{
		WorkflowID: uuid.New(), VersionID: w.versionID, VersionNumber: 1, Status: "PUBLISHED", IsValid: true, CompiledPlanJSON: string(raw),
	}}
	svc := &service.InstanceService{Instances: w.repo, Definitions: w.definitions, Temporal: w.temporal}
	w.router = newRouter(handler.New(handler.Services{Instances: svc}))
	return w
}

func w26bAdminPost(path, contentType, rawBody string) *http.Request {
	r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(rawBody))
	r.Header.Set("Content-Type", contentType)
	r.Header.Set("x-tenant-id", testTenantID.String())
	r.Header.Set("x-user-id", testUserID.String())
	r.Header.Set("x-tenant-roles", "tenant_admin")
	return r
}

func (w *w26bRouteWorld) requireNothingRead(t *testing.T) {
	t.Helper()
	assert.Empty(t, w.repo.reads, "a refused request must not reach the service")
	assert.Empty(t, w.temporal.signals, "no signal may reach the workflow")
}

func w26bForwardPath() string { return instancePath(testInstID) + "/force-forward" }

func TestForceForwardInstance_BodyIsNotJSON_Returns415(t *testing.T) {
	cases := []struct {
		name        string
		contentType string
	}{
		{name: "text/plain", contentType: "text/plain"},
		{name: "form data", contentType: "application/x-www-form-urlencoded"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			world := w26bNewRouteWorld(t)

			rec := do(world.router, w26bAdminPost(w26bForwardPath(), tc.contentType, `{"target_node_key":"finance/review","record_version":5}`))

			p := w26bProblemOf(t, rec, http.StatusUnsupportedMediaType)
			assert.Equal(t, "UNSUPPORTED_MEDIA_TYPE", p.Code)
			assert.Equal(t, w26bProblemBase+"unsupported-media-type", p.Type)
			assert.Equal(t, "request body must be application/json", p.Detail)
			world.requireNothingRead(t)
		})
	}
}

func TestForceForwardInstance_TargetNodeKeyOf256Characters_NamesTheFieldAndTheServiceIsNotCalled(t *testing.T) {
	cases := []struct {
		name string
		key  string
	}{
		{name: "256 ASCII characters", key: strings.Repeat("a", 256)},
		{name: "256 characters of a multi-byte letter", key: strings.Repeat("é", 256)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			world := w26bNewRouteWorld(t)
			body, err := json.Marshal(map[string]any{"target_node_key": tc.key, "record_version": 5})
			require.NoError(t, err)

			rec := do(world.router, w26bAdminPost(w26bForwardPath(), "application/json", string(body)))

			p := w26bProblemOf(t, rec, http.StatusBadRequest)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			assert.Equal(t, w26bProblemBase+"bad-request", p.Type)
			assert.Equal(t, "request body has missing or invalid fields", p.Detail)
			require.Len(t, p.InvalidParams, 1)
			assert.Equal(t, "target_node_key", p.InvalidParams[0].Name)
			assert.Equal(t, "is not valid", p.InvalidParams[0].Reason)
			assert.NotContains(t, rec.Body.String(), tc.key, "the refused value is not echoed")
			world.requireNothingRead(t)
		})
	}
}

func TestForceForwardInstance_TargetNodeKeyAbsentOrEmpty_NamesTheFieldAsRequired(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{name: "absent", body: `{"record_version":5}`},
		{name: "empty string", body: `{"target_node_key":"","record_version":5}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			world := w26bNewRouteWorld(t)

			rec := do(world.router, w26bAdminPost(w26bForwardPath(), "application/json", tc.body))

			p := w26bProblemOf(t, rec, http.StatusBadRequest)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			assert.Equal(t, w26bProblemBase+"bad-request", p.Type)
			assert.Equal(t, "request body has missing or invalid fields", p.Detail)
			require.Len(t, p.InvalidParams, 1)
			assert.Equal(t, "target_node_key", p.InvalidParams[0].Name)
			assert.Equal(t, "is required", p.InvalidParams[0].Reason)
			world.requireNothingRead(t)
		})
	}
}

func TestForceForwardInstance_BodyIsEmptyOrNotJSON_Returns400WithoutAnInvalidParam(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{name: "empty body", body: ""},
		{name: "truncated object", body: `{"target_node_key":"finance/review","record_ver`},
		{name: "plain text", body: `finance/review`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			world := w26bNewRouteWorld(t)

			rec := do(world.router, w26bAdminPost(w26bForwardPath(), "application/json", tc.body))

			p := w26bProblemOf(t, rec, http.StatusBadRequest)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			assert.Equal(t, w26bProblemBase+"bad-request", p.Type)
			assert.Equal(t, "request body is not valid JSON", p.Detail)
			assert.Empty(t, p.InvalidParams)
			for _, leak := range []string{"EOF", "unexpected end", "invalid character"} {
				assert.NotContains(t, rec.Body.String(), leak, "decoder text must not reach the caller")
			}
			world.requireNothingRead(t)
		})
	}
}

// A key at the 255-character cap passes binding and reaches the plan check; a key of only whitespace passes
// binding too (it is not empty): blank handling is left to the plan lookup, which finds no such node. Neither names
// a node of the plan, so the caller gets 409 and nothing is signalled; the same request naming a node of the plan
// is accepted and the key reaches the workflow.
func TestForceForwardInstance_TargetNodeKeyAtTheCapOrOnlyWhitespace_Returns409TargetNodeNotFound(t *testing.T) {
	cases := []struct {
		name string
		key  string
	}{
		{name: "exactly 255 characters", key: strings.Repeat("a", 255)},
		{name: "a single space", key: " "},
		{name: "spaces and a tab", key: "  \t "},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			world := w26bNewRouteWorld(t)
			body, err := json.Marshal(map[string]any{"target_node_key": tc.key, "record_version": 5})
			require.NoError(t, err)

			rec := do(world.router, w26bAdminPost(w26bForwardPath(), "application/json", string(body)))

			p := w26bProblemOf(t, rec, http.StatusConflict)
			assert.Equal(t, "TARGET_NODE_NOT_FOUND", p.Code)
			assert.Equal(t, w26bProblemBase+"target-node-not-found", p.Type)
			assert.Equal(t, "target node not found in the compiled plan", p.Detail)
			if tc.key != " " {
				assert.NotContains(t, rec.Body.String(), tc.key, "the refused key is not echoed")
			}
			assert.Empty(t, p.InvalidParams)
			assert.Equal(t, []w26bLookup{{testTenantID, testInstID}}, world.repo.reads, "the instance is read once, in the caller's tenant")
			assert.Equal(t, []w26bLookup{{testTenantID, world.versionID}}, world.definitions.calls, "the plan is read for the instance's version")
			assert.Empty(t, world.temporal.signals, "a refused target is never signalled")
		})
	}

	t.Run("a node of the plan is accepted", func(t *testing.T) {
		world := w26bNewRouteWorld(t)

		rec := do(world.router, w26bAdminPost(w26bForwardPath(), "application/json", `{"target_node_key":"finance/review","record_version":5}`))

		require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
		require.Len(t, world.temporal.signals, 1)
		sent := world.temporal.signals[0]
		assert.Equal(t, "tenant:subject:w26b", sent.workflowID)
		assert.Equal(t, testInstID, sent.instanceID)
		assert.Equal(t, port.SignalInstanceForceFwd, sent.name)
		raw, err := json.Marshal(sent.payload)
		require.NoError(t, err)
		var wire w26bAdminWire
		require.NoError(t, json.Unmarshal(raw, &wire))
		assert.Equal(t, testUserID.String(), wire.AdminUserID)
		assert.Equal(t, "finance/review", wire.TargetNodeKey)
		assert.EqualValues(t, 5, wire.RecordVersion)
	})
}

// The length cap is checked at binding only: 255 characters reach the service whole and 256 are refused with 400.
func TestForceBackInstance_TargetNodeKeyLength_255ReachesTheServiceAnd256IsRefusedAtBinding(t *testing.T) {
	cases := []struct {
		name       string
		key        string
		wantStatus int
	}{
		{name: "255 characters", key: strings.Repeat("k", 255), wantStatus: http.StatusAccepted},
		{name: "256 characters", key: strings.Repeat("k", 256), wantStatus: http.StatusBadRequest},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var keys []string
			fake := &fakeInstanceService{forceBack: func(_ context.Context, tenantID, instanceID, actorUserID uuid.UUID, key string, recordVersion int64) error {
				assert.Equal(t, testTenantID, tenantID)
				assert.Equal(t, testInstID, instanceID)
				assert.Equal(t, testUserID, actorUserID)
				assert.EqualValues(t, 5, recordVersion)
				keys = append(keys, key)
				return nil
			}}
			router := newRouter(newInstanceHandler(fake))
			body, err := json.Marshal(map[string]any{"target_node_key": tc.key, "record_version": 5})
			require.NoError(t, err)

			rec := do(router, w26bAdminPost(instancePath(testInstID)+"/force-back", "application/json", string(body)))

			require.Equal(t, tc.wantStatus, rec.Code, rec.Body.String())
			if tc.wantStatus == http.StatusAccepted {
				assert.Equal(t, []string{tc.key}, keys, "the whole key reaches the service")
				return
			}
			p := w26bProblemOf(t, rec, http.StatusBadRequest)
			require.Len(t, p.InvalidParams, 1)
			assert.Equal(t, "target_node_key", p.InvalidParams[0].Name)
			assert.Equal(t, "is not valid", p.InvalidParams[0].Reason)
			assert.Empty(t, keys, "a refused request must not reach the service")
		})
	}
}

func TestForceBackInstance_TargetNodeOnADegradedInstance_Returns202AndSignalsTheKey(t *testing.T) {
	world := w26bNewRouteWorld(t)
	key := "finance/review"
	body, err := json.Marshal(map[string]any{"target_node_key": key, "record_version": 5})
	require.NoError(t, err)

	rec := do(world.router, w26bAdminPost(instancePath(testInstID)+"/force-back", "application/json", string(body)))

	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	assert.Equal(t, []w26bLookup{{testTenantID, testInstID}}, world.repo.reads, "the instance is read once, in the caller's tenant")
	require.Len(t, world.temporal.signals, 1, "the signal is sent once")
	sent := world.temporal.signals[0]
	assert.Equal(t, "tenant:subject:w26b", sent.workflowID)
	assert.Equal(t, testInstID, sent.instanceID)
	assert.Equal(t, port.SignalInstanceForceBack, sent.name)
	raw, err := json.Marshal(sent.payload)
	require.NoError(t, err)
	var wire w26bAdminWire
	require.NoError(t, json.Unmarshal(raw, &wire))
	assert.Equal(t, key, wire.TargetNodeKey, "the key reaches the workflow")
	assert.Equal(t, testUserID.String(), wire.AdminUserID)
	assert.EqualValues(t, 5, wire.RecordVersion)
}
