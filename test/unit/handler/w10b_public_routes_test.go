package handler_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	inboundhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/valkeystream"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"
)

type w10bParam struct {
	Name   string `json:"name"`
	Reason string `json:"reason"`
}

type w10bProblem struct {
	Code          string      `json:"code"`
	Status        int         `json:"status"`
	Detail        string      `json:"detail"`
	Instance      string      `json:"instance"`
	InvalidParams []w10bParam `json:"invalid_params"`
}

func w10bDecodeProblem(t *testing.T, w *httptest.ResponseRecorder, status int) w10bProblem {
	t.Helper()
	require.Equal(t, status, w.Code, w.Body.String())
	assert.Equal(t, "application/problem+json", strings.Split(w.Header().Get("Content-Type"), ";")[0])
	var p w10bProblem
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &p), w.Body.String())
	assert.Equal(t, status, p.Status)
	return p
}

// w10bRequest builds a request with the gateway identity headers, an admin
// role and a raw body.
func w10bRequest(method, target, contentType, rawBody string) *http.Request {
	r := httptest.NewRequest(method, target, strings.NewReader(rawBody))
	if contentType != "" {
		r.Header.Set("Content-Type", contentType)
	}
	r.Header.Set("x-tenant-id", testTenantID.String())
	r.Header.Set("x-user-id", testUserID.String())
	r.Header.Set("x-tenant-roles", "tenant_admin")
	return r
}

// --- connector task events: department id spelling ---

type w10bConnectorPublisher struct {
	mu     sync.Mutex
	events []port.ConnectorTaskCreatedEvent
}

func (p *w10bConnectorPublisher) PublishTaskCreated(_ context.Context, event port.ConnectorTaskCreatedEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, event)
	return nil
}

type w10bStreamRecorder struct {
	streamKey string
	fields    map[string]string
	calls     int
}

func (r *w10bStreamRecorder) Publish(_ context.Context, streamKey string, fields map[string]string) (string, error) {
	r.calls++
	r.streamKey = streamKey
	r.fields = fields
	return "1-0", nil
}

func TestWorkflowTaskCreated_NonCanonicalDepartmentID_StreamGetsCanonicalLowerCaseForm(t *testing.T) {
	const canonical = "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d"
	cases := []struct {
		name     string
		spelling string
	}{
		{name: "upper case", spelling: strings.ToUpper(canonical)},
		{name: "braces", spelling: "{" + canonical + "}"},
		{name: "urn prefix", spelling: "urn:uuid:" + canonical},
		{name: "no hyphens", spelling: strings.ReplaceAll(canonical, "-", "")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			publisher := &w10bConnectorPublisher{}
			router := inboundhttp.NewRouter(inboundhttp.RouterConfig{
				GinConfig: gincommon.Config{
					ServiceName: "workflow-execution-service", BuildVersion: "test", Domain: observability.Domain, Environment: "test",
				},
				AppEnv: "test",
				Handler: handler.New(handler.Services{
					ProcessedEvents: newFakeProcessedEventRepository(),
					ConnectorEvents: publisher,
				}),
			}).Handler()
			eventID := uuid.New()
			body := map[string]any{
				"id":        eventID.String(),
				"type":      "WorkflowTaskCreated",
				"tenant_id": testTenantID.String(),
				"time":      time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC).Format(time.RFC3339),
				"data": map[string]any{
					"workflow_instance_id": testInstID.String(),
					"task_id":              testTaskID.String(),
					"node_key":             "dept-1/nodeA",
					"department_id":        tc.spelling,
					"connector_type":       "storage",
					"resolved_inputs":      map[string]any{"bucket": "b1"},
					"output_mapping":       []map[string]any{},
				},
			}

			rawBody, err := json.Marshal(body)
			require.NoError(t, err)
			r := httptest.NewRequest(http.MethodPost, "/api/v1/internal/events/workflow-task", strings.NewReader(string(rawBody)))
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("x-user-id", testUserID.String())
			r.Header.Set("x-tenant-roles", "iam-system")
			w := httptest.NewRecorder()
			router.ServeHTTP(w, r)

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			require.Len(t, publisher.events, 1)
			event := publisher.events[0]
			assert.Equal(t, eventID, event.EventID)
			assert.Equal(t, testTenantID, event.TenantID)
			assert.Equal(t, testInstID, event.InstanceID)
			assert.Equal(t, testTaskID, event.TaskID)
			assert.Equal(t, canonical, event.DepartmentID.String())

			stream := &w10bStreamRecorder{}
			require.NoError(t, valkeystream.NewEventPublisher(stream, "w10b-connector-tasks").PublishTaskCreated(context.Background(), event))
			require.Equal(t, 1, stream.calls)
			assert.Equal(t, "w10b-connector-tasks", stream.streamKey)
			assert.Equal(t, canonical, stream.fields["department_id"], "the stream carries the canonical lower-case form, not the caller's spelling")
			assert.Equal(t, testTaskID.String(), stream.fields["task_id"])
		})
	}
}

// --- invalid uuid fields: list limits ---

func TestStartInstance_MalformedOverrideMapIDs_InvalidParamsAreCappedSortedAndCut(t *testing.T) {
	const prefix = "override_map."
	manyKeys := map[string]any{}
	var wantFirstTwenty []string
	for i := range 25 {
		key := fmt.Sprintf("k%02d", i)
		manyKeys[key] = "not-a-uuid"
		if i < 20 {
			wantFirstTwenty = append(wantFirstTwenty, prefix+key)
		}
	}
	longKey := strings.Repeat("é", 100)
	exactKey := strings.Repeat("é", 64-len([]rune(prefix)))

	cases := []struct {
		name        string
		overrideMap map[string]any
		want        []string
	}{
		{
			name:        "more than twenty names list the first twenty ascending",
			overrideMap: manyKeys,
			want:        wantFirstTwenty,
		},
		{
			name:        "a path longer than 64 runes is cut to 64 runes",
			overrideMap: map[string]any{longKey: "not-a-uuid", "a": "not-a-uuid"},
			want:        []string{prefix + "a", prefix + strings.Repeat("é", 64-len([]rune(prefix)))},
		},
		{
			name:        "a path of exactly 64 runes is kept whole",
			overrideMap: map[string]any{exactKey: "not-a-uuid"},
			want:        []string{prefix + exactKey},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			router := newRouter(newInstanceHandler(&fakeInstanceService{
				start: func(context.Context, port.StartInstanceInput) (*port.Instance, error) {
					called = true
					return &port.Instance{}, nil
				},
			}))
			raw, err := json.Marshal(map[string]any{
				"subject_id":          "SUBJ-1",
				"business_key":        "TND-001",
				"workflow_version_id": uuid.NewString(),
				"override_map":        tc.overrideMap,
			})
			require.NoError(t, err)

			w := do(router, w10bRequest(http.MethodPost, "/api/v1/instances", "application/json", string(raw)))

			p := w10bDecodeProblem(t, w, http.StatusBadRequest)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			assert.Equal(t, "request body has missing or invalid fields", p.Detail)
			got := make([]string, len(p.InvalidParams))
			for i, param := range p.InvalidParams {
				got[i] = param.Name
				assert.Equal(t, "is not valid", param.Reason)
				assert.LessOrEqual(t, len([]rune(param.Name)), 64)
			}
			assert.Equal(t, tc.want, got)
			assert.False(t, called, "a refused body must not start an instance")
		})
	}
}

// --- terminate: refused requests ---

func TestTerminateInstance_RefusedRequest_Returns400WithProblemBody(t *testing.T) {
	cases := []struct {
		name       string
		id         string
		body       string
		wantDetail string
		wantParams []w10bParam
	}{
		{
			name:       "data after the json value",
			id:         testInstID.String(),
			body:       `{"reason":"x"}{"reason":"y"}`,
			wantDetail: "request body has data after its JSON value",
		},
		{
			name:       "reason of the wrong type",
			id:         testInstID.String(),
			body:       `{"reason":123}`,
			wantDetail: "request body has a field of the wrong type",
			wantParams: []w10bParam{{Name: "reason", Reason: "has the wrong type"}},
		},
		{
			name:       "instance id is not a uuid",
			id:         "not-a-uuid",
			body:       `{"reason":"x"}`,
			wantDetail: "invalid id path parameter",
			wantParams: []w10bParam{{Name: "id", Reason: "is not valid"}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			terminated := 0
			router := newRouter(newInstanceHandler(&fakeInstanceService{
				terminate: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string) error {
					terminated++
					return nil
				},
			}))

			w := do(router, w10bRequest(http.MethodPost, "/api/v1/instances/"+tc.id+"/terminate", "application/json", tc.body))

			p := w10bDecodeProblem(t, w, http.StatusBadRequest)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			assert.Equal(t, tc.wantDetail, p.Detail)
			assert.Equal(t, tc.wantParams, p.InvalidParams)
			assert.Zero(t, terminated, "a refused request must not terminate the instance")
		})
	}
}

// --- complete outside wait: body framing ---

func TestCompleteOutsideWait_UnacceptableBody_IsRefusedBeforeTheService(t *testing.T) {
	oversize := `{"variables":{"a":"` + strings.Repeat("x", 10<<20) + `"}}`
	cases := []struct {
		name        string
		contentType string
		body        string
		wantStatus  int
		wantCode    string
		wantDetail  string
	}{
		{
			name:        "text/plain body",
			contentType: "text/plain",
			body:        `{"variables":{"a":1}}`,
			wantStatus:  http.StatusUnsupportedMediaType,
			wantCode:    "UNSUPPORTED_MEDIA_TYPE",
			wantDetail:  "request body must be application/json",
		},
		{
			name:        "body over 10 MB",
			contentType: "application/json",
			body:        oversize,
			wantStatus:  http.StatusRequestEntityTooLarge,
			wantCode:    "PAYLOAD_TOO_LARGE",
			wantDetail:  "request body exceeds the 10 MB limit",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			answered := 0
			router := newRouter(newInstanceHandler(&fakeInstanceService{
				answerOutsideWait: func(context.Context, uuid.UUID, uuid.UUID, port.ReadScope, string, map[string]any) error {
					answered++
					return nil
				},
			}))

			w := do(router, w10bRequest(http.MethodPost, instancePath(testInstID)+"/nodes/review/complete", tc.contentType, tc.body))

			p := w10bDecodeProblem(t, w, tc.wantStatus)
			assert.Equal(t, tc.wantCode, p.Code)
			assert.Equal(t, tc.wantDetail, p.Detail)
			assert.Zero(t, answered, "a refused body must not signal the instance")
		})
	}
}
