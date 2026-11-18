package http_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	inboundhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var (
	w10bTenantID   = uuid.MustParse("11111111-1111-4111-8111-111111111111")
	w10bUserID     = uuid.MustParse("22222222-2222-4222-8222-222222222222")
	w10bInstanceID = uuid.MustParse("44444444-4444-4444-8444-444444444444")
	w10bNewUserID  = uuid.MustParse("55555555-5555-4555-8555-555555555555")
)

type w10bCall struct {
	op            string
	tenantID      uuid.UUID
	instanceID    uuid.UUID
	userID        uuid.UUID
	reason        string
	node          string
	recordVersion int64
	variables     map[string]any
}

type w10bRecorder struct {
	mu    sync.Mutex
	calls []w10bCall
}

func (r *w10bRecorder) add(c w10bCall) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, c)
}

func (r *w10bRecorder) all() []w10bCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]w10bCall(nil), r.calls...)
}

// w10bInstances implements only the instance operations these tests send;
// the embedded interface is nil, so any other call panics and fails the test.
type w10bInstances struct {
	port.InstanceService
	rec *w10bRecorder
}

func (f *w10bInstances) Pause(_ context.Context, tenantID, instanceID, actorUserID uuid.UUID, reason string, recordVersion int64) error {
	f.rec.add(w10bCall{op: "pause", tenantID: tenantID, instanceID: instanceID, userID: actorUserID, reason: reason, recordVersion: recordVersion})
	return nil
}

func (f *w10bInstances) Cancel(_ context.Context, tenantID, instanceID, actorUserID uuid.UUID, reason string, recordVersion int64) error {
	f.rec.add(w10bCall{op: "cancel", tenantID: tenantID, instanceID: instanceID, userID: actorUserID, reason: reason, recordVersion: recordVersion})
	return nil
}

func (f *w10bInstances) ForceBack(_ context.Context, tenantID, instanceID, actorUserID uuid.UUID, targetNodeKey string, recordVersion int64) error {
	f.rec.add(w10bCall{op: "force-back", tenantID: tenantID, instanceID: instanceID, userID: actorUserID, node: targetNodeKey, recordVersion: recordVersion})
	return nil
}

func (f *w10bInstances) AnswerOutsideWait(_ context.Context, tenantID, instanceID uuid.UUID, scope port.ReadScope, key string, variables map[string]any) error {
	f.rec.add(w10bCall{op: "answer-outside-wait", tenantID: tenantID, instanceID: instanceID, userID: scope.CallerUserID, node: key, variables: variables})
	return nil
}

type w10bTasks struct {
	port.TaskService
	rec *w10bRecorder
}

func (f *w10bTasks) OverrideAssignee(_ context.Context, in port.AssigneeOverrideInput) (*port.AssigneeOverride, error) {
	f.rec.add(w10bCall{op: "override", tenantID: in.TenantID, instanceID: in.InstanceID, userID: in.ActorUserID, node: in.NodeKey, reason: in.Reason, recordVersion: in.RecordVersion})
	return &port.AssigneeOverride{
		WorkflowInstanceID: in.InstanceID,
		NodeKey:            in.NodeKey,
		NewUserID:          in.NewUserID,
		Reason:             in.Reason,
		ActorUserID:        in.ActorUserID,
		RecordVersion:      in.RecordVersion + 1,
	}, nil
}

type w10bCache struct {
	mu   sync.Mutex
	data map[string]string
}

func (c *w10bCache) Get(_ context.Context, key string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.data[key], nil
}

func (c *w10bCache) Set(_ context.Context, key, value string, _ time.Duration) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.data[key] = value
	return nil
}

func (c *w10bCache) Del(_ context.Context, keys ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, k := range keys {
		delete(c.data, k)
	}
	return nil
}

func (c *w10bCache) SetNX(_ context.Context, key, value string, _ time.Duration) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, taken := c.data[key]; taken {
		return false, nil
	}
	c.data[key] = value
	return true, nil
}

func (c *w10bCache) Ping(context.Context) error { return nil }

type w10bLogger struct{}

func (w10bLogger) Debug(string, map[string]interface{}) {}
func (w10bLogger) Info(string, map[string]interface{})  {}
func (w10bLogger) Warn(string, map[string]interface{})  {}
func (w10bLogger) Error(string, map[string]interface{}) {}

func w10bNewRouter(t *testing.T) (http.Handler, *w10bRecorder) {
	t.Helper()
	rec := &w10bRecorder{}
	router := inboundhttp.NewRouter(inboundhttp.RouterConfig{
		GinConfig: gincommon.Config{
			ServiceName:  "workflow-execution-service",
			BuildVersion: "test",
			Domain:       "workflow",
			Environment:  "test",
			Logger:       w10bLogger{},
		},
		AppEnv: "test",
		Handler: handler.New(handler.Services{
			Instances:      &w10bInstances{rec: rec},
			Tasks:          &w10bTasks{rec: rec},
			Cache:          &w10bCache{data: map[string]string{}},
			IdempotencyTTL: time.Hour,
		}),
	})
	return router.Handler(), rec
}

// w10bPost sends a JSON POST as an admin of the test tenant. A header value
// in `extra` replaces the default; an empty value removes the header.
func w10bPost(h http.Handler, target, body string, extra map[string]string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, target, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("x-tenant-id", w10bTenantID.String())
	r.Header.Set("x-user-id", w10bUserID.String())
	r.Header.Set("x-tenant-roles", "tenant_admin")
	for k, v := range extra {
		if v == "" {
			r.Header.Del(k)
			continue
		}
		r.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func w10bInstancePath(suffix string) string {
	return "/api/v1/instances/" + w10bInstanceID.String() + suffix
}

func TestRouter_IdempotencyKeyRepeated_ReplaysTheFirstAnswerWithOneCall(t *testing.T) {
	cases := []struct {
		name       string
		path       string
		body       string
		wantStatus int
		wantOp     string
		wantCall   w10bCall
	}{
		{
			name: "pause", path: w10bInstancePath("/pause"), body: `{"reason":"audit hold","record_version":3}`,
			wantStatus: http.StatusAccepted, wantOp: "pause",
			wantCall: w10bCall{op: "pause", tenantID: w10bTenantID, instanceID: w10bInstanceID, userID: w10bUserID, reason: "audit hold", recordVersion: 3},
		},
		{
			name: "force back", path: w10bInstancePath("/force-back"), body: `{"target_node_key":"dept/stage","record_version":4}`,
			wantStatus: http.StatusAccepted, wantOp: "force-back",
			wantCall: w10bCall{op: "force-back", tenantID: w10bTenantID, instanceID: w10bInstanceID, userID: w10bUserID, node: "dept/stage", recordVersion: 4},
		},
		{
			name: "node override", path: w10bInstancePath("/nodes/review/override"),
			body:       `{"new_user_id":"` + w10bNewUserID.String() + `","reason":"cover","record_version":5}`,
			wantStatus: http.StatusOK, wantOp: "override",
			wantCall: w10bCall{op: "override", tenantID: w10bTenantID, instanceID: w10bInstanceID, userID: w10bUserID, node: "review", reason: "cover", recordVersion: 5},
		},
		{
			name: "complete outside wait", path: w10bInstancePath("/nodes/review/complete"), body: `{"variables":{"approved":true}}`,
			wantStatus: http.StatusAccepted, wantOp: "answer-outside-wait",
			wantCall: w10bCall{op: "answer-outside-wait", tenantID: w10bTenantID, instanceID: w10bInstanceID, userID: w10bUserID, node: "review", variables: map[string]any{"approved": true}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, rec := w10bNewRouter(t)
			headers := map[string]string{"Idempotency-Key": "w10b-" + uuid.NewString()}

			first := w10bPost(h, tc.path, tc.body, headers)
			second := w10bPost(h, tc.path, tc.body, headers)

			require.Equal(t, tc.wantStatus, first.Code, first.Body.String())
			assert.Equal(t, tc.wantStatus, second.Code, "the repeat answers with the first status")
			assert.NotEmpty(t, first.Body.String())
			assert.JSONEq(t, first.Body.String(), second.Body.String(), "the repeat replays the first body")
			calls := rec.all()
			require.Len(t, calls, 1, "the repeat must not reach the service a second time")
			assert.Equal(t, tc.wantCall, calls[0])
			assert.Equal(t, tc.wantOp, calls[0].op)
		})
	}
}

func TestRouter_PauseIdempotencyKeyReusedWithAnotherBody_Returns409(t *testing.T) {
	h, rec := w10bNewRouter(t)
	headers := map[string]string{"Idempotency-Key": "w10b-" + uuid.NewString()}
	path := w10bInstancePath("/pause")

	first := w10bPost(h, path, `{"reason":"audit hold","record_version":3}`, headers)
	second := w10bPost(h, path, `{"reason":"another reason","record_version":3}`, headers)

	require.Equal(t, http.StatusAccepted, first.Code, first.Body.String())
	require.Equal(t, http.StatusConflict, second.Code, second.Body.String())
	body := w10bBody(t, second)
	assert.Equal(t, "IDEMPOTENCY_KEY_REPLAY", body["code"])
	assert.Equal(t, path, body["instance"])
	calls := rec.all()
	require.Len(t, calls, 1, "only the first body is applied")
	assert.Equal(t, "audit hold", calls[0].reason)
}

func TestRouter_UnencodedSlashInNodeSegment_Returns404FromTheNoRouteHandler(t *testing.T) {
	cases := []struct {
		name string
		path string
	}{
		{name: "fire boundary", path: w10bInstancePath("/nodes/Dept/Stage/boundaries/timeout/fire")},
		{name: "node override", path: w10bInstancePath("/nodes/eng/Review_Task/override")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, rec := w10bNewRouter(t)

			w := w10bPost(h, tc.path, `{"new_user_id":"`+w10bNewUserID.String()+`","record_version":1}`, map[string]string{"Idempotency-Key": "w10b-" + uuid.NewString()})

			require.Equal(t, http.StatusNotFound, w.Code, w.Body.String())
			body := w10bBody(t, w)
			assert.Equal(t, "NOT_FOUND", body["code"])
			assert.Equal(t, "https://errors.bcbp.io/execution/not-found", body["type"])
			assert.Equal(t, "no route matches this method and path", body["detail"])
			assert.Equal(t, tc.path, body["instance"])
			assert.Empty(t, rec.all(), "no service call for an unrouted path")
		})
	}
}

func TestRouter_CancelWithoutGatewayIdentity_Returns401WithoutReachingTheService(t *testing.T) {
	cases := []struct {
		name    string
		headers map[string]string
	}{
		{name: "no tenant id", headers: map[string]string{"x-tenant-id": ""}},
		{name: "no user id", headers: map[string]string{"x-user-id": ""}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, rec := w10bNewRouter(t)

			w := w10bPost(h, w10bInstancePath("/cancel"), `{"reason":"duplicate","record_version":1}`, tc.headers)

			require.Equal(t, http.StatusUnauthorized, w.Code, w.Body.String())
			body := w10bBody(t, w)
			assert.Equal(t, "missing or invalid authentication headers", body["error"])
			assert.EqualValues(t, http.StatusUnauthorized, body["status"])
			assert.Empty(t, rec.all(), "a refused request must not cancel the instance")
		})
	}
}

func w10bBody(t *testing.T, w *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var out map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &out), "body is not JSON: %s", w.Body.String())
	return out
}
