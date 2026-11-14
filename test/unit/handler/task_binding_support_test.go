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

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type hreParam struct {
	Name   string `json:"name"`
	Reason string `json:"reason"`
}

type hreProblem struct {
	Code          string     `json:"code"`
	Status        int        `json:"status"`
	Detail        string     `json:"detail"`
	InvalidParams []hreParam `json:"invalid_params"`
}

// hreRawRequest sends the body text exactly as given, as an admin caller.
func hreRawRequest(method, path, rawBody string) *http.Request {
	r := httptest.NewRequest(method, path, strings.NewReader(rawBody))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("x-tenant-id", testTenantID.String())
	r.Header.Set("x-user-id", testUserID.String())
	r.Header.Set("x-tenant-roles", "tenant_admin")
	return r
}

func hreDecodeProblem(t *testing.T, w *httptest.ResponseRecorder) hreProblem {
	t.Helper()
	assert.Equal(t, "application/problem+json", strings.Split(w.Header().Get("Content-Type"), ";")[0])
	var prob hreProblem
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &prob), "body: %s", w.Body.String())
	assert.Equal(t, w.Code, prob.Status)
	return prob
}

// hreAssertNoInternalText fails when a decoder, validator, parser or runtime
// message reached the response body.
func hreAssertNoInternalText(t *testing.T, w *httptest.ResponseRecorder) {
	t.Helper()
	body := w.Body.String()
	for _, leak := range []string{"Go struct", "int64", "unexpected end", "EOF", "invalid character", "Key:", "Field validation", "goroutine", "panic", "invalid UUID", "UUID length", "UUID format"} {
		assert.NotContains(t, body, leak)
	}
}

func hreTaskPath(id, op string) string { return "/api/v1/tasks/" + id + "/" + op }

type hreTaskCall struct {
	op            string
	tenantID      uuid.UUID
	taskID        uuid.UUID
	userID        uuid.UUID
	newUserID     uuid.UUID
	result        string
	reason        string
	recordVersion int64
}

// hreTaskFake records every complete, defer and reassign call with the ids
// the handler resolved from the request.
func hreTaskFake() (*fakeTaskService, *[]hreTaskCall) {
	var calls []hreTaskCall
	fake := &fakeTaskService{
		complete: func(_ context.Context, tenantID, taskID, userID uuid.UUID, result json.RawMessage, recordVersion int64) (*port.Task, error) {
			calls = append(calls, hreTaskCall{op: "complete", tenantID: tenantID, taskID: taskID, userID: userID, result: string(result), recordVersion: recordVersion})
			return newTestTask(), nil
		},
		deferTask: func(_ context.Context, tenantID, taskID, userID uuid.UUID, reason string, recordVersion int64) (*port.Task, error) {
			calls = append(calls, hreTaskCall{op: "defer", tenantID: tenantID, taskID: taskID, userID: userID, reason: reason, recordVersion: recordVersion})
			task := newTestTask()
			task.Status = port.TaskStatusDeferred
			return task, nil
		},
		reassign: func(_ context.Context, tenantID, taskID, actorUserID, newUserID uuid.UUID, recordVersion int64) (*port.Task, error) {
			calls = append(calls, hreTaskCall{op: "reassign", tenantID: tenantID, taskID: taskID, userID: actorUserID, newUserID: newUserID, recordVersion: recordVersion})
			return newTestTask(), nil
		},
	}
	return fake, &calls
}

type hreIdentityCase struct {
	name       string
	header     string
	bad        string
	bare       bool
	nonAdmin   bool
	wantDetail string
}

// hreIdentityCases are the ways a handler can be reached without a usable
// tenant and user id: a header the shared middleware lets through but that is
// not a UUID, or no middleware at all.
func hreIdentityCases() []hreIdentityCase {
	return []hreIdentityCase{
		{name: "malformed tenant id", header: "x-tenant-id", bad: "not-a-uuid", wantDetail: "malformed tenant id in request context"},
		{name: "malformed user id", header: "x-user-id", bad: "not-a-uuid", wantDetail: "malformed user id in request context"},
		{name: "no request context", bare: true, wantDetail: "missing request context"},
	}
}

// hreBareEngine registers one handler with no identity middleware in front.
func hreBareEngine(method, path string, h gin.HandlerFunc) *gin.Engine {
	gin.SetMode(gin.TestMode)
	engine := gin.New()
	engine.Handle(method, path, h)
	return engine
}
