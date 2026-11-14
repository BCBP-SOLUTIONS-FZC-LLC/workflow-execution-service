package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Execution names the actor from the tenant's automation principal, so a
// callback carries the tenant and the outcome and nothing that asserts who acted.
func TestCompletionClient_NamesNoActor(t *testing.T) {
	t.Parallel()

	tenant := uuid.New()
	var bodies []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		bodies = append(bodies, body)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	c := newCompletionClient(srv.URL, 5*time.Second)

	require.NoError(t, c.Complete(context.Background(), tenant, uuid.New(), map[string]any{"k": "v"}))
	require.NoError(t, c.Fail(context.Background(), tenant, uuid.New(), "upstream_error", nil))

	require.Len(t, bodies, 2)
	for _, body := range bodies {
		assert.Equal(t, tenant.String(), body["tenant_id"])
		assert.NotContains(t, body, "actor_user_id")
	}
	assert.Equal(t, "upstream_error", bodies[1]["error_class"])
}

func TestCompletionClient_FailForwardsTheConnectorOutput(t *testing.T) {
	t.Parallel()

	var bodies []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		bodies = append(bodies, body)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)

	c := newCompletionClient(srv.URL, 5*time.Second)
	require.NoError(t, c.Fail(context.Background(), uuid.New(), uuid.New(), "upstream_error", map[string]any{"deliveryOutcome": "unknown"}))
	require.NoError(t, c.Fail(context.Background(), uuid.New(), uuid.New(), "upstream_error", nil))

	require.Len(t, bodies, 2)
	assert.Equal(t, map[string]any{"deliveryOutcome": "unknown"}, bodies[0]["output"])
	assert.NotContains(t, bodies[1], "output")
}

// A callback refused with 401 or 404 is retried like any other failure; the
// log and the error must say which status came back and what the body said, or
// the cause (a wrong mesh principal, a removed route) is invisible.
func TestCompletionClient_ErrorNamesStatusAndBodySnippet(t *testing.T) {
	t.Parallel()

	longBody := `{"detail":"` + strings.Repeat("x", 600) + `"}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(longBody))
	}))
	t.Cleanup(srv.Close)

	c := newCompletionClient(srv.URL, 5*time.Second)
	err := c.Complete(context.Background(), uuid.New(), uuid.New(), nil)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "status 401")
	assert.Contains(t, err.Error(), strings.Repeat("x", 50), "the body snippet is part of the error")
	assert.NotContains(t, err.Error(), strings.Repeat("x", 300), "the snippet is capped at 200 bytes")
	var statusErr *callbackStatusError
	require.ErrorAs(t, err, &statusErr)
	assert.Equal(t, http.StatusUnauthorized, statusErr.Status)
	assert.Len(t, statusErr.Body, callbackBodySnippetBytes)
}

func TestReport_FailedCallbackLogsStatusAndBody(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte("no such route"))
	}))
	t.Cleanup(srv.Close)

	log := &recordingLogger{}
	d := &deps{completion: newCompletionClient(srv.URL, 5*time.Second)}
	require.Error(t, report(context.Background(), d, dispatchJob{tenantID: uuid.New(), taskID: uuid.New()}, dispatchOutcome{}, "1-0", log))

	require.Len(t, log.warns, 1)
	assert.Equal(t, http.StatusNotFound, log.warns[0]["status"])
	assert.Equal(t, "no such route", log.warns[0]["body"])
	assert.Contains(t, log.warns[0]["error"], "status 404")
}

// redirectingServer answers every request with a 307 to a second server and
// counts the requests that reach that one.
func redirectingServer(t *testing.T) (url string, targetHits *atomic.Int32) {
	t.Helper()
	targetHits = &atomic.Int32{}
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		targetHits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(target.Close)
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+r.URL.Path, http.StatusTemporaryRedirect)
	}))
	t.Cleanup(front.Close)
	return front.URL, targetHits
}

// The callback carries the system caller identity as headers, which a followed
// redirect would hand to whatever the Location names.
func TestCompletionClient_DoesNotFollowRedirects(t *testing.T) {
	t.Parallel()
	url, hits := redirectingServer(t)

	err := newCompletionClient(url, 5*time.Second).Complete(context.Background(), uuid.New(), uuid.New(), nil)

	var statusErr *callbackStatusError
	require.ErrorAs(t, err, &statusErr)
	assert.Equal(t, http.StatusTemporaryRedirect, statusErr.Status, "the 307 is the answer")
	assert.Zero(t, hits.Load(), "the redirect target is never called")
}
