package http

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// w30aRoundTripper answers every request through fn and counts them, so a test can cancel the caller's
// context at an exact point of a call instead of racing a timer.
type w30aRoundTripper struct {
	calls atomic.Int32
	fn    func(req *http.Request) (*http.Response, error)
}

func (rt *w30aRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	rt.calls.Add(1)
	return rt.fn(req)
}

func w30aStatusResponse(req *http.Request, status int) *http.Response {
	return &http.Response{
		StatusCode: status,
		Status:     http.StatusText(status),
		Header:     http.Header{},
		Body:       http.NoBody,
		Request:    req,
	}
}

func TestNewClient_GivenTimeout_KeepsItAndReturnsRedirectsAsTheAnswer(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		timeout time.Duration
		status  int
	}{
		{"moved permanently", 3 * time.Second, http.StatusMovedPermanently},
		{"found", 750 * time.Millisecond, http.StatusFound},
		{"see other", time.Minute, http.StatusSeeOther},
		{"temporary redirect", 2 * time.Second, http.StatusTemporaryRedirect},
		{"permanent redirect", 5 * time.Second, http.StatusPermanentRedirect},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var targetHits atomic.Int32
			target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { targetHits.Add(1) }))
			t.Cleanup(target.Close)
			front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, target.URL+"/elsewhere", tc.status)
			}))
			t.Cleanup(front.Close)
			client := NewClient(tc.timeout)

			resp, err := client.Get(front.URL + "/start")

			require.NoError(t, err, "a redirect is returned as the response, not as an error")
			t.Cleanup(func() { _ = resp.Body.Close() })
			assert.Equal(t, tc.timeout, client.Timeout)
			assert.Equal(t, tc.status, resp.StatusCode)
			assert.Equal(t, target.URL+"/elsewhere", resp.Header.Get("Location"))
			assert.Zero(t, targetHits.Load(), "the redirect target is never called")
		})
	}
}

func TestEligibilityClient_CancelledDuringBackoff_StopsWithoutAnotherAttempt(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	rt := &w30aRoundTripper{fn: func(req *http.Request) (*http.Response, error) {
		cancel()
		return w30aStatusResponse(req, http.StatusServiceUnavailable), nil
	}}
	c := NewEligibilityClient("http://iam.invalid", time.Second)
	c.client.Transport = rt

	eligible, err := c.CheckEligibility(ctx, uuid.New(), uuid.New(), uuid.New(), "reviewer")

	assert.False(t, eligible)
	require.ErrorIs(t, err, ErrUpstreamUnavailable)
	require.ErrorIs(t, err, context.Canceled)
	assert.EqualValues(t, 1, rt.calls.Load(), "no attempt after the cancel")
}

func TestEligibilityClient_EligibleNotABoolean_FailsAfterOneAttempt(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name     string
		body     string
		wantText string
	}{
		{"string true", `{"eligible":"true"}`, "decode eligibility response"},
		{"string false", `{"eligible":"false"}`, "decode eligibility response"},
		{"number one", `{"eligible":1}`, "decode eligibility response"},
		{"number zero", `{"eligible":0}`, "decode eligibility response"},
		{"array", `{"eligible":[true]}`, "decode eligibility response"},
		{"object", `{"eligible":{}}`, "decode eligibility response"},
		{"null", `{"eligible":null}`, `missing "eligible" field`},
		{"absent", `{"reason":"x"}`, `missing "eligible" field`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				_, _ = w.Write([]byte(tc.body))
			}))
			t.Cleanup(srv.Close)

			eligible, err := NewEligibilityClient(srv.URL, time.Second).CheckEligibility(context.Background(), uuid.New(), uuid.New(), uuid.New(), "approver")

			assert.False(t, eligible)
			require.ErrorIs(t, err, ErrUpstreamUnavailable)
			assert.Contains(t, err.Error(), tc.wantText)
			assert.EqualValues(t, 1, calls.Load(), "an undecodable answer is not retried")
		})
	}
}

func TestDefinitionClient_CancelledMidCall_StopsWithoutAnotherAttempt(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		// answer is what the transport does after cancelling the caller's context.
		answer func(req *http.Request) (*http.Response, error)
	}{
		{"cancelled while the call is in flight", func(*http.Request) (*http.Response, error) { return nil, context.Canceled }},
		{"cancelled while waiting to retry", func(req *http.Request) (*http.Response, error) {
			return w30aStatusResponse(req, http.StatusServiceUnavailable), nil
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			rt := &w30aRoundTripper{fn: func(req *http.Request) (*http.Response, error) {
				cancel()
				return tc.answer(req)
			}}
			c := NewDefinitionClient("http://definition.invalid", time.Second)
			c.client.Transport = rt

			got, err := c.GetCompiledWorkflow(ctx, uuid.New(), uuid.New())

			assert.Nil(t, got)
			require.ErrorIs(t, err, port.ErrDefinitionUnavailable)
			require.ErrorIs(t, err, context.Canceled)
			require.NotErrorIs(t, err, port.ErrDefinitionRejected)
			assert.EqualValues(t, 1, rt.calls.Load(), "no attempt after the cancel")
		})
	}
}

func w30aDefinitionAnswering(t *testing.T, body string) (*DefinitionClient, *atomic.Int32) {
	t.Helper()
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return NewDefinitionClient(srv.URL, time.Second), &calls
}

func TestDefinitionClient_IdentifiersNotUUIDs_AreRejectedNamingTheField(t *testing.T) {
	t.Parallel()
	const good = "8fa88cde-824c-47bc-836b-665cd42c2222"
	cases := []struct {
		name       string
		body       string
		wantNamed  string
		wantAbsent string
	}{
		{"workflow_id not a uuid", `{"workflow_id":"x","version_id":"` + good + `"}`, "workflow_id", "version_id"},
		{"workflow_id empty", `{"workflow_id":"","version_id":"` + good + `"}`, "workflow_id", "version_id"},
		{"workflow_id absent", `{"version_id":"` + good + `"}`, "workflow_id", "version_id"},
		{"version_id not a uuid", `{"workflow_id":"` + good + `","version_id":"y"}`, "version_id", "workflow_id"},
		{"version_id empty", `{"workflow_id":"` + good + `","version_id":""}`, "version_id", "workflow_id"},
		{"version_id absent", `{"workflow_id":"` + good + `"}`, "version_id", "workflow_id"},
		{"both bad reports workflow_id first", `{"workflow_id":"x","version_id":"y"}`, "workflow_id", "version_id"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, calls := w30aDefinitionAnswering(t, tc.body)

			got, err := c.GetCompiledWorkflow(context.Background(), uuid.New(), uuid.New())

			assert.Nil(t, got)
			require.ErrorIs(t, err, port.ErrDefinitionRejected)
			require.NotErrorIs(t, err, port.ErrDefinitionUnavailable)
			assert.Contains(t, err.Error(), tc.wantNamed+" ")
			assert.Contains(t, err.Error(), "is not a UUID")
			assert.NotContains(t, err.Error(), tc.wantAbsent)
			assert.EqualValues(t, 1, calls.Load(), "a rejected answer is not retried")
		})
	}
}

func TestDefinitionClient_CompiledPlanValue_KeptVerbatimOrEmpty(t *testing.T) {
	t.Parallel()
	const ids = `"workflow_id":"8fa88cde-824c-47bc-836b-665cd42c2222","version_id":"11abcc22-0000-7000-8000-000000000001"`
	cases := []struct {
		name string
		body string
		want string
	}{
		{"absent", `{` + ids + `}`, ""},
		{"null", `{` + ids + `,"compiled_plan":null}`, ""},
		{"object with its own spacing", `{` + ids + `,"compiled_plan":{"nodes": [ 1,  2 ]}}`, `{"nodes": [ 1,  2 ]}`},
		{"empty object", `{` + ids + `,"compiled_plan":{}}`, `{}`},
		{"array", `{` + ids + `,"compiled_plan":[1, 2]}`, `[1, 2]`},
		{"string", `{` + ids + `,"compiled_plan":"text"}`, `"text"`},
		{"quoted null is a string", `{` + ids + `,"compiled_plan":"null"}`, `"null"`},
		{"number", `{` + ids + `,"compiled_plan":42}`, `42`},
		{"false", `{` + ids + `,"compiled_plan":false}`, `false`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c, _ := w30aDefinitionAnswering(t, tc.body)

			got, err := c.GetCompiledWorkflow(context.Background(), uuid.New(), uuid.New())

			require.NoError(t, err)
			assert.Equal(t, tc.want, got.CompiledPlanJSON)
		})
	}
}

func TestToCompiledWorkflow_CompiledPlanWhitespaceAroundValue_IsTrimmedOrEmpty(t *testing.T) {
	t.Parallel()
	const (
		workflowID = "8fa88cde-824c-47bc-836b-665cd42c2222"
		versionID  = "11abcc22-0000-7000-8000-000000000001"
	)
	cases := []struct {
		name string
		plan json.RawMessage
		want string
	}{
		{"whitespace only", json.RawMessage("  \n\t "), ""},
		{"empty", json.RawMessage(""), ""},
		{"nil", nil, ""},
		{"padded null", json.RawMessage("  null \n"), ""},
		{"padded object", json.RawMessage(" \n{\"a\": 1}\t "), `{"a": 1}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := toCompiledWorkflow(compiledPlanResp{WorkflowID: workflowID, VersionID: versionID, CompiledPlan: tc.plan})

			require.NoError(t, err)
			assert.Equal(t, tc.want, got.CompiledPlanJSON)
			assert.Equal(t, uuid.MustParse(workflowID), got.WorkflowID)
			assert.Equal(t, uuid.MustParse(versionID), got.VersionID)
		})
	}

	t.Run("a bad workflow_id is refused before the plan is read", func(t *testing.T) {
		t.Parallel()
		got, err := toCompiledWorkflow(compiledPlanResp{WorkflowID: "x", VersionID: versionID, CompiledPlan: json.RawMessage(`{}`)})

		assert.Nil(t, got)
		require.True(t, errors.Is(err, port.ErrDefinitionRejected))
	})
}
