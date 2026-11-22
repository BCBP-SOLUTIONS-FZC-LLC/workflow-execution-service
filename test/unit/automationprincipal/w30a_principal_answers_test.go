package automationprincipal_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	httpadapter "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

const w30aPrincipalID = "2b1f0000-0000-4000-8000-0000000000c1"

// w30aServe answers every request with the status and body and counts the requests.
func w30aServe(t *testing.T, status int, body string) (url string, hits *atomic.Int32) {
	t.Helper()
	hits = &atomic.Int32{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv.URL, hits
}

func TestAutomationPrincipal_UnusableAnswerBody_NamesWhatIsMissingAndIsNotCached(t *testing.T) {
	cases := []struct {
		name     string
		body     string
		wantText string
	}{
		{"not json", `<html>`, "an unreadable principal"},
		{"empty body", ``, "an unreadable principal"},
		{"principal_id absent", `{"principal_sub":"s","status":"active"}`, "without a principal_id"},
		{"principal_id null", `{"principal_id":null,"principal_sub":"s","status":"active"}`, "without a principal_id"},
		{"principal_id nil uuid", `{"principal_id":"00000000-0000-0000-0000-000000000000","principal_sub":"s","status":"active"}`, "without a principal_id"},
		{"principal_id malformed", `{"principal_id":"nope","principal_sub":"s","status":"active"}`, "an unreadable principal"},
		{"principal_id wrong type", `{"principal_id":7,"principal_sub":"s","status":"active"}`, "an unreadable principal"},
		{"active with empty principal_sub", `{"principal_id":"` + w30aPrincipalID + `","principal_sub":"","status":"active"}`, "without a principal_sub"},
		{"active with principal_sub absent", `{"principal_id":"` + w30aPrincipalID + `","status":"active"}`, "without a principal_sub"},
		{"body past the 1 MiB read limit", `{"pad":"` + strings.Repeat("x", 1<<20+16) + `","principal_id":"` + w30aPrincipalID + `","principal_sub":"s","status":"active"}`, "an unreadable principal"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tenant := uuid.New()
			url, hits := w30aServe(t, http.StatusOK, tc.body)
			c := httpadapter.NewAutomationPrincipalClient(url, time.Second, time.Minute)

			for range 2 {
				got, err := c.AutomationPrincipal(context.Background(), tenant)

				require.ErrorIs(t, err, httpadapter.ErrUpstreamUnavailable)
				require.NotErrorIs(t, err, port.ErrNoAutomationPrincipal)
				assert.Contains(t, err.Error(), tc.wantText)
				assert.Equal(t, port.AutomationPrincipal{}, got)
			}
			assert.EqualValues(t, 2, hits.Load(), "a bad answer is not cached")
		})
	}
}

func TestAutomationPrincipal_StatusOtherThanActive_IsNoPrincipalEvenWithoutSub(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"suspended", `{"principal_id":"` + w30aPrincipalID + `","principal_sub":"s","status":"suspended"}`},
		{"suspended without sub", `{"principal_id":"` + w30aPrincipalID + `","status":"suspended"}`},
		{"disabled without sub", `{"principal_id":"` + w30aPrincipalID + `","principal_sub":"","status":"disabled"}`},
		{"capitalised Active", `{"principal_id":"` + w30aPrincipalID + `","principal_sub":"s","status":"Active"}`},
		{"upper case ACTIVE", `{"principal_id":"` + w30aPrincipalID + `","principal_sub":"s","status":"ACTIVE"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			url, hits := w30aServe(t, http.StatusOK, tc.body)
			c := httpadapter.NewAutomationPrincipalClient(url, time.Second, time.Minute)

			for range 2 {
				got, err := c.AutomationPrincipal(context.Background(), uuid.New())

				require.ErrorIs(t, err, port.ErrNoAutomationPrincipal)
				require.NotErrorIs(t, err, httpadapter.ErrUpstreamUnavailable)
				assert.Equal(t, port.AutomationPrincipal{}, got)
			}
			assert.EqualValues(t, 2, hits.Load(), "an inactive answer is not cached")
		})
	}
}

func TestAutomationPrincipal_StatusOtherThan200Or404_IsUpstreamUnavailableAfterOneRead(t *testing.T) {
	cases := []struct {
		name   string
		status int
	}{
		{"unauthorized", http.StatusUnauthorized},
		{"forbidden", http.StatusForbidden},
		{"server error", http.StatusInternalServerError},
		{"bad gateway", http.StatusBadGateway},
		{"service unavailable", http.StatusServiceUnavailable},
		{"accepted", http.StatusAccepted},
		{"no content", http.StatusNoContent},
		{"redirect", http.StatusTemporaryRedirect},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			url, hits := w30aServe(t, tc.status, `{"principal_id":"`+w30aPrincipalID+`","principal_sub":"s","status":"active"}`)
			c := httpadapter.NewAutomationPrincipalClient(url, time.Second, time.Minute)

			for range 2 {
				_, err := c.AutomationPrincipal(context.Background(), uuid.New())

				require.ErrorIs(t, err, httpadapter.ErrUpstreamUnavailable)
				require.NotErrorIs(t, err, port.ErrNoAutomationPrincipal)
				assert.Contains(t, err.Error(), "answered "+strconv.Itoa(tc.status))
			}
			assert.EqualValues(t, 2, hits.Load(), "one read per call, no retry and no cached answer")
		})
	}
}

func TestAutomationPrincipal_TokenServiceRefusesConnection_IsUpstreamUnavailable(t *testing.T) {
	c := httpadapter.NewAutomationPrincipalClient("http://127.0.0.1:1", time.Second, time.Minute)

	got, err := c.AutomationPrincipal(context.Background(), uuid.New())

	require.ErrorIs(t, err, httpadapter.ErrUpstreamUnavailable)
	require.NotErrorIs(t, err, port.ErrNoAutomationPrincipal)
	assert.Contains(t, err.Error(), "token service")
	assert.Equal(t, port.AutomationPrincipal{}, got)
}
