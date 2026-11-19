package main

import (
	"context"
	"errors"
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
	"go.opentelemetry.io/otel/trace"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/connectors/aliasconfig"
)

// w27bTracedContext starts a real span so the propagator has a trace to inject. Not for parallel tests: tracing is process-global.
func w27bTracedContext(t *testing.T) (context.Context, trace.TraceID) {
	t.Helper()
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "none")
	shutdown, err := gincommon.InitTracingWithConfig(gincommon.TracingConfig{
		ServiceName: "workflow-execution-service", BuildVersion: "test", Domain: "workflow", Environment: "test",
	})
	require.NoError(t, err)
	t.Cleanup(shutdown)
	ctx, span := gincommon.NewTracer("test").Start(context.Background(), "caller")
	t.Cleanup(func() { span.End() })
	return ctx, span.SpanContext().TraceID()
}

type w27bAliasRequest struct {
	method      string
	path        string
	traceparent string
}

// w27bAliasServer answers every request with status and body and records what it was asked.
func w27bAliasServer(t *testing.T, status int, body string) (*httptest.Server, func() []w27bAliasRequest) {
	t.Helper()
	var mu sync.Mutex
	var seen []w27bAliasRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen = append(seen, w27bAliasRequest{method: r.Method, path: r.URL.Path, traceparent: r.Header.Get("Traceparent")})
		mu.Unlock()
		if status >= 300 && status < 400 {
			w.Header().Set("Location", "http://"+r.Host+"/elsewhere")
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv, func() []w27bAliasRequest {
		mu.Lock()
		defer mu.Unlock()
		return append([]w27bAliasRequest(nil), seen...)
	}
}

func TestFetchAliases_TraceContextAndTrailingSlash(t *testing.T) {
	cases := []struct {
		name       string
		addrSuffix string
	}{
		{name: "plain address", addrSuffix: ""},
		{name: "one trailing slash", addrSuffix: "/"},
		{name: "several trailing slashes", addrSuffix: "///"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, requests := w27bAliasServer(t, http.StatusOK, `{"version":1,"restCall":[],"sqlQuery":[]}`)
			ctx, traceID := w27bTracedContext(t)

			_, err := fetchAliases(ctx, srv.URL+tc.addrSuffix, time.Second, noopLogger{})

			require.NoError(t, err)
			got := requests()
			require.Len(t, got, 1)
			assert.Equal(t, http.MethodGet, got[0].method)
			assert.Equal(t, "/api/v1/internal/connector-aliases", got[0].path, "no doubled slash in the path")
			assert.Contains(t, got[0].traceparent, traceID.String(), "the call joins the caller's trace")
		})
	}
}

func TestFetchAliases_AnyNon200Status_IsAnError(t *testing.T) {
	cases := []struct {
		name   string
		status int
	}{
		{name: "204 no content", status: http.StatusNoContent},
		{name: "201 created", status: http.StatusCreated},
		{name: "301 moved", status: http.StatusMovedPermanently},
		{name: "500", status: http.StatusInternalServerError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, requests := w27bAliasServer(t, tc.status, `{"version":1,"restCall":[{"alias":"a","method":"GET","baseURL":"http://x","pathTemplate":"/y"}]}`)

			cfg, err := fetchAliases(context.Background(), srv.URL, time.Second, noopLogger{})

			require.Error(t, err)
			assert.Contains(t, err.Error(), fmt.Sprintf("status %d", tc.status))
			assert.Contains(t, err.Error(), srv.URL+"/api/v1/internal/connector-aliases")
			assert.Zero(t, aliasCount(cfg), "a refused answer yields no rows even when its body parses")
			assert.Len(t, requests(), 1, "a 3xx is not followed")
		})
	}
}

// The registry body is capped at 1 MiB: exactly the cap is read, one byte more is refused.
func TestFetchAliases_BodyAtTheCapEdge(t *testing.T) {
	pad := func(total int) string {
		const frame = `{"version":1,"restCall":[],"sqlQuery":[],"pad":""}`
		return strings.TrimSuffix(frame, `"}`) + strings.Repeat("a", total-len(frame)) + `"}`
	}
	cases := []struct {
		name    string
		size    int
		wantErr bool
	}{
		{name: "exactly 1 MiB is read", size: 1 << 20},
		{name: "1 MiB plus one byte is refused", size: 1<<20 + 1, wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := pad(tc.size)
			require.Len(t, body, tc.size)
			srv, _ := w27bAliasServer(t, http.StatusOK, body)

			_, err := fetchAliases(context.Background(), srv.URL, 5*time.Second, noopLogger{})

			if tc.wantErr {
				require.Error(t, err)
				assert.EqualError(t, err, "alias registry response exceeds 1 MiB")
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestFetchAliases_DecodesKeysCaseInsensitivelyAndTimeoutAsNanoseconds(t *testing.T) {
	cases := []struct {
		name        string
		body        string
		wantAlias   string
		wantTimeout time.Duration
	}{
		{
			name:        "upper-case keys decode",
			body:        `{"VERSION":1,"RESTCALL":[{"ALIAS":"up","METHOD":"GET","BASEURL":"http://x","PATHTEMPLATE":"/y","TIMEOUT":2000000000}]}`,
			wantAlias:   "up",
			wantTimeout: 2 * time.Second,
		},
		{
			name:        "Go field names decode",
			body:        `{"Version":1,"RestCall":[{"Alias":"go","Method":"GET","BaseURL":"http://x","PathTemplate":"/y","Timeout":1500000000}]}`,
			wantAlias:   "go",
			wantTimeout: 1500 * time.Millisecond,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, _ := w27bAliasServer(t, http.StatusOK, tc.body)

			cfg, err := fetchAliases(context.Background(), srv.URL, time.Second, noopLogger{})

			require.NoError(t, err)
			require.Len(t, cfg.RestCall, 1)
			assert.Equal(t, tc.wantAlias, cfg.RestCall[0].Alias)
			assert.Equal(t, tc.wantTimeout, cfg.RestCall[0].Timeout)
		})
	}
}

func TestValidAliasRows_DuplicateSQLQueryAlias_KeepsTheFirstAndLogsTheSecond(t *testing.T) {
	first := aliasconfig.Query{Alias: "lookup", BaseURL: "http://db.example", Path: "/q", QueryID: "first", ParamCount: 1}
	second := aliasconfig.Query{Alias: "lookup", BaseURL: "http://db.example", Path: "/q", QueryID: "second", ParamCount: 1}
	other := aliasconfig.Query{Alias: "other", BaseURL: "http://db.example", Path: "/q", QueryID: "third", ParamCount: 1}
	require.NoError(t, aliasconfig.Config{SQLQuery: []aliasconfig.Query{first}}.Validate(), "the fixture row is valid on its own")
	log := &w27bFieldLogger{}

	out := validAliasRows(aliasconfig.Config{Version: 3, SQLQuery: []aliasconfig.Query{first, second, other}}, log)

	assert.Equal(t, []aliasconfig.Query{first, other}, out.SQLQuery)
	assert.Equal(t, 3, out.Version)
	assert.Equal(t, []map[string]any{
		{"kind": "sqlQuery", "alias": "lookup", "failure": "duplicate alias"},
	}, log.warns)
}

// w27bFieldLogger keeps the fields of every Warn.
type w27bFieldLogger struct{ warns []map[string]any }

func (*w27bFieldLogger) Debug(string, map[string]any) {}
func (*w27bFieldLogger) Info(string, map[string]any)  {}
func (l *w27bFieldLogger) Warn(_ string, f map[string]any) {
	l.warns = append(l.warns, f)
}
func (*w27bFieldLogger) Error(string, map[string]any) {}

func TestCallbackStatusError_FormatsPathStatusAndQuotedBody(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{name: "plain text body", body: "forbidden", want: `call /api/v1/internal/connector-tasks/T/complete: status 403: "forbidden"`},
		{name: "json body has its quotes escaped", body: `{"detail":"no"}`, want: `call /api/v1/internal/connector-tasks/T/complete: status 403: "{\"detail\":\"no\"}"`},
		{name: "empty body is an empty pair of quotes", body: "", want: `call /api/v1/internal/connector-tasks/T/complete: status 403: ""`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			taskID := uuid.New()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(tc.body))
			}))
			t.Cleanup(srv.Close)

			err := newCompletionClient(srv.URL, 5*time.Second).Complete(context.Background(), uuid.New(), taskID, nil)

			require.Error(t, err)
			assert.EqualError(t, err, strings.ReplaceAll(tc.want, "/T/", "/"+taskID.String()+"/"))
		})
	}
}

func TestRefusedForGood_ClassifiesTheCallbackAnswer(t *testing.T) {
	statusErr := func(status int) error {
		return &callbackStatusError{Path: "/p", Status: status, Body: "b"}
	}
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "an error without a status", err: errors.New("dial tcp: refused"), want: false},
		{name: "400", err: statusErr(http.StatusBadRequest), want: true},
		{name: "401", err: statusErr(http.StatusUnauthorized), want: true},
		{name: "404", err: statusErr(http.StatusNotFound), want: true},
		{name: "409", err: statusErr(http.StatusConflict), want: true},
		{name: "422", err: statusErr(http.StatusUnprocessableEntity), want: true},
		{name: "499", err: statusErr(499), want: true},
		{name: "a wrapped 404", err: fmt.Errorf("resend: %w", statusErr(http.StatusNotFound)), want: true},
		{name: "408 is a timeout", err: statusErr(http.StatusRequestTimeout), want: false},
		{name: "429 is throttling", err: statusErr(http.StatusTooManyRequests), want: false},
		{name: "307 is not a 4xx", err: statusErr(http.StatusTemporaryRedirect), want: false},
		{name: "500", err: statusErr(http.StatusInternalServerError), want: false},
		{name: "503", err: statusErr(http.StatusServiceUnavailable), want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, refusedForGood(tc.err))
		})
	}
}
