package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/connectors/aliasconfig"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/registry"
)

func TestFetchAliases_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/api/v1/internal/connector-aliases", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"version":1,"restCall":[{"alias":"a","method":"GET","baseURL":"http://x","pathTemplate":"/y","timeout":5000000000}],"sqlQuery":[]}`))
	}))
	defer srv.Close()

	cfg, err := fetchAliases(context.Background(), srv.URL, time.Second, noopLogger{})
	require.NoError(t, err)
	require.Len(t, cfg.RestCall, 1)
	assert.Equal(t, "a", cfg.RestCall[0].Alias)
	assert.Equal(t, 5*time.Second, cfg.RestCall[0].Timeout)
}

func TestFetchAliases_NonOKStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()

	_, err := fetchAliases(context.Background(), srv.URL, time.Second, noopLogger{})
	require.Error(t, err)
}

func TestFetchAliases_MalformedJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`not json`))
	}))
	defer srv.Close()

	_, err := fetchAliases(context.Background(), srv.URL, time.Second, noopLogger{})
	require.Error(t, err)
}

func TestFetchAliases_ConnectionRefused(t *testing.T) {
	_, err := fetchAliases(context.Background(), "http://127.0.0.1:1", 100*time.Millisecond, noopLogger{})
	require.Error(t, err)
}

const mixedAliasBody = `{"version":1,"restCall":[
	{"alias":"good","method":"GET","baseURL":"http://x","pathTemplate":"/y","timeout":5000000000},
	{"alias":"bad-escape","method":"GET","baseURL":"http://x","pathTemplate":"/x/%{id}","timeout":5000000000},
	{"alias":"bad-pipe","method":"GET","baseURL":"http://x","pathTemplate":"/a|b","timeout":5000000000},
	{"alias":"good","method":"POST","baseURL":"http://x","pathTemplate":"/dup","timeout":5000000000},
	{"alias":"","method":"GET","baseURL":"http://x","pathTemplate":"/y"}],
	"sqlQuery":[{"alias":"bad-sql","baseURL":"http://x","path":"/q"}]}`

// connectors.New validates every alias at startup, so one row the library
// refuses used to crash-loop every worker; a bad row is skipped by name and the
// good ones are kept.
func TestFetchAliases_SkipsInvalidRowsAndNamesThem(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(mixedAliasBody))
	}))
	defer srv.Close()
	log := &recordingLogger{}

	cfg, err := fetchAliases(context.Background(), srv.URL, time.Second, log)

	require.NoError(t, err)
	require.Len(t, cfg.RestCall, 1)
	assert.Equal(t, "GET", cfg.RestCall[0].Method, "the first row of a duplicated alias wins")
	assert.Empty(t, cfg.SQLQuery)
	require.NoError(t, cfg.Validate(), "what is kept passes the library's own validation")
	var named []any
	for _, w := range log.warns {
		named = append(named, w["alias"])
	}
	assert.ElementsMatch(t, []any{"bad-escape", "bad-pipe", "good", "", "bad-sql"}, named)
}

// captureLogger keeps every line at every level with its fields, as the text a log reader would see.
type captureLogger struct{ lines []string }

func (l *captureLogger) add(msg string, f map[string]any) {
	l.lines = append(l.lines, fmt.Sprint(msg, " ", f))
}
func (l *captureLogger) Debug(m string, f map[string]any) { l.add(m, f) }
func (l *captureLogger) Info(m string, f map[string]any)  { l.add(m, f) }
func (l *captureLogger) Warn(m string, f map[string]any)  { l.add(m, f) }
func (l *captureLogger) Error(m string, f map[string]any) { l.add(m, f) }

// The library's refusal text quotes the whole base URL, userinfo included; a skipped row is logged by its alias and
// the kind of failure only.
func TestValidAliasRows_TheSkipLogLineNamesTheAliasAndNeverTheBaseURL(t *testing.T) {
	in := aliasconfig.Config{
		RestCall: []aliasconfig.Endpoint{
			{Alias: "with-secret", Method: "GET", BaseURL: "http://user:pass@host.example", PathTemplate: "/y"},
			{Alias: "bad-query", Method: "GET", BaseURL: "http://host.example?token=hunter2", PathTemplate: "/y"},
			{Alias: "bad-path", Method: "GET", BaseURL: "http://host.example", PathTemplate: "/a|b"},
			{Alias: "bad-method", Method: "NOPE", BaseURL: "http://host.example", PathTemplate: "/y"},
			{Alias: "twice", Method: "GET", BaseURL: "http://host.example", PathTemplate: "/y"},
			{Alias: "twice", Method: "GET", BaseURL: "http://host.example", PathTemplate: "/y"},
		},
		SQLQuery: []aliasconfig.Query{{Alias: "bad-sql", BaseURL: "http://u:pw@host.example", Path: "/q"}},
	}
	log := &captureLogger{}

	out := validAliasRows(in, log)

	require.Len(t, out.RestCall, 1, "only the first of the duplicated alias is kept")
	require.Len(t, log.lines, 6)
	all := strings.Join(log.lines, "\n")
	for _, alias := range []string{"with-secret", "bad-query", "bad-path", "bad-method", "twice", "bad-sql"} {
		assert.Contains(t, all, alias)
	}
	for _, kind := range []string{"failure:base URL", "failure:path", "failure:method", "failure:duplicate alias"} {
		assert.Contains(t, all, kind)
	}
	for _, leaked := range []string{"pass", "hunter2", "pw", "user:", "host.example"} {
		assert.NotContains(t, all, leaked)
	}
}

func TestFetchAliases_RefusesAnOversizeBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"restCall":[],"pad":"` + strings.Repeat("a", maxAliasBodyBytes) + `"}`))
	}))
	defer srv.Close()

	_, err := fetchAliases(context.Background(), srv.URL, time.Second, noopLogger{})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "1 MiB")
}

func aliasRefreshFixture(t *testing.T, body *atomic.Value) (*deps, *config.Config) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(body.Load().(string)))
	}))
	t.Cleanup(srv.Close)
	d := &deps{
		pools: map[string]*typePool{registry.TypeRestCall: {}},
		aliases: aliasconfig.Config{RestCall: []aliasconfig.Endpoint{
			{Alias: "old", Method: "GET", BaseURL: "http://x", PathTemplate: "/y"},
		}},
	}
	return d, &config.Config{DefinitionServiceURL: srv.URL, ConnectorAliasFetchTimeout: time.Second}
}

// An empty 200 (an empty object, a wrong shape, or every row skipped) must not
// replace a working registry with nothing.
func TestRefreshAliases_KeepsThePreviousRegistryOnAnEmptyAnswer(t *testing.T) {
	for _, body := range []string{`{}`, `{"version":1,"restCall":[],"sqlQuery":[]}`, `{"restCall":[{"alias":"bad","method":"NOPE","baseURL":"http://x","pathTemplate":"/y"}]}`} {
		var b atomic.Value
		b.Store(body)
		d, cfg := aliasRefreshFixture(t, &b)
		log := &recordingLogger{}

		refreshAliases(context.Background(), d, cfg, log)

		require.Len(t, d.currentAliases().RestCall, 1, body)
		assert.Equal(t, "old", d.currentAliases().RestCall[0].Alias, body)
		assert.NotEmpty(t, log.warns, body)
	}
}

// A registry whose last alias was deleted answers empty from then on. Held for ever, that answer would leave the
// deleted alias callable on every running worker; the third empty answer in a row is taken as real.
func TestRefreshAliases_AppliesAnEmptyAnswerAfterThreeInARow(t *testing.T) {
	var b atomic.Value
	b.Store(`{"version":1,"restCall":[]}`)
	d, cfg := aliasRefreshFixture(t, &b)
	log := &recordingLogger{}

	for i := 1; i <= 2; i++ {
		refreshAliases(context.Background(), d, cfg, log)
		require.Len(t, d.currentAliases().RestCall, 1, "empty answer %d keeps the held registry", i)
		assert.Len(t, log.warns, i, "a Warn for each of the first two")
	}

	refreshAliases(context.Background(), d, cfg, log)

	assert.Zero(t, aliasCount(d.currentAliases()), "the third empty answer in a row is applied")
}

func TestRefreshAliases_ANonEmptyAnswerOrAnErrorStartsTheEmptyCountAgain(t *testing.T) {
	const empty = `{"version":1,"restCall":[]}`
	const nonEmpty = `{"restCall":[{"alias":"new","method":"GET","baseURL":"http://x","pathTemplate":"/y"}]}`
	tests := map[string]string{"a non-empty answer": nonEmpty, "a failed fetch": `not json`}
	for name, between := range tests {
		t.Run(name, func(t *testing.T) {
			var b atomic.Value
			b.Store(empty)
			d, cfg := aliasRefreshFixture(t, &b)
			refresh := func(body string) {
				b.Store(body)
				refreshAliases(context.Background(), d, cfg, noopLogger{})
			}

			refresh(empty)
			refresh(empty)
			refresh(between)
			refresh(empty)
			refresh(empty)
			assert.NotZero(t, aliasCount(d.currentAliases()), "two empty answers after the reset are still held")

			refresh(empty)
			assert.Zero(t, aliasCount(d.currentAliases()), "the third one after the reset is applied")
		})
	}
}

func TestRefreshAliases_AppliesANonEmptyAnswerAndAnEmptyOneOverAnEmptyRegistry(t *testing.T) {
	var b atomic.Value
	b.Store(`{"restCall":[{"alias":"new","method":"GET","baseURL":"http://x","pathTemplate":"/y"}]}`)
	d, cfg := aliasRefreshFixture(t, &b)

	refreshAliases(context.Background(), d, cfg, noopLogger{})
	require.Len(t, d.currentAliases().RestCall, 1)
	assert.Equal(t, "new", d.currentAliases().RestCall[0].Alias)

	d.setAliases(aliasconfig.Config{})
	b.Store(`{}`)
	log := &recordingLogger{}
	refreshAliases(context.Background(), d, cfg, log)
	assert.Empty(t, log.warns, "nothing to protect when the registry is already empty")
}

func TestRefreshAliases_AgeGaugeResetsOnSuccessAndGrowsOnFailure(t *testing.T) {
	obstest.Init(t)
	const metric = "workflow_execution_service_connector_alias_registry_age_seconds"
	var b atomic.Value
	b.Store(`{"restCall":[{"alias":"new","method":"GET","baseURL":"http://x","pathTemplate":"/y"}]}`)
	d, cfg := aliasRefreshFixture(t, &b)
	now := time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)
	d.now = func() time.Time { return now }

	refreshAliases(context.Background(), d, cfg, noopLogger{})
	assert.Equal(t, 0.0, obstest.Sum(t, metric, nil))

	b.Store(`not json`)
	now = now.Add(90 * time.Second)
	refreshAliases(context.Background(), d, cfg, noopLogger{})
	assert.Equal(t, 90.0, obstest.Sum(t, metric, nil))
}

func TestFetchAliases_DoesNotFollowRedirects(t *testing.T) {
	url, hits := redirectingServer(t)

	_, err := fetchAliases(context.Background(), url, time.Second, noopLogger{})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "status 307", "the 307 is the answer")
	assert.Zero(t, hits.Load(), "the redirect target is never called")
}
