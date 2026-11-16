package http_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	inboundhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

const (
	hrfTenantID = "11111111-1111-4111-8111-111111111111"
	hrfUserID   = "22222222-2222-4222-8222-222222222222"
)

type hrfLogEntry struct {
	level  string
	msg    string
	fields map[string]any
}

type hrfLogger struct {
	mu      sync.Mutex
	entries []hrfLogEntry
}

func (l *hrfLogger) record(level, msg string, fields map[string]interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	cp := make(map[string]any, len(fields))
	for k, v := range fields {
		cp[k] = v
	}
	l.entries = append(l.entries, hrfLogEntry{level: level, msg: msg, fields: cp})
}

func (l *hrfLogger) Debug(msg string, fields map[string]interface{}) { l.record("debug", msg, fields) }
func (l *hrfLogger) Info(msg string, fields map[string]interface{})  { l.record("info", msg, fields) }
func (l *hrfLogger) Warn(msg string, fields map[string]interface{})  { l.record("warn", msg, fields) }
func (l *hrfLogger) Error(msg string, fields map[string]interface{}) { l.record("error", msg, fields) }

func (l *hrfLogger) accessLogs() []hrfLogEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []hrfLogEntry
	for _, e := range l.entries {
		if e.msg == "http_request" {
			out = append(out, e)
		}
	}
	return out
}

type hrfCache struct {
	mu        sync.Mutex
	setNXKeys []string
}

func (c *hrfCache) Get(context.Context, string) (string, error)              { return "", nil }
func (c *hrfCache) Set(context.Context, string, string, time.Duration) error { return nil }
func (c *hrfCache) Del(context.Context, ...string) error                     { return nil }
func (c *hrfCache) Ping(context.Context) error                               { return nil }
func (c *hrfCache) SetNX(_ context.Context, key, _ string, _ time.Duration) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.setNXKeys = append(c.setNXKeys, key)
	return true, nil
}

func (c *hrfCache) claimCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.setNXKeys)
}

func (c *hrfCache) claimsSince(from int) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.setNXKeys[from:]...)
}

type hrfPinger struct {
	fn    func(ctx context.Context) error
	calls int
}

func (p *hrfPinger) Ping(ctx context.Context) error {
	p.calls++
	if p.fn == nil {
		return nil
	}
	return p.fn(ctx)
}

type hrfDB struct {
	fn    func(ctx context.Context) inboundhttp.DBHealth
	calls int
}

func (d *hrfDB) Health(ctx context.Context) inboundhttp.DBHealth {
	d.calls++
	if d.fn == nil {
		return inboundhttp.DBHealth{Healthy: true}
	}
	return d.fn(ctx)
}

type hrfRig struct {
	handler  http.Handler
	logger   *hrfLogger
	cache    *hrfCache
	db       *hrfDB
	cacheP   *hrfPinger
	temporal *hrfPinger
}

func hrfNewRig(t *testing.T, appEnv string) *hrfRig {
	t.Helper()
	gin.SetMode(gin.TestMode)
	rig := &hrfRig{logger: &hrfLogger{}, cache: &hrfCache{}, db: &hrfDB{}, cacheP: &hrfPinger{}, temporal: &hrfPinger{}}
	router := inboundhttp.NewRouter(inboundhttp.RouterConfig{
		GinConfig: gincommon.Config{
			ServiceName:  "workflow-execution-service",
			BuildVersion: "test",
			Domain:       "workflow",
			Environment:  "test",
			Logger:       rig.logger,
		},
		AppEnv:   appEnv,
		Handler:  handler.New(handler.Services{Cache: rig.cache}),
		DB:       rig.db,
		Cache:    rig.cacheP,
		Temporal: rig.temporal,
	})
	rig.handler = router.Handler()
	return rig
}

func (r *hrfRig) do(method, target string, headers map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	r.handler.ServeHTTP(w, req)
	return w
}

func hrfAuthHeaders() map[string]string {
	return map[string]string{"x-tenant-id": hrfTenantID, "x-user-id": hrfUserID}
}

func hrfSystemHeaders() map[string]string {
	return map[string]string{"x-user-id": hrfUserID, "x-tenant-roles": "iam-system"}
}

func hrfBody(t *testing.T, w *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	body, err := io.ReadAll(w.Result().Body)
	require.NoError(t, err)
	var out map[string]any
	require.NoError(t, json.Unmarshal(body, &out), "body is not JSON: %s", string(body))
	return out
}

func hrfRequestsTotal(t *testing.T, method, route, statusClass string) float64 {
	t.Helper()
	return obstest.Sum(t, "platform_http_requests_total", map[string]string{
		"method": method, "route": route, "status_class": statusClass,
	})
}

func hrfAnyMetricLabelContains(t *testing.T, needle string) bool {
	t.Helper()
	families, err := gincommon.MetricsGatherer().Gather()
	require.NoError(t, err)
	for _, f := range families {
		for _, m := range f.GetMetric() {
			for _, lp := range m.GetLabel() {
				if strings.Contains(lp.GetValue(), needle) {
					return true
				}
			}
		}
	}
	return false
}

func hrfAccessLogFor(t *testing.T, l *hrfLogger, path string) hrfLogEntry {
	t.Helper()
	var found []hrfLogEntry
	for _, e := range l.accessLogs() {
		if fmt.Sprint(e.fields["path"]) == path {
			found = append(found, e)
		}
	}
	require.Len(t, found, 1, "want exactly one access log entry with path %q, got %+v", path, l.accessLogs())
	return found[0]
}
