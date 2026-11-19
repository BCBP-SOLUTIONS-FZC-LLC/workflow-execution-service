package middleware_test

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/middleware"
)

type w29aLogLine struct {
	level  string
	msg    string
	fields map[string]any
}

// w29aRecorder keeps every line written, with its level.
type w29aRecorder struct {
	mu    sync.Mutex
	lines []w29aLogLine
}

func (r *w29aRecorder) add(level, msg string, fields map[string]any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lines = append(r.lines, w29aLogLine{level: level, msg: msg, fields: fields})
}

func (r *w29aRecorder) Debug(msg string, fields map[string]any) { r.add("debug", msg, fields) }
func (r *w29aRecorder) Info(msg string, fields map[string]any)  { r.add("info", msg, fields) }
func (r *w29aRecorder) Warn(msg string, fields map[string]any)  { r.add("warn", msg, fields) }
func (r *w29aRecorder) Error(msg string, fields map[string]any) { r.add("error", msg, fields) }

func (r *w29aRecorder) requestFailed() []w29aLogLine {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []w29aLogLine
	for _, l := range r.lines {
		if l.msg == "request failed" {
			out = append(out, l)
		}
	}
	return out
}

func w29aErrorLogRouter(logger gincommon.Logger, handler gin.HandlerFunc, inner ...gin.HandlerFunc) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(middleware.LogRequestErrors(logger))
	for _, mw := range inner {
		r.Use(mw)
	}
	r.GET("/things/:id", handler)
	return r
}

func w29aGet(r *gin.Engine, path string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
	return w
}

func TestLogRequestErrors_NoCauseAttached_WritesNoRequestFailedLine(t *testing.T) {
	cases := []struct {
		name       string
		handler    gin.HandlerFunc
		wantStatus int
	}{
		{
			name:       "a successful request with no cause",
			handler:    func(c *gin.Context) { c.Status(http.StatusOK) },
			wantStatus: http.StatusOK,
		},
		{
			name:       "a refusal that attached no cause",
			handler:    func(c *gin.Context) { c.Status(http.StatusConflict) },
			wantStatus: http.StatusConflict,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &w29aRecorder{}
			r := w29aErrorLogRouter(rec, tc.handler)

			w := w29aGet(r, "/things/7")

			assert.Equal(t, tc.wantStatus, w.Code)
			assert.Empty(t, rec.requestFailed(), "no 'request failed' line")
			assert.Empty(t, rec.lines, "nothing else is logged either")
		})
	}
}

// With no logger the middleware must neither panic nor change the answer.
func TestLogRequestErrors_RouterBuiltWithoutALogger_AnswersAsUsualWithoutPanicking(t *testing.T) {
	r := w29aErrorLogRouter(nil, func(c *gin.Context) {
		_ = c.Error(errors.New("storage unreachable"))
		c.Status(http.StatusInternalServerError)
	})

	var w *httptest.ResponseRecorder
	require.NotPanics(t, func() { w = w29aGet(r, "/things/7") })

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestLogRequestErrors_SeveralCauses_AreLoggedOnceEachWithTheFinalStatus(t *testing.T) {
	cases := []struct {
		name      string
		status    int
		wantLevel string
	}{
		{name: "a refusal logs each cause at warn", status: http.StatusConflict, wantLevel: "warn"},
		{name: "a server failure logs each cause at error", status: http.StatusInternalServerError, wantLevel: "error"},
	}
	causes := []string{"first cause", "second cause", "third cause"}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &w29aRecorder{}
			r := w29aErrorLogRouter(rec, func(c *gin.Context) {
				for _, cause := range causes {
					_ = c.Error(errors.New(cause))
				}
				c.Status(tc.status)
			})

			w := w29aGet(r, "/things/7")

			require.Equal(t, tc.status, w.Code)
			lines := rec.requestFailed()
			require.Len(t, lines, len(causes), "one line per cause")
			for i, line := range lines {
				assert.Equal(t, tc.wantLevel, line.level)
				assert.Equal(t, causes[i], line.fields["error"], "causes are logged in the order they were attached")
				assert.Equal(t, tc.status, line.fields["status"], "every line carries the final status")
				assert.Equal(t, http.MethodGet, line.fields["method"])
				assert.Equal(t, "/things/:id", line.fields["route"], "the route pattern, not the path with its id")
			}
			assert.Len(t, rec.lines, len(causes), "nothing but those lines")
		})
	}
}

// The log runs after the handlers, so a cause attached after the handler has
// returned (by a middleware around it) is in the list it reads.
func TestLogRequestErrors_ACauseAttachedAfterTheHandlerReturned_IsLoggedWithTheFinalStatus(t *testing.T) {
	rec := &w29aRecorder{}
	attachAfter := func(c *gin.Context) {
		c.Next()
		_ = c.Error(errors.New("attached after the handler"))
		c.Status(http.StatusBadGateway)
	}
	r := w29aErrorLogRouter(rec, func(c *gin.Context) { c.Status(http.StatusOK) }, attachAfter)

	w := w29aGet(r, "/things/7")

	require.Equal(t, http.StatusBadGateway, w.Code)
	lines := rec.requestFailed()
	require.Len(t, lines, 1)
	assert.Equal(t, "error", lines[0].level)
	assert.Equal(t, "attached after the handler", lines[0].fields["error"])
	assert.Equal(t, http.StatusBadGateway, lines[0].fields["status"])
}

func TestLogRequestErrors_ARequestNoRouteMatched_IsLoggedUnderTheRouteUnknown(t *testing.T) {
	gin.SetMode(gin.TestMode)
	rec := &w29aRecorder{}
	r := gin.New()
	r.Use(middleware.LogRequestErrors(rec))
	r.NoRoute(func(c *gin.Context) {
		_ = c.Error(errors.New("no such route"))
		c.Status(http.StatusNotFound)
	})

	w := w29aGet(r, "/nowhere")

	require.Equal(t, http.StatusNotFound, w.Code)
	lines := rec.requestFailed()
	require.Len(t, lines, 1)
	assert.Equal(t, "warn", lines[0].level)
	assert.Equal(t, "unknown", lines[0].fields["route"])
	assert.Equal(t, "no such route", lines[0].fields["error"])
}
