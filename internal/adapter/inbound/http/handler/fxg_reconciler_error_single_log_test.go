package handler

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	adapterhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/http"
)

type fxgLogEntry struct {
	level  string
	msg    string
	fields map[string]any
}

type fxgRecordingLogger struct{ entries []fxgLogEntry }

func (l *fxgRecordingLogger) add(level, msg string, fields map[string]any) {
	l.entries = append(l.entries, fxgLogEntry{level, msg, fields})
}
func (l *fxgRecordingLogger) Debug(msg string, f map[string]any) { l.add("debug", msg, f) }
func (l *fxgRecordingLogger) Info(msg string, f map[string]any)  { l.add("info", msg, f) }
func (l *fxgRecordingLogger) Warn(msg string, f map[string]any)  { l.add("warn", msg, f) }
func (l *fxgRecordingLogger) Error(msg string, f map[string]any) { l.add("error", msg, f) }

// The reconciler failure is logged by the handler with its event type, so the 503 answer must not attach the
// cause to the request again: the request-error middleware would log it a second time.
func TestReconcilerError_UpstreamUnavailable_IsLoggedOnceByTheHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)
	log := &fxgRecordingLogger{}
	h := New(Services{Log: log})
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/api/v1/internal/events/tenant", nil)

	h.reconcilerError(c, "TenantStateChanged", fmt.Errorf("%w: token service answered 502", adapterhttp.ErrUpstreamUnavailable))

	require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	assert.Empty(t, c.Errors, "the cause is not attached to the request")
	require.Len(t, log.entries, 1, "the handler's own log line is the only one")
	assert.Equal(t, "error", log.entries[0].level)
	assert.Equal(t, "TenantStateChanged", log.entries[0].fields["event_type"])
}
