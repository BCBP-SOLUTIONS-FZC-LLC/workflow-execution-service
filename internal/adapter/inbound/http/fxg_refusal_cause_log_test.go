package http_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	inboundhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

const fxgCauseText = "frontend 10.9.8.7:7233 token=s3cr3t-value"

type fxgInstances struct {
	port.InstanceService
	err   error
	reads []fxgInstanceRead
}

type fxgInstanceRead struct{ tenantID, instanceID uuid.UUID }

func (f *fxgInstances) Get(_ context.Context, tenantID, instanceID uuid.UUID, _ port.ReadScope) (*port.Instance, []*port.Task, error) {
	f.reads = append(f.reads, fxgInstanceRead{tenantID, instanceID})
	return nil, nil, f.err
}

func fxgGetInstance(t *testing.T, err error) (*httptest.ResponseRecorder, *hrfLogger) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	logger := &hrfLogger{}
	instances := &fxgInstances{err: err}
	router := inboundhttp.NewRouter(inboundhttp.RouterConfig{
		GinConfig: gincommon.Config{ServiceName: "workflow-execution-service", BuildVersion: "test", Domain: "workflow", Environment: "test", Logger: logger},
		AppEnv:    "test",
		Handler:   handler.New(handler.Services{Instances: instances}),
		DB:        &hrfDB{}, Cache: &hrfPinger{}, Temporal: &hrfPinger{},
	})
	rig := &hrfRig{handler: router.Handler(), logger: logger}
	instanceID := uuid.New()
	w := rig.do(http.MethodGet, "/api/v1/instances/"+instanceID.String(), hrfAuthHeaders())
	assert.Equal(t, []fxgInstanceRead{{uuid.MustParse(hrfTenantID), instanceID}}, instances.reads, "the service is asked once, for the caller's tenant and the path's instance")
	return w, logger
}

func (l *hrfLogger) causeLogs() []hrfLogEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []hrfLogEntry
	for _, e := range l.entries {
		if e.msg == "request failed" {
			out = append(out, e)
		}
	}
	return out
}

func TestRouter_RefusalWithAWrappedCause_BodyHidesItAndTheLogHoldsItOnceWithTheRequestID(t *testing.T) {
	cases := []struct {
		name      string
		sentinel  error
		wantCode  int
		wantLevel string
	}{
		{"a refusal", port.ErrInstanceNotFound, http.StatusNotFound, "warn"},
		{"a conflict", port.ErrWorkflowNotFound, http.StatusConflict, "warn"},
		{"an unexpected failure", nil, http.StatusInternalServerError, "error"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cause := errors.New(fxgCauseText)
			err := cause
			if tc.sentinel != nil {
				err = fmt.Errorf("get instance: %w: %w", tc.sentinel, cause)
			}

			w, logger := fxgGetInstance(t, err)

			require.Equal(t, tc.wantCode, w.Code, w.Body.String())
			assert.NotContains(t, w.Body.String(), "s3cr3t-value")
			logs := logger.causeLogs()
			require.Len(t, logs, 1, "the cause is logged once")
			assert.Equal(t, tc.wantLevel, logs[0].level)
			assert.Contains(t, logs[0].fields["error"], fxgCauseText)
			assert.Equal(t, tc.wantCode, logs[0].fields["status"])
			assert.Equal(t, "/api/v1/instances/:id", logs[0].fields["route"])
			assert.NotEmpty(t, w.Header().Get("X-Request-ID"))
			assert.Equal(t, w.Header().Get("X-Request-ID"), logs[0].fields["request_id"])
		})
	}
}
