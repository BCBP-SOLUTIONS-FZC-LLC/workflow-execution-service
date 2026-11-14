package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type cwcliSeenRequest struct {
	method, path, tenantQuery, tenantHeader string
}

// A 2xx answer other than 200 is not a plan: the call is rejected after one attempt, with no retry.
func TestDefinitionClient_NonOKSuccessStatus_IsRejectedAfterOneAttempt(t *testing.T) {
	for name, status := range map[string]int{"created": http.StatusCreated, "no content": http.StatusNoContent} {
		t.Run(name, func(t *testing.T) {
			var mu sync.Mutex
			var seen []cwcliSeenRequest
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				seen = append(seen, cwcliSeenRequest{r.Method, r.URL.Path, r.URL.Query().Get("tenant_id"), r.Header.Get("x-tenant-id")})
				mu.Unlock()
				w.WriteHeader(status)
				if status != http.StatusNoContent {
					_, _ = w.Write([]byte(`{"workflow_id":"` + uuid.NewString() + `","version_id":"` + uuid.NewString() + `"}`))
				}
			}))
			t.Cleanup(srv.Close)
			tenantID, versionID := uuid.New(), uuid.New()

			cw, err := NewDefinitionClient(srv.URL, time.Second).GetCompiledWorkflow(context.Background(), tenantID, versionID)

			require.ErrorIs(t, err, port.ErrDefinitionRejected)
			assert.NotErrorIs(t, err, port.ErrDefinitionUnavailable)
			assert.Nil(t, cw)
			mu.Lock()
			defer mu.Unlock()
			assert.Equal(t, []cwcliSeenRequest{{
				method: http.MethodGet, path: "/api/v1/internal/workflow-versions/" + versionID.String() + "/compiled-plan",
				tenantQuery: tenantID.String(), tenantHeader: tenantID.String(),
			}}, seen, "exactly one attempt for this tenant and version")
		})
	}
}
