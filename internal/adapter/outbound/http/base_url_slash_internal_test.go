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
)

type pathRecorder struct {
	mu    sync.Mutex
	paths []string
}

func (p *pathRecorder) handler(answer string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		p.mu.Lock()
		p.paths = append(p.paths, r.URL.Path)
		p.mu.Unlock()
		_, _ = w.Write([]byte(answer))
	}
}

func TestDefinitionClient_ABaseURLWithATrailingSlashCallsTheSameRoute(t *testing.T) {
	versionID := uuid.New()
	rec := &pathRecorder{}
	srv := httptest.NewServer(rec.handler(`{"workflow_id":"` + uuid.NewString() + `","version_id":"` + versionID.String() + `","compiled_plan":{}}`))
	t.Cleanup(srv.Close)

	_, _ = NewDefinitionClient(srv.URL+"/", time.Second).GetCompiledWorkflow(context.Background(), uuid.New(), versionID)

	require.Len(t, rec.paths, 1)
	assert.Equal(t, "/api/v1/internal/workflow-versions/"+versionID.String()+"/compiled-plan", rec.paths[0])
}

func TestAutomationPrincipalClient_ABaseURLWithTrailingSlashesCallsTheSameRoute(t *testing.T) {
	tenantID := uuid.New()
	rec := &pathRecorder{}
	srv := httptest.NewServer(rec.handler(`{"principal_id":"` + uuid.NewString() + `","principal_sub":"sub","status":"active"}`))
	t.Cleanup(srv.Close)

	_, err := NewAutomationPrincipalClient(srv.URL+"//", time.Second, time.Minute).AutomationPrincipal(context.Background(), tenantID)

	require.NoError(t, err)
	require.Len(t, rec.paths, 1)
	assert.Equal(t, "/api/v1/internal/tenants/"+tenantID.String()+"/service-accounts/platform-automation", rec.paths[0])
}
