package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestDefinitionClient_NotFound_IsMarkedAsUnknownVersionAndStaysRejected(t *testing.T) {
	for name, status := range map[string]int{"not found": http.StatusNotFound, "bad request": http.StatusBadRequest, "forbidden": http.StatusForbidden} {
		t.Run(name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) }))
			t.Cleanup(srv.Close)

			_, err := NewDefinitionClient(srv.URL, time.Second).GetCompiledWorkflow(context.Background(), uuid.New(), uuid.New())

			require.ErrorIs(t, err, port.ErrDefinitionRejected)
			assert.NotErrorIs(t, err, port.ErrDefinitionUnavailable)
			if status == http.StatusNotFound {
				assert.ErrorIs(t, err, port.ErrDefinitionNotFound)
			} else {
				assert.NotErrorIs(t, err, port.ErrDefinitionNotFound)
			}
		})
	}
}
