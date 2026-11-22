//go:build integration

package main

import (
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

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func userDeletedRequest(t *testing.T) *http.Request {
	t.Helper()
	body := fmt.Sprintf(
		`{"id":%q,"type":"UserDeleted","tenant_id":%q,"time":"2030-01-02T03:04:05Z",`+
			`"data":{"user_id":%q,"deleted_at":"2030-01-02T03:04:05Z"}}`,
		uuid.NewString(), uuid.NewString(), uuid.NewString())
	r := httptest.NewRequest(http.MethodPost, "/api/v1/internal/events/user-profile",
		strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("x-user-id", uuid.NewString())
	r.Header.Set("x-tenant-roles", domain.SystemCaller)
	return r
}

func TestInboundEvents_MoreConcurrentDeliveriesThanAppPoolConnectionsAllComplete(t *testing.T) {
	_, pgDSN := fixtures.NewTestPoolAndDSN(t)
	temporalAddr := w9aTemporalAddr(t)
	cfg := w9aAppConfig(t, pgDSN, temporalAddr, map[string]string{
		"PG_MAX_CONNS": "2",
		"PG_MIN_CONNS": "0",
	})
	a, err := newApp(cfg)
	require.NoError(t, err)
	t.Cleanup(func() { w9aCloseApp(a) })

	const deliveries = 8
	codes := make([]int, deliveries)
	var wg sync.WaitGroup
	for i := range codes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec := httptest.NewRecorder()
			a.httpServer.Handler.ServeHTTP(rec, userDeletedRequest(t))
			codes[i] = rec.Code
		}()
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(w9aWait):
		t.Fatal("inbound events starved for database connections")
	}
	for i, code := range codes {
		assert.Equal(t, http.StatusOK, code, "delivery %d", i)
	}
}
