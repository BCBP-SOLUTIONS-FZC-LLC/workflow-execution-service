package eligibilityclient_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	httpadapter "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestCheckEligibilityBatch_TheFirstFailedReadEndsTheBatch(t *testing.T) {
	failingDept := uuid.New()
	var received sync.Map
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received.Store(r.URL.Query().Get("department_id"), true)
		if r.URL.Query().Get("department_id") == failingDept.String() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		<-r.Context().Done()
	}))
	defer srv.Close()

	client := httpadapter.NewEligibilityClient(srv.URL, 2*time.Second)
	requests := make([]port.EligibilityCheckRequest, 60)
	for i := range requests {
		requests[i] = port.EligibilityCheckRequest{NewUserID: uuid.New(), DepartmentID: uuid.New(), RequiredLevel: "reviewer"}
	}
	requests[0].DepartmentID = failingDept

	results, err := client.CheckEligibilityBatch(context.Background(), uuid.New(), requests)

	require.ErrorIs(t, err, httpadapter.ErrUpstreamUnavailable)
	assert.Nil(t, results)
	distinct := 0
	received.Range(func(any, any) bool { distinct++; return true })
	assert.LessOrEqual(t, distinct, 20, "no new check is started after the first read failed")
}

func TestCheckEligibilityBatch_ARequestThatWasNeverSentIsAnUpstreamFailureNotAnIneligibleAnswer(t *testing.T) {
	var received atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		received.Add(1)
		_, _ = w.Write([]byte(`{"eligible":true}`))
	}))
	defer srv.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client := httpadapter.NewEligibilityClient(srv.URL, 2*time.Second)

	results, err := client.CheckEligibilityBatch(ctx, uuid.New(), []port.EligibilityCheckRequest{
		{NewUserID: uuid.New(), DepartmentID: uuid.New(), RequiredLevel: "reviewer"},
		{NewUserID: uuid.New(), DepartmentID: uuid.New(), RequiredLevel: "reviewer"},
	})

	require.ErrorIs(t, err, httpadapter.ErrUpstreamUnavailable)
	require.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, results)
	assert.Zero(t, received.Load())
}
