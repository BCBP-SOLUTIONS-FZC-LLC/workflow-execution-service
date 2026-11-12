package entitlementsclient_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	httpadapter "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

func serve(t *testing.T, status int, body string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestFeatureFlags_SendsTheSystemPrincipalRequestToTheTenantsEntitlements(t *testing.T) {
	tenantID := uuid.New()
	var got *http.Request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r
		_ = json.NewEncoder(w).Encode(map[string]any{"tenant_id": tenantID.String(), "feature_flags": []string{"sso_enabled"}})
	}))
	t.Cleanup(srv.Close)

	_, err := httpadapter.NewEntitlementsClient(srv.URL, time.Second).FeatureFlags(context.Background(), tenantID)

	require.NoError(t, err)
	assert.Equal(t, http.MethodGet, got.Method)
	assert.Equal(t, "/api/v1/internal/tenants/"+tenantID.String()+"/entitlements", got.URL.Path)
	assert.Equal(t, "iam-system", got.Header.Get("x-user-id"))
	assert.Equal(t, "iam-system", got.Header.Get("x-tenant-roles"))
	assert.Equal(t, tenantID.String(), got.Header.Get("x-tenant-id"))
}

func TestFeatureFlags_ReturnsTheWholeFlagSet(t *testing.T) {
	tenantID := uuid.New()
	srv := serve(t, http.StatusOK, `{"tenant_id":"`+tenantID.String()+`","feature_flags":["sso_enabled","dedicated_workflow_queue"],"limits":{}}`)

	flags, err := httpadapter.NewEntitlementsClient(srv.URL, time.Second).FeatureFlags(context.Background(), tenantID)

	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"sso_enabled", "dedicated_workflow_queue"}, flags)
}

func TestFeatureFlags_AnEmptyFlagListIsAnAnswer(t *testing.T) {
	tenantID := uuid.New()
	srv := serve(t, http.StatusOK, `{"tenant_id":"`+tenantID.String()+`","feature_flags":[]}`)

	flags, err := httpadapter.NewEntitlementsClient(srv.URL, time.Second).FeatureFlags(context.Background(), tenantID)

	require.NoError(t, err)
	assert.Empty(t, flags)
}

func TestFeatureFlags_RefusesAnAbsentOrNullTenantID(t *testing.T) {
	for name, body := range map[string]string{
		"absent": `{"feature_flags":["a"]}`,
		"null":   `{"tenant_id":null,"feature_flags":["a"]}`,
		"empty":  `{"tenant_id":"","feature_flags":["a"]}`,
	} {
		t.Run(name, func(t *testing.T) {
			srv := serve(t, http.StatusOK, body)
			flags, err := httpadapter.NewEntitlementsClient(srv.URL, time.Second).FeatureFlags(context.Background(), uuid.New())
			require.Error(t, err)
			assert.ErrorIs(t, err, port.ErrEntitlementsUnavailable)
			assert.Nil(t, flags)
		})
	}
}

func TestFeatureFlags_ANullLimitStaysUnlimitedAndDoesNotFailTheRead(t *testing.T) {
	tenantID := uuid.New()
	srv := serve(t, http.StatusOK, `{"tenant_id":"`+tenantID.String()+`","feature_flags":["a"],"limits":{"seats":{"value":null}}}`)

	flags, err := httpadapter.NewEntitlementsClient(srv.URL, time.Second).FeatureFlags(context.Background(), tenantID)

	require.NoError(t, err)
	assert.Equal(t, []string{"a"}, flags)
}

func TestFeatureFlags_RefusesAnAnswerItCannotTrust(t *testing.T) {
	tenantID := uuid.New()
	tests := []struct {
		name   string
		status int
		body   string
	}{
		{"a non-200 status", http.StatusNotFound, `{"tenant_id":"` + tenantID.String() + `","feature_flags":["dedicated_workflow_queue"]}`},
		{"a server error", http.StatusServiceUnavailable, ``},
		{"a 201 status", http.StatusCreated, `{"tenant_id":"` + tenantID.String() + `","feature_flags":["dedicated_workflow_queue"]}`},
		{"a body that is not JSON", http.StatusOK, `<html>`},
		{"an empty body", http.StatusOK, ``},
		{"a missing feature_flags key", http.StatusOK, `{"tenant_id":"` + tenantID.String() + `"}`},
		{"a null feature_flags", http.StatusOK, `{"tenant_id":"` + tenantID.String() + `","feature_flags":null}`},
		{"feature_flags that is not an array", http.StatusOK, `{"tenant_id":"` + tenantID.String() + `","feature_flags":"dedicated_workflow_queue"}`},
		{"feature_flags that is an object", http.StatusOK, `{"tenant_id":"` + tenantID.String() + `","feature_flags":{"dedicated_workflow_queue":true}}`},
		{"a non-string flag", http.StatusOK, `{"tenant_id":"` + tenantID.String() + `","feature_flags":["a",3]}`},
		{"a null flag", http.StatusOK, `{"tenant_id":"` + tenantID.String() + `","feature_flags":["a",null]}`},
		{"another tenant's answer", http.StatusOK, `{"tenant_id":"` + uuid.NewString() + `","feature_flags":["dedicated_workflow_queue"]}`},
		{"a tenant_id that is not a uuid", http.StatusOK, `{"tenant_id":"nope","feature_flags":[]}`},
		{"a missing tenant_id", http.StatusOK, `{"feature_flags":["dedicated_workflow_queue"]}`},
		{"a tenant_id that is not a string", http.StatusOK, `{"tenant_id":7,"feature_flags":[]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := serve(t, tt.status, tt.body)

			flags, err := httpadapter.NewEntitlementsClient(srv.URL, time.Second).FeatureFlags(context.Background(), tenantID)

			require.Error(t, err)
			assert.ErrorIs(t, err, port.ErrEntitlementsUnavailable)
			assert.Nil(t, flags)
		})
	}
}

const entitlementsBodyCap = 1_048_576

// bodyOfSize pads a valid answer with trailing spaces to exactly size bytes.
func bodyOfSize(t *testing.T, tenantID uuid.UUID, size int) string {
	t.Helper()
	answer := `{"tenant_id":"` + tenantID.String() + `","feature_flags":["dedicated_workflow_queue"]}`
	require.LessOrEqual(t, len(answer), size)
	return answer + strings.Repeat(" ", size-len(answer))
}

func TestFeatureFlags_AcceptsABodyOfExactlyTheCap(t *testing.T) {
	tenantID := uuid.New()
	srv := serve(t, http.StatusOK, bodyOfSize(t, tenantID, entitlementsBodyCap))

	flags, err := httpadapter.NewEntitlementsClient(srv.URL, 5*time.Second).FeatureFlags(context.Background(), tenantID)

	require.NoError(t, err)
	assert.Equal(t, []string{"dedicated_workflow_queue"}, flags)
}

func TestFeatureFlags_RefusesABodyOneByteOverTheCap(t *testing.T) {
	tenantID := uuid.New()
	srv := serve(t, http.StatusOK, bodyOfSize(t, tenantID, entitlementsBodyCap+1))

	flags, err := httpadapter.NewEntitlementsClient(srv.URL, 5*time.Second).FeatureFlags(context.Background(), tenantID)

	require.Error(t, err)
	assert.ErrorIs(t, err, port.ErrEntitlementsUnavailable)
	assert.Nil(t, flags)
}

func TestFeatureFlags_ATrailingSlashOnTheBaseURLMakesNoDifference(t *testing.T) {
	tenantID := uuid.New()
	var path atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path.Store(r.URL.Path)
		_, _ = w.Write([]byte(`{"tenant_id":"` + tenantID.String() + `","feature_flags":[]}`))
	}))
	t.Cleanup(srv.Close)

	_, err := httpadapter.NewEntitlementsClient(srv.URL+"/", time.Second).FeatureFlags(context.Background(), tenantID)

	require.NoError(t, err)
	assert.Equal(t, "/api/v1/internal/tenants/"+tenantID.String()+"/entitlements", path.Load())
}

func TestFeatureFlags_ATransportErrorIsAnUnavailableError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := srv.URL
	srv.Close()

	_, err := httpadapter.NewEntitlementsClient(url, time.Second).FeatureFlags(context.Background(), uuid.New())

	require.Error(t, err)
	assert.ErrorIs(t, err, port.ErrEntitlementsUnavailable)
}

func TestFeatureFlags_ASlowAnswerTimesOutAndIsNotRetried(t *testing.T) {
	var calls atomic.Int32
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		calls.Add(1)
		<-release
	}))
	t.Cleanup(func() { close(release); srv.Close() })

	_, err := httpadapter.NewEntitlementsClient(srv.URL, 50*time.Millisecond).FeatureFlags(context.Background(), uuid.New())

	require.Error(t, err)
	assert.ErrorIs(t, err, port.ErrEntitlementsUnavailable)
	assert.EqualValues(t, 1, calls.Load(), "the cache's backoff is the retry policy, the client makes one attempt")
}

func TestFeatureFlags_RecordsTheCallDurationByOutcomeAndCountsFailuresByReason(t *testing.T) {
	obstest.Init(t)
	tenantID := uuid.New()
	duration := func(outcome string) uint64 {
		return histogramCount(t, observability.EntitlementsCallDurationSeconds.WithLabelValues(outcome))
	}
	failures := func(reason string) float64 {
		return testutil.ToFloat64(observability.EntitlementsCallErrorsTotal.WithLabelValues(reason))
	}
	good := serve(t, http.StatusOK, `{"tenant_id":"`+tenantID.String()+`","feature_flags":[]}`)
	status := serve(t, http.StatusBadGateway, ``)
	garbled := serve(t, http.StatusOK, `<html>`)
	closed := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	closedURL := closed.URL
	closed.Close()
	for name, tt := range map[string]struct {
		url, outcome, reason string
	}{
		"success":   {good.URL, "success", ""},
		"status":    {status.URL, "error", "status"},
		"response":  {garbled.URL, "error", "response"},
		"transport": {closedURL, "error", "transport"},
	} {
		t.Run(name, func(t *testing.T) {
			okBefore, errBefore, reasonBefore := duration("success"), duration("error"), failures(tt.reason)

			_, _ = httpadapter.NewEntitlementsClient(tt.url, time.Second).FeatureFlags(context.Background(), tenantID)

			if tt.outcome == "success" {
				assert.Equal(t, okBefore+1, duration("success"))
				assert.Equal(t, errBefore, duration("error"))
				return
			}
			assert.Equal(t, errBefore+1, duration("error"))
			assert.Equal(t, okBefore, duration("success"))
			assert.Equal(t, reasonBefore+1, failures(tt.reason))
		})
	}
}

func histogramCount(t *testing.T, o prometheus.Observer) uint64 {
	t.Helper()
	var m dto.Metric
	require.NoError(t, o.(prometheus.Metric).Write(&m))
	return m.GetHistogram().GetSampleCount()
}
