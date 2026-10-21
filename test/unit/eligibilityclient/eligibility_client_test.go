// Package eligibilityclient_test mirrors definition_service's
// test/unit/membershipclient's httptest.NewServer table-driven pattern.
package eligibilityclient_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	httpadapter "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestCheckEligibility(t *testing.T) {
	tests := []struct {
		name         string
		makeHandler  func(t *testing.T, calls *int) http.HandlerFunc
		closeServer  bool
		makeCtx      func(t *testing.T) context.Context
		wantEligible bool
		wantErr      bool
		wantSentinel error
		wantCalls    int
		checkElapsed func(t *testing.T, elapsed time.Duration)
	}{
		{
			name: "eligible",
			makeHandler: func(t *testing.T, calls *int) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					*calls++
					require.Equal(t, http.MethodGet, r.Method)
					w.WriteHeader(http.StatusOK)
					_ = json.NewEncoder(w).Encode(map[string]bool{"eligible": true})
				}
			},
			wantEligible: true,
			wantCalls:    1,
		},
		{
			name: "ineligible",
			makeHandler: func(t *testing.T, calls *int) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					*calls++
					w.WriteHeader(http.StatusOK)
					_ = json.NewEncoder(w).Encode(map[string]any{"eligible": false, "reason": "level_insufficient", "role_level": "preparator"})
				}
			},
			wantEligible: false,
			wantCalls:    1,
		},
		{
			name: "retries then succeeds",
			makeHandler: func(t *testing.T, calls *int) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					*calls++
					if *calls < 3 {
						w.WriteHeader(http.StatusServiceUnavailable)
						return
					}
					w.WriteHeader(http.StatusOK)
					_ = json.NewEncoder(w).Encode(map[string]bool{"eligible": true})
				}
			},
			wantEligible: true,
			wantCalls:    3,
		},
		{
			name: "retries exhausted",
			makeHandler: func(t *testing.T, calls *int) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					*calls++
					w.WriteHeader(http.StatusServiceUnavailable)
				}
			},
			wantErr:      true,
			wantSentinel: httpadapter.ErrUpstreamUnavailable,
			wantCalls:    3,
		},
		{
			name: "decode error",
			makeHandler: func(t *testing.T, calls *int) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					*calls++
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte("not json"))
				}
			},
			wantErr:      true,
			wantSentinel: httpadapter.ErrUpstreamUnavailable,
			wantCalls:    1,
		},
		{
			name: "missing eligible field fails closed",
			makeHandler: func(t *testing.T, calls *int) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					*calls++
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte("{}"))
				}
			},
			wantErr:      true,
			wantSentinel: httpadapter.ErrUpstreamUnavailable,
			wantCalls:    1,
		},
		{
			name: "a 409 is not an answer",
			makeHandler: func(t *testing.T, calls *int) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					*calls++
					w.WriteHeader(http.StatusConflict)
				}
			},
			wantErr:      true,
			wantSentinel: httpadapter.ErrUpstreamUnavailable,
			wantCalls:    1,
		},
		{
			name: "a 403 is not retried",
			makeHandler: func(t *testing.T, calls *int) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					*calls++
					w.WriteHeader(http.StatusForbidden)
				}
			},
			wantErr:      true,
			wantSentinel: httpadapter.ErrUpstreamUnavailable,
			wantCalls:    1,
		},
		{
			name: "unrecognized non-5xx status",
			makeHandler: func(t *testing.T, calls *int) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					*calls++
					w.WriteHeader(http.StatusBadRequest)
				}
			},
			wantErr:      true,
			wantSentinel: httpadapter.ErrUpstreamUnavailable,
			wantCalls:    1,
		},
		{
			name: "request error (server closed)",
			makeHandler: func(t *testing.T, calls *int) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {}
			},
			closeServer:  true,
			wantErr:      true,
			wantSentinel: httpadapter.ErrUpstreamUnavailable,
		},
		{
			name: "context cancelled during backoff short-circuits retry",
			makeHandler: func(t *testing.T, calls *int) http.HandlerFunc {
				return func(w http.ResponseWriter, r *http.Request) {
					*calls++
					w.WriteHeader(http.StatusServiceUnavailable)
				}
			},
			makeCtx: func(t *testing.T) context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				time.AfterFunc(30*time.Millisecond, cancel)
				t.Cleanup(cancel)
				return ctx
			},
			wantErr:      true,
			wantSentinel: httpadapter.ErrUpstreamUnavailable,
			checkElapsed: func(t *testing.T, elapsed time.Duration) {
				assert.LessOrEqual(t, elapsed, 200*time.Millisecond)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var calls int
			srv := httptest.NewServer(tt.makeHandler(t, &calls))
			if tt.closeServer {
				srv.Close()
			} else {
				defer srv.Close()
			}

			client := httpadapter.NewEligibilityClient(srv.URL, 10*time.Second)
			ctx := context.Background()
			if tt.makeCtx != nil {
				ctx = tt.makeCtx(t)
			}

			start := time.Now()
			eligible, err := client.CheckEligibility(ctx, uuid.New(), uuid.New(), uuid.New(), "reviewer")
			elapsed := time.Since(start)

			if tt.wantErr {
				require.Error(t, err)
				if tt.wantSentinel != nil {
					assert.True(t, errors.Is(err, tt.wantSentinel), "expected error to wrap %v, got %v", tt.wantSentinel, err)
				}
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.wantEligible, eligible)
			}
			if tt.checkElapsed != nil {
				tt.checkElapsed(t, elapsed)
			}
			if tt.wantCalls > 0 {
				assert.Equal(t, tt.wantCalls, calls)
			}
		})
	}
}

func TestNewEligibilityClient(t *testing.T) {
	client := httpadapter.NewEligibilityClient("http://example.invalid", 5*time.Second)
	require.NotNil(t, client)
}

// A base URL containing a control character fails http.NewRequestWithContext's
// own URL parsing before any network call is attempted — a malformed-request
// error, not an upstream one, so it must fail closed on the first attempt
// without retrying.
func TestCheckEligibility_MalformedBaseURLFailsWithoutRetry(t *testing.T) {
	client := httpadapter.NewEligibilityClient("http://example.invalid\x7f", 5*time.Second)

	_, err := client.CheckEligibility(context.Background(), uuid.New(), uuid.New(), uuid.New(), "reviewer")

	require.Error(t, err)
	assert.Contains(t, err.Error(), "build eligibility request")
	assert.False(t, errors.Is(err, httpadapter.ErrUpstreamUnavailable),
		"a malformed request is rejected, not an upstream failure")
}

func TestCheckEligibilityBatch(t *testing.T) {
	t.Run("all eligible, one real call per request", func(t *testing.T) {
		var calls int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&calls, 1)
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]bool{"eligible": true})
		}))
		defer srv.Close()

		client := httpadapter.NewEligibilityClient(srv.URL, 5*time.Second)
		requests := []port.EligibilityCheckRequest{
			{NewUserID: uuid.New(), DepartmentID: uuid.New(), RequiredLevel: "reviewer"},
			{NewUserID: uuid.New(), DepartmentID: uuid.New(), RequiredLevel: "approver"},
			{NewUserID: uuid.New(), DepartmentID: uuid.New(), RequiredLevel: "reviewer"},
		}

		results, err := client.CheckEligibilityBatch(context.Background(), uuid.New(), requests)
		require.NoError(t, err)
		require.Len(t, results, 3)
		for _, result := range results {
			assert.NoError(t, result.Err)
			assert.True(t, result.Eligible)
		}
		assert.Equal(t, int32(3), atomic.LoadInt32(&calls))
	})

	t.Run("one request failing does not discard the others' results", func(t *testing.T) {
		// One user's department (dept 1, below) always 500s; the other two
		// succeed with different eligible/ineligible outcomes. A single
		// failing request must not poison the whole batch (the fan-out's own
		// prior all-or-nothing behavior, fixed as a real reliability gap).
		failingDept := uuid.New()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			department := uuid.MustParse(r.URL.Query().Get("department_id"))
			if department == failingDept {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			eligible := department != uuid.Nil
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]bool{"eligible": eligible})
		}))
		defer srv.Close()

		client := httpadapter.NewEligibilityClient(srv.URL, 200*time.Millisecond)
		eligibleDept := uuid.New()
		requests := []port.EligibilityCheckRequest{
			{NewUserID: uuid.New(), DepartmentID: eligibleDept, RequiredLevel: "reviewer"},
			{NewUserID: uuid.New(), DepartmentID: failingDept, RequiredLevel: "reviewer"},
			{NewUserID: uuid.New(), DepartmentID: uuid.Nil, RequiredLevel: "reviewer"},
		}

		results, err := client.CheckEligibilityBatch(context.Background(), uuid.New(), requests)
		require.NoError(t, err, "a per-item failure must not surface as a whole-batch error")
		require.Len(t, results, 3)
		assert.NoError(t, results[0].Err)
		assert.True(t, results[0].Eligible, "the eligible-department request must still succeed despite request 1 failing")
		assert.Error(t, results[1].Err, "the failing request's own error must be reported on its own result")
		assert.NoError(t, results[2].Err)
		assert.False(t, results[2].Eligible, "the third request must resolve on its own, independent of request 1's failure")
	})

	t.Run("empty request list returns an empty, non-nil-error result", func(t *testing.T) {
		client := httpadapter.NewEligibilityClient("http://example.invalid", 5*time.Second)
		results, err := client.CheckEligibilityBatch(context.Background(), uuid.New(), nil)
		require.NoError(t, err)
		assert.Empty(t, results)
	})

	t.Run("fan-out concurrency is capped, not one goroutine per request", func(t *testing.T) {
		var inFlight, peak int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			cur := atomic.AddInt32(&inFlight, 1)
			defer atomic.AddInt32(&inFlight, -1)
			for {
				p := atomic.LoadInt32(&peak)
				if cur <= p || atomic.CompareAndSwapInt32(&peak, p, cur) {
					break
				}
			}
			time.Sleep(10 * time.Millisecond)
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(map[string]bool{"eligible": true})
		}))
		defer srv.Close()

		client := httpadapter.NewEligibilityClient(srv.URL, 5*time.Second)
		requests := make([]port.EligibilityCheckRequest, 60)
		for i := range requests {
			requests[i] = port.EligibilityCheckRequest{NewUserID: uuid.New(), DepartmentID: uuid.New(), RequiredLevel: "reviewer"}
		}

		results, err := client.CheckEligibilityBatch(context.Background(), uuid.New(), requests)
		require.NoError(t, err)
		require.Len(t, results, 60)
		assert.LessOrEqual(t, atomic.LoadInt32(&peak), int32(20), "batch fan-out must stay under its concurrency cap, not open one goroutine per request")
	})
}

func TestCheckEligibility_RequestShape(t *testing.T) {
	tenantID, userID, departmentID := uuid.New(), uuid.New(), uuid.New()
	var got *http.Request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = r
		_ = json.NewEncoder(w).Encode(map[string]bool{"eligible": true})
	}))
	defer srv.Close()

	client := httpadapter.NewEligibilityClient(srv.URL+"/", 5*time.Second)
	_, err := client.CheckEligibility(context.Background(), tenantID, userID, departmentID, "approver")
	require.NoError(t, err)

	assert.Equal(t, "/api/v1/internal/tenants/"+tenantID.String()+"/members/"+userID.String()+"/eligibility", got.URL.Path)
	assert.Equal(t, departmentID.String(), got.URL.Query().Get("department_id"))
	assert.Equal(t, "approver", got.URL.Query().Get("required_level"))
	assert.Equal(t, "iam-system", got.Header.Get("x-user-id"))
	assert.Equal(t, "iam-system", got.Header.Get("x-tenant-roles"))
	assert.Equal(t, tenantID.String(), got.Header.Get("x-tenant-id"))
}

func TestCheckEligibility_ALevelThatIsNotARoleLevelIsIneligibleWithoutACall(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		_ = json.NewEncoder(w).Encode(map[string]bool{"eligible": true})
	}))
	defer srv.Close()
	client := httpadapter.NewEligibilityClient(srv.URL, 5*time.Second)

	eligible, err := client.CheckEligibility(context.Background(), uuid.New(), uuid.New(), uuid.New(), "engineering")
	require.NoError(t, err)
	assert.False(t, eligible)

	results, err := client.CheckEligibilityBatch(context.Background(), uuid.New(), []port.EligibilityCheckRequest{
		{NewUserID: uuid.New(), DepartmentID: uuid.New(), RequiredLevel: "checker"},
		{NewUserID: uuid.New(), DepartmentID: uuid.New(), RequiredLevel: "reviewer"},
	})
	require.NoError(t, err)
	require.Len(t, results, 2)
	assert.False(t, results[0].Eligible)
	assert.NoError(t, results[0].Err)
	assert.True(t, results[1].Eligible)
	assert.Equal(t, int32(1), atomic.LoadInt32(&calls), "only the reviewer request reached the service")
}
