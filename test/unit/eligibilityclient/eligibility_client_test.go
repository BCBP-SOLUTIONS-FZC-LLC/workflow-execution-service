// Package eligibilityclient_test mirrors definition_service's
// test/unit/membershipclient's httptest.NewServer table-driven pattern.
package eligibilityclient_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	httpadapter "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/http"
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
					require.Equal(t, http.MethodPost, r.Method)
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
					_ = json.NewEncoder(w).Encode(map[string]bool{"eligible": false})
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
			eligible, err := client.CheckEligibility(ctx, uuid.New(), uuid.New(), "reviewer", uuid.New())
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
