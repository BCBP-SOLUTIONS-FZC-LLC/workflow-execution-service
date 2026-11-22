package openbao

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewReader_GivenTimeout_KeepsItAndReturnsARedirectAsTheAnswer(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		timeout time.Duration
		status  int
	}{
		{"temporary redirect", 4 * time.Second, http.StatusTemporaryRedirect},
		{"found", 900 * time.Millisecond, http.StatusFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var targetHits atomic.Int32
			target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { targetHits.Add(1) }))
			t.Cleanup(target.Close)
			front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, target.URL+"/elsewhere", tc.status)
			}))
			t.Cleanup(front.Close)
			reader := NewReader(front.URL, "tok", "secret", tc.timeout)

			resp, err := reader.client.Get(front.URL + "/start")

			require.NoError(t, err, "the reader's client returns the redirect as the response")
			t.Cleanup(func() { _ = resp.Body.Close() })
			assert.Equal(t, tc.timeout, reader.client.Timeout)
			assert.Equal(t, tc.status, resp.StatusCode)
			assert.Equal(t, target.URL+"/elsewhere", resp.Header.Get("Location"))
			assert.Zero(t, targetHits.Load(), "the redirect target is never called")
		})
	}
}
