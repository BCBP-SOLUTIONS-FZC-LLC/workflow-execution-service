package openbao_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/openbao"
)

func TestReader_Read_ARefusedTokenIsUnauthorizedWhicheverStatusRefusesIt(t *testing.T) {
	t.Parallel()
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) }))
			t.Cleanup(srv.Close)

			_, err := openbao.NewReader(srv.URL, "test-token", "secret", time.Second).Read(context.Background(), validPath, "accessKey")

			require.ErrorIs(t, err, openbao.ErrUnauthorized)
			assert.NotErrorIs(t, err, openbao.ErrUpstreamUnavailable)
		})
	}
}
