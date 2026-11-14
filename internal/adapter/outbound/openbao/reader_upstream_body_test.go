package openbao

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const strfixUpstreamMarker = "strfix-upstream-body-marker-5530"

func TestReader_Read_UpstreamErrorAnswer_ErrorNamesPathFieldAndStatusButNotTheBody(t *testing.T) {
	for _, status := range []int{http.StatusInternalServerError, http.StatusBadGateway, http.StatusServiceUnavailable} {
		t.Run(strconv.Itoa(status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
				_, _ = w.Write([]byte(`{"errors":["` + strfixUpstreamMarker + `"]}`))
			}))
			t.Cleanup(srv.Close)

			_, err := NewReader(srv.URL, cwcliToken, "secret", time.Second).Read(context.Background(), cwcliPath, cwcliField)

			require.ErrorIs(t, err, ErrUpstreamUnavailable)
			assert.Contains(t, err.Error(), cwcliPath)
			assert.Contains(t, err.Error(), `field "`+cwcliField+`"`)
			assert.Contains(t, err.Error(), strconv.Itoa(status))
			assert.NotContains(t, err.Error(), strfixUpstreamMarker)
		})
	}
}
