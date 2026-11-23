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

func TestReader_Read_AnAnswerWithoutTheRequestedField(t *testing.T) {
	cases := []struct {
		name        string
		body        string
		wantErr     error
		wantMessage string
	}{
		{"field absent from the data", `{"data":{"data":{"other":"x"},"metadata":{}}}`, openbao.ErrSecretNotFound, "has no field"},
		{"data object null", `{"data":{"data":null,"metadata":{}}}`, openbao.ErrSecretNotFound, "has no field"},
		{"data object missing", `{"data":{"metadata":{}}}`, openbao.ErrSecretNotFound, "has no field"},
		{"outer data missing", `{}`, openbao.ErrSecretNotFound, "has no field"},
		{"empty body", ``, openbao.ErrUpstreamUnavailable, "decode response"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			reader := openbao.NewReader(srv.URL, "test-token", "secret", time.Second)

			_, err := reader.Read(context.Background(), validPath, "accessKey")

			require.ErrorIs(t, err, tc.wantErr)
			assert.Contains(t, err.Error(), tc.wantMessage)
		})
	}
}
