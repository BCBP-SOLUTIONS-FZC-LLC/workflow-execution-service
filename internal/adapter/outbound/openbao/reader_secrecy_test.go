package openbao

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	cwcliToken       = "cwcli-vault-token-4471"
	cwcliSecretValue = "cwcli-secret-value-9082"
)

// Whatever OpenBao sends back (the secret itself, or the token echoed in an error page), the errors the Reader
// returns carry the path, the field name and the kind of failure, never the vault token or a secret value.
func TestReader_Read_ErrorsNeverCarryTheVaultTokenOrASecretValue(t *testing.T) {
	cases := []struct {
		name    string
		status  int
		body    string
		wantAll []string
	}{
		{"field missing next to another secret", http.StatusOK, `{"data":{"data":{"other":"` + cwcliSecretValue + `"}}}`, []string{cwcliPath, cwcliField}},
		{"truncated answer holding the secret", http.StatusOK, `{"data":{"data":{"accessKey":"` + cwcliSecretValue, []string{cwcliPath}},
		{"answer of the wrong shape holding the secret", http.StatusOK, `{"data":{"data":{"accessKey":["` + cwcliSecretValue + `"]}}}`, []string{cwcliPath}},
		{"secret not found", http.StatusNotFound, `{"errors":["` + cwcliToken + `","` + cwcliSecretValue + `"]}`, []string{cwcliPath}},
		{"forbidden", http.StatusForbidden, `{"errors":["` + cwcliToken + `","` + cwcliSecretValue + `"]}`, []string{cwcliPath}},
		{"server error", http.StatusInternalServerError, `server error`, []string{cwcliPath, "500"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var gotToken string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotToken = r.Header.Get("X-Vault-Token")
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			t.Cleanup(srv.Close)

			_, err := NewReader(srv.URL, cwcliToken, "secret", time.Second).Read(context.Background(), cwcliPath, cwcliField)

			require.Error(t, err)
			assert.Equal(t, cwcliToken, gotToken, "the token was sent to OpenBao")
			for _, want := range tc.wantAll {
				assert.Contains(t, err.Error(), want)
			}
			assert.NotContains(t, err.Error(), cwcliToken)
			assert.NotContains(t, err.Error(), cwcliSecretValue)
		})
	}

	t.Run("OpenBao unreachable", func(t *testing.T) {
		_, err := NewReader("http://127.0.0.1:1", cwcliToken, "secret", time.Second).Read(context.Background(), cwcliPath, cwcliField)

		require.ErrorIs(t, err, ErrUpstreamUnavailable)
		assert.Contains(t, err.Error(), cwcliPath)
		assert.NotContains(t, err.Error(), cwcliToken)
	})

	t.Run("path refused before any call", func(t *testing.T) {
		_, err := NewReader("http://127.0.0.1:1", cwcliToken, "secret", time.Second).Read(context.Background(), "connectors/a/../b/c", cwcliField)

		require.ErrorIs(t, err, ErrInvalidPath)
		assert.NotContains(t, err.Error(), cwcliToken)
	})
}
