package openbao_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/openbao"
)

func TestReader_Read_ASiblingFieldOfAnotherTypeDoesNotFailTheRead(t *testing.T) {
	cases := []struct {
		name    string
		secret  string
		field   string
		want    string
		wantErr error
	}{
		{"number sibling", `{"accessKey":"secret-value","expiresIn":3600}`, "accessKey", "secret-value", nil},
		{"object sibling", `{"accessKey":"secret-value","labels":{"team":"a"}}`, "accessKey", "secret-value", nil},
		{"null sibling", `{"accessKey":"secret-value","note":null}`, "accessKey", "secret-value", nil},
		{"absent field beside a number", `{"expiresIn":3600}`, "accessKey", "", openbao.ErrSecretNotFound},
		{"requested field is a number", `{"accessKey":3600}`, "accessKey", "", openbao.ErrUpstreamUnavailable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(`{"data":{"data":` + tc.secret + `,"metadata":{}}}`))
			}))
			defer srv.Close()
			reader := openbao.NewReader(srv.URL, "test-token", "secret", time.Second)

			value, err := reader.Read(context.Background(), validPath, tc.field)

			if tc.wantErr != nil {
				require.Error(t, err)
				assert.True(t, errors.Is(err, tc.wantErr), err.Error())
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, value)
		})
	}
}
