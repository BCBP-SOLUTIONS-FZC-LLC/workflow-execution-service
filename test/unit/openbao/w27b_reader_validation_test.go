package openbao_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/openbao"
)

const w27bSecretToken = "w27b-token-5f3a9c"

// w27bCountingServer answers 200 with a secret holding every field name used below and counts the requests.
func w27bCountingServer(t *testing.T) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte(`{"data":{"data":{"accessKey":"v","a-b_C9":"ok"}}}`))
	}))
	t.Cleanup(srv.Close)
	return srv, &hits
}

func TestReader_Read_SecretPathShape(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		path    string
		wantErr bool
	}{
		{name: "the three-segment path is read", path: "connectors/tenant-1/storage/accessKey"},
		{name: "extra segment", path: "connectors/tenant/storage/accessKey/extra", wantErr: true},
		{name: "missing last segment", path: "connectors/tenant/storage", wantErr: true},
		{name: "only the prefix", path: "connectors", wantErr: true},
		{name: "dot as a segment", path: "connectors/tenant/./accessKey", wantErr: true},
		{name: "dot in a segment", path: "connectors/tenant/storage/access.Key", wantErr: true},
		{name: "dot-dot as the last segment", path: "connectors/tenant/storage/..", wantErr: true},
		{name: "traversal in front", path: "../connectors/tenant/storage/accessKey", wantErr: true},
		{name: "leading slash", path: "/connectors/tenant/storage/accessKey", wantErr: true},
		{name: "trailing slash", path: "connectors/tenant/storage/accessKey/", wantErr: true},
		{name: "trailing newline", path: "connectors/tenant/storage/accessKey\n", wantErr: true},
		{name: "empty path", path: "", wantErr: true},
		{name: "unicode letter in a segment", path: "connectors/ténant/storage/accessKey", wantErr: true},
		{name: "full-width letter in a segment", path: "connectors/tenant/storage/ａccessKey", wantErr: true},
		{name: "space in a segment", path: "connectors/ten ant/storage/accessKey", wantErr: true},
		{name: "upper-case prefix", path: "Connectors/tenant/storage/accessKey", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv, hits := w27bCountingServer(t)
			reader := openbao.NewReader(srv.URL, w27bSecretToken, "secret", time.Second)

			value, err := reader.Read(context.Background(), tc.path, "accessKey")

			if !tc.wantErr {
				require.NoError(t, err)
				assert.Equal(t, "v", value)
				assert.Equal(t, int32(1), hits.Load())
				return
			}
			require.ErrorIs(t, err, openbao.ErrInvalidPath)
			assert.Equal(t, fmt.Sprintf("%s: %q", openbao.ErrInvalidPath, tc.path), err.Error())
			assert.Empty(t, value)
			assert.Zero(t, hits.Load(), "no HTTP call for a refused path")
		})
	}
}

func TestReader_Read_FieldNameShape(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		field   string
		wantErr bool
	}{
		{name: "letters, digits, underscore and hyphen", field: "a-b_C9"},
		{name: "empty", field: "", wantErr: true},
		{name: "with a dot", field: "a.b", wantErr: true},
		{name: "with a slash", field: "a/b", wantErr: true},
		{name: "with a space", field: "a b", wantErr: true},
		{name: "with a trailing newline", field: "a-b_C9\n", wantErr: true},
		{name: "unicode letter", field: "é", wantErr: true},
		{name: "dot-dot", field: "..", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv, hits := w27bCountingServer(t)
			reader := openbao.NewReader(srv.URL, w27bSecretToken, "secret", time.Second)

			value, err := reader.Read(context.Background(), "connectors/tenant/storage/accessKey", tc.field)

			if !tc.wantErr {
				require.NoError(t, err)
				assert.Equal(t, "ok", value)
				assert.Equal(t, int32(1), hits.Load())
				return
			}
			require.ErrorIs(t, err, openbao.ErrInvalidPath)
			assert.Equal(t, fmt.Sprintf("%s: field name %q", openbao.ErrInvalidPath, tc.field), err.Error())
			assert.Empty(t, value)
			assert.Zero(t, hits.Load(), "no HTTP call for a refused field name")
		})
	}
}

func TestReader_Read_TransportFailure_WrapsUpstreamUnavailableWithPathAndURLButNotTheToken(t *testing.T) {
	t.Parallel()

	const path = "connectors/tenant/storage/accessKey"
	hang := make(chan struct{})
	slow := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-hang:
		}
	}))
	t.Cleanup(func() {
		close(hang)
		slow.Close()
	})

	cases := []struct {
		name    string
		addr    string
		timeout time.Duration
	}{
		{name: "connection refused", addr: "http://127.0.0.1:1", timeout: time.Second},
		{name: "client timeout", addr: slow.URL, timeout: 50 * time.Millisecond},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			reader := openbao.NewReader(tc.addr, w27bSecretToken, "secret", tc.timeout)

			value, err := reader.Read(context.Background(), path, "accessKey")

			require.ErrorIs(t, err, openbao.ErrUpstreamUnavailable)
			assert.Empty(t, value)
			prefix := fmt.Sprintf("%s: openbao read %q: ", openbao.ErrUpstreamUnavailable, path)
			assert.Contains(t, err.Error(), prefix, "the message names the secret path after the sentinel text")
			assert.Contains(t, err.Error(), tc.addr+"/v1/secret/data/"+path, "the transport error carries the request URL")
			assert.NotContains(t, err.Error(), w27bSecretToken, "the token never reaches the message")
		})
	}
}
