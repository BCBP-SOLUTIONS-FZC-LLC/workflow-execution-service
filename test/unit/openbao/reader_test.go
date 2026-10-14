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

const validPath = "connectors/11111111-1111-1111-1111-111111111111/storage/accessKey"

func TestReader_Read_Success(t *testing.T) {
	t.Parallel()

	var gotPath, gotToken string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotToken = r.Header.Get("X-Vault-Token")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"data":{"accessKey":"secret-value"},"metadata":{}}}`))
	}))
	defer srv.Close()

	reader := openbao.NewReader(srv.URL, "test-token", "secret", time.Second)
	value, err := reader.Read(context.Background(), validPath, "accessKey")
	require.NoError(t, err)
	assert.Equal(t, "secret-value", value)
	assert.Equal(t, "/v1/secret/data/"+validPath, gotPath)
	assert.Equal(t, "test-token", gotToken)
}

func TestReader_Read_NotFound(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	reader := openbao.NewReader(srv.URL, "test-token", "secret", time.Second)
	_, err := reader.Read(context.Background(), validPath, "accessKey")
	require.Error(t, err)
	assert.True(t, errors.Is(err, openbao.ErrSecretNotFound))
}

func TestReader_Read_Forbidden(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()

	reader := openbao.NewReader(srv.URL, "test-token", "secret", time.Second)
	_, err := reader.Read(context.Background(), validPath, "accessKey")
	require.Error(t, err)
	assert.True(t, errors.Is(err, openbao.ErrUnauthorized))
}

func TestReader_Read_OtherStatus_IsUpstreamUnavailable(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	reader := openbao.NewReader(srv.URL, "test-token", "secret", time.Second)
	_, err := reader.Read(context.Background(), validPath, "accessKey")
	require.Error(t, err)
	assert.True(t, errors.Is(err, openbao.ErrUpstreamUnavailable))
}

func TestReader_Read_FieldMissingFromResponse(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"data":{"otherField":"x"}}}`))
	}))
	defer srv.Close()

	reader := openbao.NewReader(srv.URL, "test-token", "secret", time.Second)
	_, err := reader.Read(context.Background(), validPath, "accessKey")
	require.Error(t, err)
	assert.True(t, errors.Is(err, openbao.ErrSecretNotFound))
}

func TestReader_Read_MalformedJSON(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`not json`))
	}))
	defer srv.Close()

	reader := openbao.NewReader(srv.URL, "test-token", "secret", time.Second)
	_, err := reader.Read(context.Background(), validPath, "accessKey")
	require.Error(t, err)
	assert.True(t, errors.Is(err, openbao.ErrUpstreamUnavailable))
}

func TestReader_Read_ConnectionError(t *testing.T) {
	t.Parallel()

	reader := openbao.NewReader("http://127.0.0.1:1", "test-token", "secret", 200*time.Millisecond)
	_, err := reader.Read(context.Background(), validPath, "accessKey")
	require.Error(t, err)
	assert.True(t, errors.Is(err, openbao.ErrUpstreamUnavailable))
}

func TestReader_Read_NilContext_BuildRequestFails(t *testing.T) {
	t.Parallel()

	reader := openbao.NewReader("http://127.0.0.1", "test-token", "secret", time.Second)
	//nolint:staticcheck // deliberately nil to exercise http.NewRequestWithContext's own nil-context guard
	_, err := reader.Read(nil, validPath, "accessKey")
	require.Error(t, err)
}

func TestReader_Read_PathTraversal_RejectedBeforeAnyCall(t *testing.T) {
	t.Parallel()

	called := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
	}))
	defer srv.Close()

	reader := openbao.NewReader(srv.URL, "test-token", "secret", time.Second)

	cases := []struct {
		name, path, fieldName string
	}{
		{"traversal in connectorType", "connectors/tenant/../../etc/accessKey", "accessKey"},
		{"traversal in fieldName", validPath, "../secretKey"},
		{"wrong prefix", "other/tenant/storage/accessKey", "accessKey"},
		{"too few segments", "connectors/tenant/storage", "accessKey"},
		{"slash in field name", validPath, "a/b"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := reader.Read(context.Background(), tc.path, tc.fieldName)
			require.Error(t, err)
			assert.True(t, errors.Is(err, openbao.ErrInvalidPath))
		})
	}
	assert.False(t, called, "no HTTP call should be made for a rejected path")
}
