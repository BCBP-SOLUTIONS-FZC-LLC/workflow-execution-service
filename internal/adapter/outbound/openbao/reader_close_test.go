package openbao

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	cwcliPath  = "connectors/tenant-1/storage/accessKey"
	cwcliField = "accessKey"
)

type cwcliCloseTracker struct {
	io.Reader
	closed atomic.Int32
}

func (b *cwcliCloseTracker) Close() error {
	b.closed.Add(1)
	return nil
}

type cwcliRoundTripper struct {
	status int
	body   string
	last   *cwcliCloseTracker
}

func (rt *cwcliRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	rt.last = &cwcliCloseTracker{Reader: strings.NewReader(rt.body)}
	return &http.Response{StatusCode: rt.status, Body: rt.last, Header: http.Header{}, Request: req}, nil
}

func TestReader_Read_ClosesTheResponseBodyOnEveryPath(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		want   error
	}{
		{"value found", http.StatusOK, `{"data":{"data":{"accessKey":"v"}}}`, nil},
		{"field missing", http.StatusOK, `{"data":{"data":{"other":"v"}}}`, ErrSecretNotFound},
		{"undecodable answer", http.StatusOK, `not json`, ErrUpstreamUnavailable},
		{"secret not found", http.StatusNotFound, ``, ErrSecretNotFound},
		{"forbidden", http.StatusForbidden, ``, ErrUnauthorized},
		{"server error", http.StatusInternalServerError, `oops`, ErrUpstreamUnavailable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rt := &cwcliRoundTripper{status: tc.status, body: tc.body}
			r := NewReader("http://openbao.invalid", "tok", "secret", time.Second)
			r.client = &http.Client{Transport: rt}

			_, err := r.Read(context.Background(), cwcliPath, cwcliField)

			if tc.want == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.want)
			}
			require.NotNil(t, rt.last)
			assert.EqualValues(t, 1, rt.last.closed.Load(), "the body is closed exactly once")
		})
	}
}

func TestReader_Read_ContextCancelledBeforeTheCall_FailsUnavailableWithoutReachingTheServer(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	value, err := NewReader(srv.URL, "tok", "secret", time.Second).Read(ctx, cwcliPath, cwcliField)

	require.ErrorIs(t, err, ErrUpstreamUnavailable)
	assert.Empty(t, value)
	assert.Zero(t, hits.Load(), "a cancelled call never reaches OpenBao")
}

func TestReader_Read_ContextCancelledDuringTheCall_FailsUnavailable(t *testing.T) {
	arrived := make(chan struct {
		path, token string
	}, 1)
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		arrived <- struct{ path, token string }{r.URL.Path, r.Header.Get("X-Vault-Token")}
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(release) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type result struct {
		value string
		err   error
	}
	results := make(chan result, 1)
	go func() {
		v, err := NewReader(srv.URL, "tok", "secret", 30*time.Second).Read(ctx, cwcliPath, cwcliField)
		results <- result{v, err}
	}()

	got := <-arrived
	cancel()
	res := <-results

	assert.Equal(t, "/v1/secret/data/"+cwcliPath, got.path)
	assert.Equal(t, "tok", got.token)
	require.ErrorIs(t, res.err, ErrUpstreamUnavailable)
	assert.False(t, errors.Is(res.err, ErrSecretNotFound))
	assert.Empty(t, res.value)
}
