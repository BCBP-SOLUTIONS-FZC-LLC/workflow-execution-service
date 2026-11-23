package main

import (
	"context"
	"net"
	"net/http"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPingValkeyOrClose_ClosesTheClientWhenThePingFails(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: "127.0.0.1:1", DialTimeout: 100 * time.Millisecond})

	err := pingValkeyOrClose(client, 200*time.Millisecond)

	require.Error(t, err)
	assert.ErrorContains(t, err, "valkey ping")
	assert.ErrorIs(t, client.Ping(context.Background()).Err(), redis.ErrClosed)
}

func TestServeHealth_ReportsAListenFailureAndStaysQuietOnShutdown(t *testing.T) {
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = busy.Close() }()

	failing := make(chan error, 1)
	serveHealth(&http.Server{Addr: busy.Addr().String()}, failing)
	select {
	case got := <-failing:
		assert.ErrorContains(t, got, "health server")
	case <-time.After(30 * time.Second):
		t.Fatal("a listen failure was not reported")
	}

	closed := make(chan error, 1)
	srv := &http.Server{Addr: "127.0.0.1:0"}
	require.NoError(t, srv.Shutdown(context.Background()))
	serveHealth(srv, closed)
	select {
	case got := <-closed:
		t.Fatalf("an orderly shutdown was reported as a failure: %v", got)
	case <-time.After(200 * time.Millisecond):
	}
}

func TestAwaitStop_ReturnsTheListenFailureAndNilOnASignal(t *testing.T) {
	sigCh := make(chan os.Signal, 1)
	errs := make(chan error, 1)
	errs <- assert.AnError
	assert.ErrorIs(t, awaitStop(sigCh, errs, noopLogger{}), assert.AnError)

	sigCh <- syscall.SIGTERM
	assert.NoError(t, awaitStop(sigCh, make(chan error), noopLogger{}))
}
