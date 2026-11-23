package main

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRunRelay_ExitReachesTheErrorChannelUnlessItIsACleanStop(t *testing.T) {
	boom := errors.New("claim query failed")
	tests := []struct {
		name    string
		exit    error
		wantErr error
	}{
		{"a failed relay stops the process", boom, boom},
		{"a wrapped failure keeps its cause", fmt.Errorf("poll: %w", boom), boom},
		{"a clean return is no error", nil, nil},
		{"a cancelled context is a clean stop", context.Canceled, nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			a, _ := shutdownTestApp(0)
			serveErrs := make(chan error, 2)

			a.runRelay(func(context.Context) error { return tc.exit }, serveErrs)

			if tc.wantErr == nil {
				assert.Empty(t, serveErrs)
				return
			}
			require.Len(t, serveErrs, 1)
			got := <-serveErrs
			assert.ErrorIs(t, got, tc.wantErr)
			assert.Contains(t, got.Error(), "outbox relay")
		})
	}
}

func TestRunRelay_AFullErrorChannelDoesNotBlockTheRelayGoroutine(t *testing.T) {
	a, _ := shutdownTestApp(0)
	serveErrs := make(chan error, 2)
	serveErrs <- errors.New("HTTP server: bind")
	serveErrs <- errors.New("metrics server: bind")

	done := make(chan struct{})
	go func() {
		a.runRelay(func(context.Context) error { return errors.New("boom") }, serveErrs)
		close(done)
	}()

	<-done
	assert.Len(t, serveErrs, 2)
}
