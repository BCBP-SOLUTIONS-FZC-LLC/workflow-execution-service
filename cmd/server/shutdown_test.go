package main

import (
	"errors"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	httpadapter "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
)

type recordingSleep struct {
	mu              sync.Mutex
	calls           []time.Duration
	drainingAtSleep []bool
	flag            *atomic.Bool
}

func (r *recordingSleep) sleep(d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, d)
	r.drainingAtSleep = append(r.drainingAtSleep, r.flag.Load())
}

func shutdownTestApp(delay time.Duration) (*app, *recordingSleep) {
	flag := &atomic.Bool{}
	rec := &recordingSleep{flag: flag}
	return &app{
		cfg:          &config.Config{ShutdownDrainDelay: delay},
		log:          silentLogger{},
		shuttingDown: flag,
		sleep:        rec.sleep,
	}, rec
}

func TestAwaitStop_ASignalTurnsReadinessOffThenWaitsTheDrainDelay(t *testing.T) {
	a, rec := shutdownTestApp(7 * time.Second)
	quit := make(chan os.Signal, 1)
	quit <- syscall.SIGTERM

	err := a.awaitStop(quit, make(chan error))

	require.NoError(t, err)
	assert.True(t, a.shuttingDown.Load())
	assert.Equal(t, []time.Duration{7 * time.Second}, rec.calls)
	assert.Equal(t, []bool{true}, rec.drainingAtSleep, "readiness must already answer 503 while the drain waits")
}

func TestAwaitStop_AZeroDrainDelaySkipsTheWait(t *testing.T) {
	a, rec := shutdownTestApp(0)
	quit := make(chan os.Signal, 1)
	quit <- syscall.SIGTERM

	require.NoError(t, a.awaitStop(quit, make(chan error)))

	assert.True(t, a.shuttingDown.Load())
	assert.Empty(t, rec.calls)
}

func TestAwaitStop_AListenerFailureStopsThroughTheNormalPathWithoutADrain(t *testing.T) {
	a, rec := shutdownTestApp(7 * time.Second)
	serveErrs := make(chan error, 1)
	serveErrs <- errors.New("HTTP server: listen tcp :8080: bind: address already in use")

	err := a.awaitStop(make(chan os.Signal), serveErrs)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "address already in use")
	assert.Empty(t, rec.calls)
}

func TestListen_APortThatCannotBeBoundReportsOnTheChannelInsteadOfExiting(t *testing.T) {
	busy, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = busy.Close() })
	a, _ := shutdownTestApp(0)
	serveErrs := make(chan error, 1)

	a.listen("HTTP", &http.Server{Addr: busy.Addr().String(), ReadHeaderTimeout: time.Second}, serveErrs)

	select {
	case got := <-serveErrs:
		require.Error(t, got)
		assert.Contains(t, got.Error(), "HTTP server")
	case <-time.After(30 * time.Second):
		t.Fatal("a bind failure was not reported")
	}
}

func TestNewHTTPServer_WriteTimeoutOutlastsTheRequestTimeoutSoItsAnswerReachesTheCaller(t *testing.T) {
	srv := newHTTPServer(":0", http.NewServeMux())

	assert.Greater(t, srv.WriteTimeout, httpadapter.RequestTimeout)
	assert.Equal(t, ":0", srv.Addr)
}
