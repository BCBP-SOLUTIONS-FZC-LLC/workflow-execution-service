package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type deadlinePinger struct {
	deadline time.Time
	ok       bool
	err      error
}

func (p *deadlinePinger) Ping(ctx context.Context) error {
	p.deadline, p.ok = ctx.Deadline()
	return p.err
}

func TestReadyz_TheTemporalCheckRunsUnderAboutTwoSecondsAndItsFailureAnswers503NamingTemporal(t *testing.T) {
	temporal := &deadlinePinger{err: context.DeadlineExceeded}
	r := testRouter(fakeDBPinger{health: DBHealth{Healthy: true}}, fakePinger{}, temporal)
	started := time.Now()

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/readyz", nil))

	require.True(t, temporal.ok, "the check carries a deadline")
	assert.WithinDuration(t, started.Add(2*time.Second), temporal.deadline, 500*time.Millisecond)
	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.JSONEq(t, `{"status":"unavailable","temporal":"unreachable"}`, w.Body.String())
}
