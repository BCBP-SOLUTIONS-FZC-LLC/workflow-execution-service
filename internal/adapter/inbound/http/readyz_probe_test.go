package http_test

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	inboundhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http"
)

func TestReadyz_FailingDependency_Returns503AndIsLoggedAtErrorAndCountedAs5xx(t *testing.T) {
	cases := []struct {
		name    string
		failing string
		arrange func(rig *hrfRig)
	}{
		{name: "database", failing: "db", arrange: func(rig *hrfRig) {
			rig.db.fn = func(context.Context) inboundhttp.DBHealth { return inboundhttp.DBHealth{Healthy: false} }
		}},
		{name: "temporal", failing: "temporal", arrange: func(rig *hrfRig) {
			rig.temporal.fn = func(context.Context) error { return errors.New("temporal down") }
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := hrfNewRig(t, "test")
			tc.arrange(rig)
			before := hrfRequestsTotal(t, http.MethodGet, "/readyz", "5xx")

			w := rig.do(http.MethodGet, "/readyz", nil)

			require.Equal(t, http.StatusServiceUnavailable, w.Code)
			body := hrfBody(t, w)
			assert.Equal(t, "unavailable", body["status"])
			assert.Equal(t, "unreachable", body[tc.failing])
			assert.Equal(t, before+1, hrfRequestsTotal(t, http.MethodGet, "/readyz", "5xx"))
			entry := hrfAccessLogFor(t, rig.logger, "/readyz")
			assert.Equal(t, "error", entry.level)
			assert.Equal(t, http.StatusServiceUnavailable, entry.fields["status"])
		})
	}
}

// hrfRecordDeadline remembers the deadline of the context a probe hands its dependency.
type hrfRecordDeadline struct {
	deadline    time.Time
	sawDeadline bool
}

func (r *hrfRecordDeadline) record(ctx context.Context) {
	r.deadline, r.sawDeadline = ctx.Deadline()
}

func TestReadyz_DependencyProbe_GetsATwoSecondBoundedContext_AndItsFailureReturns503NamingIt(t *testing.T) {
	cases := []struct {
		name    string
		failing string
		arrange func(rig *hrfRig, r *hrfRecordDeadline)
	}{
		{name: "database", failing: "db", arrange: func(rig *hrfRig, r *hrfRecordDeadline) {
			rig.db.fn = func(ctx context.Context) inboundhttp.DBHealth {
				r.record(ctx)
				return inboundhttp.DBHealth{Healthy: false}
			}
		}},
		{name: "temporal", failing: "temporal", arrange: func(rig *hrfRig, r *hrfRecordDeadline) {
			rig.temporal.fn = func(ctx context.Context) error {
				r.record(ctx)
				return errors.New("temporal timed out")
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := hrfNewRig(t, "test")
			rec := &hrfRecordDeadline{}
			tc.arrange(rig, rec)

			w := rig.do(http.MethodGet, "/readyz", nil)

			require.Equal(t, http.StatusServiceUnavailable, w.Code)
			body := hrfBody(t, w)
			assert.Equal(t, "unavailable", body["status"])
			assert.Equal(t, "unreachable", body[tc.failing])
			require.True(t, rec.sawDeadline, "the probe must hand its dependency a bounded context")
			remaining := time.Until(rec.deadline)
			assert.Positive(t, remaining)
			assert.LessOrEqual(t, remaining, 2*time.Second)
		})
	}
}

func TestReadyz_HeadRequest_ChecksEveryDependencyLikeGet(t *testing.T) {
	rig := hrfNewRig(t, "test")

	w := rig.do(http.MethodHead, "/readyz", nil)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, 1, rig.db.calls)
	assert.Equal(t, 1, rig.cacheP.calls)
	assert.Equal(t, 1, rig.temporal.calls)
}

func TestReadyz_CachePing_GetsATwoSecondBoundedContext_AndItsFailureOnlyDegrades(t *testing.T) {
	rig := hrfNewRig(t, "test")
	rec := &hrfRecordDeadline{}
	rig.cacheP.fn = func(ctx context.Context) error {
		rec.record(ctx)
		return errors.New("valkey timed out")
	}

	w := rig.do(http.MethodGet, "/readyz", nil)

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "degraded", hrfBody(t, w)["cache"])
	require.True(t, rec.sawDeadline, "the probe must hand the cache a bounded context")
	remaining := time.Until(rec.deadline)
	assert.Positive(t, remaining)
	assert.LessOrEqual(t, remaining, 2*time.Second)
}
