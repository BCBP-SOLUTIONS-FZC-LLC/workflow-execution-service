package main

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

// w27bQueues lists a fixed set of active queues, or fails.
type w27bQueues struct {
	port.ActiveTaskQueueRepository
	active []*domain.ActiveTaskQueue
	err    error
	calls  int
}

func (q *w27bQueues) ListActive(context.Context) ([]*domain.ActiveTaskQueue, error) {
	q.calls++
	return q.active, q.err
}

// w27bPollSeries reads the last-poll gauge series of the given queues; a queue with no series is absent from the result.
func w27bPollSeries(t *testing.T, queues ...string) map[string]float64 {
	t.Helper()
	wanted := map[string]bool{}
	for _, q := range queues {
		wanted[q] = true
	}
	ch := make(chan prometheus.Metric, 1024)
	observability.WorkerQueueLastPollTimestamp.Collect(ch)
	close(ch)
	out := map[string]float64{}
	for m := range ch {
		var pb dto.Metric
		require.NoError(t, m.Write(&pb))
		for _, label := range pb.GetLabel() {
			if label.GetName() == "queue" && wanted[label.GetValue()] {
				out[label.GetValue()] = pb.GetGauge().GetValue()
			}
		}
	}
	return out
}

func TestPollQueueTopologyOnce_LastPollGaugePerQueue(t *testing.T) {
	cases := []struct {
		name string
		// listErr makes ListActive fail.
		listErr error
		// failStart makes the dynamic queue's worker fail to start.
		failStart bool
		// preexisting registers the dynamic queue before the poll.
		preexisting bool
		wantDefault bool
		wantDynamic bool
	}{
		{name: "a successful poll stamps the default queue and a newly started queue", wantDefault: true, wantDynamic: true},
		{name: "a poll that finds the queue already running stamps it again", preexisting: true, wantDefault: true, wantDynamic: true},
		{name: "a queue whose worker fails to start gets no stamp, the default queue still does", failStart: true, wantDefault: true},
		{name: "a failed listing stamps nothing", listErr: assert.AnError},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obstest.Init(t)
			tenant := uuid.New()
			defaultQueue := "w27b-default-" + uuid.NewString()
			dynamicQueue := domain.TenantTaskQueueName(tenant)
			var failing []string
			if tc.failStart {
				failing = append(failing, dynamicQueue)
			}
			t.Cleanup(func() {
				observability.WorkerQueueLastPollTimestamp.DeleteLabelValues(defaultQueue)
				observability.WorkerQueueLastPollTimestamp.DeleteLabelValues(dynamicQueue)
			})
			starter := useTopoStarter(t, failing...)
			registry := newWorkerRegistry()
			if tc.preexisting {
				registry.add(dynamicQueue, nil)
			}
			queues := &w27bQueues{active: []*domain.ActiveTaskQueue{{TenantID: tenant, QueueName: dynamicQueue}}, err: tc.listErr}
			begin := float64(time.Now().Unix())

			pollQueueTopologyOnce(context.Background(), nil, nil, queues, defaultQueue, registry, quietLogger{})

			assert.Equal(t, 1, queues.calls)
			series := w27bPollSeries(t, defaultQueue, dynamicQueue)
			if tc.wantDefault {
				assert.GreaterOrEqual(t, series[defaultQueue], begin, "the default queue's gauge is set to the time of the poll")
			} else {
				assert.NotContains(t, series, defaultQueue)
			}
			if tc.wantDynamic {
				assert.GreaterOrEqual(t, series[dynamicQueue], begin, "the dynamic queue's gauge is set to the time of the poll")
			} else {
				assert.NotContains(t, series, dynamicQueue)
			}
			_, started := starter.started[dynamicQueue]
			assert.Equal(t, !tc.preexisting && !tc.failStart && tc.listErr == nil, started, "a worker is started only for a queue that is new")
		})
	}
}
