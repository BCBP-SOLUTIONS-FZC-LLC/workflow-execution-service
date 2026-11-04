package main

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

type fakeQueues struct {
	port.ActiveTaskQueueRepository
	active []*domain.ActiveTaskQueue
}

func (f *fakeQueues) ListActive(context.Context) ([]*domain.ActiveTaskQueue, error) {
	return f.active, nil
}

type quietLogger struct{}

func (quietLogger) Debug(string, map[string]any) {}
func (quietLogger) Info(string, map[string]any)  {}
func (quietLogger) Warn(string, map[string]any)  {}
func (quietLogger) Error(string, map[string]any) {}

func TestPollQueueTopologyOnce_EveryTickRefreshesThePollTimestampOfEveryQueueThisWorkerServes(t *testing.T) {
	obstest.Init(t)
	tenant := uuid.New()
	queueName := domain.TenantTaskQueueName(tenant)
	registry := newWorkerRegistry()
	registry.add(queueName, nil)
	queues := &fakeQueues{active: []*domain.ActiveTaskQueue{{TenantID: tenant, QueueName: queueName}}}
	gauge := observability.WorkerQueueLastPollTimestamp.WithLabelValues(queueName)
	gauge.Set(1)

	pollQueueTopologyOnce(context.Background(), nil, nil, queues, "wf-default", registry, quietLogger{})

	if got := testutil.ToFloat64(gauge); time.Since(time.Unix(int64(got), 0)) > time.Minute {
		t.Errorf("poll timestamp of a queue already served = %v: it was set once at start, so WorkerQueuePollingStalled fires for it five minutes later", time.Unix(int64(got), 0))
	}
}
