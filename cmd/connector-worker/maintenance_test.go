package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/connectors"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/connectors/aliasconfig"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/registry"
)

type fakePruner struct {
	mu      sync.Mutex
	batches []int64
	cutoffs []time.Time
	err     error
}

func (p *fakePruner) pruneCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.cutoffs)
}

func (p *fakePruner) PruneAttempts(_ context.Context, cutoff time.Time, _ int) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cutoffs = append(p.cutoffs, cutoff)
	if p.err != nil {
		return 0, p.err
	}
	n := p.batches[0]
	p.batches = p.batches[1:]
	return n, nil
}

func TestPruneAttempts(t *testing.T) {
	now := time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC)

	t.Run("prunes again while a full batch was deleted, from the retention cutoff", func(t *testing.T) {
		p := &fakePruner{batches: []int64{auditPruneBatch, auditPruneBatch, 17}}
		require.NoError(t, pruneAttempts(context.Background(), p, now, 90*24*time.Hour))
		assert.Len(t, p.cutoffs, 3)
		assert.Equal(t, now.Add(-90*24*time.Hour), p.cutoffs[0])
	})
	t.Run("stops at the first short batch", func(t *testing.T) {
		p := &fakePruner{batches: []int64{3}}
		require.NoError(t, pruneAttempts(context.Background(), p, now, time.Hour))
		assert.Len(t, p.cutoffs, 1)
	})
	t.Run("returns the store's error", func(t *testing.T) {
		p := &fakePruner{err: errors.New("db down")}
		assert.ErrorContains(t, pruneAttempts(context.Background(), p, now, time.Hour), "db down")
	})
}

type resettableConnector struct {
	countingConnector
	mu     sync.Mutex
	resets int
}

func (c *resettableConnector) ResetClients() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.resets++
}

func (c *resettableConnector) resetCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.resets
}

func TestResetProviderClients_ResetsOnlyConnectorsThatCache(t *testing.T) {
	caching := &resettableConnector{countingConnector: countingConnector{typ: "caching"}}
	plain := &countingConnector{typ: "plain"}
	d := &deps{pools: map[string]*typePool{
		"caching": {connectorType: "caching", connector: caching},
		"plain":   {connectorType: "plain", connector: plain},
	}}

	resetProviderClients(d)

	assert.Equal(t, 1, caching.resetCount())
}

func TestRunMaintenanceLoop_ResetsClientsOnItsPeriodAndStopsWithTheContext(t *testing.T) {
	caching := &resettableConnector{countingConnector: countingConnector{typ: "caching"}}
	d := &deps{pruner: &fakePruner{batches: []int64{0}}, pools: map[string]*typePool{"caching": {connectorType: "caching", connector: caching}}}
	cfg := &config.Config{ConnectorAliasRefreshInterval: 5 * time.Millisecond, ConnectorAuditRetention: time.Hour}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runMaintenanceLoop(ctx, d, cfg, noopLogger{})
		close(done)
	}()

	require.Eventually(t, func() bool { return caching.resetCount() > 0 }, time.Second, 5*time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("the loop did not stop with its context")
	}
}

func TestBuildDocRefs_IsDisabledWithoutABucket(t *testing.T) {
	refs, err := buildDocRefs(context.Background(), &config.Config{}, nil)
	require.NoError(t, err)
	assert.Nil(t, refs)
}

func TestObserveFailures_LogsTheDecisionAndCountsIt(t *testing.T) {
	obstest.Init(t)
	const metric = "workflow_execution_service_connector_task_failures_total"
	labels := map[string]string{"connector_type": "storage", "provider": "aws-s3", "class": "transient", "retried": "true"}
	before := obstest.Sum(t, metric, labels)
	log := &recordingLogger{}
	job := dispatchJob{tenantID: uuid.New(), taskID: uuid.New(), connectorType: "storage"}

	observeFailures(log, job, map[string]any{"provider": "aws-s3"})(2, connectors.RetryDecision{Retry: true, Class: connectors.ClassTransient, Reason: "http 503", Rule: "transient"})

	assert.Equal(t, before+1, obstest.Sum(t, metric, labels))
	require.Len(t, log.warns, 1)
	assert.Equal(t, "aws-s3", log.warns[0]["provider"])
	assert.Equal(t, 2, log.warns[0]["attempt"])
	assert.Equal(t, "transient", log.warns[0]["error_class"])
	assert.Equal(t, true, log.warns[0]["retry"])
}

type recordingLogger struct{ warns []map[string]any }

func (*recordingLogger) Debug(string, map[string]any) {}
func (*recordingLogger) Info(string, map[string]any)  {}
func (l *recordingLogger) Warn(_ string, fields map[string]any) {
	l.warns = append(l.warns, fields)
}
func (*recordingLogger) Error(string, map[string]any) {}

func TestRunMaintenanceLoop_PrunesOnceAtStartup(t *testing.T) {
	pruner := &fakePruner{batches: []int64{0}}
	d := &deps{pruner: pruner, pools: map[string]*typePool{}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runMaintenanceLoop(ctx, d, &config.Config{ConnectorAliasRefreshInterval: time.Hour, ConnectorAuditRetention: time.Hour}, noopLogger{})
		close(done)
	}()

	require.Eventually(t, func() bool { return pruner.pruneCount() == 1 }, time.Second, 5*time.Millisecond)
	cancel()
	<-done
}

func TestAliases_AreReplacedOnRefresh(t *testing.T) {
	d := &deps{aliases: aliasconfig.Config{RestCall: []aliasconfig.Endpoint{{Alias: "a", Method: "GET"}}}}
	assert.Equal(t, "GET", aliasMethod(registry.TypeRestCall, map[string]any{"endpointAlias": "a"}, d.currentAliases()))

	d.setAliases(aliasconfig.Config{RestCall: []aliasconfig.Endpoint{{Alias: "a", Method: "POST"}}})

	assert.Equal(t, "POST", aliasMethod(registry.TypeRestCall, map[string]any{"endpointAlias": "a"}, d.currentAliases()))
}
