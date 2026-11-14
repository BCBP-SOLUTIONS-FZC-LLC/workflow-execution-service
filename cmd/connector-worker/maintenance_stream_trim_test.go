package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/valkeystream"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
)

var _ streamTrimmer = (*valkeystream.Consumer)(nil)

type strfixTrimCall struct {
	stream string
	group  string
	cutoff time.Time
}

type strfixTrimConsumer struct {
	streamConsumer
	mu          sync.Mutex
	pendingTrim []strfixTrimCall
	ageTrim     []strfixTrimCall
	pendingErr  error
	ageErr      error
}

func (c *strfixTrimConsumer) TrimToOldestPending(_ context.Context, stream, group string) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pendingTrim = append(c.pendingTrim, strfixTrimCall{stream: stream, group: group})
	return 0, c.pendingErr
}

func (c *strfixTrimConsumer) TrimOlderThan(_ context.Context, stream string, cutoff time.Time) (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ageTrim = append(c.ageTrim, strfixTrimCall{stream: stream, cutoff: cutoff})
	return 0, c.ageErr
}

func (c *strfixTrimConsumer) trimCounts() (int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pendingTrim), len(c.ageTrim)
}

func strfixStreamDeps(c *strfixTrimConsumer) *deps {
	return &deps{consumer: c, streamKey: "strfix-tasks", streamGroup: "strfix-group", deadLetterKey: "strfix-tasks-dead"}
}

func TestTrimStreams_TrimsTaskStreamToPendingAndDeadLettersByAge(t *testing.T) {
	now := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	consumer := &strfixTrimConsumer{}

	trimStreams(context.Background(), strfixStreamDeps(consumer), now, 72*time.Hour, &recordingLogger{})

	require.Len(t, consumer.pendingTrim, 1)
	assert.Equal(t, strfixTrimCall{stream: "strfix-tasks", group: "strfix-group"}, consumer.pendingTrim[0])
	require.Len(t, consumer.ageTrim, 1)
	assert.Equal(t, "strfix-tasks-dead", consumer.ageTrim[0].stream)
	assert.Equal(t, now.Add(-72*time.Hour), consumer.ageTrim[0].cutoff)
}

func TestTrimStreams_TaskStreamTrimFails_DeadLetterStillTrimmedAndFailureLogged(t *testing.T) {
	consumer := &strfixTrimConsumer{pendingErr: errors.New("strfix trim refused")}
	log := &recordingLogger{}

	trimStreams(context.Background(), strfixStreamDeps(consumer), time.Now(), time.Hour, log)

	assert.Len(t, consumer.ageTrim, 1)
	require.Len(t, log.warns, 1)
	assert.Equal(t, "strfix trim refused", log.warns[0]["error"])
}

func TestTrimStreams_DeadLetterTrimFails_FailureLogged(t *testing.T) {
	consumer := &strfixTrimConsumer{ageErr: errors.New("strfix age trim refused")}
	log := &recordingLogger{}

	trimStreams(context.Background(), strfixStreamDeps(consumer), time.Now(), time.Hour, log)

	assert.Len(t, consumer.pendingTrim, 1)
	require.Len(t, log.warns, 1)
	assert.Equal(t, "strfix age trim refused", log.warns[0]["error"])
}

func TestRunMaintenanceLoop_TrimsStreamsOnceAtStartup(t *testing.T) {
	consumer := &strfixTrimConsumer{}
	d := strfixStreamDeps(consumer)
	d.pruner = &fakePruner{batches: []int64{0}}
	d.pools = map[string]*typePool{}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runMaintenanceLoop(ctx, d, &config.Config{ConnectorAliasRefreshInterval: time.Hour, ConnectorAuditRetention: time.Hour}, noopLogger{})
		close(done)
	}()

	require.Eventually(t, func() bool {
		pending, age := consumer.trimCounts()
		return pending == 1 && age == 1
	}, time.Second, 5*time.Millisecond)
	cancel()
	<-done
}

func TestRunMaintenanceLoop_TrimsDeadLettersOlderThanTheConfiguredWindow(t *testing.T) {
	now := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	consumer := &strfixTrimConsumer{}
	d := strfixStreamDeps(consumer)
	d.pruner = &fakePruner{batches: []int64{0}}
	d.pools = map[string]*typePool{}
	d.now = func() time.Time { return now }
	cfg := &config.Config{ConnectorAliasRefreshInterval: time.Hour, ConnectorAuditRetention: time.Hour, ConnectorStreamDeadLetterRetention: 48 * time.Hour}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runMaintenanceLoop(ctx, d, cfg, noopLogger{})
		close(done)
	}()

	require.Eventually(t, func() bool {
		_, age := consumer.trimCounts()
		return age == 1
	}, time.Second, 5*time.Millisecond)
	cancel()
	<-done

	consumer.mu.Lock()
	defer consumer.mu.Unlock()
	assert.Equal(t, now.Add(-48*time.Hour), consumer.ageTrim[0].cutoff)
}
