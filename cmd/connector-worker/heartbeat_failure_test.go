package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const cwcliHeartbeatFailedMsg = "connector-worker: heartbeat failed, in-flight entries may be reclaimed"

func TestHeartbeatOnce_TouchFailure_WarnsWithTheEntryCountAndNamesTheStreamAndConsumer(t *testing.T) {
	consumer := &cwcliConsumer{touchErr: errors.New("xclaim: loading")}
	d := &deps{consumer: consumer, streamKey: cwcliStream, streamGroup: cwcliGroup, consumerName: cwcliConsumerName}
	require.True(t, d.inflight.claim("1-0"))
	require.True(t, d.inflight.claim("2-0"))
	log := &cwcliLogger{}

	heartbeatOnce(context.Background(), d, log)

	calls := consumer.touchCalls()
	require.Len(t, calls, 1)
	assert.Equal(t, cwcliStream, calls[0].stream)
	assert.Equal(t, cwcliGroup, calls[0].group)
	assert.Equal(t, cwcliConsumerName, calls[0].consumer)
	assert.ElementsMatch(t, []string{"1-0", "2-0"}, calls[0].ids)
	warns := log.withMessage("warn", cwcliHeartbeatFailedMsg)
	require.Len(t, warns, 1)
	assert.Equal(t, 2, warns[0].fields["entries"])
	assert.Equal(t, "xclaim: loading", warns[0].fields["error"])
}

func TestHeartbeatLoop_TouchFailure_IsLoggedAndTheNextTickTriesAgain(t *testing.T) {
	consumer := &cwcliConsumer{touchErr: errors.New("xclaim: loading")}
	d := &deps{consumer: consumer, claimMinIdle: 3 * time.Millisecond, streamKey: cwcliStream, streamGroup: cwcliGroup, consumerName: cwcliConsumerName}
	require.True(t, d.inflight.claim("1-0"))
	log := &cwcliLogger{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		heartbeatLoop(ctx, d, log)
		close(done)
	}()

	require.Eventually(t, func() bool { return len(consumer.touchCalls()) >= 2 }, 10*time.Second, time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the heartbeat loop did not stop with its context")
	}

	assert.GreaterOrEqual(t, len(log.withMessage("warn", cwcliHeartbeatFailedMsg)), 2, "every failed touch is logged")
	for _, call := range consumer.touchCalls() {
		assert.Equal(t, []string{"1-0"}, call.ids, "a failed touch does not drop the entry from the heartbeat")
	}
	assert.Equal(t, []string{"1-0"}, d.inflight.live(time.Now()))
}
