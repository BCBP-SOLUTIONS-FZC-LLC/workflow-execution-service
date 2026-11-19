package main

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFx11bAwaitLoopsWaitsForACancelledLoopToFinishItsTick(t *testing.T) {
	a := &app{log: silentLogger{}}
	ctx, cancel := context.WithCancel(context.Background())
	var finished atomic.Bool
	a.goLoop(ctx, func(ctx context.Context) {
		<-ctx.Done()
		time.Sleep(50 * time.Millisecond)
		finished.Store(true)
	})

	cancel()
	a.awaitLoops(context.Background())

	assert.True(t, finished.Load(), "pools must not close while a loop is mid-tick")
}

func TestFx11bAwaitLoopsGivesUpAtTheShutdownDeadline(t *testing.T) {
	a := &app{log: silentLogger{}}
	release := make(chan struct{})
	defer close(release)
	a.goLoop(context.Background(), func(context.Context) { <-release })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	start := time.Now()
	a.awaitLoops(ctx)

	assert.Less(t, time.Since(start), time.Second)
}
