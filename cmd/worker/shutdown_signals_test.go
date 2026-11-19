package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"
)

type wkbootFailingPool struct{ err error }

func (p wkbootFailingPool) DrainAndClose(context.Context) error { return p.err }

func TestWaitForStop_ReleasesTheSignalSoASecondOneEndsAStuckDrain(t *testing.T) {
	witness := make(chan os.Signal, 2)
	signal.Notify(witness, syscall.SIGUSR1)
	defer signal.Stop(witness)
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGUSR1)
	defer signal.Stop(stop)

	if err := syscall.Kill(syscall.Getpid(), syscall.SIGUSR1); err != nil {
		t.Fatal(err)
	}
	waitForStop(stop)

	if err := syscall.Kill(syscall.Getpid(), syscall.SIGUSR1); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		select {
		case <-witness:
		case <-time.After(2 * time.Second):
			t.Fatal("witness missed a signal")
		}
	}
	select {
	case <-stop:
		t.Fatal("the stop channel still receives signals after the first one: a second SIGTERM would be swallowed")
	case <-time.After(200 * time.Millisecond):
	}
}

func TestDrainPool_LogsADrainErrorAndStaysQuietOtherwise(t *testing.T) {
	log := &wkbootRecordingLog{}

	drainPool(wkbootFailingPool{}, log)
	if len(log.entries) != 0 {
		t.Fatalf("a clean drain logged %v, want nothing", log.entries)
	}

	drainPool(wkbootFailingPool{err: errors.New("pool busy")}, log)
	if len(log.entries) != 1 || log.entries[0].level != "warn" {
		t.Fatalf("log entries = %v, want one warning for the failed drain", log.entries)
	}
	if got := log.entries[0].fields["error"]; got != "pool busy" {
		t.Errorf("logged error = %v, want pool busy", got)
	}
}
