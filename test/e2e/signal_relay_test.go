//go:build e2e

package e2e_test

import (
	"context"
	"testing"
	"time"

	"go.temporal.io/sdk/client"

	eventsmock "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events/mock"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/outbox"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/signalrelay"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporalclient"
)

type relayLogger struct{ t *testing.T }

func (l relayLogger) Debug(msg string, fields map[string]interface{}) {}
func (l relayLogger) Info(msg string, fields map[string]interface{})  {}
func (l relayLogger) Warn(msg string, fields map[string]interface{}) {
	l.t.Logf("WARN %s %v", msg, fields)
}
func (l relayLogger) Error(msg string, fields map[string]interface{}) {
	l.t.Logf("ERROR %s %v", msg, fields)
}

// startSignalRelay delivers the workflow signals the services queue in the outbox, as the server's relay does.
func startSignalRelay(t *testing.T, pool *pgcommon.Pool, sdk client.Client) {
	t.Helper()
	runner, err := outbox.NewRunner(outbox.Config{
		Pool: pool, Publisher: signalrelay.New(&eventsmock.Publisher{}, temporalclient.New(sdk), nil),
		PollInterval: 50 * time.Millisecond, BatchSize: 10, Logger: relayLogger{t},
	})
	if err != nil {
		t.Fatalf("outbox runner: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- runner.Start(context.Background()) }()
	t.Cleanup(func() {
		if err := runner.Stop(); err != nil {
			t.Errorf("stop relay: %v", err)
		}
		<-done
	})
}
