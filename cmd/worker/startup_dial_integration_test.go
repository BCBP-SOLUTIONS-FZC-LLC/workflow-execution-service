//go:build integration

package main

import (
	"strings"
	"testing"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestWorkerBinary_TemporalDialFailureLogsDialTemporalAndExitsOne(t *testing.T) {
	_, dsn := fixtures.NewTestPoolAndDSN(t)
	bin := wkbootWorkerBinary(t)

	// The database is reachable, so startup gets past the dependency build and
	// fails at the Temporal dial.
	res := wkbootRunWorker(t, bin, []string{
		"DATABASE_URL=" + dsn,
		"DEFINITION_SERVICE_URL=http://definition.invalid",
		"TEMPORAL_HOST_PORT=127.0.0.1:1",
	})

	if res.exitCode != 1 {
		t.Errorf("exit code = %d, want 1 (stderr: %s)", res.exitCode, res.stderr)
	}
	if !strings.Contains(res.stderr, "dial temporal") {
		t.Errorf("stderr lacks the 'dial temporal' error log:\n%s", res.stderr)
	}
	for _, other := range []string{"startup failed", "telemetry init failed", "start default worker"} {
		if strings.Contains(res.stderr, other) {
			t.Errorf("stderr mentions %q: the failure came from another step:\n%s", other, res.stderr)
		}
	}
}
