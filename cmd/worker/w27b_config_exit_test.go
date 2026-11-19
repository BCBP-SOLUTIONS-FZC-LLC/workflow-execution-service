package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// A worker started with an unusable setting prints one "config error:" line on stderr and exits with code 1,
// before any logger, telemetry or dependency starts.
func TestWorkerBinary_InvalidSettings_PrintTheConfigErrorAndExitOne(t *testing.T) {
	cases := []struct {
		name       string
		env        []string
		wantStderr string
	}{
		{
			name:       "an unparsable health port and poll interval are both named",
			env:        []string{"WORKER_HEALTH_PORT=abc", "QUEUE_TOPOLOGY_POLL_INTERVAL=soon"},
			wantStderr: "config error: invalid environment variable(s): WORKER_HEALTH_PORT=\"abc\": strconv.Atoi: parsing \"abc\": invalid syntax; QUEUE_TOPOLOGY_POLL_INTERVAL=\"soon\": time: invalid duration \"soon\"\n",
		},
		{
			name:       "health port 0",
			env:        []string{"WORKER_HEALTH_PORT=0"},
			wantStderr: "config error: WORKER_HEALTH_PORT must be in [1, 65535]\n",
		},
		{
			name:       "metrics port 65536",
			env:        []string{"WORKER_METRICS_PORT=65536"},
			wantStderr: "config error: WORKER_METRICS_PORT must be in [1, 65535]\n",
		},
		{
			name:       "poll interval of zero",
			env:        []string{"QUEUE_TOPOLOGY_POLL_INTERVAL=0"},
			wantStderr: "config error: QUEUE_TOPOLOGY_POLL_INTERVAL must be > 0 and at most 2m: the queue polling alerts treat a queue unpolled for 5 minutes as stalled\n",
		},
		{
			name:       "poll interval above two minutes",
			env:        []string{"QUEUE_TOPOLOGY_POLL_INTERVAL=2m1s"},
			wantStderr: "config error: QUEUE_TOPOLOGY_POLL_INTERVAL must be > 0 and at most 2m: the queue polling alerts treat a queue unpolled for 5 minutes as stalled\n",
		},
	}
	bin := wkbootWorkerBinary(t)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := wkbootRunWorker(t, bin, append([]string{"DATABASE_URL=postgres://w27b", "DEFINITION_SERVICE_URL=http://definition.invalid"}, tc.env...))

			assert.Equal(t, 1, res.exitCode)
			assert.Equal(t, tc.wantStderr, res.stderr)
			assert.Empty(t, res.stdout)
		})
	}
}
