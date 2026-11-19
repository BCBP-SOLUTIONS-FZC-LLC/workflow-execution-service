package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLoad_WorkerHealthPortAndPollIntervalThatDoNotParse_AreNamedTogether(t *testing.T) {
	w19aRunLoadCases(t, []w19aLoadCase{
		{
			name: "both bad values are named in one message, in the order they are read",
			env:  map[string]string{"WORKER_HEALTH_PORT": "abc", "QUEUE_TOPOLOGY_POLL_INTERVAL": "soon"},
			wantErr: `invalid environment variable(s): WORKER_HEALTH_PORT="abc": strconv.Atoi: parsing "abc": invalid syntax; ` +
				`QUEUE_TOPOLOGY_POLL_INTERVAL="soon": time: invalid duration "soon"`,
		},
		{
			name:    "a bad health port alone",
			env:     map[string]string{"WORKER_HEALTH_PORT": "abc"},
			wantErr: `invalid environment variable(s): WORKER_HEALTH_PORT="abc": strconv.Atoi: parsing "abc": invalid syntax`,
		},
		{
			name:    "a bad poll interval alone",
			env:     map[string]string{"QUEUE_TOPOLOGY_POLL_INTERVAL": "soon"},
			wantErr: `invalid environment variable(s): QUEUE_TOPOLOGY_POLL_INTERVAL="soon": time: invalid duration "soon"`,
		},
		{
			name:    "a bad metrics port is named with the others",
			env:     map[string]string{"WORKER_HEALTH_PORT": "abc", "WORKER_METRICS_PORT": "9.5"},
			wantErr: `invalid environment variable(s): WORKER_HEALTH_PORT="abc": strconv.Atoi: parsing "abc": invalid syntax; WORKER_METRICS_PORT="9.5": strconv.Atoi: parsing "9.5": invalid syntax`,
		},
	})
}

func TestLoad_WorkerPortBoundaries(t *testing.T) {
	const healthMessage = "WORKER_HEALTH_PORT must be in [1, 65535]"
	const metricsMessage = "WORKER_METRICS_PORT must be in [1, 65535]"
	ports := func(health, metrics int) func(*testing.T, *Config) {
		return func(t *testing.T, cfg *Config) {
			assert.Equal(t, health, cfg.WorkerHealthPort)
			assert.Equal(t, metrics, cfg.WorkerMetricsPort)
		}
	}
	w19aRunLoadCases(t, []w19aLoadCase{
		{name: "health port 0", env: map[string]string{"WORKER_HEALTH_PORT": "0"}, wantErr: healthMessage},
		{name: "health port negative", env: map[string]string{"WORKER_HEALTH_PORT": "-1"}, wantErr: healthMessage},
		{name: "health port 65536", env: map[string]string{"WORKER_HEALTH_PORT": "65536"}, wantErr: healthMessage},
		{name: "metrics port 0", env: map[string]string{"WORKER_METRICS_PORT": "0"}, wantErr: metricsMessage},
		{name: "metrics port negative", env: map[string]string{"WORKER_METRICS_PORT": "-1"}, wantErr: metricsMessage},
		{name: "metrics port 65536", env: map[string]string{"WORKER_METRICS_PORT": "65536"}, wantErr: metricsMessage},
		{name: "health port 1", env: map[string]string{"WORKER_HEALTH_PORT": "1"}, check: ports(1, 8082)},
		{name: "health port 65535", env: map[string]string{"WORKER_HEALTH_PORT": "65535"}, check: ports(65535, 8082)},
		{name: "metrics port 1", env: map[string]string{"WORKER_METRICS_PORT": "1"}, check: ports(8081, 1)},
		{name: "metrics port 65535", env: map[string]string{"WORKER_METRICS_PORT": "65535"}, check: ports(8081, 65535)},
	})
}

func TestLoad_QueueTopologyPollIntervalBoundaries(t *testing.T) {
	const message = "QUEUE_TOPOLOGY_POLL_INTERVAL must be > 0 and at most 2m: the queue polling alerts treat a queue unpolled for 5 minutes as stalled"
	interval := func(want time.Duration) func(*testing.T, *Config) {
		return func(t *testing.T, cfg *Config) { assert.Equal(t, want, cfg.QueueTopologyPollInterval) }
	}
	w19aRunLoadCases(t, []w19aLoadCase{
		{name: "zero", env: map[string]string{"QUEUE_TOPOLOGY_POLL_INTERVAL": "0"}, wantErr: message},
		{name: "negative", env: map[string]string{"QUEUE_TOPOLOGY_POLL_INTERVAL": "-1s"}, wantErr: message},
		{name: "two minutes and one nanosecond", env: map[string]string{"QUEUE_TOPOLOGY_POLL_INTERVAL": "2m0.000000001s"}, wantErr: message},
		{name: "three minutes", env: map[string]string{"QUEUE_TOPOLOGY_POLL_INTERVAL": "3m"}, wantErr: message},
		{name: "one nanosecond", env: map[string]string{"QUEUE_TOPOLOGY_POLL_INTERVAL": "1ns"}, check: interval(time.Nanosecond)},
		{name: "exactly two minutes", env: map[string]string{"QUEUE_TOPOLOGY_POLL_INTERVAL": "2m"}, check: interval(2 * time.Minute)},
	})
}
