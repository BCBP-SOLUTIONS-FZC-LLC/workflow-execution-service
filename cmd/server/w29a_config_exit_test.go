package main

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// w29aServeEnv is everything the server needs outside dev and local, with the
// relay URL left out. The database address refuses connections, so a run that
// passes the settings check stops at the database instead of serving.
func w29aServeEnv(appEnv string, extra ...string) []string {
	env := []string{
		"APP_ENV=" + appEnv,
		"DATABASE_URL=" + w9aRefusedDSN,
		"DEFINITION_SERVICE_URL=http://127.0.0.1:1",
		"ELIGIBILITY_BASE_URL=http://127.0.0.1:1",
		"TOKEN_SERVICE_BASE_URL=http://127.0.0.1:1",
		"TEMPORAL_HOST_PORT=127.0.0.1:1",
		"TEMPORAL_NAMESPACE=w29a",
		"AWS_USE_STUB=false",
		"SNS_TOPIC_ARN=arn:aws:sns:eu-west-1:000000000000:w29a",
		"GLUE_REGISTRY_NAME=w29a",
		"SHUTDOWN_DRAIN_DELAY=0s",
	}
	return append(env, extra...)
}

// The server prints one "config error:" line on stderr and exits with code 1
// before any logger, telemetry or database exists. Unparsable settings are
// listed together and stop the range checks; the range checks name only the
// first violation.
func TestServerBinary_InvalidSettings_PrintOneConfigErrorLineAndExitOne(t *testing.T) {
	cases := []struct {
		name       string
		env        []string
		wantStderr string
	}{
		{
			name:       "two unparsable ports are both named",
			env:        []string{"HTTP_PORT=abc", "METRICS_PORT=xyz"},
			wantStderr: "config error: invalid environment variable(s): HTTP_PORT=\"abc\": strconv.Atoi: parsing \"abc\": invalid syntax; METRICS_PORT=\"xyz\": strconv.Atoi: parsing \"xyz\": invalid syntax\n",
		},
		{
			name:       "an unparsable port and an unparsable duration are both named",
			env:        []string{"HTTP_PORT=abc", "OUTBOX_POLL_INTERVAL=soon"},
			wantStderr: "config error: invalid environment variable(s): HTTP_PORT=\"abc\": strconv.Atoi: parsing \"abc\": invalid syntax; OUTBOX_POLL_INTERVAL=\"soon\": time: invalid duration \"soon\"\n",
		},
		{
			name:       "a parse error hides a range violation in another setting",
			env:        []string{"HTTP_PORT=abc", "PG_MAX_CONNS=0"},
			wantStderr: "config error: invalid environment variable(s): HTTP_PORT=\"abc\": strconv.Atoi: parsing \"abc\": invalid syntax\n",
		},
		{
			name:       "two range violations report only the first",
			env:        []string{"HTTP_PORT=0", "METRICS_PORT=70000"},
			wantStderr: "config error: HTTP_PORT must be in [1, 65535]\n",
		},
		{
			name:       "an application environment that is not known",
			env:        []string{"APP_ENV=qa"},
			wantStderr: "config error: APP_ENV must be one of local, dev, test, staging, prod\n",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := w9aRunServer(t, append([]string{"DATABASE_URL=" + w9aRefusedDSN}, tc.env...))

			assert.Equal(t, 1, res.exitCode)
			assert.Equal(t, tc.wantStderr, res.stderr)
			assert.Empty(t, res.stdout)
		})
	}
}

// migrate behind a transaction-pooling proxy needs the direct address, and the
// refusal is the same one-line config error.
func TestServerBinary_MigrateBehindAPoolerWithoutItsDirectAddress_PrintsTheConfigErrorAndExitsOne(t *testing.T) {
	res := w9aRunServer(t, []string{"DATABASE_URL=" + w9aRefusedDSN, "PG_BOUNCER_MODE=true"}, "migrate")

	assert.Equal(t, 1, res.exitCode)
	assert.True(t, strings.HasPrefix(res.stderr, "config error: MIGRATION_DATABASE_URL is required when PG_BOUNCER_MODE is true"), "stderr = %q", res.stderr)
	assert.Equal(t, 1, strings.Count(res.stderr, "\n"), "one line, and no logger output: %q", res.stderr)
	assert.NotContains(t, res.stderr, "migrate error", "the refused address was never dialled")
	assert.NotContains(t, res.combined(), "w9a-secret", "the database password stays out of the output")
	assert.Empty(t, res.stdout)
}

// migrate checks its own settings only: it still validates the shared ones,
// and it does not ask for the outbound service addresses the server needs.
func TestServerBinary_MigrateChecksItsOwnSettingsOnlyWhileServingNeedsTheOutboundAddresses(t *testing.T) {
	cases := []struct {
		name       string
		env        []string
		args       []string
		wantStderr func(t *testing.T, stderr string)
	}{
		{
			name: "migrate still refuses an application environment that is not known",
			env:  []string{"DATABASE_URL=" + w9aRefusedDSN, "APP_ENV=qa"},
			args: []string{"migrate"},
			wantStderr: func(t *testing.T, stderr string) {
				assert.Equal(t, "config error: APP_ENV must be one of local, dev, test, staging, prod\n", stderr)
			},
		},
		{
			name: "migrate still refuses a port out of range",
			env:  []string{"DATABASE_URL=" + w9aRefusedDSN, "HTTP_PORT=0"},
			args: []string{"migrate"},
			wantStderr: func(t *testing.T, stderr string) {
				assert.Equal(t, "config error: HTTP_PORT must be in [1, 65535]\n", stderr)
			},
		},
		{
			name: "migrate without a database",
			env:  nil,
			args: []string{"migrate"},
			wantStderr: func(t *testing.T, stderr string) {
				assert.Equal(t, "config error: DATABASE_URL is required\n", stderr)
			},
		},
		{
			name: "serving with only the database asks for the definition service",
			env:  []string{"DATABASE_URL=" + w9aRefusedDSN},
			args: nil,
			wantStderr: func(t *testing.T, stderr string) {
				assert.Equal(t, "config error: DEFINITION_SERVICE_URL is required\n", stderr)
			},
		},
		{
			name: "migrate with only the database passes the settings and stops at the unreachable database",
			env:  []string{"DATABASE_URL=" + w9aRefusedDSN},
			args: []string{"migrate"},
			wantStderr: func(t *testing.T, stderr string) {
				assert.NotContains(t, stderr, "config error")
				assert.Contains(t, stderr, "migrate error: ")
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res := w9aRunServer(t, tc.env, tc.args...)

			assert.Equal(t, 1, res.exitCode)
			tc.wantStderr(t, res.stderr)
			assert.NotContains(t, res.combined(), "w9a-secret", "the database password stays out of the output")
			assert.Empty(t, res.stdout)
		})
	}
}

// Outside dev and local the server refuses to start without the relay
// address; a missing one would fall back to the row-level-security role and
// see no outbox rows for ever.
func TestServerBinary_OutsideDevWithoutTheRelayAddress_PrintsTheConfigErrorAndExitsOne(t *testing.T) {
	for _, appEnv := range []string{"test", "staging", "prod"} {
		t.Run(appEnv+" without the relay address", func(t *testing.T) {
			res := w9aRunServer(t, w29aServeEnv(appEnv))

			assert.Equal(t, 1, res.exitCode)
			assert.True(t, strings.HasPrefix(res.stderr, "config error: OUTBOX_RELAY_DATABASE_URL is required outside dev and local"), "stderr = %q", res.stderr)
			assert.Equal(t, 1, strings.Count(res.stderr, "\n"), "one line: %q", res.stderr)
			assert.NotContains(t, res.combined(), "server starting", "no listener was opened")
			assert.NotContains(t, res.combined(), "w9a-secret", "the database password stays out of the output")
			assert.Empty(t, res.stdout)
		})
		t.Run(appEnv+" with the relay address passes the settings check", func(t *testing.T) {
			res := w9aRunServer(t, w29aServeEnv(appEnv, "OUTBOX_RELAY_DATABASE_URL="+w9aRefusedDSN))

			assert.Equal(t, 1, res.exitCode)
			assert.NotContains(t, res.stderr, "config error", "only the missing relay address held the start back")
			assert.Contains(t, res.stderr, "cmd/server: app db pool: ", "the start went on to open the database")
			assert.NotContains(t, res.combined(), "w9a-secret", "the database password stays out of the output")
		})
	}
}
