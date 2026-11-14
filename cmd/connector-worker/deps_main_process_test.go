package main

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	cwdepsHelperEnv     = "CWDEPS_RUN_MAIN"
	cwdepsHelperArgsEnv = "CWDEPS_MAIN_ARGS"
	cwdepsHelperTest    = "TestCwdepsMainProcessHelper"
)

// TestCwdepsMainProcessHelper is not a test of its own: the tests below start
// this test binary again with the helper variable set, and it then runs main()
// as the process entry point so os.Exit and stdio can be observed.
func TestCwdepsMainProcessHelper(t *testing.T) {
	if os.Getenv(cwdepsHelperEnv) == "" {
		t.Skip("only runs as the child process of the main() tests")
	}
	os.Args = append([]string{"connector-worker"}, strings.Fields(os.Getenv(cwdepsHelperArgsEnv))...)
	main()
	os.Exit(0)
}

type cwdepsRun struct {
	stdout, stderr string
	exitCode       int
}

// cwdepsRunMain runs main() in a child process with exactly the given
// environment (no inherited variables, no .env file), so the outcome depends
// only on what the test sets.
func cwdepsRunMain(t *testing.T, args string, env map[string]string) cwdepsRun {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^"+cwdepsHelperTest+"$")
	cmd.Dir = t.TempDir()
	cmd.Env = []string{cwdepsHelperEnv + "=1", cwdepsHelperArgsEnv + "=" + args}
	if coverDir := os.Getenv("GOCOVERDIR"); coverDir != "" {
		cmd.Env = append(cmd.Env, "GOCOVERDIR="+coverDir)
	}
	for k, v := range env {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr

	err := cmd.Run()
	run := cwdepsRun{stdout: stdout.String(), stderr: stderr.String()}
	if err != nil {
		exitErr, ok := err.(*exec.ExitError)
		require.True(t, ok, "child did not start or was killed: %v", err)
		run.exitCode = exitErr.ExitCode()
	}
	return run
}

func cwdepsValidEnv() map[string]string {
	return map[string]string{
		"DEFINITION_SERVICE_URL":          "http://127.0.0.1:1",
		"DATABASE_URL":                    "postgres://user:pass@127.0.0.1:1/db",
		"OPENBAO_ADDR":                    "http://127.0.0.1:1",
		"EXECUTION_SERVICE_INTERNAL_ADDR": "http://127.0.0.1:1",
		"CONNECTOR_ALIAS_FETCH_TIMEOUT":   "2s",
	}
}

func cwdepsWith(base map[string]string, k, v string) map[string]string {
	out := make(map[string]string, len(base)+1)
	for key, val := range base {
		out[key] = val
	}
	out[k] = v
	return out
}

func TestMain_VersionArgument_PrintsVersionAndExitsZeroWithoutConfig(t *testing.T) {
	run := cwdepsRunMain(t, "version", nil)

	assert.Equal(t, 0, run.exitCode, "stderr: %s", run.stderr)
	assert.Equal(t, version+"\n", run.stdout)
	assert.NotContains(t, run.stderr, "config error")
}

func TestMain_ConfigErrors_ExitOneBeforeAnyDependencyIsOpened(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		want string
	}{
		{name: "nothing configured", env: nil, want: "config error: "},
		{name: "unparsable number", env: cwdepsWith(cwdepsValidEnv(), "CONNECTOR_RETRY_MAX_ATTEMPTS", "many"), want: "CONNECTOR_RETRY_MAX_ATTEMPTS"},
		{name: "worker requirement refused", env: cwdepsWith(cwdepsValidEnv(), "CONNECTOR_RETRY_MAX_ATTEMPTS", "0"), want: "config error: CONNECTOR_RETRY_MAX_ATTEMPTS must be at least 1"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			run := cwdepsRunMain(t, "", tc.env)

			assert.Equal(t, 1, run.exitCode)
			assert.Contains(t, run.stderr, "config error: ")
			assert.Contains(t, run.stderr, tc.want)
			assert.Empty(t, run.stdout)
			assert.NotContains(t, run.stderr+run.stdout, "startup failed", "a dependency was opened despite the config error")
		})
	}
}

func TestMain_RequiredWorkerSettingMissing_ExitsOneNamingIt(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		want string
	}{
		{name: "dead-letter key equals the stream key", env: cwdepsWith(cwdepsWith(cwdepsValidEnv(), "CONNECTOR_STREAM_KEY", "same"), "CONNECTOR_STREAM_DEAD_LETTER_KEY", "same"), want: "CONNECTOR_STREAM_DEAD_LETTER_KEY"},
		{name: "claim idle too short for the heartbeat", env: cwdepsWith(cwdepsValidEnv(), "CONNECTOR_STREAM_CLAIM_MIN_IDLE", "1s"), want: "CONNECTOR_STREAM_CLAIM_MIN_IDLE"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			run := cwdepsRunMain(t, "", tc.env)

			assert.Equal(t, 1, run.exitCode)
			assert.Contains(t, run.stderr, "cmd/connector-worker: ")
			assert.Contains(t, run.stderr, tc.want)
			assert.NotContains(t, run.stderr+run.stdout, "startup failed")
		})
	}
}

func TestMain_LoggerCannotBeCreated_ExitsOneWithoutStartingDependencies(t *testing.T) {
	notADir := filepath.Join(t.TempDir(), "file")
	require.NoError(t, os.WriteFile(notADir, []byte("x"), 0o600))

	run := cwdepsRunMain(t, "", cwdepsWith(cwdepsValidEnv(), "LOG_FILE", filepath.Join(notADir, "sub", "worker.log")))

	assert.Equal(t, 1, run.exitCode)
	assert.Contains(t, run.stderr, "cmd/connector-worker: new logger: ")
	assert.NotContains(t, run.stderr+run.stdout, "startup failed", "a dependency was opened although the logger failed")
}

func TestMain_TelemetryCannotBeInitialised_ExitsOneWithoutStartingDependencies(t *testing.T) {
	run := cwdepsRunMain(t, "", cwdepsWith(cwdepsValidEnv(), "OTEL_SERVICE_NAME", "   "))

	assert.Equal(t, 1, run.exitCode)
	assert.Contains(t, run.stderr+run.stdout, "telemetry init failed")
	assert.NotContains(t, run.stderr+run.stdout, "startup failed", "a dependency was opened although telemetry failed")
}

func TestMain_DependencyStartupFailure_LogsAndExitsOne(t *testing.T) {
	run := cwdepsRunMain(t, "", cwdepsValidEnv())

	assert.Equal(t, 1, run.exitCode)
	out := run.stderr + run.stdout
	assert.Contains(t, out, "startup failed")
	assert.Contains(t, out, "fetch alias registry")
}
