package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
)

type wkbootRunResult struct {
	stdout   string
	stderr   string
	exitCode int
}

const wkbootBuildVersion = "wkboot-9.8.7"

var (
	wkbootBinaryOnce sync.Once
	wkbootBinaryPath string
	wkbootBinaryErr  error
)

// wkbootWorkerBinary compiles this package once per test run into a temporary
// directory, stamping wkbootBuildVersion into it, and returns the path. The
// built-binary tests are skipped under -short.
func wkbootWorkerBinary(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping built-binary test: -short flag set")
	}
	wkbootBinaryOnce.Do(func() {
		dir, err := os.MkdirTemp("", "wkboot-worker-")
		if err != nil {
			wkbootBinaryErr = err
			return
		}
		bin := filepath.Join(dir, "worker")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		build := []string{"build", "-ldflags", "-X main.version=" + wkbootBuildVersion, "-o", bin}
		if wkbootCoverDir() != "" {
			// Under a per-test coverage run the child's lines count for the test that runs it.
			build = append(build, "-cover", "-covermode="+testing.CoverMode())
		}
		out, err := exec.CommandContext(ctx, "go", append(build, ".")...).CombinedOutput()
		if err != nil {
			wkbootBinaryErr = fmt.Errorf("go build ./cmd/worker: %w\n%s", err, out)
			return
		}
		wkbootBinaryPath = bin
	})
	if wkbootBinaryErr != nil {
		t.Fatalf("build the worker binary: %v", wkbootBinaryErr)
	}
	return wkbootBinaryPath
}

// wkbootCoverDir is the coverage directory of this test run, empty unless the test binary was
// started with -test.gocoverdir (as per-test coverage tools do). Children that write there count
// for the test that started them.
func wkbootCoverDir() string {
	if f := flag.Lookup("test.gocoverdir"); f != nil {
		return f.Value.String()
	}
	return ""
}

// wkbootRunWorker runs the binary with exactly the given environment and
// arguments.
func wkbootRunWorker(t *testing.T, bin string, env []string, args ...string) wkbootRunResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Env = env
	if dir := wkbootCoverDir(); dir != "" {
		cmd.Env = append(append([]string{}, env...), "GOCOVERDIR="+dir)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	res := wkbootRunResult{stdout: stdout.String(), stderr: stderr.String()}
	var exitErr *exec.ExitError
	switch {
	case err == nil:
	case errors.As(err, &exitErr):
		res.exitCode = exitErr.ExitCode()
	default:
		t.Fatalf("run worker binary: %v (stderr: %s)", err, res.stderr)
	}
	return res
}

func TestWorkerBinary_VersionArgumentPrintsTheBuildVersionAndExitsBeforeReadingConfig(t *testing.T) {
	bin := wkbootWorkerBinary(t)

	// With no environment at all, the config check would fail with exit 1.
	res := wkbootRunWorker(t, bin, []string{}, "version")

	if res.exitCode != 0 {
		t.Errorf("exit code = %d, want 0 (stderr: %q)", res.exitCode, res.stderr)
	}
	if res.stdout != wkbootBuildVersion+"\n" {
		t.Errorf("stdout = %q, want the stamped build version and a newline", res.stdout)
	}
	if res.stderr != "" {
		t.Errorf("stderr = %q, want nothing: no config, logger or dial output", res.stderr)
	}
}

func TestWorkerBinary_MissingDatabaseURLPrintsConfigErrorAndExitsOne(t *testing.T) {
	bin := wkbootWorkerBinary(t)

	res := wkbootRunWorker(t, bin, []string{"DEFINITION_SERVICE_URL=http://definition.invalid"})

	if res.exitCode != 1 {
		t.Errorf("exit code = %d, want 1", res.exitCode)
	}
	if res.stderr != "config error: DATABASE_URL is required\n" {
		t.Errorf("stderr = %q, want the plain config error line and no logger output", res.stderr)
	}
	if res.stdout != "" {
		t.Errorf("stdout = %q, want nothing", res.stdout)
	}
}

func TestWorkerBinary_TelemetryInitFailureLogsAndExitsOneBeforeTouchingTheDatabase(t *testing.T) {
	bin := wkbootWorkerBinary(t)

	// A blank service name makes telemetry refuse to start. The unparseable
	// database URL would fail the pool with "startup failed" if it were reached.
	res := wkbootRunWorker(t, bin, []string{
		"DATABASE_URL=wkboot-not-a-dsn",
		"DEFINITION_SERVICE_URL=http://definition.invalid",
		"OTEL_SERVICE_NAME= ",
	})

	if res.exitCode != 1 {
		t.Errorf("exit code = %d, want 1", res.exitCode)
	}
	if !strings.Contains(res.stderr, "telemetry init failed") {
		t.Errorf("stderr lacks the 'telemetry init failed' error log:\n%s", res.stderr)
	}
	for _, later := range []string{"startup failed", "new postgres pool", "dial temporal"} {
		if strings.Contains(res.stderr, later) {
			t.Errorf("stderr mentions %q: startup went past the telemetry failure:\n%s", later, res.stderr)
		}
	}
}

func TestWorkerBinary_UnusableDatabaseURLLogsStartupFailedAndExitsOne(t *testing.T) {
	bin := wkbootWorkerBinary(t)

	res := wkbootRunWorker(t, bin, []string{
		"DATABASE_URL=wkboot-not-a-dsn",
		"DEFINITION_SERVICE_URL=http://definition.invalid",
	})

	if res.exitCode != 1 {
		t.Errorf("exit code = %d, want 1", res.exitCode)
	}
	if !strings.Contains(res.stderr, "startup failed") || !strings.Contains(res.stderr, "new postgres pool") {
		t.Errorf("stderr lacks the 'startup failed' error log naming the postgres pool:\n%s", res.stderr)
	}
	if strings.Contains(res.stderr, "dial temporal") {
		t.Errorf("stderr mentions dialing Temporal: startup continued past the failed dependency build:\n%s", res.stderr)
	}
}

func TestBuildDeps_UnusableDatabaseURLReturnsAWrappedPoolErrorAndNoDependencies(t *testing.T) {
	cfg := &config.Config{
		DatabaseURL:          "wkboot-not-a-dsn",
		DefinitionServiceURL: "http://definition.invalid",
		PGMaxConns:           4,
		OTELServiceName:      "wkboot-worker",
	}

	deps, queues, pool, cleanup, err := buildDeps(cfg, &wkbootRecordingLog{})

	if err == nil || !strings.HasPrefix(err.Error(), "new postgres pool: ") {
		t.Fatalf("error = %v, want one starting with 'new postgres pool: '", err)
	}
	if deps != nil || queues != nil || pool != nil || cleanup != nil {
		t.Errorf("buildDeps returned deps=%v queues=%v pool=%v cleanup-set=%v alongside the error, want all empty", deps, queues, pool, cleanup != nil)
	}
}
