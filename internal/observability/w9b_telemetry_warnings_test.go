package observability_test

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
)

const (
	w9bTelemetryChildMarker = "W9B_TELEMETRY_CHILD"
	w9bTelemetryResultTag   = "w9b-result: "
	w9bEventsMetricName     = "platform_messages_received_total"
	w9bPGMetricName         = "platform_db_queries_total"
)

type w9bLogEntry struct {
	Level  string            `json:"level"`
	Msg    string            `json:"msg"`
	Fields map[string]string `json:"fields"`
}

type w9bChildResult struct {
	Entries     []w9bLogEntry `json:"entries"`
	InitError   string        `json:"init_error"`
	ShutdownSet bool          `json:"shutdown_set"`
}

type w9bRecordingLogger struct {
	mu      sync.Mutex
	entries []w9bLogEntry
}

func (l *w9bRecordingLogger) record(level, msg string, fields map[string]any) {
	flat := make(map[string]string, len(fields))
	for k, v := range fields {
		flat[k] = fmt.Sprint(v)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, w9bLogEntry{Level: level, Msg: msg, Fields: flat})
}

func (l *w9bRecordingLogger) Debug(msg string, f map[string]any) { l.record("debug", msg, f) }
func (l *w9bRecordingLogger) Info(msg string, f map[string]any)  { l.record("info", msg, f) }
func (l *w9bRecordingLogger) Warn(msg string, f map[string]any)  { l.record("warn", msg, f) }
func (l *w9bRecordingLogger) Error(msg string, f map[string]any) { l.record("error", msg, f) }

// w9bRunTelemetryChild runs inside the child process, where nothing has
// registered the platform metrics yet: it occupies two platform metric names
// with a different shape, then starts telemetry.
func w9bRunTelemetryChild() {
	for _, name := range []string{w9bEventsMetricName, w9bPGMetricName} {
		prometheus.DefaultRegisterer.MustRegister(prometheus.NewCounter(prometheus.CounterOpts{Name: name, Help: "occupied by another shape"}))
	}
	log := &w9bRecordingLogger{}
	shutdown, err := observability.InitTelemetry(observability.GinConfig("workflow-execution-service", "test", "test", log), log)
	res := w9bChildResult{ShutdownSet: shutdown != nil}
	if err != nil {
		res.InitError = err.Error()
	}
	if shutdown != nil {
		shutdown()
	}
	log.mu.Lock()
	res.Entries = append([]w9bLogEntry(nil), log.entries...)
	log.mu.Unlock()
	raw, mErr := json.Marshal(res)
	if mErr != nil {
		fmt.Println("w9b-marshal-failed")
		os.Exit(2)
	}
	fmt.Println(w9bTelemetryResultTag + string(raw))
}

func w9bCoverDir() string {
	if f := flag.Lookup("test.gocoverdir"); f != nil {
		return f.Value.String()
	}
	return ""
}

func TestInitTelemetry_PlatformMetricsThatCannotRegister_AreLoggedAsWarningsAndStartupContinues(t *testing.T) {
	if os.Getenv(w9bTelemetryChildMarker) == "1" {
		w9bRunTelemetryChild()
		return
	}

	exe, err := os.Executable()
	require.NoError(t, err)
	args := []string{"-test.run=^TestInitTelemetry_PlatformMetricsThatCannotRegister_AreLoggedAsWarningsAndStartupContinues$"}
	if dir := w9bCoverDir(); dir != "" {
		args = append(args, "-test.gocoverdir="+dir)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, exe, args...)
	cmd.Env = []string{w9bTelemetryChildMarker + "=1"}
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	require.NoError(t, cmd.Run(), "child stderr: %s", stderr.String())

	var res w9bChildResult
	found := false
	for _, line := range strings.Split(stdout.String(), "\n") {
		if raw, ok := strings.CutPrefix(line, w9bTelemetryResultTag); ok {
			require.NoError(t, json.Unmarshal([]byte(raw), &res))
			found = true
		}
	}
	require.True(t, found, "child printed no result; stdout: %s", stdout.String())

	assert.Empty(t, res.InitError, "a metric that cannot register must not fail startup")
	assert.True(t, res.ShutdownSet, "startup continues with a working shutdown func")

	warned := map[string][]w9bLogEntry{}
	for _, e := range res.Entries {
		if e.Level == "warn" {
			warned[e.Msg] = append(warned[e.Msg], e)
		}
	}
	for msg, metric := range map[string]string{
		"platform-events metric not registered":   w9bEventsMetricName,
		"platform-pgcommon metric not registered": w9bPGMetricName,
	} {
		entries := warned[msg]
		require.Len(t, entries, 1, "exactly the one occupied metric is reported for %q", msg)
		assert.Equal(t, metric, entries[0].Fields["metric"])
		assert.NotEmpty(t, entries[0].Fields["error"], "the warning carries the registration error")
	}
}
