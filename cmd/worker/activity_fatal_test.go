package main

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const wkactFatalChildMarker = "WKACT_FATAL_CHILD"

// wkactStdoutLogger prints one tagged line per log call, so the parent can
// read the order of events inside the child process.
type wkactStdoutLogger struct{}

func (wkactStdoutLogger) Debug(string, map[string]any) {}
func (wkactStdoutLogger) Info(string, map[string]any)  {}
func (wkactStdoutLogger) Warn(string, map[string]any)  {}
func (wkactStdoutLogger) Error(msg string, fields map[string]any) {
	fmt.Printf("wkact: error-logged %s error=%v\n", msg, fields["error"])
}

func TestFatal_LogsTheErrorThenRunsTheFlushThenExitsOneWithoutRunningDeferredCalls(t *testing.T) {
	if os.Getenv(wkactFatalChildMarker) == "1" {
		defer fmt.Println("wkact: deferred-ran")
		fatal(wkactStdoutLogger{}, func() { fmt.Println("wkact: flushed") }, "startup failed", errors.New("database down"))
		fmt.Println("wkact: returned")
		return
	}

	exe, err := os.Executable()
	require.NoError(t, err)
	cmd := exec.Command(exe, "-test.run=^TestFatal_LogsTheErrorThenRunsTheFlushThenExitsOneWithoutRunningDeferredCalls$")
	cmd.Env = []string{wkactFatalChildMarker + "=1"}
	out, err := cmd.Output()

	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr, "the child must exit non-zero")
	assert.Equal(t, 1, exitErr.ExitCode())
	var lines []string
	for _, line := range strings.Split(string(out), "\n") {
		if strings.HasPrefix(line, "wkact: ") {
			lines = append(lines, line)
		}
	}
	assert.Equal(t, []string{
		"wkact: error-logged startup failed error=database down",
		"wkact: flushed",
	}, lines, "log first, flush second, nothing after os.Exit")
}
