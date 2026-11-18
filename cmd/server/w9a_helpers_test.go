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
	"sync"
	"testing"
	"time"
)

type w9aLogEntry struct {
	level  string
	msg    string
	fields map[string]any
}

// w9aLogger records every call so a test can assert what was logged and at which level.
type w9aLogger struct {
	mu      sync.Mutex
	records []w9aLogEntry
}

func (l *w9aLogger) add(level, msg string, fields map[string]any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.records = append(l.records, w9aLogEntry{level: level, msg: msg, fields: fields})
}

func (l *w9aLogger) Debug(msg string, fields map[string]any) { l.add("debug", msg, fields) }
func (l *w9aLogger) Info(msg string, fields map[string]any)  { l.add("info", msg, fields) }
func (l *w9aLogger) Warn(msg string, fields map[string]any)  { l.add("warn", msg, fields) }
func (l *w9aLogger) Error(msg string, fields map[string]any) { l.add("error", msg, fields) }

func (l *w9aLogger) all() []w9aLogEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]w9aLogEntry(nil), l.records...)
}

func (l *w9aLogger) at(level string) []w9aLogEntry {
	var out []w9aLogEntry
	for _, r := range l.all() {
		if r.level == level {
			out = append(out, r)
		}
	}
	return out
}

var (
	w9aBinaryOnce sync.Once
	w9aBinaryPath string
	w9aBinaryDir  string
	w9aBinaryErr  error
)

// TestMain removes the directory holding the built binary once every test has run.
func TestMain(m *testing.M) {
	code := m.Run()
	if w9aBinaryDir != "" {
		_ = os.RemoveAll(w9aBinaryDir)
	}
	os.Exit(code)
}

func w9aCoverDir() string {
	if f := flag.Lookup("test.gocoverdir"); f != nil {
		return f.Value.String()
	}
	return ""
}

// w9aServerBinary builds this package's binary once per test run, instrumented when the run collects coverage.
func w9aServerBinary(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping built-binary test: -short flag set")
	}
	w9aBinaryOnce.Do(func() {
		dir, err := os.MkdirTemp("", "w9a-server-")
		if err != nil {
			w9aBinaryErr = err
			return
		}
		w9aBinaryDir = dir
		bin := filepath.Join(dir, "server")
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		args := []string{"build", "-o", bin}
		if w9aCoverDir() != "" {
			args = append(args, "-cover", "-covermode="+testing.CoverMode())
		}
		if out, err := exec.CommandContext(ctx, "go", append(args, ".")...).CombinedOutput(); err != nil {
			w9aBinaryErr = fmt.Errorf("go build ./cmd/server: %w\n%s", err, out)
			return
		}
		w9aBinaryPath = bin
	})
	if w9aBinaryErr != nil {
		t.Fatalf("build the server binary: %v", w9aBinaryErr)
	}
	return w9aBinaryPath
}

type w9aRun struct {
	stdout   string
	stderr   string
	exitCode int
}

// w9aRunServer runs the binary with exactly the given environment (plus the coverage directory) in an empty
// working directory.
func w9aRunServer(t *testing.T, env []string, args ...string) w9aRun {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, w9aServerBinary(t), args...)
	cmd.Dir = t.TempDir()
	cmd.Env = append([]string{}, env...)
	if dir := w9aCoverDir(); dir != "" {
		cmd.Env = append(cmd.Env, "GOCOVERDIR="+dir)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	res := w9aRun{stdout: stdout.String(), stderr: stderr.String()}
	var exit *exec.ExitError
	switch {
	case err == nil:
	case errors.As(err, &exit):
		res.exitCode = exit.ExitCode()
	default:
		t.Fatalf("run the server binary: %v (stderr: %s)", err, res.stderr)
	}
	return res
}

func (r w9aRun) combined() string { return r.stdout + "\n" + r.stderr }
