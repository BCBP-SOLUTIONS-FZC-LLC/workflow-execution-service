package observability_test

import (
	"bytes"
	"context"
	"flag"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"
)

const w28bChildMarker = "W28B_CHILD_TEST"

// w28bInFreshProcess reports whether the caller is the child run. In the parent
// it re-runs the calling test alone in a new process, where nothing has
// registered a collector yet, and requires that run to pass; the parent then
// has nothing more to do.
func w28bInFreshProcess(t *testing.T, extraEnv ...string) bool {
	t.Helper()
	if os.Getenv(w28bChildMarker) == t.Name() {
		return true
	}
	exe, err := os.Executable()
	require.NoError(t, err)
	args := []string{"-test.run=^" + t.Name() + "$", "-test.v"}
	if f := flag.Lookup("test.gocoverdir"); f != nil && f.Value.String() != "" {
		args = append(args, "-test.gocoverdir="+f.Value.String())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, exe, args...)
	cmd.Env = append([]string{w28bChildMarker + "=" + t.Name()}, extraEnv...)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out
	require.NoError(t, cmd.Run(), "child output:\n%s", out.String())
	require.Contains(t, out.String(), "--- PASS: "+t.Name(), "the child must have run the test body")
	return false
}

type w28bSample struct {
	labels map[string]string
	value  float64
	count  uint64
	sum    float64
}

func w28bFamilies(t *testing.T) map[string]*dto.MetricFamily {
	t.Helper()
	gathered, err := gincommon.MetricsGatherer().Gather()
	require.NoError(t, err)
	byName := make(map[string]*dto.MetricFamily, len(gathered))
	for _, mf := range gathered {
		byName[mf.GetName()] = mf
	}
	return byName
}

func w28bSamplesOf(mf *dto.MetricFamily) []w28bSample {
	var out []w28bSample
	for _, m := range mf.GetMetric() {
		s := w28bSample{labels: map[string]string{}}
		for _, lp := range m.GetLabel() {
			s.labels[lp.GetName()] = lp.GetValue()
		}
		switch mf.GetType() {
		case dto.MetricType_COUNTER:
			s.value = m.GetCounter().GetValue()
		case dto.MetricType_GAUGE:
			s.value = m.GetGauge().GetValue()
		case dto.MetricType_HISTOGRAM:
			s.count = m.GetHistogram().GetSampleCount()
			s.sum = m.GetHistogram().GetSampleSum()
		case dto.MetricType_SUMMARY, dto.MetricType_UNTYPED, dto.MetricType_GAUGE_HISTOGRAM:
			// the service registers none of these; the value stays zero
		}
		out = append(out, s)
	}
	return out
}

// w28bFind returns the samples of a family whose labels include every wanted pair.
func w28bFind(t *testing.T, family string, want map[string]string) []w28bSample {
	t.Helper()
	mf, ok := w28bFamilies(t)[family]
	if !ok {
		return nil
	}
	var out []w28bSample
	for _, s := range w28bSamplesOf(mf) {
		match := true
		for k, v := range want {
			if s.labels[k] != v {
				match = false
			}
		}
		if match {
			out = append(out, s)
		}
	}
	return out
}

// w28bOnly requires exactly one matching series and returns it.
func w28bOnly(t *testing.T, family string, want map[string]string) w28bSample {
	t.Helper()
	found := w28bFind(t, family, want)
	require.Lenf(t, found, 1, "series %s%v", family, want)
	return found[0]
}

type w28bLogEntry struct {
	level  string
	msg    string
	fields map[string]any
}

// w28bLogger records every log call so a test can assert on warnings.
type w28bLogger struct{ entries []w28bLogEntry }

func (l *w28bLogger) record(level, msg string, fields map[string]any) {
	l.entries = append(l.entries, w28bLogEntry{level: level, msg: msg, fields: fields})
}

func (l *w28bLogger) Debug(msg string, f map[string]any) { l.record("debug", msg, f) }
func (l *w28bLogger) Info(msg string, f map[string]any)  { l.record("info", msg, f) }
func (l *w28bLogger) Warn(msg string, f map[string]any)  { l.record("warn", msg, f) }
func (l *w28bLogger) Error(msg string, f map[string]any) { l.record("error", msg, f) }

func (l *w28bLogger) at(level string) []w28bLogEntry {
	var out []w28bLogEntry
	for _, e := range l.entries {
		if e.level == level {
			out = append(out, e)
		}
	}
	return out
}

func w28bHasPrefixedFamily(t *testing.T, prefix string) bool {
	t.Helper()
	gathered, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, mf := range gathered {
		if strings.HasPrefix(mf.GetName(), prefix) {
			return true
		}
	}
	return false
}
