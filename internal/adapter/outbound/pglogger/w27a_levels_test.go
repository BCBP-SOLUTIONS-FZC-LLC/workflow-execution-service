package pglogger

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	pgdomain "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/domain"
)

type w27aLogCall struct {
	level  string
	msg    string
	fields map[string]any
}

// w27aLevelLogger records the level each call arrived on, so a swapped level shows up.
type w27aLevelLogger struct {
	calls []w27aLogCall
}

func (l *w27aLevelLogger) Debug(msg string, fields map[string]any) { l.add("debug", msg, fields) }
func (l *w27aLevelLogger) Info(msg string, fields map[string]any)  { l.add("info", msg, fields) }
func (l *w27aLevelLogger) Warn(msg string, fields map[string]any)  { l.add("warn", msg, fields) }
func (l *w27aLevelLogger) Error(msg string, fields map[string]any) { l.add("error", msg, fields) }

func (l *w27aLevelLogger) add(level, msg string, fields map[string]any) {
	l.calls = append(l.calls, w27aLogCall{level: level, msg: msg, fields: fields})
}

func TestAdapter_EachLevelForwardsToTheSameLevelOfThePortLogger(t *testing.T) {
	cases := []struct {
		name      string
		call      func(pgdomain.Logger, string, ...pgdomain.Field)
		wantLevel string
	}{
		{name: "debug", call: pgdomain.Logger.Debug, wantLevel: "debug"},
		{name: "info", call: pgdomain.Logger.Info, wantLevel: "info"},
		{name: "warn", call: pgdomain.Logger.Warn, wantLevel: "warn"},
		{name: "error", call: pgdomain.Logger.Error, wantLevel: "error"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			log := &w27aLevelLogger{}

			tc.call(New(log), "pool event", pgdomain.Field{Key: "attempt", Value: 3})

			require.Equal(t, []w27aLogCall{{level: tc.wantLevel, msg: "pool event", fields: map[string]any{"attempt": 3}}}, log.calls,
				"exactly one call, on the matching level only")
		})
	}
}

func TestAdapter_Fields_BuildAKeyedMapAndPassEveryValueThroughUnchanged(t *testing.T) {
	boom := errors.New("connection reset")
	cases := []struct {
		name   string
		fields []pgdomain.Field
		want   map[string]any
	}{
		{
			name: "several fields",
			fields: []pgdomain.Field{
				{Key: "attempt", Value: 3},
				{Key: "host", Value: "db.internal"},
				{Key: "ok", Value: false},
			},
			want: map[string]any{"attempt": 3, "host": "db.internal", "ok": false},
		},
		{
			name: "a repeated key keeps the last value",
			fields: []pgdomain.Field{
				{Key: "attempt", Value: 1},
				{Key: "host", Value: "db.internal"},
				{Key: "attempt", Value: 2},
			},
			want: map[string]any{"attempt": 2, "host": "db.internal"},
		},
		{
			name:   "a nil value stays a present key",
			fields: []pgdomain.Field{{Key: "cause", Value: nil}},
			want:   map[string]any{"cause": nil},
		},
		{
			name:   "an error value is passed as the same error",
			fields: []pgdomain.Field{{Key: "error", Value: boom}},
			want:   map[string]any{"error": boom},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			log := &w27aLevelLogger{}

			New(log).Warn("query slow", tc.fields...)

			require.Len(t, log.calls, 1)
			assert.Equal(t, "query slow", log.calls[0].msg)
			assert.Equal(t, tc.want, log.calls[0].fields)
			if len(tc.fields) > 0 && tc.fields[0].Key == "error" {
				assert.Same(t, boom, log.calls[0].fields["error"], "the very error value, not a copy or its text")
			}
		})
	}
}
