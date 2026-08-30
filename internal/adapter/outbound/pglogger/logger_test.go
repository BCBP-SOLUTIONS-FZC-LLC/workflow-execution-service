package pglogger

import (
	"testing"

	"github.com/stretchr/testify/require"

	pgdomain "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/domain"
)

type fakeLogger struct {
	msg    string
	fields map[string]any
}

func (l *fakeLogger) Debug(msg string, fields map[string]any) { l.record(msg, fields) }
func (l *fakeLogger) Info(msg string, fields map[string]any)  { l.record(msg, fields) }
func (l *fakeLogger) Warn(msg string, fields map[string]any)  { l.record(msg, fields) }
func (l *fakeLogger) Error(msg string, fields map[string]any) { l.record(msg, fields) }

func (l *fakeLogger) record(msg string, fields map[string]any) {
	l.msg = msg
	l.fields = fields
}

func TestAdapter_ConvertsFieldsAndDelegates(t *testing.T) {
	tests := []struct {
		name string
		call func(pgdomain.Logger, string, ...pgdomain.Field)
	}{
		{"Debug", pgdomain.Logger.Debug},
		{"Info", pgdomain.Logger.Info},
		{"Warn", pgdomain.Logger.Warn},
		{"Error", pgdomain.Logger.Error},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := &fakeLogger{}
			adapted := New(log)

			tt.call(adapted, "something happened", pgdomain.Field{Key: "attempt", Value: 3})

			require.Equal(t, "something happened", log.msg)
			require.Equal(t, map[string]any{"attempt": 3}, log.fields)
		})
	}
}

func TestAdapter_NoFieldsYieldsNilMap(t *testing.T) {
	log := &fakeLogger{}
	adapted := New(log)

	adapted.Info("no fields here")

	require.Equal(t, "no fields here", log.msg)
	require.Nil(t, log.fields)
}
