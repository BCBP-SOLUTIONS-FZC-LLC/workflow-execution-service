package signalrelay_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/signalrelay"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type w30bLogLine struct {
	level  string
	msg    string
	fields map[string]any
}

type w30bRecordingLogger struct {
	lines []w30bLogLine
}

func (l *w30bRecordingLogger) record(level, msg string, fields map[string]any) {
	l.lines = append(l.lines, w30bLogLine{level: level, msg: msg, fields: fields})
}
func (l *w30bRecordingLogger) Debug(msg string, f map[string]any) { l.record("debug", msg, f) }
func (l *w30bRecordingLogger) Info(msg string, f map[string]any)  { l.record("info", msg, f) }
func (l *w30bRecordingLogger) Warn(msg string, f map[string]any)  { l.record("warn", msg, f) }
func (l *w30bRecordingLogger) Error(msg string, f map[string]any) { l.record("error", msg, f) }

func TestPublisher_WorkflowNoLongerExists_LogsOneWarningNamingTheSignalAndCountsAsDelivered(t *testing.T) {
	logger := &w30bRecordingLogger{}
	temporal := &fakeTemporal{errs: []error{fmt.Errorf("signal: %w", port.ErrWorkflowNotFound), nil}}
	gone := uuid.New()
	gonePayload := signalEnvelope(t, gone, "w30b-wf-gone", port.SignalInstanceReassign, `{}`)
	next := signalEnvelope(t, uuid.New(), "w30b-wf-live", port.SignalInstanceTaskVacated, `{}`)

	err := signalrelay.New(&fakeBus{}, temporal, logger).PublishBatch(context.Background(), []events.Envelope[json.RawMessage]{gonePayload, next})

	require.NoError(t, err, "the dropped signal is reported as delivered, so the relay does not retry it")
	require.Len(t, logger.lines, 1)
	line := logger.lines[0]
	assert.Equal(t, "warn", line.level)
	assert.Equal(t, "signal dropped: the workflow no longer exists", line.msg)
	assert.Equal(t, map[string]any{
		"signal":      port.SignalInstanceReassign,
		"workflow_id": "w30b-wf-gone",
		"instance_id": gone.String(),
	}, line.fields)
	require.Len(t, temporal.sent, 1, "the later record of the batch is still sent")
	assert.Equal(t, "w30b-wf-live", temporal.sent[0].workflowID)
}

func TestPublisher_RecordItCannotUse_NamesTheRecordIsNotRetryableAndSendsNothing(t *testing.T) {
	instance := uuid.NewString()
	cases := []struct {
		name    string
		payload string
		want    string
	}{
		{"not a signal message", `"x"`, "is not a signal message"},
		{"instance id that is not a uuid", `{"target_instance_id":"nope","temporal_workflow_id":"wf","signal_name":"s"}`, "names no instance"},
		{"no signal name", `{"target_instance_id":"` + instance + `","temporal_workflow_id":"wf"}`, "names no signal or workflow"},
		{"no workflow id", `{"target_instance_id":"` + instance + `","signal_name":"s"}`, "names no signal or workflow"},
		{"neither signal name nor workflow id", `{"target_instance_id":"` + instance + `"}`, "names no signal or workflow"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			temporal := &fakeTemporal{}
			env := events.NewEnvelope[json.RawMessage](port.WorkflowSignalEventType, "execution", json.RawMessage(tc.payload))
			pub := signalrelay.New(&fakeBus{}, temporal, nil)

			err := pub.Publish(context.Background(), env)

			require.Error(t, err)
			assert.Contains(t, err.Error(), "signalrelay: record "+env.ID+" ")
			assert.Contains(t, err.Error(), tc.want)
			assert.NotErrorIs(t, err, events.ErrRetryable)
			assert.Empty(t, temporal.sent)

			batchErr := pub.PublishBatch(context.Background(), []events.Envelope[json.RawMessage]{env})
			var failures *events.BatchError
			require.ErrorAs(t, batchErr, &failures)
			require.Len(t, failures.Failures, 1)
			assert.Equal(t, env.ID, failures.Failures[0].ID)
			assert.False(t, failures.Failures[0].Retryable, "the relay dead-letters a record it cannot read")
			assert.Empty(t, temporal.sent)
		})
	}
}
