package signalrelay_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/converter"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/signalrelay"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type sentSignal struct {
	workflowID string
	instanceID uuid.UUID
	name       string
	payload    any
}

type fakeTemporal struct {
	port.TemporalClient
	sent []sentSignal
	errs []error
}

func (f *fakeTemporal) SignalWorkflow(_ context.Context, workflowID string, instanceID uuid.UUID, name string, payload any) error {
	if len(f.errs) > 0 {
		err := f.errs[0]
		f.errs = f.errs[1:]
		if err != nil {
			return err
		}
	}
	f.sent = append(f.sent, sentSignal{workflowID: workflowID, instanceID: instanceID, name: name, payload: payload})
	return nil
}

type fakeBus struct {
	published []events.Envelope[json.RawMessage]
	batches   [][]events.Envelope[json.RawMessage]
	batchErr  error
}

func (b *fakeBus) Publish(_ context.Context, env events.Envelope[json.RawMessage]) error {
	b.published = append(b.published, env)
	return nil
}

func (b *fakeBus) PublishBatch(_ context.Context, envs []events.Envelope[json.RawMessage]) error {
	b.batches = append(b.batches, envs)
	return b.batchErr
}

func signalEnvelope(t *testing.T, instanceID uuid.UUID, workflowID, name string, payload string) events.Envelope[json.RawMessage] {
	t.Helper()
	data, err := json.Marshal(port.WorkflowSignalMessage{
		TargetInstanceID: instanceID.String(), TemporalWorkflowID: workflowID, SignalName: name, SignalPayload: json.RawMessage(payload),
	})
	require.NoError(t, err)
	return events.NewEnvelope[json.RawMessage](port.WorkflowSignalEventType, "execution", data, events.WithTenantID(uuid.NewString()))
}

func eventEnvelope() events.Envelope[json.RawMessage] {
	return events.NewEnvelope[json.RawMessage]("WorkflowTaskReassigned", "execution", json.RawMessage(`{}`), events.WithTenantID(uuid.NewString()))
}

func TestPublisher_ASignalRecordIsSentToTheNamedWorkflowAndNeverToTheBus(t *testing.T) {
	temporal, bus := &fakeTemporal{}, &fakeBus{}
	instanceID := uuid.New()
	pub := signalrelay.New(bus, temporal, nil)

	err := pub.Publish(context.Background(), signalEnvelope(t, instanceID, "wf-1", port.SignalInstanceReassign, `{"TaskID":"t-1"}`))

	require.NoError(t, err)
	require.Len(t, temporal.sent, 1)
	got := temporal.sent[0]
	assert.Equal(t, "wf-1", got.workflowID)
	assert.Equal(t, instanceID, got.instanceID)
	assert.Equal(t, port.SignalInstanceReassign, got.name)
	assert.Empty(t, bus.published, "a signal record is not an event")
}

func TestPublisher_TheSignalPayloadArrivesAsTheJSONTheServiceRecorded(t *testing.T) {
	temporal := &fakeTemporal{}
	pub := signalrelay.New(&fakeBus{}, temporal, nil)
	require.NoError(t, pub.Publish(context.Background(), signalEnvelope(t, uuid.New(), "wf-1", port.SignalInstanceReassign,
		`{"TaskID":"t-1","OldUserID":"u-1","NewUserID":"u-2","AdminUserID":"","RecordVersion":3}`)))

	payload, err := converter.GetDefaultDataConverter().ToPayload(temporal.sent[0].payload)
	require.NoError(t, err)
	var decoded struct {
		TaskID, OldUserID, NewUserID, AdminUserID string
		RecordVersion                             int64
	}
	require.NoError(t, converter.GetDefaultDataConverter().FromPayload(payload, &decoded))
	assert.Equal(t, "t-1", decoded.TaskID)
	assert.Equal(t, "u-1", decoded.OldUserID)
	assert.Equal(t, "u-2", decoded.NewUserID)
	assert.Equal(t, int64(3), decoded.RecordVersion, "the workflow decodes the signal as it would a direct one")
}

func TestPublisher_AFailedSendIsRetryableSoTheRelayTriesItAgain(t *testing.T) {
	temporal := &fakeTemporal{errs: []error{fmt.Errorf("signal: %w", port.ErrTemporalUnavailable)}}
	pub := signalrelay.New(&fakeBus{}, temporal, nil)
	env := signalEnvelope(t, uuid.New(), "wf-1", port.SignalInstanceReassign, `{}`)

	err := pub.Publish(context.Background(), env)

	require.Error(t, err)
	assert.ErrorIs(t, err, events.ErrRetryable, "an outage says nothing about the signal, so retries do not use up its attempts")
	assert.Empty(t, temporal.sent)

	require.NoError(t, pub.Publish(context.Background(), env), "the retry delivers it")
	assert.Len(t, temporal.sent, 1, "and it arrives once")
}

func TestPublisher_AWorkflowThatNoLongerExistsIsNotAnError(t *testing.T) {
	temporal := &fakeTemporal{errs: []error{fmt.Errorf("signal: %w", port.ErrWorkflowNotFound)}}
	pub := signalrelay.New(&fakeBus{}, temporal, nil)

	err := pub.Publish(context.Background(), signalEnvelope(t, uuid.New(), "wf-gone", port.SignalInstanceReassign, `{}`))

	require.NoError(t, err, "nothing is left to tell, and an error here would hold up every later signal of the instance")
}

func TestPublisher_ARecordItCannotReadIsNotRetryable(t *testing.T) {
	cases := map[string]events.Envelope[json.RawMessage]{
		"not json": events.NewEnvelope[json.RawMessage](port.WorkflowSignalEventType, "execution", json.RawMessage(`"x"`)),
		"an instance id that is not a uuid": events.NewEnvelope[json.RawMessage](port.WorkflowSignalEventType, "execution",
			json.RawMessage(`{"target_instance_id":"nope","temporal_workflow_id":"wf","signal_name":"s"}`)),
		"no signal name": events.NewEnvelope[json.RawMessage](port.WorkflowSignalEventType, "execution",
			json.RawMessage(`{"target_instance_id":"`+uuid.NewString()+`","temporal_workflow_id":"wf"}`)),
	}
	for name, env := range cases {
		t.Run(name, func(t *testing.T) {
			temporal := &fakeTemporal{}
			err := signalrelay.New(&fakeBus{}, temporal, nil).Publish(context.Background(), env)

			require.Error(t, err)
			assert.NotErrorIs(t, err, events.ErrRetryable, "retrying cannot make it readable, so it is dead-lettered")
			assert.Empty(t, temporal.sent)
		})
	}
}

func TestPublisher_AnEventGoesToTheBusUnchanged(t *testing.T) {
	temporal, bus := &fakeTemporal{}, &fakeBus{}
	env := eventEnvelope()

	require.NoError(t, signalrelay.New(bus, temporal, nil).Publish(context.Background(), env))

	assert.Equal(t, []events.Envelope[json.RawMessage]{env}, bus.published)
	assert.Empty(t, temporal.sent)
}

func TestPublisher_ABatchSplitsSignalsFromEventsAndReportsEachFailureByID(t *testing.T) {
	temporal := &fakeTemporal{errs: []error{nil, fmt.Errorf("signal: %w", port.ErrTemporalUnavailable)}}
	bus := &fakeBus{}
	delivered := signalEnvelope(t, uuid.New(), "wf-1", port.SignalInstanceReassign, `{}`)
	failing := signalEnvelope(t, uuid.New(), "wf-2", port.SignalInstanceTaskVacated, `{}`)
	event := eventEnvelope()

	err := signalrelay.New(bus, temporal, nil).PublishBatch(context.Background(), []events.Envelope[json.RawMessage]{delivered, event, failing})

	var batchErr *events.BatchError
	require.ErrorAs(t, err, &batchErr)
	require.Len(t, batchErr.Failures, 1, "only the signal that could not be sent fails")
	assert.Equal(t, failing.ID, batchErr.Failures[0].ID)
	assert.True(t, batchErr.Failures[0].Retryable)
	require.Len(t, temporal.sent, 1)
	assert.Equal(t, "wf-1", temporal.sent[0].workflowID)
	require.Len(t, bus.batches, 1)
	assert.Equal(t, []events.Envelope[json.RawMessage]{event}, bus.batches[0], "the bus gets the events only")
}

func TestPublisher_ABatchKeepsTheBusFailuresAlongsideTheSignalFailures(t *testing.T) {
	temporal := &fakeTemporal{errs: []error{errors.New("temporal unavailable")}}
	event := eventEnvelope()
	bus := &fakeBus{batchErr: &events.BatchError{Failures: []events.BatchFailure{{ID: event.ID, Code: "Throttled", Retryable: true}}}}
	failing := signalEnvelope(t, uuid.New(), "wf-2", port.SignalInstanceTaskVacated, `{}`)

	err := signalrelay.New(bus, temporal, nil).PublishBatch(context.Background(), []events.Envelope[json.RawMessage]{event, failing})

	var batchErr *events.BatchError
	require.ErrorAs(t, err, &batchErr)
	ids := []string{}
	for _, f := range batchErr.Failures {
		ids = append(ids, f.ID)
	}
	assert.ElementsMatch(t, []string{event.ID, failing.ID}, ids)
}

func TestPublisher_ABatchOfSignalsAloneNeverTouchesTheBus(t *testing.T) {
	temporal, bus := &fakeTemporal{}, &fakeBus{}

	err := signalrelay.New(bus, temporal, nil).PublishBatch(context.Background(), []events.Envelope[json.RawMessage]{
		signalEnvelope(t, uuid.New(), "wf-1", port.SignalInstanceReassign, `{}`),
	})

	require.NoError(t, err)
	assert.Empty(t, bus.batches)
	assert.Len(t, temporal.sent, 1)
}
