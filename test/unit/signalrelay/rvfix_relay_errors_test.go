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

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/signalrelay"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var errRvfixRefused = errors.New("the cluster refused the signal for good")

type rvfixFailingBus struct {
	err error
}

func (b *rvfixFailingBus) Publish(context.Context, events.Envelope[json.RawMessage]) error {
	return b.err
}

func (b *rvfixFailingBus) PublishBatch(context.Context, []events.Envelope[json.RawMessage]) error {
	return b.err
}

func TestPublisher_APermanentRefusalIsNotRetryableSoItDeadLetters(t *testing.T) {
	temporal := &fakeTemporal{errs: []error{fmt.Errorf("signal: %w", errRvfixRefused)}}
	env := signalEnvelope(t, uuid.New(), "wf-1", port.SignalInstanceReassign, `{}`)

	err := signalrelay.New(&fakeBus{}, temporal, nil).Publish(context.Background(), env)

	require.Error(t, err)
	assert.NotErrorIs(t, err, events.ErrRetryable, "a refusal retrying cannot cure must count toward the attempt limit")
	assert.ErrorIs(t, err, errRvfixRefused, "the cause stays reachable")
}

func TestPublisher_AClusterOutageIsRetryable(t *testing.T) {
	temporal := &fakeTemporal{errs: []error{fmt.Errorf("signal: %w", port.ErrTemporalUnavailable)}}
	env := signalEnvelope(t, uuid.New(), "wf-1", port.SignalInstanceReassign, `{}`)

	err := signalrelay.New(&fakeBus{}, temporal, nil).Publish(context.Background(), env)

	assert.ErrorIs(t, err, events.ErrRetryable)
	assert.ErrorIs(t, err, port.ErrTemporalUnavailable)
}

func TestPublisher_ABatchReportsAPermanentRefusalAsNotRetryableAndAnOutageAsRetryable(t *testing.T) {
	temporal := &fakeTemporal{errs: []error{fmt.Errorf("signal: %w", errRvfixRefused), fmt.Errorf("signal: %w", port.ErrTemporalUnavailable)}}
	refused := signalEnvelope(t, uuid.New(), "wf-1", port.SignalInstanceReassign, `{}`)
	outage := signalEnvelope(t, uuid.New(), "wf-2", port.SignalInstanceReassign, `{}`)

	err := signalrelay.New(&fakeBus{}, temporal, nil).PublishBatch(context.Background(), []events.Envelope[json.RawMessage]{refused, outage})

	var batchErr *events.BatchError
	require.ErrorAs(t, err, &batchErr)
	retryable := map[string]bool{}
	for _, f := range batchErr.Failures {
		retryable[f.ID] = f.Retryable
	}
	assert.Equal(t, map[string]bool{refused.ID: false, outage.ID: true}, retryable)
}

func TestPublisher_ABusFailureOfAWholeBatchKeepsTheSignalOutcomes(t *testing.T) {
	busErr := errors.New("bus unreachable")
	temporal := &fakeTemporal{errs: []error{nil, fmt.Errorf("signal: %w", port.ErrTemporalUnavailable)}}
	delivered := signalEnvelope(t, uuid.New(), "wf-1", port.SignalInstanceReassign, `{}`)
	failing := signalEnvelope(t, uuid.New(), "wf-2", port.SignalInstanceReassign, `{}`)
	eventA, eventB := eventEnvelope(), eventEnvelope()

	err := signalrelay.New(&rvfixFailingBus{err: busErr}, temporal, nil).PublishBatch(context.Background(),
		[]events.Envelope[json.RawMessage]{delivered, eventA, failing, eventB})

	var batchErr *events.BatchError
	require.ErrorAs(t, err, &batchErr, "a bare bus error would make the relay retry the signal that was already delivered")
	byID := map[string]events.BatchFailure{}
	for _, f := range batchErr.Failures {
		byID[f.ID] = f
	}
	require.Len(t, byID, 3)
	assert.NotContains(t, byID, delivered.ID, "the delivered signal is not reported")
	assert.Equal(t, "SignalDeliveryFailed", byID[failing.ID].Code)
	for _, ev := range []events.Envelope[json.RawMessage]{eventA, eventB} {
		f := byID[ev.ID]
		assert.Equal(t, "PublishFailed", f.Code)
		assert.True(t, f.Retryable)
		assert.Contains(t, f.Message, busErr.Error())
	}
}

func TestPublisher_AnEventThatTheBusRefusesKeepsTheCauseReachable(t *testing.T) {
	busErr := errors.New("bus unreachable")

	err := signalrelay.New(&rvfixFailingBus{err: busErr}, &fakeTemporal{}, nil).Publish(context.Background(), eventEnvelope())

	require.Error(t, err)
	require.ErrorIs(t, err, busErr)
	assert.NotEqual(t, busErr.Error(), err.Error(), "the error says where it came from")
}

func TestPublisher_ABusRetryableMarkSurvivesTheWrap(t *testing.T) {
	busErr := fmt.Errorf("throttled: %w", events.ErrRetryable)

	err := signalrelay.New(&rvfixFailingBus{err: busErr}, &fakeTemporal{}, nil).Publish(context.Background(), eventEnvelope())

	assert.ErrorIs(t, err, events.ErrRetryable)
}
