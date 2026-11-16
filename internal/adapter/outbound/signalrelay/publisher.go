// Package signalrelay lets the outbox relay deliver workflow signals. A signal
// is recorded as an outbox record in the transaction of the change it announces
// (execution LLD §5.8); the relay hands it to Temporal here, and everything
// else to the event bus.
package signalrelay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

const (
	signalFailureCode = "SignalDeliveryFailed"
	busFailureCode    = "PublishFailed"
)

var _ events.Publisher = (*Publisher)(nil)

// Publisher sends signal records to Temporal and passes every other record to Next.
type Publisher struct {
	next     events.Publisher
	temporal port.TemporalClient
	log      port.Logger
}

func New(next events.Publisher, temporal port.TemporalClient, log port.Logger) *Publisher {
	return &Publisher{next: next, temporal: temporal, log: log}
}

func (p *Publisher) Publish(ctx context.Context, env events.Envelope[json.RawMessage]) error {
	if env.Type != port.WorkflowSignalEventType {
		if err := p.next.Publish(ctx, env); err != nil {
			return fmt.Errorf("signalrelay: publish %s: %w", env.ID, err)
		}
		return nil
	}
	return p.deliver(ctx, env)
}

// PublishBatch delivers the signals one at a time, in batch order, and hands the
// events to the bus as one batch. A signal that fails is reported by id, so the
// relay retries that record alone.
func (p *Publisher) PublishBatch(ctx context.Context, envs []events.Envelope[json.RawMessage]) error {
	var failures []events.BatchFailure
	var busEvents []events.Envelope[json.RawMessage]
	for _, env := range envs {
		if env.Type != port.WorkflowSignalEventType {
			busEvents = append(busEvents, env)
			continue
		}
		if err := p.deliver(ctx, env); err != nil {
			failures = append(failures, events.BatchFailure{
				ID: env.ID, Code: signalFailureCode, Message: err.Error(), Retryable: errors.Is(err, events.ErrRetryable),
			})
		}
	}
	if len(busEvents) > 0 {
		if err := p.next.PublishBatch(ctx, busEvents); err != nil {
			var busFailures *events.BatchError
			if errors.As(err, &busFailures) {
				failures = append(failures, busFailures.Failures...)
			} else {
				failures = append(failures, busBatchFailures(busEvents, err)...)
			}
		}
	}
	if len(failures) == 0 {
		return nil
	}
	return &events.BatchError{Failures: failures}
}

// busBatchFailures reports every event of a batch the bus refused as a whole, so the
// signal outcomes of the same batch are not lost and the relay retries only the events.
func busBatchFailures(envs []events.Envelope[json.RawMessage], err error) []events.BatchFailure {
	failures := make([]events.BatchFailure, 0, len(envs))
	for _, env := range envs {
		failures = append(failures, events.BatchFailure{ID: env.ID, Code: busFailureCode, Message: err.Error(), Retryable: true})
	}
	return failures
}

func (p *Publisher) deliver(ctx context.Context, env events.Envelope[json.RawMessage]) error {
	msg, instanceID, err := decode(env)
	if err != nil {
		return err
	}
	err = p.temporal.SignalWorkflow(ctx, msg.TemporalWorkflowID, instanceID, msg.SignalName, msg.SignalPayload)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, port.ErrWorkflowNotFound):
		p.warn("signal dropped: the workflow no longer exists", msg)
		return nil
	case errors.Is(err, port.ErrTemporalUnavailable):
		return fmt.Errorf("signalrelay: send %s to %s: %w: %w", msg.SignalName, msg.TemporalWorkflowID, events.ErrRetryable, err)
	default:
		return fmt.Errorf("signalrelay: send %s to %s: %w", msg.SignalName, msg.TemporalWorkflowID, err)
	}
}

func decode(env events.Envelope[json.RawMessage]) (port.WorkflowSignalMessage, uuid.UUID, error) {
	var msg port.WorkflowSignalMessage
	if err := json.Unmarshal(env.Payload, &msg); err != nil {
		return msg, uuid.Nil, fmt.Errorf("signalrelay: record %s is not a signal message: %w", env.ID, err)
	}
	instanceID, err := uuid.Parse(msg.TargetInstanceID)
	if err != nil {
		return msg, uuid.Nil, fmt.Errorf("signalrelay: record %s names no instance: %w", env.ID, err)
	}
	if msg.SignalName == "" || msg.TemporalWorkflowID == "" {
		return msg, uuid.Nil, fmt.Errorf("signalrelay: record %s names no signal or workflow", env.ID)
	}
	return msg, instanceID, nil
}

func (p *Publisher) warn(text string, msg port.WorkflowSignalMessage) {
	if p.log == nil {
		return
	}
	p.log.Warn(text, map[string]any{"signal": msg.SignalName, "workflow_id": msg.TemporalWorkflowID, "instance_id": msg.TargetInstanceID})
}
