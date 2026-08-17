package valkeystream

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var _ port.ConnectorEventPublisher = (*EventPublisher)(nil)

// streamPublisher is the narrow seam EventPublisher depends on instead of
// the concrete *Producer type — lets unit tests exercise the field-mapping
// logic below with a fake, while *Producer itself only has real integration
// coverage (this repo's own convention for the raw Stream-command wrappers,
// matching internal/adapter/outbound/valkey.Cache's own test-tier split).
type streamPublisher interface {
	Publish(ctx context.Context, streamKey string, fields map[string]string) (string, error)
}

// EventPublisher implements port.ConnectorEventPublisher by flattening a
// ConnectorTaskCreatedEvent onto streamKey via a streamPublisher — Stream
// fields are string-only, so ResolvedInputs/OutputMapping travel as JSON
// strings.
type EventPublisher struct {
	publisher streamPublisher
	streamKey string
}

func NewEventPublisher(publisher streamPublisher, streamKey string) *EventPublisher {
	return &EventPublisher{publisher: publisher, streamKey: streamKey}
}

func (p *EventPublisher) PublishTaskCreated(ctx context.Context, event port.ConnectorTaskCreatedEvent) error {
	resolvedInputs, err := json.Marshal(event.ResolvedInputs)
	if err != nil {
		return fmt.Errorf("valkeystream: marshal resolved_inputs: %w", err)
	}
	outputMapping, err := json.Marshal(event.OutputMapping)
	if err != nil {
		return fmt.Errorf("valkeystream: marshal output_mapping: %w", err)
	}

	_, err = p.publisher.Publish(ctx, p.streamKey, map[string]string{
		"event_id":        event.EventID.String(),
		"tenant_id":       event.TenantID.String(),
		"instance_id":     event.InstanceID.String(),
		"task_id":         event.TaskID.String(),
		"node_key":        event.NodeKey,
		"department_id":   event.DepartmentID.String(),
		"connector_type":  event.ConnectorType,
		"resolved_inputs": string(resolvedInputs),
		"output_mapping":  string(outputMapping),
	})
	if err != nil {
		return fmt.Errorf("valkeystream: publish task-created: %w", err)
	}
	return nil
}
