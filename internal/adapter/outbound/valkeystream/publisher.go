package valkeystream

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var _ port.ConnectorEventPublisher = (*EventPublisher)(nil)

type streamPublisher interface {
	Publish(ctx context.Context, streamKey string, fields map[string]string) (string, error)
}

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
