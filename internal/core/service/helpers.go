// Package service holds the envelope/codec plumbing every outbound-event
// producing call site (T1.5-T1.8) builds on top of.
package service

import (
	"context"
	"encoding/json"
	"fmt"

	"go.opentelemetry.io/otel/trace"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// BuildEnvelope marshals payload, validates it against its registered JSON
// Schema, and wraps it in an outbound events.Envelope — the shared plumbing
// every outbound-event-producing call site (T1.5-T1.8, internal/adapter/outbound/temporal)
// builds on top of.
func BuildEnvelope[T any](
	ctx context.Context,
	validator port.EventValidator,
	eventType, tenantID, subject, actor string,
	payload T,
) (events.Envelope[json.RawMessage], error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return events.Envelope[json.RawMessage]{}, fmt.Errorf("marshal event payload: %w", err)
	}
	if err := validator.Validate(ctx, eventType, raw); err != nil {
		return events.Envelope[json.RawMessage]{}, fmt.Errorf("validate event payload: %w", err)
	}
	opts := []events.EnvelopeOpt{
		events.WithTenantID(tenantID),
		events.WithSchemaVersion("1"),
	}
	if subject != "" {
		opts = append(opts, events.WithSubject(subject))
	}
	if actor != "" {
		opts = append(opts, events.WithActor(actor))
	}
	if sc := trace.SpanFromContext(ctx).SpanContext(); sc.IsValid() {
		opts = append(opts, events.WithTraceID(sc.TraceID().String()))
	}
	return events.NewEnvelope[json.RawMessage](eventType, domain.EventSource, raw, opts...), nil
}

// noopValidator is the zero-value fallback used when a caller's
// EventValidator dependency is nil.
type noopValidator struct{}

func (noopValidator) Validate(_ context.Context, _ string, _ json.RawMessage) error {
	return nil
}
