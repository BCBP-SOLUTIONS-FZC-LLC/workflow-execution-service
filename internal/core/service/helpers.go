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

func buildEnvelope[T any](
	ctx context.Context,
	codec port.GlueCodec,
	eventType, tenantID, subject, actor string,
	payload T,
) (events.Envelope[json.RawMessage], error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return events.Envelope[json.RawMessage]{}, fmt.Errorf("marshal event payload: %w", err)
	}
	encoded, err := codec.Encode(ctx, eventType, raw)
	if err != nil {
		return events.Envelope[json.RawMessage]{}, fmt.Errorf("glue encode event payload: %w", err)
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
	return events.NewEnvelope[json.RawMessage](eventType, domain.EventSource, encoded, opts...), nil
}

// noopGlueCodec is the zero-value fallback used when a caller's GlueCodec
// dependency is nil - distinct from the real adapter's own useStub flag
// (internal/adapter/outbound/glue.Codec), which is the dev/test no-op path
// when a GlueCodec is actually wired up.
type noopGlueCodec struct{}

func (noopGlueCodec) Encode(_ context.Context, _ string, payload []byte) ([]byte, error) {
	return payload, nil
}
