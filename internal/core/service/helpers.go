// Package service holds the envelope/codec plumbing every outbound-event
// producing call site (T1.5-T1.8) builds on top of.
package service

import (
	"context"
	"encoding/json"
	"fmt"

	"go.opentelemetry.io/otel/trace"

	pgdomain "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"
	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/events"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func withTenantGUC(ctx context.Context, tenantID uuid.UUID) context.Context {
	return pgcommon.WithGUCSet(ctx, pgdomain.GUCSet{TenantID: tenantID.String()})
}

// BuildEnvelope marshals payload, validates it against its registered JSON
// Schema, and wraps it in an outbound events.Envelope
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

type noopValidator struct{}

func (noopValidator) Validate(_ context.Context, _ string, _ json.RawMessage) error {
	return nil
}

func pageAfter(page port.Page) *port.Cursor {
	if page.Cursor == nil {
		return nil
	}
	return &port.Cursor{CreatedAt: page.Cursor.CreatedAt, ID: page.Cursor.ID}
}

func encodeNextCursor(c *port.Cursor) string {
	if c == nil {
		return ""
	}
	return port.EncodeCursor(port.CursorPosition{CreatedAt: c.CreatedAt, ID: c.ID})
}

type noopLogger struct{}

func (noopLogger) Debug(string, map[string]any) { /* no-op fallback */ }
func (noopLogger) Info(string, map[string]any)  { /* no-op fallback */ }
func (noopLogger) Warn(string, map[string]any)  { /* no-op fallback */ }
func (noopLogger) Error(string, map[string]any) { /* no-op fallback */ }

func instanceCore(inst *domain.Instance) domain.CommonCore {
	return domain.CommonCore{WorkflowInstanceID: inst.ID, BusinessKey: inst.BusinessKey, WorkflowVersionID: inst.WorkflowVersionID}
}

type instanceEventSink struct {
	Outbox    port.OutboxRepository
	Validator port.EventValidator
}

func (s instanceEventSink) enqueueInstanceEvent(ctx context.Context, tenantID, eventType, instanceID, actor string, payload any) error {
	envelope, err := BuildEnvelope(ctx, s.Validator, eventType, tenantID, "instances/"+instanceID, actor, payload)
	if err != nil {
		return err
	}
	return s.Outbox.Enqueue(ctx, envelope)
}
