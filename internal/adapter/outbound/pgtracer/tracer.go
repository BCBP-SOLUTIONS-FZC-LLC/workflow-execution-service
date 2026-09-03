// Package pgtracer adapts an OTel tracer to platform-pgcommon's own
// pgcommon.Config.Tracer port so every query gets a "db.query" span nested
// under the request-level span, instead of pgcommon.NewPool going unset and
// query-level tracing never happening. The port type lives in pgcommon's
// internal package and can't be named directly, but Go's structural typing
// satisfies it as long as StartSpan matches — no import of that internal
// package is needed.
package pgtracer

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

type Tracer struct {
	tracer trace.Tracer
}

// New wraps otel.Tracer(serviceName) so it satisfies pgcommon.Config.Tracer.
func New(serviceName string) *Tracer {
	return &Tracer{tracer: otel.Tracer(serviceName)}
}

func (t *Tracer) StartSpan(ctx context.Context, name string) (context.Context, func()) {
	ctx, span := t.tracer.Start(ctx, name)
	return ctx, func() { span.End() }
}
