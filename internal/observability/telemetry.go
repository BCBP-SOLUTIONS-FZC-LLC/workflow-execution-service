package observability

import (
	"context"
	"fmt"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgmetrics"
)

type Logger interface {
	Debug(msg string, fields map[string]any)
	Info(msg string, fields map[string]any)
	Warn(msg string, fields map[string]any)
	Error(msg string, fields map[string]any)
}

func GinConfig(serviceName, buildVersion, environment string, log Logger) gincommon.Config {
	return gincommon.Config{
		Logger:       log,
		ServiceName:  serviceName,
		BuildVersion: buildVersion,
		Domain:       Domain,
		Environment:  environment,
	}
}

func InitTelemetry(ginCfg gincommon.Config, log Logger) (func(), error) {
	shutdown, err := gincommon.InitTracingWithConfig(gincommon.TracingConfig{
		ServiceName:  ginCfg.ServiceName,
		BuildVersion: ginCfg.BuildVersion,
		Domain:       ginCfg.Domain,
		Environment:  ginCfg.Environment,
		Logger:       ginCfg.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("tracing: %w", err)
	}

	_ = gincommon.ObservabilityMiddlewares(ginCfg)
	Register()

	identity := gincommon.MetricsConstLabels()
	registerer := gincommon.MetricsRegisterer()

	eventWarnings, err := events.InitMetrics(events.MetricsIdentityFromLabels(identity), registerer)
	if err != nil {
		shutdown()
		return nil, fmt.Errorf("platform-events metrics: %w", err)
	}
	for _, w := range eventWarnings {
		log.Warn("platform-events metric not registered", map[string]any{"metric": w.Metric, "error": w.Error()})
	}

	pgWarnings, err := pgmetrics.InitWithIdentity(pgmetrics.IdentityFromLabels(identity), registerer)
	if err != nil {
		shutdown()
		return nil, fmt.Errorf("platform-pgcommon metrics: %w", err)
	}
	for _, w := range pgWarnings {
		log.Warn("platform-pgcommon metric not registered", map[string]any{"metric": w.Metric, "error": w.Error()})
	}

	return shutdown, nil
}

func StartSpan(ctx context.Context, name string) (context.Context, func()) {
	ctx, span := gincommon.NewTracer(Domain+"/"+serviceScope).Start(ctx, name)
	return ctx, func() { span.End() }
}

func TraceID(ctx context.Context) string {
	return gincommon.SpanTraceID(ctx)
}
