package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/opentelemetry"
	"go.temporal.io/sdk/interceptor"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/pkg/outbox"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/logger"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/grpc"
	httpadapter "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	outboundgrpc "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/grpc"
	outboundhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporalclient"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/valkeystream"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
)

// newApp is cmd/server's composition root (LLD §1.7): the only place this
// binary wires concrete adapters into internal/core/service's struct
// literals. Every construction step appends its own cleanup to cleanups so a
// failure partway through unwinds everything already built, most-recent
// first.
func newApp(cfg *config.Config) (*app, error) {
	var cleanups []func()
	unwind := func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}

	log, err := logger.NewLogger(cfg.AppEnv)
	if err != nil {
		return nil, fmt.Errorf("logger: %w", err)
	}

	// Must run before any gin engine/ObservabilityMiddlewares is constructed:
	// gincommon.ObservabilityMiddlewares installs a TracerProvider only if
	// none is registered yet, so whichever of the two runs first wins and
	// produces the real, capturable shutdown func.
	observability.Register()
	observability.RegisterPGMetrics(cfg.OTELServiceName, cfg.BuildVersion)
	observability.RegisterEventsMetrics(cfg.OTELServiceName, cfg.BuildVersion)
	tracingShutdown := observability.InitTracing()
	cleanups = append(cleanups, tracingShutdown)

	ctx := context.Background()

	appPool, err := newAppPool(ctx, cfg, log)
	if err != nil {
		unwind()
		return nil, err
	}
	cleanups = append(cleanups, appPool.Close)
	observability.RegisterPoolStats(appPool, cfg.OTELServiceName+"-app")

	relayPool, err := newRelayPool(ctx, cfg, log)
	if err != nil {
		unwind()
		return nil, err
	}
	cleanups = append(cleanups, relayPool.Close)
	observability.RegisterPoolStats(relayPool, cfg.OTELServiceName+"-relay")

	cache, cacheClient, err := newCacheStore(ctx, cfg)
	if err != nil {
		unwind()
		return nil, err
	}
	cleanups = append(cleanups, func() { _ = cacheClient.Close() })

	// connectorEvents reuses cacheClient's own Valkey connection — the same
	// instance backs both the plain KV cache above and the connector-task
	// Stream cmd/connector-worker consumes; no separate client needed for a
	// producer-only role.
	connectorEvents := valkeystream.NewEventPublisher(valkeystream.NewProducer(cacheClient), cfg.ConnectorStreamKey)

	glueCodec, err := newGlueCodec(ctx, cfg)
	if err != nil {
		unwind()
		return nil, err
	}

	publisher, err := newPublisher(cfg, log, glueCodec)
	if err != nil {
		unwind()
		return nil, err
	}

	relay, err := outbox.NewRunner(outbox.Config{
		Pool:         relayPool,
		Publisher:    publisher,
		Logger:       log,
		PollInterval: cfg.OutboxPollInterval,
		BatchSize:    cfg.OutboxBatchSize,
	})
	if err != nil {
		unwind()
		return nil, fmt.Errorf("outbox runner: %w", err)
	}

	var interceptors []interceptor.ClientInterceptor
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		tracingInterceptor, err := observability.NewTemporalTracingInterceptor(opentelemetry.TracerOptions{})
		if err != nil {
			unwind()
			return nil, err
		}
		interceptors = append(interceptors, tracingInterceptor)
	}
	sdkClient, err := client.Dial(client.Options{
		HostPort:     cfg.TemporalHostPort,
		Namespace:    cfg.TemporalNamespace,
		Interceptors: interceptors,
	})
	if err != nil {
		unwind()
		return nil, fmt.Errorf("dial temporal: %w", err)
	}
	cleanups = append(cleanups, sdkClient.Close)
	temporal := temporalclient.New(sdkClient)

	validator, err := eventbus.NewSchemaValidator()
	if err != nil {
		unwind()
		return nil, fmt.Errorf("new schema validator: %w", err)
	}

	if cfg.DefinitionServiceAddr == "" {
		unwind()
		return nil, fmt.Errorf("DEFINITION_SERVICE_ADDR is required for cmd/server")
	}
	if cfg.EligibilityBaseURL == "" {
		unwind()
		return nil, fmt.Errorf("ELIGIBILITY_BASE_URL is required for cmd/server")
	}

	definitions, err := outboundgrpc.NewDefinitionClient(cfg.DefinitionServiceAddr, cfg.DefinitionClientTimeout)
	if err != nil {
		unwind()
		return nil, fmt.Errorf("new definition client: %w", err)
	}
	cleanups = append(cleanups, func() { _ = definitions.Close() })

	eligibility := outboundhttp.NewEligibilityClient(cfg.EligibilityBaseURL, cfg.EligibilityClientTimeout)
	iam := outboundhttp.NewIAMClient(cfg.IAMBaseURL, cfg.IAMClientTimeout)

	instances := postgres.NewInstanceRepo(appPool)
	tasks := postgres.NewTaskRepo(appPool)
	assignments := postgres.NewTaskAssignmentRepo(appPool)
	outboxRepo := postgres.NewOutboxRepo(appPool)
	processedEvents := postgres.NewProcessedEventRepo(appPool)
	recency := postgres.NewRecencyGuardRepo(appPool)
	overrides := postgres.NewAssigneeOverrideRepo(appPool)
	queues := postgres.NewActiveTaskQueueRepo(appPool)
	transactor := postgres.NewTransactor(appPool)

	instanceService := &service.InstanceService{
		Instances: instances, Tasks: tasks, Assignments: assignments, Outbox: outboxRepo,
		Transactor: transactor, Temporal: temporal, Definitions: definitions, Eligibility: eligibility,
		Validator: validator, Cache: cache, Log: log,
	}
	taskService := &service.TaskService{
		Instances: instances, Tasks: tasks, Assignments: assignments, Overrides: overrides,
		Temporal: temporal, IAM: iam, Log: log,
	}
	connectorTaskService := &service.ConnectorTaskService{
		Instances: instances, Tasks: tasks, Temporal: temporal, Cache: cache, Log: log,
	}
	workflowClient := &service.WorkflowClient{
		Instances: instances, Tasks: tasks, Assignments: assignments, Temporal: temporal, Log: log,
	}
	guard := &service.ArchiveGuard{Instances: instances}
	pauser := &service.UserTaskPauser{
		Instances: instances, Assignments: assignments, Tasks: tasks, Temporal: temporal, Log: log,
	}
	delegationReconciler := &service.DelegationReconciler{
		Instances: instances, Tasks: tasks, Assignments: assignments, Outbox: outboxRepo,
		Transactor: transactor, Temporal: temporal, Definitions: definitions, Eligibility: eligibility,
		Validator: validator, Log: log,
	}
	tenantLifecycleReconciler := &service.TenantLifecycleReconciler{
		Instances: instances, Tasks: tasks, Assignments: assignments, Queues: queues, Outbox: outboxRepo,
		Transactor: transactor, Temporal: temporal, Validator: validator, Log: log,
	}
	userSafetyNetReconciler := &service.UserSafetyNetReconciler{Assignments: assignments}
	oooAvailabilityReconciler := &service.OOOAvailabilityReconciler{
		Instances: instances, Assignments: assignments, Tasks: tasks, Temporal: temporal, Log: log,
	}

	h := handler.New(handler.Services{
		Tasks: taskService, Instances: instanceService, Eligibility: eligibility, WorkflowClient: workflowClient,
		Cache: cache, IdempotencyTTL: cfg.IdempotencyTTL, ProcessedEvents: processedEvents, Recency: recency,
		Delegation: delegationReconciler, TenantLifecycle: tenantLifecycleReconciler,
		UserSafetyNet: userSafetyNetReconciler, OOOAvailability: oooAvailabilityReconciler,
		ConnectorTasks:  connectorTaskService,
		ConnectorEvents: connectorEvents, Log: log,
	})

	grpcSrv := grpc.NewGRPCServer(cfg.OTELServiceName, cfg.BuildVersion, cfg.InternalAPIToken, log, guard, pauser)

	router := httpadapter.NewRouter(httpadapter.RouterConfig{
		GinConfig:        gincommon.Config{Logger: log, ServiceName: cfg.OTELServiceName, BuildVersion: cfg.BuildVersion},
		AppEnv:           cfg.AppEnv,
		InternalAPIToken: cfg.InternalAPIToken,
		Handler:          h,
		DB:               dbPinger{pool: appPool},
		Cache:            cache,
		Temporal:         temporalPinger{sdk: sdkClient},
	})
	httpServer := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.HTTPPort),
		Handler:      router.Handler(),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.Handler())
	metricsServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.MetricsPort),
		Handler: metricsMux,
	}

	return &app{
		cfg:             cfg,
		log:             log,
		appPool:         appPool,
		relayPool:       relayPool,
		cache:           cacheClient,
		httpServer:      httpServer,
		metricsServer:   metricsServer,
		grpcServer:      grpcSrv,
		outboxRelay:     relay,
		definitions:     definitions,
		sdkClient:       sdkClient,
		tracingShutdown: tracingShutdown,
	}, nil
}
