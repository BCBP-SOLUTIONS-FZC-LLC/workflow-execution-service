package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/opentelemetry"
	"go.temporal.io/sdk/interceptor"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/outbox"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/logger"

	httpadapter "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	outboundhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporalclient"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/valkeystream"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
)

const automationPrincipalCacheTTL = 5 * time.Minute

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

	ginCfg := observability.GinConfig(cfg.OTELServiceName, cfg.BuildVersion, cfg.AppEnv, log)
	tracingShutdown, err := observability.InitTelemetry(ginCfg, log)
	if err != nil {
		return nil, err
	}
	cleanups = append(cleanups, tracingShutdown)

	ctx := context.Background()

	appPool, err := newAppPool(ctx, cfg, log)
	if err != nil {
		unwind()
		return nil, err
	}
	cleanups = append(cleanups, appPool.Close)

	relayPool, err := newRelayPool(ctx, cfg, log)
	if err != nil {
		unwind()
		return nil, err
	}
	cleanups = append(cleanups, relayPool.Close)

	cache, cacheClient, err := newCacheStore(ctx, cfg)
	if err != nil {
		unwind()
		return nil, err
	}
	cleanups = append(cleanups, func() { _ = cacheClient.Close() })

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

	rlsExporter := service.NewRLSViolationExporter(service.RLSViolationExporterDeps{
		Log:       postgres.NewRLSViolationRepo(relayPool),
		Record:    observability.AddRLSViolations,
		Interval:  cfg.RLSViolationExportInterval,
		Retention: cfg.RLSViolationRetention,
		Logger:    log,
	})
	statsSampler := service.NewOperationalGaugeSampler(service.OperationalGaugeSamplerDeps{
		Stats: postgres.NewOperationalStatsRepo(relayPool),
		Record: func(s domain.OperationalStats) {
			observability.RecordOperationalStats(s.DegradedInstances, s.OldestDegradedAgeSeconds, s.OldestReadyTaskAgeSeconds)
		},
		Interval: cfg.OperationalStatsInterval,
		Logger:   log,
	})

	retentionSweeper := service.NewRetentionSweeper(service.RetentionSweeperDeps{
		Outbox:                  relay,
		ProcessedEvents:         postgres.NewProcessedEventRepo(appPool),
		OutboxRetention:         cfg.OutboxRetention,
		ProcessedEventRetention: cfg.ProcessedEventRetention,
		Batch:                   cfg.RetentionSweepBatch,
		Interval:                cfg.RetentionSweepInterval,
		Record:                  func(table string, deleted int64) { observability.AddRetentionRowsDeleted(table, deleted) },
		Logger:                  log,
	})

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

	definitions := outboundhttp.NewDefinitionClient(cfg.DefinitionServiceURL, cfg.DefinitionServiceToken, cfg.DefinitionClientTimeout)

	eligibility := outboundhttp.NewEligibilityClient(cfg.EligibilityBaseURL, cfg.EligibilityClientTimeout)
	iam := outboundhttp.NewIAMClient(cfg.IAMBaseURL, cfg.IAMClientTimeout)
	principals := outboundhttp.NewAutomationPrincipalClient(cfg.TokenServiceBaseURL, cfg.TokenServiceClientTimeout, automationPrincipalCacheTTL)

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
		Queues: queues, Validator: validator, Cache: cache, Log: log,
	}
	taskService := &service.TaskService{
		Instances: instances, Tasks: tasks, Assignments: assignments, Overrides: overrides,
		Temporal: temporal, IAM: iam, Eligibility: eligibility, Definitions: definitions, Log: log,
	}
	connectorTaskService := &service.ConnectorTaskService{
		Instances: instances, Tasks: tasks, Assignments: assignments,
		Temporal: temporal, Cache: cache, Principals: principals, Log: log,
	}
	workflowClient := &service.WorkflowClient{
		Instances: instances, Tasks: tasks, Assignments: assignments, Temporal: temporal,
		IAM: iam, Eligibility: eligibility, Definitions: definitions, Log: log,
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
		ConnectorTasks:       connectorTaskService,
		ConnectorEvents:      connectorEvents,
		AutomationPrincipals: principals,
		ArchiveGuard:         guard, UserTaskPauser: pauser,
		Log: log,
	})

	router := httpadapter.NewRouter(httpadapter.RouterConfig{
		GinConfig:        ginCfg,
		AppEnv:           cfg.AppEnv,
		InternalAPIToken: cfg.InternalAPIToken,
		Handler:          h,
		DB:               dbPinger{pool: appPool},
		Cache:            cache,
		Temporal:         temporalPinger{sdk: sdkClient},
	})
	httpServer := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.HTTPPort),
		Handler:           router.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", gincommon.MetricsHandler())
	metricsServer := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.MetricsPort),
		Handler:           metricsMux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	return &app{
		cfg:              cfg,
		log:              log,
		appPool:          appPool,
		relayPool:        relayPool,
		cache:            cacheClient,
		httpServer:       httpServer,
		metricsServer:    metricsServer,
		outboxRelay:      relay,
		rlsExporter:      rlsExporter,
		statsSampler:     statsSampler,
		retentionSweeper: retentionSweeper,
		sdkClient:        sdkClient,
		tracingShutdown:  tracingShutdown,
	}, nil
}
