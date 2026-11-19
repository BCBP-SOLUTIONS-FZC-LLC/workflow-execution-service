package main

import (
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"go.temporal.io/sdk/client"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/outbox"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/logger"

	httpadapter "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	outboundhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/signalrelay"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporalclient"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/valkeystream"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
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

	inboxPool, err := newInboxPool(ctx, cfg, log)
	if err != nil {
		unwind()
		return nil, err
	}
	cleanups = append(cleanups, inboxPool.Close)

	processedEvents, err := eventbus.NewInbox(inboxPool, port.EventConsumers...)
	if err != nil {
		unwind()
		return nil, err
	}

	relayPool, err := newRelayPool(ctx, cfg, log)
	if err != nil {
		unwind()
		return nil, err
	}
	cleanups = append(cleanups, relayPool.Close)
	if err := checkRelayRole(ctx, roleBypassesRLS(relayPool), cfg, log); err != nil {
		unwind()
		return nil, err
	}

	lockPool, err := newLockPool(ctx, cfg, log)
	if err != nil {
		unwind()
		return nil, err
	}
	cleanups = append(cleanups, lockPool.Close)

	cache, cacheClient := newCacheStore(ctx, cfg, log)
	cleanups = append(cleanups, func() { _ = cacheClient.Close() })

	connectorEvents := valkeystream.NewEventPublisher(valkeystream.NewProducer(cacheClient), cfg.ConnectorStreamKey)

	glueCodec, err := newGlueCodec(ctx, cfg, log)
	if err != nil {
		unwind()
		return nil, err
	}

	publisher, err := newPublisher(cfg, log, glueCodec)
	if err != nil {
		unwind()
		return nil, err
	}

	dialOptions, err := temporalDialOptions(cfg)
	if err != nil {
		unwind()
		return nil, err
	}
	sdkClient, err := client.Dial(dialOptions)
	if err != nil {
		unwind()
		return nil, fmt.Errorf("dial temporal: %w", err)
	}
	cleanups = append(cleanups, sdkClient.Close)
	temporal := temporalclient.New(sdkClient)

	relay, err := outbox.NewRunner(outbox.Config{
		Pool: relayPool,
		// Signal records go to Temporal, every other record to the event bus.
		Publisher:    signalrelay.New(publisher, temporal, log),
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
		Leader:    postgres.NewRecencyGuardRepo(appPool, lockPool),
		Interval:  cfg.RLSViolationExportInterval,
		Retention: cfg.RLSViolationRetention,
		Logger:    log,
	})
	statsSampler := service.NewOperationalGaugeSampler(service.OperationalGaugeSamplerDeps{
		Stats: postgres.NewOperationalStatsRepo(relayPool),
		Record: func(s domain.OperationalStats) {
			observability.RecordOperationalStats(s.DegradedInstances, s.OldestDegradedAgeSeconds, s.OldestReadyTaskAgeSeconds, s.InstancesByQueue)
			observability.SetGauge(observability.OldestReadyConnectorTaskAgeSeconds, s.OldestReadyConnectorTaskAgeSeconds)
		},
		Interval: cfg.OperationalStatsInterval,
		Logger:   log,
	})

	retentionSweeper := service.NewRetentionSweeper(service.RetentionSweeperDeps{
		Outbox:                  relay,
		ProcessedEvents:         processedEvents,
		OutboxRetention:         cfg.OutboxRetention,
		ProcessedEventRetention: cfg.ProcessedEventRetention,
		Batch:                   cfg.RetentionSweepBatch,
		Interval:                cfg.RetentionSweepInterval,
		Record:                  func(table string, deleted int64) { observability.AddRetentionRowsDeleted(table, deleted) },
		Logger:                  log,
	})

	var tenantPurger *service.TenantPurger
	if cfg.TenantPurgeEnabled {
		purgeRepo := postgres.NewTenantRetentionRepo(appPool)
		tenantPurger = service.NewTenantPurger(service.TenantPurgerDeps{
			Retention: purgeRepo,
			Data:      purgeRepo,
			Batch:     cfg.RetentionSweepBatch,
			Interval:  cfg.RetentionSweepInterval,
			Record:    func(table string, deleted int64) { observability.AddRetentionRowsDeleted(table, deleted) },
			Failed:    func() { observability.IncCounter(observability.TenantPurgeFailuresTotal) },
			Logger:    log,
		})
	}

	validator, err := eventbus.NewSchemaValidator()
	if err != nil {
		unwind()
		return nil, fmt.Errorf("new schema validator: %w", err)
	}

	definitions := outboundhttp.NewDefinitionClient(cfg.DefinitionServiceURL, cfg.DefinitionClientTimeout)

	eligibility := outboundhttp.NewEligibilityClient(cfg.EligibilityBaseURL, cfg.EligibilityClientTimeout).WithLogger(log)
	principals := outboundhttp.NewAutomationPrincipalClient(cfg.TokenServiceBaseURL, cfg.TokenServiceClientTimeout, automationPrincipalCacheTTL)

	instances := postgres.NewInstanceRepo(appPool)
	tasks := postgres.NewTaskRepo(appPool)
	assignments := postgres.NewTaskAssignmentRepo(appPool)
	outboxRepo := postgres.NewOutboxRepo(appPool)
	recency := postgres.NewRecencyGuardRepo(appPool, lockPool)
	overrides := postgres.NewAssigneeOverrideRepo(appPool)
	queues := postgres.NewActiveTaskQueueRepo(appPool)
	tenantStates := postgres.NewTenantStateRepo(appPool)
	transactor := postgres.NewTransactor(appPool)

	instanceService, err := newInstanceService(cfg, log, service.InstanceService{
		Instances: instances, Tasks: tasks, Assignments: assignments, Outbox: outboxRepo,
		Transactor: transactor, Temporal: temporal, Definitions: definitions, Eligibility: eligibility,
		Queues: queues, Gate: service.NewTenantStartGate(tenantStates, cfg.StartRefusedStatuses),
		Validator: validator, Cache: cache, Log: log,
	})
	if err != nil {
		unwind()
		return nil, err
	}
	taskService := &service.TaskService{
		Instances: instances, Tasks: tasks, Assignments: assignments, Overrides: overrides, Transactor: transactor, Outbox: outboxRepo, Signals: outboxRepo,
		Validator: validator, Temporal: temporal, Eligibility: eligibility, Definitions: definitions, Cache: cache, Log: log,
	}
	connectorTaskService := &service.ConnectorTaskService{
		Instances: instances, Tasks: tasks, Assignments: assignments,
		Temporal: temporal, Cache: cache, Principals: principals, Log: log,
	}
	workflowClient := &service.WorkflowClient{
		Instances: instances, Tasks: tasks, Assignments: assignments, Transactor: transactor, Outbox: outboxRepo, Signals: outboxRepo,
		Validator: validator, Eligibility: eligibility, Definitions: definitions, Ended: postgres.NewEndedDelegationRepo(appPool), Locks: recency, Log: log,
	}
	guard := &service.ArchiveGuard{Instances: instances}
	pauser := &service.UserTaskPauser{
		Instances: instances, Assignments: assignments, Tasks: tasks, Temporal: temporal, Log: log,
	}
	availability := postgres.NewUserAvailabilityRepo(appPool)
	delegationReconciler := &service.DelegationReconciler{
		Instances: instances, Tasks: tasks, Assignments: assignments, Outbox: outboxRepo, Signals: outboxRepo,
		Transactor: transactor, Temporal: temporal, Definitions: definitions, Eligibility: eligibility,
		Validator: validator, Availability: availability, Locks: recency, Ended: postgres.NewEndedDelegationRepo(appPool), Log: log,
	}
	tenantLifecycleReconciler := &service.TenantLifecycleReconciler{
		Instances: instances, Tasks: tasks, Assignments: assignments, Queues: queues, Outbox: outboxRepo,
		Transactor: transactor, Temporal: temporal, Validator: validator, Log: log,
		States: tenantStates, Retention: postgres.NewTenantRetentionRepo(appPool), HistoryRetention: cfg.TenantHistoryRetention,
	}
	userSafetyNetReconciler := &service.UserSafetyNetReconciler{Assignments: assignments}
	oooAvailabilityReconciler := &service.OOOAvailabilityReconciler{
		Instances: instances, Assignments: assignments, Tasks: tasks, Temporal: temporal, Availability: availability, Log: log,
	}

	h := handler.New(handler.Services{
		Tasks: taskService, Instances: instanceService, Eligibility: eligibility, WorkflowClient: workflowClient,
		Cache: cache, IdempotencyTTL: cfg.IdempotencyTTL, ProcessedEvents: processedEvents, Recency: recency,
		Delegation: delegationReconciler, TenantLifecycle: tenantLifecycleReconciler,
		UserSafetyNet: userSafetyNetReconciler, OOOAvailability: oooAvailabilityReconciler,
		ConnectorTasks:       connectorTaskService,
		ConnectorEvents:      connectorEvents,
		ConnectorDispatch:    &service.ConnectorDispatchGate{Instances: instances, Tasks: tasks},
		AutomationPrincipals: principals,
		ArchiveGuard:         guard, UserTaskPauser: pauser,
		Log: log,
	})

	shuttingDown := &atomic.Bool{}
	router := httpadapter.NewRouter(httpadapter.RouterConfig{
		GinConfig: ginCfg,
		AppEnv:    cfg.AppEnv,
		Handler:   h,
		DB:        dbPinger{pool: appPool},
		Cache:     cache,
		Temporal:  temporalPinger{sdk: sdkClient},

		ShuttingDown: shuttingDown.Load,
	})
	httpServer := newHTTPServer(fmt.Sprintf(":%d", cfg.HTTPPort), router.Handler())

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
		inboxPool:        inboxPool,
		relayPool:        relayPool,
		lockPool:         lockPool,
		cache:            cacheClient,
		httpServer:       httpServer,
		metricsServer:    metricsServer,
		outboxRelay:      relay,
		rlsExporter:      rlsExporter,
		statsSampler:     statsSampler,
		retentionSweeper: retentionSweeper,
		tenantPurger:     tenantPurger,
		sdkClient:        sdkClient,
		tracingShutdown:  tracingShutdown,
		shuttingDown:     shuttingDown,
		sleep:            time.Sleep,
	}, nil
}

const writeTimeoutMargin = 5 * time.Second

func newHTTPServer(addr string, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      httpadapter.RequestTimeout + writeTimeoutMargin,
		IdleTimeout:       60 * time.Second,
	}
}

func temporalDialOptions(cfg *config.Config) (client.Options, error) {
	interceptors, err := observability.TracingClientInterceptors(cfg.TraceExportEnabled())
	if err != nil {
		return client.Options{}, err
	}
	return temporalclient.DialOptions(cfg.TemporalHostPort, cfg.TemporalNamespace, cfg.TemporalTLSCertPath, cfg.TemporalTLSKeyPath, interceptors)
}
