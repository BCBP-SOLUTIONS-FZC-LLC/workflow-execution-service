package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	outboundgrpc "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/grpc"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	outboundtemporal "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintf(os.Stderr, "config error: %v\n", err)
		os.Exit(1)
	}
	if cfg.DefinitionServiceAddr == "" {
		fmt.Fprintln(os.Stderr, "DEFINITION_SERVICE_ADDR is required for cmd/worker")
		os.Exit(1)
	}

	deps, cleanup, err := buildDeps(cfg)
	if err != nil {
		log.Fatalf("cmd/worker: %v", err)
	}
	defer cleanup()

	c, err := client.Dial(client.Options{HostPort: cfg.TemporalHostPort, Namespace: cfg.TemporalNamespace})
	if err != nil {
		log.Fatalf("cmd/worker: dial temporal: %v", err)
	}
	defer c.Close()

	w := worker.New(c, cfg.TemporalTaskQueue, worker.Options{})
	w.RegisterWorkflow(wfengine.Execute)
	registerActivities(w, deps)

	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalf("cmd/worker: worker run: %v", err)
	}
}

// buildDeps is cmd/worker's composition root (LLD §1.7): the only place this
// binary wires concrete adapters to internal/adapter/outbound/temporal's
// Deps struct. cleanup closes the Postgres pool and the Definition Service
// gRPC connection.
func buildDeps(cfg *config.Config) (*outboundtemporal.Deps, func(), error) {
	pool, err := pgcommon.NewPool(context.Background(), pgcommon.Config{
		DSN: cfg.DatabaseURL, MaxConns: cfg.PGMaxConns, MinConns: cfg.PGMinConns, PGBouncerMode: cfg.PGBouncerMode,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("new postgres pool: %w", err)
	}

	validator, err := eventbus.NewSchemaValidator()
	if err != nil {
		pool.Close()
		return nil, nil, fmt.Errorf("new schema validator: %w", err)
	}

	definitions, err := outboundgrpc.NewDefinitionClient(cfg.DefinitionServiceAddr, cfg.DefinitionClientTimeout)
	if err != nil {
		pool.Close()
		return nil, nil, fmt.Errorf("new definition client: %w", err)
	}

	deps := &outboundtemporal.Deps{
		Instances:   postgres.NewInstanceRepo(pool),
		Tasks:       postgres.NewTaskRepo(pool),
		Assignments: postgres.NewTaskAssignmentRepo(pool),
		Outbox:      postgres.NewOutboxRepo(pool),
		Transactor:  postgres.NewTransactor(pool),
		Validator:   validator,
		Definitions: definitions,
	}
	cleanup := func() {
		_ = definitions.Close()
		pool.Close()
	}
	return deps, cleanup, nil
}

// registerActivities registers every Activity constant in
// internal/core/port/activities.go against its implementation in deps —
// the two must stay in lockstep, checked by test/workflow's fake-activity
// registration mirroring the same name list.
func registerActivities(w worker.Worker, deps *outboundtemporal.Deps) {
	w.RegisterActivityWithOptions(deps.GetCompiledPlan, activity.RegisterOptions{Name: port.ActivityGetCompiledPlan})
	w.RegisterActivityWithOptions(deps.CreateTask, activity.RegisterOptions{Name: port.ActivityCreateTask})
	w.RegisterActivityWithOptions(deps.UpdateInstanceNodes, activity.RegisterOptions{Name: port.ActivityUpdateInstanceNodes})
	w.RegisterActivityWithOptions(deps.ClaimAssignment, activity.RegisterOptions{Name: port.ActivityClaimAssignment})
	w.RegisterActivityWithOptions(deps.CompleteAssignment, activity.RegisterOptions{Name: port.ActivityCompleteAssignment})
	w.RegisterActivityWithOptions(deps.DeferTask, activity.RegisterOptions{Name: port.ActivityDeferTask})
	w.RegisterActivityWithOptions(deps.UpdateInstanceStatus, activity.RegisterOptions{Name: port.ActivityUpdateInstanceStatus})
	w.RegisterActivityWithOptions(deps.RecordForceRoute, activity.RegisterOptions{Name: port.ActivityRecordForceRoute})
	w.RegisterActivityWithOptions(deps.RecordSLAWarning, activity.RegisterOptions{Name: port.ActivityRecordSLAWarning})
	w.RegisterActivityWithOptions(deps.RecordSLABreach, activity.RegisterOptions{Name: port.ActivityRecordSLABreach})
	w.RegisterActivityWithOptions(deps.PauseInstance, activity.RegisterOptions{Name: port.ActivityPauseInstance})
	w.RegisterActivityWithOptions(deps.ResumeInstance, activity.RegisterOptions{Name: port.ActivityResumeInstance})
	w.RegisterActivityWithOptions(deps.CancelInstance, activity.RegisterOptions{Name: port.ActivityCancelInstance})
	w.RegisterActivityWithOptions(deps.ReassignAssignment, activity.RegisterOptions{Name: port.ActivityReassignAssignment})
	w.RegisterActivityWithOptions(deps.UpdateTaskStatus, activity.RegisterOptions{Name: port.ActivityUpdateTaskStatus})
}
