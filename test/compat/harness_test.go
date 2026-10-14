//go:build compat

// Package compat_test runs the Definition Service's real binary against this
// service's real router, interpreter, activities, Postgres, Valkey and
// Temporal, and drives both public APIs over HTTP. Every other tier on either
// side tests against its own copy of the other's contract; this one proves the
// two builds work together.
package compat_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	sdkworkflow "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	httpadapter "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	outboundhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	temporaladapter "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporalclient"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/valkey"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

const (
	definitionToken = "compat-definition-token"
	executionToken  = "compat-execution-token"

	// The Definition compiler stamps every plan with this queue, and it is
	// Execution's TEMPORAL_TASK_QUEUE default, so the worker polls it here.
	defaultTaskQueue = "wf-queue-default"

	aliasHost = "partner.example.com"

	pollTimeout  = 90 * time.Second
	pollInterval = 250 * time.Millisecond
)

type env struct {
	definitionDir string
	definitionBin string

	definitionURL string
	executionURL  string

	temporal      client.Client
	definitionDB  definitionDB
	valkeyAddr    string
	membershipURL string
}

type definitionDB struct {
	appDSN, migratorDSN string
}

type eligibleForAll struct{}

func (eligibleForAll) CheckEligibility(context.Context, uuidT, uuidT, string, uuidT) (bool, error) {
	return true, nil
}

func (eligibleForAll) CheckEligibilityBatch(_ context.Context, reqs []port.EligibilityCheckRequest, _ uuidT) ([]port.EligibilityResult, error) {
	out := make([]port.EligibilityResult, len(reqs))
	for i := range reqs {
		out[i] = port.EligibilityResult{Eligible: true}
	}
	return out, nil
}

type alwaysHealthy struct{}

func (alwaysHealthy) Ping(context.Context) error { return nil }
func (alwaysHealthy) Health(context.Context) httpadapter.DBHealth {
	return httpadapter.DBHealth{Healthy: true}
}

func definitionServiceDir(t *testing.T) string {
	t.Helper()
	dir := os.Getenv("DEFINITION_SERVICE_DIR")
	if dir == "" {
		dir = filepath.Join("..", "..", "..", "definition_service")
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		t.Fatalf("resolve DEFINITION_SERVICE_DIR: %v", err)
	}
	if _, err := os.Stat(filepath.Join(abs, "cmd", "server")); err != nil {
		t.Fatalf("DEFINITION_SERVICE_DIR=%s is not a Definition Service checkout: %v", abs, err)
	}
	return abs
}

func goBuild(t *testing.T, dir, pkg, out string) {
	t.Helper()
	cmd := exec.Command("go", "build", "-o", out, pkg)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOWORK=off", "CGO_ENABLED=0")
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build %s in %s: %v\n%s", pkg, dir, err, b)
	}
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port
}

func startDefinitionPostgres(t *testing.T, definitionDir string) definitionDB {
	t.Helper()
	ctx := context.Background()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:18-alpine",
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"POSTGRES_USER":     "postgres",
				"POSTGRES_PASSWORD": "postgres",
				"POSTGRES_DB":       "workflow_definition",
			},
			Files: []testcontainers.ContainerFile{{
				HostFilePath:      filepath.Join(definitionDir, "scripts", "init-db.sql"),
				ContainerFilePath: "/docker-entrypoint-initdb.d/init-db.sql",
				FileMode:          0o644,
			}},
			WaitingFor: wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(120 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("start Definition Postgres: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(context.Background()) })
	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("Definition Postgres host: %v", err)
	}
	port, err := c.MappedPort(ctx, "5432")
	if err != nil {
		t.Fatalf("Definition Postgres port: %v", err)
	}
	dsn := func(role string) string {
		return fmt.Sprintf("postgres://%s:devpassword@%s:%s/workflow_definition?sslmode=disable", role, host, port.Port())
	}
	return definitionDB{appDSN: dsn("workflow_definition_app"), migratorDSN: dsn("workflow_definition_migrator")}
}

func (e *env) definitionEnv(t *testing.T, httpPort int, executionToken string) []string {
	t.Helper()
	return []string{
		"PATH=" + os.Getenv("PATH"),
		"HOME=" + os.Getenv("HOME"),
		"APP_ENV=dev",
		"HTTP_PORT=" + strconv.Itoa(httpPort),
		"METRICS_PORT=" + strconv.Itoa(freePort(t)),
		"DATABASE_URL=" + e.definitionDB.appDSN,
		"MIGRATION_DATABASE_URL=" + e.definitionDB.migratorDSN,
		"SYSTEM_DATABASE_URL=" + e.definitionDB.migratorDSN,
		"VALKEY_ADDR=" + e.valkeyAddr,
		"INTERNAL_API_TOKEN=" + definitionToken,
		"ORG_MEMBERSHIP_BASE_URL=" + e.membershipURL,
		"EXECUTION_SERVICE_URL=" + e.executionURL,
		"EXECUTION_SERVICE_TOKEN=" + executionToken,
		"CONNECTOR_ALIAS_ALLOWED_HOSTS=" + aliasHost,
		"SHUTDOWN_DRAIN_DELAY=0s",
		"OTEL_SDK_DISABLED=true",
	}
}

func (e *env) migrateDefinition(t *testing.T) {
	t.Helper()
	cmd := exec.Command(e.definitionBin, "migrate")
	cmd.Dir = t.TempDir()
	cmd.Env = e.definitionEnv(t, freePort(t), executionToken)
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("Definition migrate: %v\n%s", err, b)
	}
}

// startDefinition runs one Definition server process and waits for /readyz.
// Its working directory is empty so a developer's .env is never read.
func (e *env) startDefinition(t *testing.T, httpPort int, executionToken string) string {
	t.Helper()
	var logs bytes.Buffer
	cmd := exec.Command(e.definitionBin)
	cmd.Dir = t.TempDir()
	cmd.Env = e.definitionEnv(t, httpPort, executionToken)
	cmd.Stdout = &logs
	cmd.Stderr = &logs
	if err := cmd.Start(); err != nil {
		t.Fatalf("start Definition: %v", err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		<-exited
		if t.Failed() {
			t.Logf("Definition (port %d) output:\n%s", httpPort, logs.String())
		}
	})

	base := "http://127.0.0.1:" + strconv.Itoa(httpPort)
	deadline := time.Now().Add(60 * time.Second)
	for {
		resp, err := http.Get(base + "/readyz")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return base
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("Definition never became ready on %s:\n%s", base, logs.String())
		}
		select {
		case err := <-exited:
			exited <- err
			t.Fatalf("Definition exited before it was ready (%v):\n%s", err, logs.String())
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func (e *env) startMembershipStub(t *testing.T) {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "membership-stub")
	goBuild(t, e.definitionDir, "./cmd/stub/membership", bin)
	port := freePort(t)
	cmd := exec.Command(bin)
	cmd.Env = []string{"STUB_ADDR=127.0.0.1:" + strconv.Itoa(port)}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start membership stub: %v", err)
	}
	t.Cleanup(func() { _ = cmd.Process.Kill(); _ = cmd.Wait() })
	e.membershipURL = "http://127.0.0.1:" + strconv.Itoa(port)
	waitTCP(t, "127.0.0.1:"+strconv.Itoa(port))
}

func waitTCP(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		c, err := net.DialTimeout("tcp", addr, time.Second)
		if err == nil {
			_ = c.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s never accepted connections: %v", addr, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func startWorker(t *testing.T, sdk client.Client, deps *temporaladapter.Deps) {
	t.Helper()
	w := worker.New(sdk, defaultTaskQueue, worker.Options{})
	w.RegisterWorkflowWithOptions(wfengine.Execute, sdkworkflow.RegisterOptions{Name: port.WorkflowTypeExecute})
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
	if err := w.Start(); err != nil {
		t.Fatalf("start worker: %v", err)
	}
	t.Cleanup(w.Stop)
}

// buildExecution wires the API the way cmd/server/wire.go does, with the
// real Definition client and Valkey cache; only eligibility is faked, since
// the IAM route it calls is not built yet.
func buildExecution(t *testing.T, pool *pgcommon.Pool, sdk client.Client, cache port.CacheStore, definitions port.DefinitionServiceClient) http.Handler {
	instances := postgres.NewInstanceRepo(pool)
	tasks := postgres.NewTaskRepo(pool)
	assignments := postgres.NewTaskAssignmentRepo(pool)
	outboxRepo := postgres.NewOutboxRepo(pool)
	overrides := postgres.NewAssigneeOverrideRepo(pool)
	processedEvents := postgres.NewProcessedEventRepo(pool)
	recency := postgres.NewRecencyGuardRepo(pool)
	queues := postgres.NewActiveTaskQueueRepo(pool)
	transactor := postgres.NewTransactor(pool)
	validator, err := eventbus.NewSchemaValidator()
	if err != nil {
		t.Fatalf("schema validator: %v", err)
	}
	temporal := temporalclient.New(sdk)
	eligibility := eligibleForAll{}

	h := handler.New(handler.Services{
		Instances: &service.InstanceService{
			Instances: instances, Tasks: tasks, Assignments: assignments, Outbox: outboxRepo,
			Transactor: transactor, Temporal: temporal, Definitions: definitions, Eligibility: eligibility,
			Validator: validator, Cache: cache,
		},
		Tasks: &service.TaskService{
			Instances: instances, Tasks: tasks, Assignments: assignments, Overrides: overrides,
			Temporal: temporal, Eligibility: eligibility, Definitions: definitions,
		},
		Eligibility: eligibility, Cache: cache, ProcessedEvents: processedEvents, Recency: recency,
		Delegation: &service.DelegationReconciler{
			Instances: instances, Tasks: tasks, Assignments: assignments, Outbox: outboxRepo,
			Transactor: transactor, Temporal: temporal, Definitions: definitions, Eligibility: eligibility,
			Validator: validator,
		},
		TenantLifecycle: &service.TenantLifecycleReconciler{
			Instances: instances, Tasks: tasks, Assignments: assignments, Queues: queues, Outbox: outboxRepo,
			Transactor: transactor, Temporal: temporal, Validator: validator,
		},
		UserSafetyNet:   &service.UserSafetyNetReconciler{Assignments: assignments},
		OOOAvailability: &service.OOOAvailabilityReconciler{Instances: instances, Assignments: assignments, Tasks: tasks, Temporal: temporal},
		ArchiveGuard:    &service.ArchiveGuard{Instances: instances},
		UserTaskPauser:  &service.UserTaskPauser{Instances: instances, Assignments: assignments, Tasks: tasks, Temporal: temporal},
	})
	return httpadapter.NewRouter(httpadapter.RouterConfig{
		GinConfig:        gincommon.Config{ServiceName: "workflow-execution-service", BuildVersion: "compat", Domain: "workflow", Environment: "test"},
		AppEnv:           "test",
		InternalAPIToken: executionToken,
		Handler:          h,
		DB:               alwaysHealthy{}, Cache: alwaysHealthy{}, Temporal: alwaysHealthy{},
	}).Handler()
}

// setup starts both services. Definition's port is chosen first so that
// Execution's client can name it before either process serves.
func setup(t *testing.T) *env {
	t.Helper()
	if testing.Short() {
		t.Skip("compat needs Docker and a Definition Service checkout")
	}
	e := &env{definitionDir: definitionServiceDir(t)}
	e.definitionBin = filepath.Join(t.TempDir(), "definition-server")
	goBuild(t, e.definitionDir, "./cmd/server", e.definitionBin)

	pool := fixtures.NewTestPool(t)
	sdk := fixtures.NewTestTemporalServer(t)
	e.temporal = sdk
	e.valkeyAddr = fixtures.NewTestValkey(t)
	e.definitionDB = startDefinitionPostgres(t, e.definitionDir)
	e.startMembershipStub(t)
	e.migrateDefinition(t)
	definitionPort := freePort(t)

	rdb := redis.NewClient(&redis.Options{Addr: e.valkeyAddr})
	t.Cleanup(func() { _ = rdb.Close() })
	cache := valkey.NewCache(rdb)

	e.definitionURL = "http://127.0.0.1:" + strconv.Itoa(definitionPort)
	definitions := outboundhttp.NewDefinitionClient(e.definitionURL, definitionToken, 5*time.Second)

	validator, err := eventbus.NewSchemaValidator()
	if err != nil {
		t.Fatalf("schema validator: %v", err)
	}
	startWorker(t, sdk, &temporaladapter.Deps{
		Instances: postgres.NewInstanceRepo(pool), Tasks: postgres.NewTaskRepo(pool), Assignments: postgres.NewTaskAssignmentRepo(pool),
		Outbox: postgres.NewOutboxRepo(pool), Transactor: postgres.NewTransactor(pool), Validator: validator, Definitions: definitions,
	})

	srv := httptest.NewServer(buildExecution(t, pool, sdk, cache, definitions))
	t.Cleanup(srv.Close)
	e.executionURL = srv.URL

	e.startDefinition(t, definitionPort, executionToken)
	return e
}
