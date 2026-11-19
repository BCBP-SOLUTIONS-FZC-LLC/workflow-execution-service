//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

var errW18aRollbackAfterBothWrites = errors.New("w18a rollback after both writes")

type w18aPlanRead struct{ tenantID, versionID uuid.UUID }

type w18aDefinitions struct {
	resp  *port.CompiledWorkflow
	reads []w18aPlanRead
}

func (d *w18aDefinitions) GetCompiledWorkflow(_ context.Context, tenantID, versionID uuid.UUID) (*port.CompiledWorkflow, error) {
	d.reads = append(d.reads, w18aPlanRead{tenantID, versionID})
	return d.resp, nil
}

type w18aSignal struct{ workflowID, name string }

type w18aTemporal struct {
	port.TemporalClient
	waiting []string
	queries []string
	signals []w18aSignal
	starts  []port.StartWorkflowInput
}

func (c *w18aTemporal) QueryWorkflow(_ context.Context, workflowID, _ string, out any) error {
	c.queries = append(c.queries, workflowID)
	raw, err := json.Marshal(c.waiting)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, out)
}

func (c *w18aTemporal) SignalWorkflow(_ context.Context, workflowID string, _ uuid.UUID, name string, _ any) error {
	c.signals = append(c.signals, w18aSignal{workflowID, name})
	return nil
}

func (c *w18aTemporal) StartWorkflow(_ context.Context, in port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
	c.starts = append(c.starts, in)
	return port.StartWorkflowOutput{TemporalWorkflowID: in.TemporalWorkflowID, TemporalRunID: "run-1"}, nil
}

// w18aRollbackTransactor runs the callback in a real transaction, then fails, so the transaction rolls
// back after every write inside it has happened.
type w18aRollbackTransactor struct{ inner port.Transactor }

func (t w18aRollbackTransactor) RunInTx(ctx context.Context, fn func(context.Context) error) error {
	return t.inner.RunInTx(ctx, func(ctx context.Context) error {
		if err := fn(ctx); err != nil {
			return err
		}
		return errW18aRollbackAfterBothWrites
	})
}

func (t w18aRollbackTransactor) RunInTxWithRetry(ctx context.Context, fn func(context.Context) error) error {
	return t.RunInTx(ctx, fn)
}

func w18aSinglePlan(t *testing.T) string {
	t.Helper()
	raw, err := json.Marshal(dsl.CompiledCollaboration{
		SchemaVersion: dsl.CurrentSchemaVersion,
		MainPlan:      "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main", TaskQueue: "wf-queue-default",
			Departments: []dsl.DepartmentDef{{ID: "finance", Label: "Finance", Stages: []dsl.StageDef{
				{Type: "receive_task", NodeID: "Receive_Bid", Extras: map[string]string{"message": "bid-received"}},
			}}},
		}},
	})
	require.NoError(t, err)
	return string(raw)
}

func TestInstanceService_AnswerOutsideWait_InstanceOfAnotherTenant_IsNotFoundAndNothingIsAsked(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	ctx := context.Background()
	const key = "finance/Receive_Bid"

	type world struct {
		svc         *service.InstanceService
		temporal    *w18aTemporal
		definitions *w18aDefinitions
		owner       uuid.UUID
		instance    *domain.Instance
	}
	seed := func(t *testing.T) world {
		t.Helper()
		owner := uuid.New()
		inst := newInstance(owner, time.Now().UTC())
		require.NoError(t, instanceRepo.Create(ctx, inst))
		temporal := &w18aTemporal{waiting: []string{key}}
		definitions := &w18aDefinitions{resp: &port.CompiledWorkflow{
			WorkflowID: inst.WorkflowID, VersionID: inst.WorkflowVersionID, Status: "PUBLISHED", IsValid: true, CompiledPlanJSON: w18aSinglePlan(t),
		}}
		svc := &service.InstanceService{
			Instances:   instanceRepo,
			Tasks:       postgres.NewTaskRepo(appPool),
			Assignments: postgres.NewTaskAssignmentRepo(appPool),
			Temporal:    temporal,
			Definitions: definitions,
		}
		return world{svc: svc, temporal: temporal, definitions: definitions, owner: owner, instance: inst}
	}

	cases := []struct {
		name  string
		scope port.ReadScope
	}{
		{"an admin", port.ReadScope{CallerUserID: uuid.New(), IsAdmin: true}},
		{"a non-admin caller", port.ReadScope{CallerUserID: uuid.New()}},
	}
	for _, tc := range cases {
		t.Run(tc.name+" of another tenant gets instance not found", func(t *testing.T) {
			w := seed(t)
			otherTenant := uuid.New()

			err := w.svc.AnswerOutsideWait(ctx, otherTenant, w.instance.ID, tc.scope, key, nil)

			require.ErrorIs(t, err, port.ErrInstanceNotFound)
			assert.Empty(t, w.definitions.reads, "the other tenant's plan is not read")
			assert.Empty(t, w.temporal.queries, "the other tenant's workflow is not asked")
			assert.Empty(t, w.temporal.signals, "the other tenant's workflow is not signalled")
		})
	}

	t.Run("control: the owning tenant's admin reaches the same instance", func(t *testing.T) {
		w := seed(t)

		err := w.svc.AnswerOutsideWait(ctx, w.owner, w.instance.ID, port.ReadScope{CallerUserID: uuid.New(), IsAdmin: true}, key, nil)

		require.NoError(t, err)
		assert.Equal(t, []w18aPlanRead{{w.owner, w.instance.WorkflowVersionID}}, w.definitions.reads)
		assert.Equal(t, []string{w.instance.TemporalWorkflowID}, w.temporal.queries, "the workflow asked is the instance's own")
		assert.Equal(t, []w18aSignal{{w.instance.TemporalWorkflowID, port.SignalInstanceExternalAnswer}}, w.temporal.signals)
	})
}

func TestInstanceService_List_ReturnsOnlyTheCallersTenantsInstances(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	svc := &service.InstanceService{Instances: instanceRepo}
	ctx := context.Background()

	tenantA, tenantB := uuid.New(), uuid.New()
	startedAt := time.Now().UTC().Add(-time.Hour)
	seedFor := func(tenant uuid.UUID, n int) []uuid.UUID {
		ids := make([]uuid.UUID, n)
		for i := range ids {
			inst := newInstance(tenant, startedAt)
			require.NoError(t, instanceRepo.Create(ctx, inst))
			ids[i] = inst.ID
		}
		return ids
	}
	idsA, idsB := seedFor(tenantA, 3), seedFor(tenantB, 2)
	admin := port.ReadScope{CallerUserID: uuid.New(), IsAdmin: true}

	listed := func(t *testing.T, tenant uuid.UUID) ([]uuid.UUID, []uuid.UUID) {
		t.Helper()
		page, err := svc.List(ctx, tenant, admin, port.InstanceFilter{}, port.Page{Limit: 50})
		require.NoError(t, err)
		ids, tenants := make([]uuid.UUID, len(page.Items)), make([]uuid.UUID, len(page.Items))
		for i, item := range page.Items {
			ids[i], tenants[i] = item.ID, item.TenantID
		}
		return ids, tenants
	}

	t.Run("tenant A sees its own instances only", func(t *testing.T) {
		ids, tenants := listed(t, tenantA)
		assert.ElementsMatch(t, idsA, ids)
		for _, got := range tenants {
			assert.Equal(t, tenantA, got)
		}
	})

	t.Run("tenant B sees its own instances only", func(t *testing.T) {
		ids, tenants := listed(t, tenantB)
		assert.ElementsMatch(t, idsB, ids)
		for _, got := range tenants {
			assert.Equal(t, tenantB, got)
		}
	})

	t.Run("a tenant with no instances sees an empty list", func(t *testing.T) {
		ids, _ := listed(t, uuid.New())
		assert.Empty(t, ids)
	})

	t.Run("row-level security alone limits an unfiltered query to the tenant setting", func(t *testing.T) {
		for _, tc := range []struct {
			name   string
			tenant uuid.UUID
			want   int
		}{
			{"tenant A", tenantA, len(idsA)},
			{"tenant B", tenantB, len(idsB)},
		} {
			var count int
			err := appPool.WithConn(withGUC(ctx, tc.tenant), func(ctx context.Context, conn *pgxpool.Conn) error {
				return conn.QueryRow(ctx, "SELECT count(*) FROM workflow_instance").Scan(&count)
			})
			require.NoError(t, err, tc.name)
			assert.Equal(t, tc.want, count, "%s sees only its own rows with no tenant filter in the query", tc.name)
		}
	})
}

func TestInstanceStart_StartedEventAndInstanceRowAreWrittenInOneTransaction(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	validator, err := eventbus.NewSchemaValidator()
	require.NoError(t, err)
	versionID := uuid.New()
	plan := w18aSinglePlan(t)
	newService := func(transactor port.Transactor, temporal port.TemporalClient) (*service.InstanceService, *w18aDefinitions) {
		defs := &w18aDefinitions{resp: &port.CompiledWorkflow{
			WorkflowID: uuid.New(), VersionID: versionID, Status: "PUBLISHED", IsValid: true, CompiledPlanJSON: plan,
		}}
		return &service.InstanceService{
			Instances:   postgres.NewInstanceRepo(appPool),
			Tasks:       postgres.NewTaskRepo(appPool),
			Outbox:      postgres.NewOutboxRepo(appPool),
			Transactor:  transactor,
			Temporal:    temporal,
			Definitions: defs,
			Queues:      postgres.NewActiveTaskQueueRepo(appPool),
			Validator:   validator,
		}, defs
	}
	const instanceRows = `SELECT count(*) FROM workflow_instance WHERE tenant_id = $1::uuid`
	const outboxRows = `SELECT count(*) FROM outbox_events WHERE tenant_id = $1`
	input := func(tenantID uuid.UUID) port.StartInstanceInput {
		return port.StartInstanceInput{
			TenantID: tenantID, WorkflowVersionID: versionID, SubjectID: "SUBJ-1", BusinessKey: "w18a-1",
			ContextJSON: json.RawMessage(`{}`), StartedByUserID: uuid.New(),
		}
	}

	t.Run("a transaction that fails after both writes keeps neither", func(t *testing.T) {
		tenantID := uuid.New()
		temporal := &w18aTemporal{}
		svc, defs := newService(w18aRollbackTransactor{inner: postgres.NewTransactor(appPool)}, temporal)

		got, err := svc.Start(context.Background(), input(tenantID))

		require.ErrorIs(t, err, errW18aRollbackAfterBothWrites)
		assert.Nil(t, got)
		assert.Equal(t, []w18aPlanRead{{tenantID, versionID}}, defs.reads, "the plan was read once, for the caller's tenant and the requested version")
		assert.Empty(t, temporal.starts, "no workflow is started for a start that did not commit")
		assert.Zero(t, csfCountRows(t, superPool, instanceRows, tenantID), "the instance row rolled back with the event")
		assert.Zero(t, csfCountRows(t, superPool, outboxRows, tenantID), "the started event rolled back with the instance row")
	})

	t.Run("a committed start leaves one instance row and one started event for it", func(t *testing.T) {
		tenantID := uuid.New()
		temporal := &w18aTemporal{}
		svc, defs := newService(postgres.NewTransactor(appPool), temporal)

		got, err := svc.Start(context.Background(), input(tenantID))

		require.NoError(t, err)
		assert.Equal(t, []w18aPlanRead{{tenantID, versionID}}, defs.reads, "the plan was read once, for the caller's tenant and the requested version")
		assert.Equal(t, 1, csfCountRows(t, superPool, instanceRows, tenantID))
		var eventType, subject, aggregate string
		require.NoError(t, superPool.WithConn(context.Background(), func(ctx context.Context, conn *pgxpool.Conn) error {
			return conn.QueryRow(ctx,
				`SELECT event_type, payload ->> 'subject', payload -> 'data' ->> 'workflow_instance_id' FROM outbox_events WHERE tenant_id = $1`,
				tenantID.String()).Scan(&eventType, &subject, &aggregate)
		}), "exactly one event row for the tenant")
		assert.Equal(t, domain.EventWorkflowInstanceStarted, eventType)
		assert.Equal(t, "instances/"+got.ID.String(), subject)
		assert.Equal(t, got.ID.String(), aggregate)
		require.Len(t, temporal.starts, 1)
		assert.Equal(t, got.ID, temporal.starts[0].InstanceID)
	})
}
