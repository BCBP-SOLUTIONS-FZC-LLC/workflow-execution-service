//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

type csfStartDefinitions struct{ resp *port.CompiledWorkflow }

func (d csfStartDefinitions) GetCompiledWorkflow(context.Context, uuid.UUID, uuid.UUID) (*port.CompiledWorkflow, error) {
	return d.resp, nil
}

type csfStartTemporal struct {
	port.TemporalClient
	starts []port.StartWorkflowInput
	closes []string
}

func (c *csfStartTemporal) StartWorkflow(_ context.Context, in port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
	c.starts = append(c.starts, in)
	return port.StartWorkflowOutput{TemporalWorkflowID: in.TemporalWorkflowID, TemporalRunID: "run-1"}, nil
}

func (c *csfStartTemporal) TerminateWorkflow(_ context.Context, _, runID, _ string) error {
	c.closes = append(c.closes, runID)
	return nil
}

func csfCountRows(t *testing.T, pool *pgcommon.Pool, query string, tenantID uuid.UUID) int {
	t.Helper()
	var count int
	require.NoError(t, pool.WithConn(context.Background(), func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx, query, tenantID.String()).Scan(&count)
	}))
	return count
}

func TestInstanceStart_InsertRejectedByTheDatabase_StartsNothingAndLeavesNoRows(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	validator, err := eventbus.NewSchemaValidator()
	require.NoError(t, err)
	plan, err := json.Marshal(dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans:    []*dsl.CompiledPlan{{Name: "main", TaskQueue: "wf-queue-default"}},
	})
	require.NoError(t, err)
	versionID := uuid.New()
	newService := func(temporal port.TemporalClient) *service.InstanceService {
		return &service.InstanceService{
			Instances:   instanceRepo,
			Tasks:       postgres.NewTaskRepo(appPool),
			Assignments: postgres.NewTaskAssignmentRepo(appPool),
			Outbox:      postgres.NewOutboxRepo(appPool),
			Transactor:  postgres.NewTransactor(appPool),
			Temporal:    temporal,
			Definitions: csfStartDefinitions{resp: &port.CompiledWorkflow{
				WorkflowID: uuid.New(), VersionID: versionID, Status: "PUBLISHED", IsValid: true, CompiledPlanJSON: string(plan),
			}},
			Queues:    postgres.NewActiveTaskQueueRepo(appPool),
			Validator: validator,
		}
	}
	ctx := context.Background()
	const instanceRows = `SELECT count(*) FROM workflow_instance WHERE tenant_id = $1::uuid`
	const outboxRows = `SELECT count(*) FROM outbox_events WHERE tenant_id = $1`

	t.Run("a context the instance column cannot store fails the start with an unmapped error", func(t *testing.T) {
		tenantID, caller := uuid.New(), uuid.New()
		temporal := &csfStartTemporal{}

		got, err := newService(temporal).Start(ctx, port.StartInstanceInput{
			TenantID: tenantID, WorkflowVersionID: versionID, SubjectID: "SUBJ-1", BusinessKey: "stage-1",
			ContextJSON: json.RawMessage(`{not json`), StartedByUserID: caller,
		})

		require.Error(t, err)
		assert.Nil(t, got)
		for _, sentinel := range []error{
			port.ErrDuplicateBusinessKey, port.ErrInstanceNotFound, port.ErrRecordVersionConflict,
			port.ErrVersionNotPublished, port.ErrVersionInvalid,
		} {
			assert.NotErrorIs(t, err, sentinel, "a storage failure must not look like a client error")
		}
		assert.Empty(t, temporal.starts, "no workflow is started for an instance that was not stored")
		assert.Empty(t, temporal.closes)
		assert.Zero(t, csfCountRows(t, superPool, instanceRows, tenantID), "no instance row is left behind")
		assert.Zero(t, csfCountRows(t, superPool, outboxRows, tenantID), "no started event is queued")
		_, err = instanceRepo.GetLiveByBusinessKey(ctx, tenantID, "SUBJ-1", "stage-1")
		assert.ErrorIs(t, err, domain.ErrNotFound)
	})

	t.Run("the same start with a storable context is accepted", func(t *testing.T) {
		tenantID, caller := uuid.New(), uuid.New()
		temporal := &csfStartTemporal{}

		got, err := newService(temporal).Start(ctx, port.StartInstanceInput{
			TenantID: tenantID, WorkflowVersionID: versionID, SubjectID: "SUBJ-1", BusinessKey: "stage-1",
			ContextJSON: json.RawMessage(`{"tender":"t-1"}`), StartedByUserID: caller,
		})

		require.NoError(t, err)
		assert.Equal(t, "run-1", got.TemporalRunID)
		require.Len(t, temporal.starts, 1)
		assert.Equal(t, 1, csfCountRows(t, superPool, instanceRows, tenantID))
		assert.Equal(t, 1, csfCountRows(t, superPool, outboxRows, tenantID))
	})
}
