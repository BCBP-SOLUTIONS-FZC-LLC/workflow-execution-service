//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

var (
	errW14bEventRefused = errors.New("w14b event refused")
	errW14bOutboxDown   = errors.New("w14b outbox down")
)

type w14bRefusingValidator struct{}

func (w14bRefusingValidator) Validate(context.Context, string, json.RawMessage) error {
	return errW14bEventRefused
}

type w14bFailingOutbox struct{ port.OutboxRepository }

func (w14bFailingOutbox) Enqueue(context.Context, events.Envelope[json.RawMessage]) error {
	return errW14bOutboxDown
}

type w14bStartTemporal struct {
	port.TemporalClient
	starts []port.StartWorkflowInput
}

func (c *w14bStartTemporal) StartWorkflow(_ context.Context, in port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
	c.starts = append(c.starts, in)
	return port.StartWorkflowOutput{TemporalWorkflowID: in.TemporalWorkflowID, TemporalRunID: "run-1"}, nil
}

type w14bPlanDefinitions struct {
	resp  *port.CompiledWorkflow
	reads []w14bPlanRead
}

func (d *w14bPlanDefinitions) GetCompiledWorkflow(_ context.Context, tenantID, versionID uuid.UUID) (*port.CompiledWorkflow, error) {
	d.reads = append(d.reads, w14bPlanRead{tenantID: tenantID, versionID: versionID})
	return d.resp, nil
}

func TestInstanceStart_RefusedStartedEventRollsBackTheInstanceAndStartsNothing(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	realValidator, err := eventbus.NewSchemaValidator()
	require.NoError(t, err)
	plan, err := json.Marshal(dsl.CompiledCollaboration{
		SchemaVersion: dsl.CurrentSchemaVersion,
		MainPlan:      "main",
		Plans:         []*dsl.CompiledPlan{{Name: "main", TaskQueue: "wf-queue-default"}},
	})
	require.NoError(t, err)
	versionID := uuid.New()
	newService := func(temporal port.TemporalClient, outbox port.OutboxRepository, validator port.EventValidator) (*service.InstanceService, *w14bPlanDefinitions) {
		definitions := &w14bPlanDefinitions{resp: &port.CompiledWorkflow{
			WorkflowID: uuid.New(), VersionID: versionID, Status: "PUBLISHED", IsValid: true, CompiledPlanJSON: string(plan),
		}}
		return &service.InstanceService{
			Instances:   instanceRepo,
			Tasks:       postgres.NewTaskRepo(appPool),
			Assignments: postgres.NewTaskAssignmentRepo(appPool),
			Outbox:      outbox,
			Transactor:  postgres.NewTransactor(appPool),
			Temporal:    temporal,
			Definitions: definitions,
			Queues:      postgres.NewActiveTaskQueueRepo(appPool),
			Validator:   validator,
		}, definitions
	}
	realOutbox := postgres.NewOutboxRepo(appPool)
	const instanceRows = `SELECT count(*) FROM workflow_instance WHERE tenant_id = $1::uuid`
	const outboxRows = `SELECT count(*) FROM outbox_events WHERE tenant_id = $1`
	input := func(tenantID uuid.UUID) port.StartInstanceInput {
		return port.StartInstanceInput{
			TenantID: tenantID, WorkflowVersionID: versionID, SubjectID: "SUBJ-1", BusinessKey: "stage-1",
			ContextJSON: json.RawMessage(`{}`), StartedByUserID: uuid.New(),
		}
	}

	cases := []struct {
		name      string
		outbox    port.OutboxRepository
		validator port.EventValidator
		cause     error
	}{
		{name: "the started event fails validation", outbox: realOutbox, validator: w14bRefusingValidator{}, cause: errW14bEventRefused},
		{name: "the outbox refuses the started event", outbox: w14bFailingOutbox{OutboxRepository: realOutbox}, validator: realValidator, cause: errW14bOutboxDown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tenantID := uuid.New()
			temporal := &w14bStartTemporal{}

			svc, definitions := newService(temporal, tc.outbox, tc.validator)

			got, err := svc.Start(context.Background(), input(tenantID))

			require.ErrorIs(t, err, tc.cause)
			require.NotEmpty(t, definitions.reads)
			for _, read := range definitions.reads {
				assert.Equal(t, w14bPlanRead{tenantID: tenantID, versionID: versionID}, read, "the plan was read for the caller's tenant and the requested version")
			}
			assert.Nil(t, got)
			for _, sentinel := range []error{
				port.ErrDuplicateBusinessKey, port.ErrInstanceNotFound, port.ErrRecordVersionConflict,
				port.ErrVersionNotPublished, port.ErrVersionInvalid,
			} {
				assert.NotErrorIs(t, err, sentinel, "an event failure must not look like a client error")
			}
			assert.Empty(t, temporal.starts, "no workflow is started when the event is refused")
			assert.Zero(t, csfCountRows(t, superPool, instanceRows, tenantID), "the instance row was rolled back")
			assert.Zero(t, csfCountRows(t, superPool, outboxRows, tenantID), "no event is left in the outbox")
		})
	}

	t.Run("the same start with a working event path is stored and launched", func(t *testing.T) {
		tenantID := uuid.New()
		temporal := &w14bStartTemporal{}

		svc, definitions := newService(temporal, realOutbox, realValidator)

		got, err := svc.Start(context.Background(), input(tenantID))

		require.NoError(t, err)
		assert.Equal(t, "run-1", got.TemporalRunID)
		require.NotEmpty(t, definitions.reads)
		for _, read := range definitions.reads {
			assert.Equal(t, w14bPlanRead{tenantID: tenantID, versionID: versionID}, read)
		}
		require.Len(t, temporal.starts, 1)
		assert.Equal(t, tenantID, temporal.starts[0].TenantID)
		assert.Equal(t, got.ID, temporal.starts[0].InstanceID)
		assert.Equal(t, 1, csfCountRows(t, superPool, instanceRows, tenantID))
		assert.Equal(t, 1, csfCountRows(t, superPool, outboxRows, tenantID))
	})
}
