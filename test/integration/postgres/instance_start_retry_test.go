//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

type fixedDefinitions struct{ resp *port.CompiledWorkflow }

func (d fixedDefinitions) GetCompiledWorkflow(context.Context, uuid.UUID, uuid.UUID) (*port.CompiledWorkflow, error) {
	return d.resp, nil
}

type flakyTemporal struct {
	port.TemporalClient
	failures int
	starts   int
	closes   []string
}

func (f *flakyTemporal) StartWorkflow(_ context.Context, in port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
	f.starts++
	if f.starts <= f.failures {
		return port.StartWorkflowOutput{}, errors.New("temporal frontend unreachable")
	}
	return port.StartWorkflowOutput{TemporalWorkflowID: in.TemporalWorkflowID, TemporalRunID: "run-1"}, nil
}

func (f *flakyTemporal) TerminateWorkflow(_ context.Context, _, runID, _ string) error {
	f.closes = append(f.closes, runID)
	return nil
}

func TestInstanceStart_RetryCompletesAFailedStartThroughTheDatabase(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	validator, err := eventbus.NewSchemaValidator()
	require.NoError(t, err)
	temporal := &flakyTemporal{failures: 3}
	plan, err := json.Marshal(dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans:    []*dsl.CompiledPlan{{Name: "main", TaskQueue: "wf-queue-default"}},
	})
	require.NoError(t, err)
	versionID := uuid.New()
	svc := &service.InstanceService{
		Instances:   instanceRepo,
		Tasks:       postgres.NewTaskRepo(appPool),
		Assignments: postgres.NewTaskAssignmentRepo(appPool),
		Outbox:      postgres.NewOutboxRepo(appPool),
		Transactor:  postgres.NewTransactor(appPool),
		Temporal:    temporal,
		Definitions: fixedDefinitions{resp: &port.CompiledWorkflow{
			WorkflowID: uuid.New(), VersionID: versionID, Status: "PUBLISHED", IsValid: true, CompiledPlanJSON: string(plan),
		}},
		Queues:    postgres.NewActiveTaskQueueRepo(appPool),
		Validator: validator,
	}
	ctx := context.Background()
	tenantID, caller := uuid.New(), uuid.New()
	in := port.StartInstanceInput{
		TenantID: tenantID, WorkflowVersionID: versionID, SubjectID: "SUBJ-1", BusinessKey: "stage-1",
		ContextJSON: json.RawMessage(`{"tender":"t-1"}`), StartedByUserID: caller,
	}

	_, err = svc.Start(ctx, in)
	require.Error(t, err)
	stuck, err := instanceRepo.GetLiveByBusinessKey(ctx, tenantID, "SUBJ-1", "stage-1")
	require.NoError(t, err)
	assert.Empty(t, stuck.TemporalRunID)

	got, err := svc.Start(ctx, in)
	require.NoError(t, err)
	assert.Equal(t, stuck.ID, got.ID)
	assert.Equal(t, "run-1", got.TemporalRunID)
	assert.Equal(t, port.InstanceStatusRunning, got.Status)

	stored, err := instanceRepo.GetByID(ctx, tenantID, stuck.ID)
	require.NoError(t, err)
	assert.Equal(t, "run-1", stored.TemporalRunID)
	assert.Equal(t, stuck.RecordVersion, stored.RecordVersion)

	again, err := svc.Start(ctx, in)
	require.NoError(t, err)
	assert.Equal(t, stuck.ID, again.ID)
	assert.Equal(t, 4, temporal.starts, "a start whose run id is recorded issues nothing")

	changed := in
	changed.ContextJSON = json.RawMessage(`{"tender":"t-2"}`)
	_, err = svc.Start(ctx, changed)
	assert.ErrorIs(t, err, port.ErrDuplicateBusinessKey)

	reordered := in
	reordered.ContextJSON = json.RawMessage(`{ "tender": "t-1" }`)
	same, err := svc.Start(ctx, reordered)
	require.NoError(t, err)
	assert.Equal(t, stuck.ID, same.ID)

	other := in
	other.StartedByUserID = uuid.New()
	_, err = svc.Start(ctx, other)
	assert.ErrorIs(t, err, port.ErrDuplicateBusinessKey)
}
