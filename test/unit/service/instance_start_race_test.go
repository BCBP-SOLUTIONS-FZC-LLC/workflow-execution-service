package service_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type csbLookup struct {
	tenantID    uuid.UUID
	subjectID   string
	businessKey string
}

// csbRacingInstances plays the second of two simultaneous starts: its first
// business-key lookup finds nothing, and its first insert loses on the unique
// key because the winner's row lands in the same moment.
type csbRacingInstances struct {
	*fakeInstanceRepo
	winner  *domain.Instance
	lookups []csbLookup
	creates int
}

func (r *csbRacingInstances) GetLiveByBusinessKey(ctx context.Context, tenantID uuid.UUID, subjectID, businessKey string) (*domain.Instance, error) {
	r.lookups = append(r.lookups, csbLookup{tenantID, subjectID, businessKey})
	if len(r.lookups) == 1 {
		return nil, domain.ErrNotFound
	}
	return r.fakeInstanceRepo.GetLiveByBusinessKey(ctx, tenantID, subjectID, businessKey)
}

func (r *csbRacingInstances) Create(context.Context, *domain.Instance) error {
	r.creates++
	r.byID[r.winner.ID] = r.winner
	return domain.ErrDuplicateBusinessKey
}

func TestInstanceService_Start_InsertLosingOnTheBusinessKeyIsRecheckedAgainstTheWinner(t *testing.T) {
	tenantID, versionID, caller := uuid.New(), uuid.New(), uuid.New()
	const subject, key = "SUBJ-1", "TND-001"

	setup := func(t *testing.T, startedBy uuid.UUID, runID string) (*csbRacingInstances, *domain.Instance, func() (*port.Instance, error), *fakeOutbox, *fakeTemporalClient) {
		t.Helper()
		svc, instances, _, _, outbox, temporal, definitions, _ := newInstanceServiceHarness()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))
		winner := &domain.Instance{
			ID: uuid.New(), TenantID: tenantID, WorkflowVersionID: versionID, SubjectID: subject, BusinessKey: key,
			TemporalWorkflowID: tenantID.String() + ":" + subject + ":" + key, TemporalRunID: runID,
			Status: domain.InstanceStatusRunning, StartedByUserID: startedBy, RecordVersion: 1,
		}
		racing := &csbRacingInstances{fakeInstanceRepo: instances, winner: winner}
		svc.Instances = racing
		start := func() (*port.Instance, error) {
			return svc.Start(context.Background(), port.StartInstanceInput{
				TenantID: tenantID, WorkflowVersionID: versionID, SubjectID: subject, BusinessKey: key, StartedByUserID: caller,
			})
		}
		return racing, winner, start, outbox, temporal
	}

	t.Run("the loser returns the winner's instance and starts nothing", func(t *testing.T) {
		racing, winner, start, outbox, temporal := setup(t, caller, "run-winner")

		got, err := start()

		require.NoError(t, err)
		assert.Equal(t, winner.ID, got.ID)
		assert.Equal(t, "run-winner", got.TemporalRunID)
		assert.Equal(t, 1, racing.creates)
		assert.Equal(t, []csbLookup{{tenantID, subject, key}, {tenantID, subject, key}}, racing.lookups, "the key is looked up again after the lost insert")
		assert.Empty(t, temporal.signals)
		assert.Empty(t, outbox.enqueued, "the loser writes no started event")
		assert.Len(t, racing.byID, 1, "only the winner's row exists")
	})

	t.Run("a winner whose workflow was never launched is launched by the loser", func(t *testing.T) {
		racing, winner, start, _, temporal := setup(t, caller, "")
		var launched []port.StartWorkflowInput
		temporal.startFunc = func(_ context.Context, in port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
			launched = append(launched, in)
			return port.StartWorkflowOutput{TemporalWorkflowID: in.TemporalWorkflowID, TemporalRunID: "run-launched"}, nil
		}

		got, err := start()

		require.NoError(t, err)
		assert.Equal(t, winner.ID, got.ID)
		require.Len(t, launched, 1)
		assert.Equal(t, winner.ID, launched[0].InstanceID)
		assert.Equal(t, tenantID, launched[0].TenantID)
		assert.Equal(t, winner.TemporalWorkflowID, launched[0].TemporalWorkflowID)
		assert.Equal(t, "run-launched", racing.byID[winner.ID].TemporalRunID, "the run id is recorded on the winner's row")
	})

	t.Run("a winner that differs from the request is a duplicate", func(t *testing.T) {
		racing, _, start, _, temporal := setup(t, uuid.New(), "run-winner")

		got, err := start()

		require.ErrorIs(t, err, port.ErrDuplicateBusinessKey)
		assert.Nil(t, got)
		assert.Len(t, racing.lookups, 2)
		assert.Empty(t, temporal.signals)
	})
}
