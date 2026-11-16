package service_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestInstanceService_Start_StampsStartedAtWhenTheInstanceIsCreated(t *testing.T) {
	svc, instances, _, _, _, _, definitions, _ := newInstanceServiceHarness()
	tenantID, versionID := uuid.New(), uuid.New()
	definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))

	before := time.Now()
	got, err := svc.Start(context.Background(), port.StartInstanceInput{
		TenantID: tenantID, WorkflowVersionID: versionID, SubjectID: "SUBJ-1", BusinessKey: "TND-001",
		ContextJSON: json.RawMessage(`{}`), StartedByUserID: uuid.New(),
	})
	after := time.Now()

	require.NoError(t, err)
	stored := instances.byID[got.ID]
	require.NotNil(t, stored.StartedAt, "the stored instance records when it started")
	assert.False(t, stored.StartedAt.Before(before) || stored.StartedAt.After(after), "started_at %v is not within the start call", stored.StartedAt)
	require.NotNil(t, got.StartedAt, "the answer to the start shows when it started")
	assert.True(t, got.StartedAt.Equal(*stored.StartedAt))
}
