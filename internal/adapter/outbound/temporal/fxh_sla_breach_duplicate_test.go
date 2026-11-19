package temporal_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"

	outboundtemporal "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

// fxhAttemptedOutbox records every envelope the activity tries to insert, including the one the
// wrapped outbox refuses.
type fxhAttemptedOutbox struct {
	*fakeOutbox
	attempted []events.Envelope[json.RawMessage]
}

func (o *fxhAttemptedOutbox) Enqueue(ctx context.Context, env events.Envelope[json.RawMessage]) error {
	o.attempted = append(o.attempted, env)
	return o.fakeOutbox.Enqueue(ctx, env)
}

func TestRecordSLABreach_AnOutboxInsertThatLosesTheUniqueRaceFailsTheAttemptSoItIsRetried(t *testing.T) {
	taskID, instanceID, tenantID := uuid.New(), uuid.New(), uuid.New()
	task := &domain.Task{ID: taskID, WorkflowInstanceID: instanceID, Status: domain.TaskStatusReady}
	outbox := &fxhAttemptedOutbox{fakeOutbox: &fakeOutbox{enqueueErr: domain.ErrAlreadyExists}}
	deps := &outboundtemporal.Deps{
		Tasks: newFakeTaskRepo(task), Assignments: newFakeAssignmentRepo(), Instances: seededInstances(task),
		Outbox: outbox, Transactor: fakeTransactor{}, Validator: realValidator(),
	}
	obstest.Init(t)
	before := obstest.Sum(t, "workflow_execution_service_sla_breaches_total", nil)

	err := deps.RecordSLABreach(context.Background(), port.RecordSLABreachInput{
		InstanceID: instanceID.String(), TenantID: tenantID.String(), TaskID: taskID.String(), NodeKey: "sales/review",
	})

	require.ErrorIs(t, err, domain.ErrAlreadyExists, "the aborted transaction cannot commit, so the attempt fails")
	var appErr *temporal.ApplicationError
	assert.NotErrorAs(t, err, &appErr, "a plain error is retried by Temporal")
	require.Len(t, outbox.attempted, 1)
	assert.Equal(t, tenantID.String(), outbox.attempted[0].TenantID, "the insert that lost the race was for the caller's tenant")
	assert.Equal(t, domain.EventWorkflowTaskSLABreached, outbox.attempted[0].Type)
	assert.Equal(t, before, obstest.Sum(t, "workflow_execution_service_sla_breaches_total", nil), "a failed attempt records no breach")
}
