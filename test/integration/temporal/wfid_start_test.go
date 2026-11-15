//go:build integration

package temporal_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporalclient"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

// Each instance has its own execution id, so two instances never share an execution, while a
// repeat of one instance's start finds that instance's own open execution.
func TestStartWorkflow_ExecutionIDsOfInstancesAreIndependentOnARealCluster(t *testing.T) {
	client := temporalclient.New(fixtures.NewTestTemporalServer(t))
	ctx := context.Background()
	tenantID := uuid.New()
	queue := "wf-start-id-" + uuid.NewString()
	newStart := func() port.StartWorkflowInput {
		instanceID := uuid.New()
		return port.StartWorkflowInput{
			TemporalWorkflowID: "tenant:" + tenantID.String() + ":instance:" + instanceID.String(),
			TaskQueue:          queue,
			TenantID:           tenantID,
			InstanceID:         instanceID,
			WorkflowVersionID:  uuid.New(),
			ContextJSON:        `{}`,
		}
	}
	first, second := newStart(), newStart()

	firstRun, err := client.StartWorkflow(ctx, first)
	if err != nil {
		t.Fatalf("first start: %v", err)
	}
	t.Cleanup(func() { _ = client.TerminateWorkflow(ctx, first.TemporalWorkflowID, firstRun.TemporalRunID, "cleanup") })
	secondRun, err := client.StartWorkflow(ctx, second)
	if err != nil {
		t.Fatalf("a start of another instance while the first execution is open: %v", err)
	}
	t.Cleanup(func() {
		_ = client.TerminateWorkflow(ctx, second.TemporalWorkflowID, secondRun.TemporalRunID, "cleanup")
	})
	if secondRun.TemporalRunID == firstRun.TemporalRunID {
		t.Fatalf("two instances share run %s", firstRun.TemporalRunID)
	}

	again, err := client.StartWorkflow(ctx, first)
	if err != nil {
		t.Fatalf("a repeat start of the first instance: %v", err)
	}
	if again.TemporalRunID != firstRun.TemporalRunID {
		t.Errorf("repeat start run id = %s, want the open execution's %s", again.TemporalRunID, firstRun.TemporalRunID)
	}
}
