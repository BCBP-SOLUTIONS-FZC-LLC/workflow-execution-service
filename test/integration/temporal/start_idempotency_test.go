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

// TestStartWorkflow_AnExecutionAlreadyRunningUnderTheIdentifierIsTheSameRun
// pins the Temporal behaviour the idempotent start rests on (LLD decision 22):
// a second start under a running workflow identifier is not an error and
// returns the run that is already there, and a start after that execution
// was closed is not a new run either: it returns the closed run.
func TestStartWorkflow_AnExecutionAlreadyRunningUnderTheIdentifierIsTheSameRun(t *testing.T) {
	client := temporalclient.New(fixtures.NewTestTemporalServer(t))
	ctx := context.Background()
	in := port.StartWorkflowInput{
		TemporalWorkflowID: "tenant:" + uuid.NewString() + ":subject:key",
		TaskQueue:          "wf-start-idempotency-" + uuid.NewString(),
		TenantID:           uuid.New(),
		InstanceID:         uuid.New(),
		WorkflowVersionID:  uuid.New(),
		ContextJSON:        `{}`,
	}

	first, err := client.StartWorkflow(ctx, in)
	if err != nil {
		t.Fatalf("first start: %v", err)
	}
	if first.TemporalRunID == "" {
		t.Fatal("first start returned no run id")
	}

	again, err := client.StartWorkflow(ctx, in)
	if err != nil {
		t.Fatalf("a second start under a running identifier must not fail: %v", err)
	}
	if again.TemporalRunID != first.TemporalRunID {
		t.Errorf("second start run id = %s, want the running execution's %s", again.TemporalRunID, first.TemporalRunID)
	}

	if err := client.TerminateWorkflow(ctx, in.TemporalWorkflowID, first.TemporalRunID, "test"); err != nil {
		t.Fatalf("terminate: %v", err)
	}
	reused, err := client.StartWorkflow(ctx, in)
	if err != nil {
		t.Fatalf("a repeated start after the execution closed must not fail: %v", err)
	}
	if reused.TemporalRunID != first.TemporalRunID {
		t.Errorf("a start after a close returned run %s, want the closed run %s and no second run", reused.TemporalRunID, first.TemporalRunID)
	}

	if err := client.TerminateWorkflow(ctx, in.TemporalWorkflowID, first.TemporalRunID, "test"); err != nil {
		t.Errorf("closing an already closed run must succeed: %v", err)
	}
}
