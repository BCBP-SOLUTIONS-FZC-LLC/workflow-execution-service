//go:build integration

package temporal_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporalclient"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

// A second instance whose execution id equals an open execution's is refused, while the
// instance that started that execution finds it again when it repeats its start.
func TestStartWorkflow_AnExecutionOfAnotherInstanceIsRefusedOnARealCluster(t *testing.T) {
	client := temporalclient.New(fixtures.NewTestTemporalServer(t))
	ctx := context.Background()
	owner := port.StartWorkflowInput{
		TemporalWorkflowID: "tenant:" + uuid.NewString() + ":a:b:c",
		TaskQueue:          "wf-start-held-" + uuid.NewString(),
		TenantID:           uuid.New(),
		InstanceID:         uuid.New(),
		WorkflowVersionID:  uuid.New(),
		ContextJSON:        `{}`,
	}
	first, err := client.StartWorkflow(ctx, owner)
	if err != nil {
		t.Fatalf("first start: %v", err)
	}
	t.Cleanup(func() { _ = client.TerminateWorkflow(ctx, owner.TemporalWorkflowID, first.TemporalRunID, "cleanup") })

	other := owner
	other.InstanceID = uuid.New()
	if _, err := client.StartWorkflow(ctx, other); !errors.Is(err, port.ErrExecutionHeld) {
		t.Fatalf("a start of another instance under an open id: err = %v, want ErrExecutionHeld", err)
	}

	again, err := client.StartWorkflow(ctx, owner)
	if err != nil {
		t.Fatalf("a repeat start of the owning instance: %v", err)
	}
	if again.TemporalRunID != first.TemporalRunID {
		t.Errorf("repeat start run id = %s, want the open execution's %s", again.TemporalRunID, first.TemporalRunID)
	}
}
