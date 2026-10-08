package port

import (
	"context"

	"github.com/google/uuid"
)

// ConnectorTaskService is the completion/fail-signal path cmd/connector-worker
// calls once it has dispatched a connector-typed task to a real connector —
// the sibling of TaskService's human Complete/Defer, deliberately kept
// separate rather than added to TaskService itself, so TaskService's own
// checkHumanActionable gate (which rejects connector tasks) stays legible
// instead of growing an inverse case. connector-worker never touches the
// Temporal SDK directly (LLD workflow_connectors.md §6.1 Decision #2) — it
// calls this service's HTTP endpoint instead, which signals the workflow on
// its behalf.
// The actor is the tenant's automation principal, resolved from the identity
// subsystem, never taken from the caller: the internal token establishes that
// a call came from inside the cluster, not on whose behalf (LLD §5.7).
type ConnectorTaskService interface {
	Complete(ctx context.Context, tenantID, taskID uuid.UUID, output map[string]any) error
	Fail(ctx context.Context, tenantID, taskID uuid.UUID, errorClass string) error
}
