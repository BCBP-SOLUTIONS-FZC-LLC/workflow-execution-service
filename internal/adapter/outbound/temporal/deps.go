// Package temporal implements execution_service's Temporal Activity bodies
// (LLD §3.1's activity catalogue, plus UpdateTaskStatusActivity for
// stage-fail) — the adapter internal/workflow's wf.ExecuteActivity calls
// resolve against once registered on a real worker.Worker (cmd/worker's
// job, not this package's). Activities compose repos + Transactor +
// OutboxRepository directly; there is no InstanceService/TaskService layer
// to delegate to — deliberately not built as part of this package, a
// separate scope decision from wiring cmd/server for real HTTP traffic.
// Never imports internal/workflow, and never imported by it — see
// .go-arch-lint.yml's workflow/adapter component split.
package temporal

import (
	outboundgrpc "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/grpc"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// Deps is every dependency this package's Activity bodies compose. All
// fields are required in real wiring (cmd/worker); tests construct a Deps
// with only the fields the Activity under test actually touches.
type Deps struct {
	Instances   port.InstanceRepository
	Tasks       port.TaskRepository
	Assignments port.TaskAssignmentRepository
	Outbox      port.OutboxRepository
	Transactor  port.Transactor
	Validator   port.EventValidator
	Definitions *outboundgrpc.DefinitionClient
}
