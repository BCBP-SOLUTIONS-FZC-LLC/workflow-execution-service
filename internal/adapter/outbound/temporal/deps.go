package temporal

import (
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type Deps struct {
	Instances   port.InstanceRepository
	Tasks       port.TaskRepository
	Assignments port.TaskAssignmentRepository
	Outbox      port.OutboxRepository
	Transactor  port.Transactor
	Validator   port.EventValidator
	Definitions port.DefinitionServiceClient
}
