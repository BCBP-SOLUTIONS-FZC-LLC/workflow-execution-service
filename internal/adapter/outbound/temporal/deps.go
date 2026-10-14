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
	Log         port.Logger
}

func (d *Deps) logger() port.Logger {
	if d.Log != nil {
		return d.Log
	}
	return noopLogger{}
}

type noopLogger struct{}

func (noopLogger) Debug(string, map[string]any) {}
func (noopLogger) Info(string, map[string]any)  {}
func (noopLogger) Warn(string, map[string]any)  {}
func (noopLogger) Error(string, map[string]any) {}
