// Package pglogger adapts this service's port.Logger to
// platform-pgcommon's domain.Logger, so pgcommon.Config.Logger and
// migrate.Runner.Logger can share the one logger every other composition
// root wiring in this codebase already uses, instead of going unset.
package pglogger

import (
	pgdomain "github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/domain"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type adapter struct {
	log port.Logger
}

// New wraps log so it satisfies platform-pgcommon's domain.Logger interface.
func New(log port.Logger) pgdomain.Logger {
	return adapter{log: log}
}

func (a adapter) Debug(msg string, fields ...pgdomain.Field) { a.log.Debug(msg, toMap(fields)) }
func (a adapter) Info(msg string, fields ...pgdomain.Field)  { a.log.Info(msg, toMap(fields)) }
func (a adapter) Warn(msg string, fields ...pgdomain.Field)  { a.log.Warn(msg, toMap(fields)) }
func (a adapter) Error(msg string, fields ...pgdomain.Field) { a.log.Error(msg, toMap(fields)) }

func toMap(fields []pgdomain.Field) map[string]any {
	if len(fields) == 0 {
		return nil
	}
	m := make(map[string]any, len(fields))
	for _, f := range fields {
		m[f.Key] = f.Value
	}
	return m
}
