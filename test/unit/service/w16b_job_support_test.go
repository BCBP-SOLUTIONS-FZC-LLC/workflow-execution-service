package service_test

import (
	"sync"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type w16bLogEntry struct {
	msg    string
	fields map[string]any
}

// w16bLogger keeps every Error entry with its fields; a background loop may log while the test reads.
type w16bLogger struct {
	mu     sync.Mutex
	errors []w16bLogEntry
}

func (l *w16bLogger) Debug(string, map[string]any) {}
func (l *w16bLogger) Info(string, map[string]any)  {}
func (l *w16bLogger) Warn(string, map[string]any)  {}
func (l *w16bLogger) Error(msg string, fields map[string]any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.errors = append(l.errors, w16bLogEntry{msg: msg, fields: fields})
}

func (l *w16bLogger) errorEntries() []w16bLogEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]w16bLogEntry(nil), l.errors...)
}

var _ port.Logger = (*w16bLogger)(nil)
