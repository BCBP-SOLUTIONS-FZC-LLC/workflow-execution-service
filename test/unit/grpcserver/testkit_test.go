// Package grpcserver_test holds hand-rolled fakes (repo convention: no
// mockgen) for the inbound gRPC server's port dependencies.
package grpcserver_test

import (
	"context"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type fakeArchiveGuard struct {
	checkActiveInstances func(ctx context.Context, tenantID, workflowID uuid.UUID) (bool, int32, error)
}

func (f *fakeArchiveGuard) CheckActiveInstances(ctx context.Context, tenantID, workflowID uuid.UUID) (bool, int32, error) {
	if f.checkActiveInstances != nil {
		return f.checkActiveInstances(ctx, tenantID, workflowID)
	}
	return false, 0, nil
}

var _ port.ArchiveGuard = (*fakeArchiveGuard)(nil)

type fakeUserTaskPauser struct {
	pauseUserTasks func(ctx context.Context, tenantID, userID uuid.UUID) error
}

func (f *fakeUserTaskPauser) PauseUserTasks(ctx context.Context, tenantID, userID uuid.UUID) error {
	if f.pauseUserTasks != nil {
		return f.pauseUserTasks(ctx, tenantID, userID)
	}
	return nil
}

var _ port.UserTaskPauser = (*fakeUserTaskPauser)(nil)

// fakeLogger records nothing by default; tests that care about a specific
// call set errorFn.
type fakeLogger struct {
	errorFn func(msg string, fields map[string]any)
}

func (f *fakeLogger) Debug(string, map[string]any) {}
func (f *fakeLogger) Info(string, map[string]any)  {}
func (f *fakeLogger) Warn(string, map[string]any)  {}
func (f *fakeLogger) Error(msg string, fields map[string]any) {
	if f.errorFn != nil {
		f.errorFn(msg, fields)
	}
}

var _ port.Logger = (*fakeLogger)(nil)
