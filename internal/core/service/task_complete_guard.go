package service

import (
	"context"
	"time"

	"github.com/google/uuid"
)

// taskCompleteGuardTTL covers the narrow window between "the completion signal is sent" and "the
// resulting task row change commits"; the record_version check handles every later repeat
// (execution LLD §5.2, the two-guard rule for at-least-once signal paths).
const taskCompleteGuardTTL = 30 * time.Second

func taskCompleteGuardKey(taskID uuid.UUID) string {
	return "task-complete:" + taskID.String()
}

// reserveCompletion claims the right to signal a task's completion. It returns false when another
// completion of the same task already holds it. A cache failure fails open: the version check
// still stands, and an outage must not block people from completing tasks.
func (s *TaskService) reserveCompletion(ctx context.Context, taskID uuid.UUID) bool {
	if s.Cache == nil {
		return true
	}
	ok, err := s.Cache.SetNX(ctx, taskCompleteGuardKey(taskID), "reserved", taskCompleteGuardTTL)
	if err != nil {
		s.logger().Warn("task completion dedupe check failed, proceeding fail-open", map[string]any{"task_id": taskID, "error": err.Error()})
		return true
	}
	return ok
}

// releaseCompletion frees the claim after a signal that never reached the workflow, so the caller's
// retry is not refused.
func (s *TaskService) releaseCompletion(ctx context.Context, taskID uuid.UUID) {
	if s.Cache == nil {
		return
	}
	if err := s.Cache.Del(context.WithoutCancel(ctx), taskCompleteGuardKey(taskID)); err != nil {
		s.logger().Warn("task completion dedupe release failed after a failed signal", map[string]any{"task_id": taskID, "error": err.Error()})
	}
}
