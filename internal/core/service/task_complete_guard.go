package service

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// taskCompleteGuardTTL covers the narrow window between "the completion signal is sent" and "the
// resulting task row change commits"; the record_version check handles every later repeat
// (execution LLD §5.2, the two-guard rule for at-least-once signal paths).
const taskCompleteGuardTTL = 30 * time.Second

// taskCompleteDeliveredTTL keeps the delivered mark longer than the reservation, like the connector
// callbacks, so a repeat after the reservation window is still answered from the mark.
const taskCompleteDeliveredTTL = 5 * time.Minute

func taskCompleteGuardKey(taskID uuid.UUID) string {
	return "task-complete:" + taskID.String()
}

// reserveCompletion claims the right to signal a task's completion. A key still reserved means an
// earlier attempt has not delivered its signal, so the caller is told to retry later; a key marked
// delivered means the signal went out and the task version is about to move, so the repeat is a
// version conflict. A cache failure fails open: the version check still stands, and an outage must
// not block people from completing tasks.
func (s *TaskService) reserveCompletion(ctx context.Context, taskID uuid.UUID) error {
	if s.Cache == nil {
		return nil
	}
	key := taskCompleteGuardKey(taskID)
	ok, err := s.Cache.SetNX(ctx, key, signalReserved, taskCompleteGuardTTL)
	if err != nil {
		s.logger().Warn("task completion dedupe check failed, proceeding fail-open", map[string]any{"task_id": taskID, "error": err.Error()})
		return nil //nolint:nilerr // fail-open by design: a cache outage must not block task completion
	}
	if ok {
		return nil
	}
	state, err := s.Cache.Get(ctx, key)
	if err != nil {
		s.logger().Warn("task completion dedupe read failed, proceeding fail-open", map[string]any{"task_id": taskID, "error": err.Error()})
		return nil //nolint:nilerr // fail-open by design: a cache outage must not block task completion
	}
	if state == signalDelivered {
		return staleTaskVersion()
	}
	return port.ErrCallbackInFlight
}

// markCompletionDelivered records that the signal went out, even when the caller has given up on its
// request, so a repeat is a version conflict and not a request to retry.
func (s *TaskService) markCompletionDelivered(ctx context.Context, taskID uuid.UUID) {
	if s.Cache == nil {
		return
	}
	if err := s.Cache.Set(context.WithoutCancel(ctx), taskCompleteGuardKey(taskID), signalDelivered, taskCompleteDeliveredTTL); err != nil {
		s.logger().Warn("task completion dedupe could not be marked delivered", map[string]any{"task_id": taskID, "error": err.Error()})
	}
}

// signalMayHaveBeenAccepted reports a signal failure after which the workflow may still have received
// the signal (the caller gave up or the deadline passed), so the reservation must stay until it expires.
func signalMayHaveBeenAccepted(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)
}

// releaseCompletion frees the claim after a signal that never reached the workflow, so the caller's
// retry is not refused. A claim that cannot be freed stays reserved until it expires, and a retry in
// that window is asked to retry later.
func (s *TaskService) releaseCompletion(ctx context.Context, taskID uuid.UUID) {
	if s.Cache == nil {
		return
	}
	if err := s.Cache.Del(context.WithoutCancel(ctx), taskCompleteGuardKey(taskID)); err != nil {
		s.logger().Warn("task completion dedupe release failed after a failed signal", map[string]any{"task_id": taskID, "error": err.Error()})
	}
}
