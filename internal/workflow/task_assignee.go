package workflow

import (
	"sort"
	"time"

	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

const SignalInstanceTaskVacated = port.SignalInstanceTaskVacated

type taskVacatedSignal struct {
	TaskID        string
	VacatedUserID string
	RecordedAt    time.Time
}

// assigneeNote is what the interpreter last heard about a task's assignees, and
// when the change behind it was recorded.
type assigneeNote struct {
	vacated bool
	at      time.Time
}

// noteTaskAssignee records that a task lost its last assignee (vacated) or got
// one again. The interpreter waits for the task either way (execution LLD §3.1,
// §6.2): a task with no assignee has no one to progress it, and the instance's
// own state reflects that. Signals are delivered at least once and not in
// order, so a signal recorded before the last one applied is dropped. A signal
// that carries no time (an admin reassignment, sent directly) counts as now.
// It reports whether the signal was applied rather than dropped as stale.
func (in *interpreter) noteTaskAssignee(ctx wf.Context, taskID string, vacated bool, recordedAt time.Time) bool {
	if recordedAt.IsZero() {
		recordedAt = wf.Now(ctx)
	}
	if last, ok := in.taskAssignees[taskID]; ok && recordedAt.Before(last.at) {
		wf.GetLogger(ctx).Info("dropping an assignee signal recorded before a later one", "task_id", taskID)
		return false
	}
	in.taskAssignees[taskID] = assigneeNote{vacated: vacated, at: recordedAt}
	if vacated {
		wf.GetLogger(ctx).Warn("a task lost its last assignee and the instance waits for one", "task_id", taskID)
	}
	return true
}

func (in *interpreter) handleTaskVacated(ctx wf.Context, sig taskVacatedSignal) {
	_ = in.noteTaskAssignee(ctx, sig.TaskID, true, sig.RecordedAt)
}

func (in *interpreter) unassignedTasks() []string {
	var ids []string
	for id, note := range in.taskAssignees {
		if note.vacated {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}
