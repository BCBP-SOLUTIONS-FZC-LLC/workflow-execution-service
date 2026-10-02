package workflow

import wf "go.temporal.io/sdk/workflow"

func getVersion(ctx wf.Context, changeID string) wf.Version {
	return wf.GetVersion(ctx, changeID, wf.DefaultVersion, 1)
}

const initialInterpreterChangeID = "initial-interpreter"

const stageFailChangeID = "stage-fail-signal"

const taskStageCancelChangeID = "task-stage-cancel"

// nodeVisitKeyChangeID gates re-keying pending/pendingSignals by
// (NodeKey, VisitCount) instead of bare NodeKey (the stale-signal fix,
// known_issues.md) — DefaultVersion always resolves to visit 0 (a single
// implicit slot per NodeKey, matching the pre-fix behavior byte-for-byte)
// so replaying a history recorded before this change never diverges.
const nodeVisitKeyChangeID = "node-visit-key-signal-buffer"

// taskCompletedStatusChangeID gates the extra UpdateTaskStatus activity that
// writes COMPLETED once a stage's last assignment is done (LLD §4.3). A
// history recorded before the change has no such activity, so DefaultVersion
// keeps the old sequence.
const taskCompletedStatusChangeID = "task-completed-status"

// isoTimerDurationChangeID gates reading a boundary timer's duration as an
// ISO 8601 period (P5D) when it is not a Go duration. Before it, such a timer
// was dropped with a warning, so an older history has no timer to replay.
const isoTimerDurationChangeID = "iso-timer-duration"

// boundaryEndsPathChangeID gates ending a path at an interrupting boundary
// that has no target department, which is how a boundary leading straight to
// an end event compiles. Before it, such a boundary failed the instance.
const boundaryEndsPathChangeID = "boundary-no-target-ends-path"

// exclusiveEndCompletesPathChangeID gates ending only the path, as an end
// event does, when an exclusive branch leads to one. Before it, such a branch
// ended the instance as TERMINATED without writing the instance's status.
const exclusiveEndCompletesPathChangeID = "exclusive-end-completes-path"

// parallelJoinByBranchChangeID gates telling a Parallel step's branches apart
// by their place in the step. Before it, they were told apart by DeptID, and
// two branches with one DeptID never joined.
const parallelJoinByBranchChangeID = "parallel-join-by-branch"
