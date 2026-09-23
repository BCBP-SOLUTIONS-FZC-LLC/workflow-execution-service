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
