package workflow

import wf "go.temporal.io/sdk/workflow"

func getVersion(ctx wf.Context, changeID string) wf.Version {
	return wf.GetVersion(ctx, changeID, wf.DefaultVersion, 1)
}

const initialInterpreterChangeID = "initial-interpreter"

const stageFailChangeID = "stage-fail-signal"

const taskStageCancelChangeID = "task-stage-cancel"

const nodeVisitKeyChangeID = "node-visit-key-signal-buffer"

const taskCompletedStatusChangeID = "task-completed-status"

const isoTimerDurationChangeID = "iso-timer-duration"

const boundaryEndsPathChangeID = "boundary-no-target-ends-path"

const exclusiveEndCompletesPathChangeID = "exclusive-end-completes-path"

const deferRoutesBackChangeID = "defer-routes-back"

const forceRouteByNodeKeyChangeID = "force-route-by-node-key"

const abandonedTaskSupersededChangeID = "abandoned-task-superseded"

const subWorkflowErrorRaisedChangeID = "subworkflow-error-raised"

const parallelJoinByBranchChangeID = "parallel-join-by-branch"

const ioInputRuleChangeID = "io-input-rule"

const taskResultVariablesChangeID = "task-result-variables"

const externalWaitChangeID = "external-wait-by-key"

const receiveRecordsNodeChangeID = "receive-records-node"

const abandonedMidActivityChangeID = "abandoned-mid-activity"
