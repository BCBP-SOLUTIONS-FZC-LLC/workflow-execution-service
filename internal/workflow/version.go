package workflow

import wf "go.temporal.io/sdk/workflow"

func getVersion(ctx wf.Context, changeID string) wf.Version {
	return wf.GetVersion(ctx, changeID, wf.DefaultVersion, 1)
}

const initialInterpreterChangeID = "initial-interpreter"

const stageFailChangeID = "stage-fail-signal"
