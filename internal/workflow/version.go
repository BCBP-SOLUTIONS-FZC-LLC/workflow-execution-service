package workflow

import wf "go.temporal.io/sdk/workflow"

// getVersion is this package's single entry point for Temporal's
// patch-safety mechanism. Every future change to the deterministic
// execution path needs a new changeID here plus a fixture under
// test/workflow/replay/ (see that directory's README) — unenforced by CI
// today (LLD §7.1), so this is the convention until it is.
func getVersion(ctx wf.Context, changeID string) wf.Version {
	return wf.GetVersion(ctx, changeID, wf.DefaultVersion, 1)
}

// initialInterpreterChangeID anchors the baseline every future getVersion
// call site's before/after reasoning is relative to. No alternate branch
// exists yet — this package is greenfield.
const initialInterpreterChangeID = "initial-interpreter"
