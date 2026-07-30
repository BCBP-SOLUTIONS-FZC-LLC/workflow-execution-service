package workflow

import wf "go.temporal.io/sdk/workflow"

// getVersion is this package's single entry point for Temporal's
// patch-safety mechanism. Every future change to the deterministic
// execution path needs a new changeID here, a branch at the actual
// divergence point, and a fixture under test/workflow/replay/ (execution LLD
// §7.1; see that directory's README).
func getVersion(ctx wf.Context, changeID string) wf.Version {
	return wf.GetVersion(ctx, changeID, wf.DefaultVersion, 1)
}

// initialInterpreterChangeID anchors the baseline every future getVersion
// call site's before/after reasoning is relative to. No alternate branch
// exists yet — this call never branches, so replaying its fixture
// (test/workflow/replay/initial_interpreter_test.go) only proves the
// plumbing works, not that the pattern protects a real divergence. That only
// gets proven the first time a real fork ships behind a new changeID here.
const initialInterpreterChangeID = "initial-interpreter"
