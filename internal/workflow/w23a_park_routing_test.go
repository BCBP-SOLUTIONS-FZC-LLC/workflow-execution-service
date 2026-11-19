package workflow

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
)

// w23aRouteResult is what routeToParks did with one signal: whether it handled
// it, and the index of every park whose channel received an envelope.
type w23aRouteResult struct {
	handled  bool
	received map[int][]adminSignalEnvelope
}

// w23aRouteToParks opens one park per department set (the park at an index in
// ended has already ended), sends envelope through routeToParks and reads back
// which parks got it.
func w23aRouteToParks(t *testing.T, deptSets []map[string]bool, ended map[int]bool, envelope adminSignalEnvelope) w23aRouteResult {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	res := w23aRouteResult{received: map[int][]adminSignalEnvelope{}}
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		in := newInterpreter("tenant", "instance", "", nil, nil)
		for i, depts := range deptSets {
			in.parks = append(in.parks, &park{ch: wf.NewBufferedChannel(ctx, signalBuffer), depts: depts, ended: ended[i]})
		}
		res.handled = in.routeToParks(ctx, envelope)
		for i, p := range in.parks {
			for {
				var got adminSignalEnvelope
				if !p.ch.ReceiveAsync(&got) {
					break
				}
				res.received[i] = append(res.received[i], got)
			}
		}
		return nil
	})
	require.NoError(t, env.GetWorkflowError())
	return res
}

// A force signal goes to the park that holds its target department; when no park
// holds it, to the most recently opened live park; with no live park, or for a
// signal that is not a force route, routeToParks does not handle it and sends
// nothing.
func TestRouteToParks_ForceSignals(t *testing.T) {
	forward := func(dept string, node domain.NodeKey) adminSignalEnvelope {
		return adminSignalEnvelope{Kind: SignalInstanceForceFwd, Signal: adminSignal{AdminUserID: "admin-1", TargetDeptID: dept, TargetNodeKey: node}}
	}
	back := func(dept string) adminSignalEnvelope {
		return adminSignalEnvelope{Kind: SignalInstanceForceBack, Signal: adminSignal{AdminUserID: "admin-1", TargetDeptID: dept}}
	}
	twoParks := []map[string]bool{{"a": true}, {"b": true}}

	cases := []struct {
		name        string
		parks       []map[string]bool
		ended       map[int]bool
		envelope    adminSignalEnvelope
		wantHandled bool
		wantPark    int
	}{
		{name: "the park holding the target department", parks: twoParks, envelope: forward("a", ""), wantHandled: true, wantPark: 0},
		{name: "the later park holding the target department", parks: twoParks, envelope: back("b"), wantHandled: true, wantPark: 1},
		{name: "a target named only by its node key", parks: twoParks, envelope: forward("", "a/stage"), wantHandled: true, wantPark: 0},
		{name: "no park holds the target: the most recently opened park", parks: twoParks, envelope: forward("elsewhere", ""), wantHandled: true, wantPark: 1},
		{name: "an empty target: the most recently opened park", parks: twoParks, envelope: back(""), wantHandled: true, wantPark: 1},
		{name: "an ended park is skipped even when it holds the target", parks: twoParks, ended: map[int]bool{0: true}, envelope: forward("a", ""), wantHandled: true, wantPark: 1},
		{name: "no parks", parks: nil, envelope: forward("a", ""), wantHandled: false},
		{name: "only ended parks", parks: twoParks, ended: map[int]bool{0: true, 1: true}, envelope: forward("a", ""), wantHandled: false},
		{name: "a pause is not handled", parks: twoParks, envelope: adminSignalEnvelope{Kind: SignalInstancePause, Signal: adminSignal{TargetDeptID: "a"}}, wantHandled: false},
		{name: "a resume is not handled", parks: twoParks, envelope: adminSignalEnvelope{Kind: SignalInstanceResume}, wantHandled: false},
		{name: "an unknown kind is not handled", parks: twoParks, envelope: adminSignalEnvelope{Kind: "something-else"}, wantHandled: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			res := w23aRouteToParks(t, tc.parks, tc.ended, tc.envelope)

			assert.Equal(t, tc.wantHandled, res.handled)
			if !tc.wantHandled {
				assert.Empty(t, res.received, "nothing was sent to any park")
				return
			}
			require.Len(t, res.received, 1, "exactly one park receives the signal")
			assert.Equal(t, []adminSignalEnvelope{tc.envelope}, res.received[tc.wantPark])
		})
	}
}
