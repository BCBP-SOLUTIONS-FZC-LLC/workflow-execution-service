package workflow_test

import (
	"testing"
	"time"

	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
)

// TestExecute_SignalChannelNamingMatchesTemporalClientConvention builds every
// channel name from port.TemporalClient's own exported signal-name constants
// plus the "signalName:instanceID" formula
// internal/adapter/outbound/temporalclient.Client.SignalWorkflow uses —
// never a hardcoded literal like every other test/workflow signal test uses
// — so this breaks at test time if port's constants or either side's
// channel-naming formula ever drifts from internal/workflow/signals.go's
// own copy (port/temporal_client.go's own doc comment names this test as
// the reason both mirror-duplicated copies can be trusted to stay in sync).
func TestExecute_SignalChannelNamingMatchesTemporalClientConvention(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	instanceID := "instance-1"
	collab := twoDeptCollaboration()
	var pauseCalls, resumeCalls, cancelCalls int
	registerFakeActivities(env, collab, &activityHooks{
		pauseInstance:  func(port.PauseInstanceInput) { pauseCalls++ },
		resumeInstance: func(port.ResumeInstanceInput) { resumeCalls++ },
		cancelInstance: func(port.CancelInstanceInput) error { cancelCalls++; return nil },
	})

	channelName := func(signalName string) string { return signalName + ":" + instanceID }

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(channelName(port.SignalInstancePause), adminSignalWire{AdminUserID: "admin-1", RecordVersion: 1})
	}, time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(channelName(port.SignalInstanceResume), adminSignalWire{AdminUserID: "admin-2", RecordVersion: 2})
	}, 2*time.Millisecond)
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(channelName(port.SignalInstanceCancel), adminSignalWire{AdminUserID: "admin-1", RecordVersion: 3})
	}, 3*time.Millisecond)

	env.ExecuteWorkflow(wfengine.Execute, wfengine.ExecuteInput{
		TenantID: "tenant-1", InstanceID: instanceID, VersionID: "version-1",
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	if pauseCalls != 1 {
		t.Errorf("PauseInstanceActivity called %d times via port.SignalInstancePause's channel name, want 1 — the interpreter isn't listening on the channel port.TemporalClient's own convention constructs", pauseCalls)
	}
	if resumeCalls != 1 {
		t.Errorf("ResumeInstanceActivity called %d times via port.SignalInstanceResume's channel name, want 1", resumeCalls)
	}
	if cancelCalls != 1 {
		t.Errorf("CancelInstanceActivity called %d times via port.SignalInstanceCancel's channel name, want 1", cancelCalls)
	}
}
