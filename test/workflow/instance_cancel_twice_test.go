package workflow_test

import (
	"sync"
	"testing"

	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
)

// csdCancelRecorder collects what the faked cancel and generic status
// activities were called with. Activities run on their own goroutines.
type csdCancelRecorder struct {
	mu      sync.Mutex
	cancels []port.CancelInstanceInput
	status  []port.UpdateInstanceStatusInput
}

func (r *csdCancelRecorder) cancel(in port.CancelInstanceInput) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cancels = append(r.cancels, in)
}

func (r *csdCancelRecorder) statusWrite(in port.UpdateInstanceStatusInput) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.status = append(r.status, in)
}

// The workflow returns at the first cancel it acts on, so the second signal is
// never read. This pins that end state; it does not test deduplication of the
// two cancels (the second one is simply left unread).
func TestExecute_TwoCancelsWithTheSameRecordVersion_WorkflowEndsAtTheFirstCancel(t *testing.T) {
	const instanceCancel = "instance-cancel:instance-1"
	first := adminSignalWire{AdminUserID: "admin-1", Reason: "first", RecordVersion: 1}
	second := adminSignalWire{AdminUserID: "admin-2", Reason: "second", RecordVersion: 1}

	cases := []struct {
		name string
		// secondDuring sends the second cancel from inside the first cancel's
		// activity; otherwise both are sent together before the workflow acts.
		secondDuring bool
	}{
		{"both are sent before the workflow acts", false},
		{"the second arrives while the first cancel is being written", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			rec := &csdCancelRecorder{}
			registerFakeActivities(env, twoDeptCollaboration(), &activityHooks{
				cancelInstance: func(in port.CancelInstanceInput) error {
					rec.cancel(in)
					if tc.secondDuring {
						env.SignalWorkflow(instanceCancel, second)
					}
					return nil
				},
				updateInstanceStatus: rec.statusWrite,
			})

			env.RegisterDelayedCallback(func() {
				env.SignalWorkflow(instanceCancel, first)
				if !tc.secondDuring {
					env.SignalWorkflow(instanceCancel, second)
				}
			}, tick)

			env.ExecuteWorkflow(wfengine.Execute, wfengine.ExecuteInput{
				TenantID: "tenant-1", InstanceID: "instance-1", VersionID: "version-1",
			})

			if err := env.GetWorkflowError(); err != nil {
				t.Fatalf("workflow returned error: %v", err)
			}
			var out wfengine.ExecuteOutput
			if err := env.GetWorkflowResult(&out); err != nil {
				t.Fatalf("GetWorkflowResult error: %v", err)
			}
			if out.Status != domain.InstanceStatusTerminated {
				t.Errorf("Status = %v, want TERMINATED", out.Status)
			}

			rec.mu.Lock()
			defer rec.mu.Unlock()
			if len(rec.cancels) != 1 {
				t.Fatalf("cancel activity ran %d times, want 1 (the workflow ends at the first cancel): %+v", len(rec.cancels), rec.cancels)
			}
			got := rec.cancels[0]
			if got.TenantID != "tenant-1" || got.InstanceID != "instance-1" {
				t.Errorf("cancel activity input = %+v, want tenant-1/instance-1", got)
			}
			if got.AdminUserID != "admin-1" || got.RecordVersion != 1 {
				t.Errorf("cancel activity input = %+v, want the first cancel (admin-1, record version 1)", got)
			}
			if got.Reason == nil || *got.Reason != "first" {
				t.Errorf("cancel activity reason = %v, want the first cancel's reason", got.Reason)
			}
			if len(rec.status) != 0 {
				t.Errorf("generic status writes = %+v, want none: the cancel activity owns the terminal write", rec.status)
			}
		})
	}
}
