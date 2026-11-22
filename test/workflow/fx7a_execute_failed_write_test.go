package workflow_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// fx7aLog records the warnings of a test workflow environment.
type fx7aLog struct {
	mu       sync.Mutex
	warnings []fx7aWarning
}

type fx7aWarning struct {
	msg    string
	fields map[string]string
}

func (l *fx7aLog) Debug(string, ...any) {}
func (l *fx7aLog) Info(string, ...any)  {}
func (l *fx7aLog) Error(string, ...any) {}
func (l *fx7aLog) Warn(msg string, keyvals ...any) {
	fields := map[string]string{}
	for i := 0; i+1 < len(keyvals); i += 2 {
		fields[fmt.Sprint(keyvals[i])] = fmt.Sprint(keyvals[i+1])
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.warnings = append(l.warnings, fx7aWarning{msg: msg, fields: fields})
}

func (l *fx7aLog) all() []fx7aWarning {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]fx7aWarning(nil), l.warnings...)
}

// fx7aEarlyFailure is a run that fails before any task exists, with the status
// write left to the caller's fakes.
type fx7aEarlyFailure struct {
	name   string
	setup  func(*wflFakes)
	collab *dsl.CompiledCollaboration
	cause  string
}

func fx7aEarlyFailures() []fx7aEarlyFailure {
	missingMain := wflOneDept("sales", dsl.StageDef{Type: "approve", NodeID: "n1"})
	missingMain.MainPlan = "absent"
	return []fx7aEarlyFailure{
		{
			name: "the plan read is rejected",
			setup: func(f *wflFakes) {
				f.failFor = func(name string, _ any) error {
					if name == port.ActivityGetCompiledPlan {
						return temporal.NewNonRetryableApplicationError("version is not published", "DefinitionServiceClientError", nil)
					}
					return nil
				}
			},
			collab: wflOneDept("sales", dsl.StageDef{Type: "approve", NodeID: "n1"}),
			cause:  "version is not published",
		},
		{
			name:   "the main plan is missing",
			setup:  func(*wflFakes) {},
			collab: missingMain,
			cause:  `main plan "absent" not found`,
		},
	}
}

// An early FAILED write carries the workflow time as its completion time, like
// the final FAILED write does.
func TestExecute_EarlyFailedWriteCarriesTheWorkflowCompletionTime(t *testing.T) {
	for _, tc := range fx7aEarlyFailures() {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			fakes := wflNewFakes()
			tc.setup(fakes)
			wflRegisterAll(env, fakes, tc.collab)

			_, err := wflStart(t, env, fakes, "")

			require.ErrorContains(t, err, tc.cause)
			writes := wflInputs[port.UpdateInstanceStatusInput](fakes, port.ActivityUpdateInstanceStatus)
			require.Len(t, writes, 1)
			assert.Equal(t, domain.InstanceStatusFailed, writes[0].Status)
			assert.Equal(t, wflTenant, writes[0].TenantID)
			assert.Equal(t, wflInstance, writes[0].InstanceID)
			require.NotNil(t, writes[0].CompletedAt)
			assert.True(t, writes[0].CompletedAt.Equal(w23bBase), "CompletedAt = %v, want the workflow time", writes[0].CompletedAt)
		})
	}
}

// A refused early FAILED write leaves the instance row non-terminal; the run
// still ends with its own cause, and a warning names the tenant and instance.
func TestExecute_RefusedEarlyFailedWriteIsLoggedAndTheRunKeepsItsCause(t *testing.T) {
	for _, tc := range fx7aEarlyFailures() {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			logs := &fx7aLog{}
			suite.SetLogger(logs)
			env := suite.NewTestWorkflowEnvironment()
			fakes := wflNewFakes()
			tc.setup(fakes)
			inner := fakes.failFor
			fakes.failFor = func(name string, in any) error {
				if name == port.ActivityUpdateInstanceStatus {
					return wflRefuse("status write")
				}
				if inner != nil {
					return inner(name, in)
				}
				return nil
			}
			wflRegisterAll(env, fakes, tc.collab)

			_, err := wflStart(t, env, fakes, "")

			require.ErrorContains(t, err, tc.cause)
			var found []fx7aWarning
			for _, w := range logs.all() {
				if w.msg == "marking the instance FAILED was refused" {
					found = append(found, w)
				}
			}
			require.Len(t, found, 1)
			assert.Equal(t, wflTenant, found[0].fields["tenant_id"])
			assert.Equal(t, wflInstance, found[0].fields["instance_id"])
			assert.Contains(t, found[0].fields["error"], "status write refused")
		})
	}
}

// A failed run whose own FAILED write is refused returns both errors, not only
// the step's.
func TestExecute_FailedRunWhoseFinalFailedWriteIsRefusedReturnsBothErrors(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	fakes := wflNewFakes()
	fakes.failFor = func(name string, _ any) error {
		if name == port.ActivityUpdateInstanceStatus {
			return wflRefuse("status write")
		}
		return nil
	}
	wflRegisterAll(env, fakes, wflOneDept("sales", dsl.StageDef{Type: "approve", NodeID: "n1"}))
	wflSignalAt(env, 2*time.Second, "stage-fail", stageFailWire{DeptID: "sales", NodeID: "n1", ErrorClass: "boom", RecordVersion: 5, VisitCount: 1})

	_, err := wflStart(t, env, fakes, "")

	require.Error(t, err)
	assert.ErrorContains(t, err, `stage "sales/n1" failed: boom`)
	assert.ErrorContains(t, err, "status write refused")
}
