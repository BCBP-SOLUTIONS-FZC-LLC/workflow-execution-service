package workflow_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// w28aRunStart must match the start time wflStart gives every run (2026-01-01T00:00:00Z).
var w28aRunStart = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// A reassign signal that carries no recorded time (an admin reassignment sent
// directly) is stamped with the workflow's clock when it arrives, so a vacate
// signal recorded before that moment is stale and is dropped, while one
// recorded after it is applied.
func TestExecute_AnAdminReassignWithoutARecordedTimeIsStampedWithTheWorkflowClockBeforeTheCompare(t *testing.T) {
	cases := []struct {
		name         string
		vacatedAt    time.Duration
		wantUnassign []string
	}{
		{name: "vacate recorded before the reassign is stale", vacatedAt: time.Second, wantUnassign: nil},
		{name: "vacate recorded after the reassign is applied", vacatedAt: 10 * time.Second, wantUnassign: []string{"sales/prep"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			fakes := wflNewFakes()
			wflRegisterAll(env, fakes, wflOneDept("sales", dsl.StageDef{Type: "prep"}))
			wflSignalAt(env, 2*time.Second, "instance-reassign", reassignSignalWire{
				TaskID: "sales/prep", OldUserID: "user-1", NewUserID: "user-2", AdminUserID: "admin-1", RecordVersion: 3,
			})
			wflSignalAt(env, 4*time.Second, "instance-task-vacated", taskVacatedWire{
				TaskID: "sales/prep", VacatedUserID: "user-2", RecordedAt: w28aRunStart.Add(tc.vacatedAt),
			})
			var after wfengine.WorkflowStatusQuery
			wflQueryStatusAt(t, env, 5*time.Second, &after)
			wflSignalAt(env, 6*time.Second, "stage-transition", wflDoneStage("sales", "prep", 1))

			status, err := wflStart(t, env, fakes, "")

			require.NoError(t, err)
			assert.Equal(t, domain.InstanceStatusCompleted, status)
			assert.Equal(t, tc.wantUnassign, after.UnassignedTasks)
			reassigns := wflInputs[port.ReassignAssignmentInput](fakes, port.ActivityReassignAssignment)
			require.Len(t, reassigns, 1, "the admin reassignment itself ran once")
			assert.Equal(t, port.ReassignAssignmentInput{
				TaskID: "sales/prep", TenantID: wflTenant, OldUserID: "user-1", NewUserID: "user-2", AdminUserID: "admin-1", RecordVersion: 3,
			}, reassigns[0])
		})
	}
}
