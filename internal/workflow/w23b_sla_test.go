package workflow

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
)

func TestAddSLATimers_DateShape_RegistersATimerOnlyForAParseableFutureDate(t *testing.T) {
	cases := []struct {
		name        string
		due         string
		followUp    string
		fire        bool
		wantTimers  []time.Duration
		wantBreach  bool
		wantWarning bool
	}{
		{name: "both empty", due: "", followUp: ""},
		{name: "date without a time", due: "2026-03-02", followUp: "2026-03-02"},
		{name: "time without a zone", due: "2026-03-02T10:00:00", followUp: "2026-03-02T10:00:00"},
		{name: "free text", due: "tomorrow", followUp: "next week"},
		{
			name: "unparseable due date does not stop the follow-up timer",
			due:  "tomorrow", followUp: w23bStart.Add(2 * time.Hour).Format(time.RFC3339),
			fire: true, wantTimers: []time.Duration{2 * time.Hour}, wantWarning: true,
		},
		{
			name: "due date in the past fires at stage start",
			due:  w23bStart.Add(-time.Hour).Format(time.RFC3339), followUp: "",
			fire: true, wantBreach: true,
		},
		{
			name: "future due date waits until then",
			due:  w23bStart.Add(3 * time.Hour).Format(time.RFC3339), followUp: "",
			fire: true, wantTimers: []time.Duration{3 * time.Hour}, wantBreach: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var suite testsuite.WorkflowTestSuite
			env := suite.NewTestWorkflowEnvironment()
			env.SetStartTime(w23bStart)
			rec := &w23bRecorder{}
			w23bRegister(env, rec)

			var mu sync.Mutex
			var timers []time.Duration
			env.SetOnTimerScheduledListener(func(_ string, d time.Duration) {
				mu.Lock()
				defer mu.Unlock()
				timers = append(timers, d)
			})

			env.ExecuteWorkflow(func(ctx wf.Context) error {
				sel := wf.NewSelector(ctx)
				cancel := addSLATimers(ctx, sel, slaTimerParams{
					TenantID: "tenant-1", InstanceID: "instance-1", TaskID: "task-1", NodeKey: "sales/approve",
					DueDate: tc.due, FollowUpDate: tc.followUp,
				})
				defer cancel()
				if tc.fire {
					sel.Select(ctx)
				}
				return nil
			})
			require.NoError(t, env.GetWorkflowError())

			mu.Lock()
			defer mu.Unlock()
			assert.Equal(t, tc.wantTimers, timers)
			assert.Len(t, rec.slaBreaches, w23bBoolToInt(tc.wantBreach))
			assert.Len(t, rec.slaWarnings, w23bBoolToInt(tc.wantWarning))
			for _, b := range rec.slaBreaches {
				assert.Equal(t, "tenant-1", b.TenantID)
				assert.Equal(t, "instance-1", b.InstanceID)
				assert.Equal(t, "task-1", b.TaskID)
				assert.Equal(t, domain.NodeKey("sales/approve"), b.NodeKey)
			}
			for _, w := range rec.slaWarnings {
				assert.Equal(t, "tenant-1", w.TenantID)
				assert.Equal(t, "instance-1", w.InstanceID)
				assert.Equal(t, "task-1", w.TaskID)
				assert.Equal(t, domain.NodeKey("sales/approve"), w.NodeKey)
			}
		})
	}
}

func w23bBoolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
