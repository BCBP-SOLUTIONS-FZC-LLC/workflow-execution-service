package workflow_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// wflTimerLog records the workflow's timers by id and duration.
type wflTimerLog struct {
	mu        sync.Mutex
	scheduled map[string]time.Duration
	fired     []string
	canceled  []string
}

func wflWatchTimers(env *testsuite.TestWorkflowEnvironment) *wflTimerLog {
	log := &wflTimerLog{scheduled: map[string]time.Duration{}}
	env.SetOnTimerScheduledListener(func(id string, d time.Duration) {
		log.mu.Lock()
		defer log.mu.Unlock()
		log.scheduled[id] = d
	})
	env.SetOnTimerFiredListener(func(id string) {
		log.mu.Lock()
		defer log.mu.Unlock()
		log.fired = append(log.fired, id)
	})
	env.SetOnTimerCanceledListener(func(id string) {
		log.mu.Lock()
		defer log.mu.Unlock()
		log.canceled = append(log.canceled, id)
	})
	return log
}

// idsOf returns the ids of the timers scheduled with duration d.
func (l *wflTimerLog) idsOf(d time.Duration) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var ids []string
	for id, got := range l.scheduled {
		if got == d {
			ids = append(ids, id)
		}
	}
	return ids
}

func (l *wflTimerLog) firedIDs() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.fired...)
}

func (l *wflTimerLog) canceledIDs() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.canceled...)
}

// A task that resolves before its boundary timer cancels that timer: the timer
// never fires, the boundary's escalation path is never started, and the run
// carries on past the timer's original duration.
func TestExecute_TaskResolvedBeforeItsBoundaryTimerCancelsTheTimer(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	timers := wflWatchTimers(env)
	collab := &dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main",
			Departments: []dsl.DepartmentDef{
				{ID: "ops", Stages: []dsl.StageDef{
					{Type: "prep", NodeID: "Prepare", BoundaryTimer: &dsl.BoundaryTimer{Duration: "1h", Interrupting: true, TargetDept: "esc"}},
					{Type: "prep", NodeID: "Hold"},
				}},
				{ID: "esc", Stages: []dsl.StageDef{{Type: "prep", NodeID: "Escalate"}}},
			},
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"ops"}}}},
		}},
	}
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, collab)
	wflSignalAt(env, 10*time.Second, "stage-transition", wflDone("ops", "Prepare", 1))
	wflSignalAt(env, 2*time.Hour, "stage-transition", wflDone("ops", "Hold", 1))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	boundary := timers.idsOf(time.Hour)
	require.Len(t, boundary, 1, "one boundary timer was started")
	assert.Equal(t, boundary, timers.canceledIDs(), "that timer, and no other, was canceled")
	assert.NotContains(t, timers.firedIDs(), boundary[0])
	assert.Equal(t, []domain.NodeKey{"ops/Prepare", "ops/Hold"}, wflNodesCreated(fakes), "the escalation was never started")
	for _, in := range wflInputs[port.UpdateTaskStatusInput](fakes, port.ActivityUpdateTaskStatus) {
		assert.NotEqual(t, domain.TaskStatusSuperseded, in.Status, "task %s was superseded", in.TaskID)
	}
}
