package workflow_test

import (
	"reflect"
	"sync"
	"testing"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	wfengine "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/workflow"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

type timedSignal struct {
	at      time.Duration
	name    string
	payload any
}

type eventLog struct {
	mu     sync.Mutex
	events []string
}

func (l *eventLog) add(e string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, e)
}

func (l *eventLog) snapshot() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.events...)
}

type degradedPauseScenario struct {
	pauseBeforeFailure bool
	oldVersion         bool
	signals            []timedSignal
}

func pauseSignal(at time.Duration, initiator, admin string) timedSignal {
	return timedSignal{at: at, name: "instance-pause:instance-1", payload: adminSignalWire{AdminUserID: admin, Initiator: initiator, RecordVersion: 1}}
}

func resumeSignal(at time.Duration, initiator, admin string) timedSignal {
	return timedSignal{at: at, name: "instance-resume:instance-1", payload: adminSignalWire{AdminUserID: admin, Initiator: initiator, RecordVersion: 1}}
}

func forceForwardSignal(at time.Duration) timedSignal {
	return timedSignal{at: at, name: "instance-force-forward:instance-1", payload: adminSignalWire{
		AdminUserID: "admin-1", Initiator: domain.InitiatorAdmin, TargetDeptID: "billing", TargetNodeKey: "billing/prep", RecordVersion: 1,
	}}
}

func cancelSignal(at time.Duration) timedSignal {
	return timedSignal{at: at, name: "instance-cancel:instance-1", payload: adminSignalWire{AdminUserID: "admin-1", RecordVersion: 1}}
}

func degradedPauseCollaboration() *dsl.CompiledCollaboration {
	collab := parallelCollaboration()
	plan := collab.Plans[0]
	plan.Departments = append(plan.Departments, dsl.DepartmentDef{ID: "tail", Stages: []dsl.StageDef{{Type: "prep", Activity: "ship_order"}}})
	plan.Execution.Steps = append(plan.Execution.Steps, dsl.ExecutionStep{Sequential: []string{"tail"}})
	return collab
}

func runDegradedPause(t *testing.T, sc degradedPauseScenario) []string {
	t.Helper()
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()
	if sc.oldVersion {
		env.OnGetVersion("pause-survives-degraded", wf.DefaultVersion, 1).Return(wf.DefaultVersion)
	}

	log := &eventLog{}
	released := make(chan struct{})
	var once sync.Once
	registerFakeActivities(env, degradedPauseCollaboration(), &activityHooks{
		createTask: func(in port.CreateTaskInput) (port.CreateTaskOutput, error) {
			if in.NodeKey == "billing/prep" {
				if sc.pauseBeforeFailure {
					<-released
				}
				return port.CreateTaskOutput{}, temporal.NewApplicationError("card declined", "ValidationError")
			}
			return port.CreateTaskOutput{TaskID: string(in.NodeKey)}, nil
		},
		updateInstanceStatus: func(in port.UpdateInstanceStatusInput) { log.add("status:" + string(in.Status)) },
		pauseInstance: func(in port.PauseInstanceInput) {
			log.add("pause:" + in.Initiator)
			once.Do(func() { close(released) })
		},
		resumeInstance: func(in port.ResumeInstanceInput) { log.add("resume:" + in.Initiator) },
		cancelInstance: func(port.CancelInstanceInput) error { log.add("cancel"); return nil },
	})

	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow("stage-transition:instance-1", stageTransitionWire{
			DeptID: "warehouse", ToStage: "prep", ResultJSON: "{}", RecordVersion: 1, VisitCount: 1,
		})
	}, tick)
	for _, s := range sc.signals {
		s := s
		env.RegisterDelayedCallback(func() { env.SignalWorkflow(s.name, s.payload) }, s.at)
	}

	env.ExecuteWorkflow(wfengine.Execute, wfengine.ExecuteInput{
		TenantID: "tenant-1", InstanceID: "instance-1", VersionID: "version-1",
	})
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	var out wfengine.ExecuteOutput
	if err := env.GetWorkflowResult(&out); err != nil {
		t.Fatalf("GetWorkflowResult: %v", err)
	}
	if out.Status != domain.InstanceStatusTerminated {
		t.Fatalf("Status = %v, want TERMINATED (the scenario ends with a cancel)", out.Status)
	}
	return log.snapshot()
}

func assertEvents(t *testing.T, got, want []string) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("activity events = %v, want %v", got, want)
	}
}

func TestDegraded_PauseDuringTheParkLeavesItPaused(t *testing.T) {
	got := runDegradedPause(t, degradedPauseScenario{signals: []timedSignal{
		pauseSignal(6*tick, domain.InitiatorTenantState, ""),
		forceForwardSignal(12 * tick),
		cancelSignal(30 * tick),
	}})
	assertEvents(t, got, []string{"status:DEGRADED", "pause:tenant_state", "cancel"})
}

func TestDegraded_PauseInForceWhenTheBranchFailsIsCarriedIntoThePark(t *testing.T) {
	got := runDegradedPause(t, degradedPauseScenario{
		pauseBeforeFailure: true,
		signals: []timedSignal{
			pauseSignal(4*tick, domain.InitiatorAdmin, "admin-1"),
			forceForwardSignal(12 * tick),
			cancelSignal(30 * tick),
		},
	})
	assertEvents(t, got, []string{"pause:admin", "status:DEGRADED", "pause:admin", "cancel"})
}

func TestDegraded_PauseThenResumeByItsOwnerLeavesItRunning(t *testing.T) {
	got := runDegradedPause(t, degradedPauseScenario{signals: []timedSignal{
		pauseSignal(6*tick, domain.InitiatorTenantState, ""),
		resumeSignal(8*tick, domain.InitiatorTenantState, ""),
		forceForwardSignal(12 * tick),
		cancelSignal(30 * tick),
	}})
	assertEvents(t, got, []string{"status:DEGRADED", "status:RUNNING", "cancel"})
}

func TestDegraded_ResumeByANonOwnerDoesNotLiftTheHeldPause(t *testing.T) {
	got := runDegradedPause(t, degradedPauseScenario{signals: []timedSignal{
		pauseSignal(6*tick, domain.InitiatorAdmin, "admin-1"),
		resumeSignal(8*tick, domain.InitiatorTenantState, ""),
		forceForwardSignal(12 * tick),
		cancelSignal(30 * tick),
	}})
	assertEvents(t, got, []string{"status:DEGRADED", "pause:admin", "cancel"})
}

func TestDegraded_AnAdminResumeLiftsAHeldTenantPause(t *testing.T) {
	got := runDegradedPause(t, degradedPauseScenario{signals: []timedSignal{
		pauseSignal(6*tick, domain.InitiatorTenantState, ""),
		resumeSignal(8*tick, domain.InitiatorAdmin, "admin-1"),
		forceForwardSignal(12 * tick),
		cancelSignal(30 * tick),
	}})
	assertEvents(t, got, []string{"status:DEGRADED", "status:RUNNING", "cancel"})
}

func TestDegraded_ASecondPauseDoesNotReplaceTheFirstOwner(t *testing.T) {
	got := runDegradedPause(t, degradedPauseScenario{signals: []timedSignal{
		pauseSignal(6*tick, domain.InitiatorTenantState, ""),
		pauseSignal(8*tick, domain.InitiatorAdmin, "admin-1"),
		forceForwardSignal(12 * tick),
		cancelSignal(30 * tick),
	}})
	assertEvents(t, got, []string{"status:DEGRADED", "pause:tenant_state", "cancel"})
}

func TestDegraded_ACancelDuringThePauseHeldParkTerminates(t *testing.T) {
	got := runDegradedPause(t, degradedPauseScenario{signals: []timedSignal{
		pauseSignal(6*tick, domain.InitiatorTenantState, ""),
		cancelSignal(12 * tick),
	}})
	assertEvents(t, got, []string{"status:DEGRADED", "cancel"})
}

func TestDegraded_AnEpisodeBeganBeforeTheChangeKeepsTheOldBehaviour(t *testing.T) {
	got := runDegradedPause(t, degradedPauseScenario{
		oldVersion: true,
		signals: []timedSignal{
			pauseSignal(6*tick, domain.InitiatorTenantState, ""),
			forceForwardSignal(12 * tick),
			cancelSignal(30 * tick),
		},
	})
	assertEvents(t, got, []string{"status:DEGRADED", "status:RUNNING", "cancel"})
}

func TestDegraded_AResumeAfterTheParkEndedLiftsTheHeldPause(t *testing.T) {
	got := runDegradedPause(t, degradedPauseScenario{signals: []timedSignal{
		pauseSignal(6*tick, domain.InitiatorTenantState, ""),
		forceForwardSignal(12 * tick),
		resumeSignal(20*tick, domain.InitiatorAdmin, "admin-1"),
		cancelSignal(30 * tick),
	}})
	assertEvents(t, got, []string{"status:DEGRADED", "pause:tenant_state", "resume:admin", "cancel"})
}
