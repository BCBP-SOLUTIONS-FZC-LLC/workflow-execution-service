package workflow_test

import (
	"strings"
	"sync"
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

// w28aWarns records the warnings of a test workflow environment.
type w28aWarns struct {
	mu   sync.Mutex
	logs []w28aWarn
}

type w28aWarn struct {
	msg     string
	keyvals []any
}

func (w *w28aWarns) Debug(string, ...any) {}
func (w *w28aWarns) Info(string, ...any)  {}
func (w *w28aWarns) Error(string, ...any) {}
func (w *w28aWarns) Warn(msg string, keyvals ...any) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.logs = append(w.logs, w28aWarn{msg: msg, keyvals: keyvals})
}

// containing returns the warnings whose message holds text.
func (w *w28aWarns) containing(text string) []w28aWarn {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []w28aWarn
	for _, l := range w.logs {
		if strings.Contains(l.msg, text) {
			out = append(out, l)
		}
	}
	return out
}

func (l w28aWarn) field(name string) any {
	for i := 0; i+1 < len(l.keyvals); i += 2 {
		if l.keyvals[i] == name {
			return l.keyvals[i+1]
		}
	}
	return nil
}

func w28aNewEnv(warns *w28aWarns) *testsuite.TestWorkflowEnvironment {
	var suite testsuite.WorkflowTestSuite
	suite.SetLogger(warns)
	return suite.NewTestWorkflowEnvironment()
}

func w28aPause(initiator, admin string, version int64) adminSignalWire {
	return adminSignalWire{AdminUserID: admin, Initiator: initiator, RecordVersion: version}
}

// w28aSignalsAt sends several signals in one callback, so they are all queued
// before the workflow reads the first.
func w28aSignalsAt(env *testsuite.TestWorkflowEnvironment, delay time.Duration, signals ...func()) {
	env.RegisterDelayedCallback(func() {
		for _, send := range signals {
			send()
		}
	}, delay)
}

func w28aSend(env *testsuite.TestWorkflowEnvironment, signal string, payload any) func() {
	return func() { env.SignalWorkflow(signal+":"+wflInstance, payload) }
}

func TestExecute_PauseWhileRunning_PausesTheInstanceAndTheOwnerIsTheAdmin(t *testing.T) {
	warns := &w28aWarns{}
	env := w28aNewEnv(warns)
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, wflOneDept("sales", dsl.StageDef{Type: "prep"}))
	wflSignalAt(env, 2*time.Second, "instance-pause", w28aPause(domain.InitiatorAdmin, "admin-7", 5))
	var whilePaused, afterOtherResume wfengine.WorkflowStatusQuery
	wflQueryStatusAt(t, env, 3*time.Second, &whilePaused)
	wflSignalAt(env, 4*time.Second, "instance-resume", w28aPause(domain.InitiatorOOO, "admin-8", 6))
	wflQueryStatusAt(t, env, 5*time.Second, &afterOtherResume)
	wflSignalAt(env, 6*time.Second, "instance-resume", w28aPause(domain.InitiatorAdmin, "admin-9", 7))
	wflSignalAt(env, 8*time.Second, "stage-transition", wflDoneStage("sales", "prep", 1))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	assert.Equal(t, domain.InstanceStatusPaused, whilePaused.Status)
	assert.Equal(t, domain.InstanceStatusPaused, afterOtherResume.Status,
		"a resume from another initiator does not lift a pause the admin owns")
	pauses := wflInputs[port.PauseInstanceInput](fakes, port.ActivityPauseInstance)
	require.Len(t, pauses, 1)
	assert.Equal(t, port.PauseInstanceInput{
		InstanceID: wflInstance, TenantID: wflTenant, AdminUserID: "admin-7", Initiator: domain.InitiatorAdmin, RecordVersion: 5,
	}, pauses[0])
	resumes := wflInputs[port.ResumeInstanceInput](fakes, port.ActivityResumeInstance)
	require.Len(t, resumes, 1, "only the administrator's resume reached the status write")
	assert.Equal(t, "admin-9", resumes[0].AdminUserID)
	assert.Equal(t, domain.InitiatorAdmin, resumes[0].Initiator)
	assert.Len(t, warns.containing("initiator does not own this pause"), 1)
}

func TestExecute_TwoPausesQueuedTogetherWhileRunning_OnlyTheFirstIsApplied(t *testing.T) {
	cases := []struct {
		name            string
		secondInitiator string
	}{
		{name: "second from the same initiator", secondInitiator: domain.InitiatorAdmin},
		{name: "second from another initiator", secondInitiator: domain.InitiatorOOO},
		{name: "second from tenant state", secondInitiator: domain.InitiatorTenantState},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			warns := &w28aWarns{}
			env := w28aNewEnv(warns)
			fakes := wflNewFakes()
			wflRegisterAll(env, fakes, wflOneDept("sales", dsl.StageDef{Type: "prep"}))
			w28aSignalsAt(env, 2*time.Second,
				w28aSend(env, "instance-pause", w28aPause(domain.InitiatorAdmin, "admin-1", 5)),
				w28aSend(env, "instance-pause", w28aPause(tc.secondInitiator, "admin-2", 6)),
			)
			var whilePaused wfengine.WorkflowStatusQuery
			wflQueryStatusAt(t, env, 4*time.Second, &whilePaused)
			wflSignalAt(env, 6*time.Second, "instance-resume", w28aPause(domain.InitiatorAdmin, "admin-1", 7))
			wflSignalAt(env, 8*time.Second, "stage-transition", wflDoneStage("sales", "prep", 1))

			status, err := wflStart(t, env, fakes, "")

			require.NoError(t, err)
			assert.Equal(t, domain.InstanceStatusCompleted, status)
			assert.Equal(t, domain.InstanceStatusPaused, whilePaused.Status)
			pauses := wflInputs[port.PauseInstanceInput](fakes, port.ActivityPauseInstance)
			require.Len(t, pauses, 1, "the second pause never reached the status write")
			assert.Equal(t, "admin-1", pauses[0].AdminUserID)
			assert.Equal(t, int64(5), pauses[0].RecordVersion)
			assert.Equal(t, wflTenant, pauses[0].TenantID)
			assert.Equal(t, wflInstance, pauses[0].InstanceID)
			assert.Len(t, warns.containing("dropping admin signal"), 1, "the dropped second pause is logged")
		})
	}
}

func TestExecute_ResumeWhileRunning_IsDroppedWithoutAStatusWriteOrAnEvent(t *testing.T) {
	warns := &w28aWarns{}
	env := w28aNewEnv(warns)
	fakes := wflNewFakes()
	wflRegisterAll(env, fakes, wflOneDept("sales", dsl.StageDef{Type: "prep"}))
	wflSignalAt(env, 2*time.Second, "instance-resume", w28aPause(domain.InitiatorAdmin, "admin-1", 5))
	var afterResume wfengine.WorkflowStatusQuery
	wflQueryStatusAt(t, env, 3*time.Second, &afterResume)
	wflSignalAt(env, 4*time.Second, "stage-transition", wflDoneStage("sales", "prep", 1))

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	assert.Equal(t, domain.InstanceStatusRunning, afterResume.Status)
	assert.Zero(t, fakes.attemptsOf(port.ActivityResumeInstance), "no resume write")
	assert.Zero(t, fakes.attemptsOf(port.ActivityPauseInstance))
	assert.Equal(t, []domain.InstanceStatus{domain.InstanceStatusCompleted}, wflStatuses(fakes),
		"the only status write is the run's own end")
	dropped := warns.containing("dropping admin signal")
	require.Len(t, dropped, 1)
	assert.Equal(t, "instance-resume", dropped[0].field("signal"))
}

func w28aParkCollab() *dsl.CompiledCollaboration { return degradedPauseCollaboration() }

// w28aParkRun fails the billing branch of the fork so the instance parks
// DEGRADED once the warehouse branch is done, then runs signals and ends with a
// cancel.
func w28aParkRun(t *testing.T, warns *w28aWarns, steps func(env *testsuite.TestWorkflowEnvironment)) (*wflFakes, domain.InstanceStatus) {
	t.Helper()
	env := w28aNewEnv(warns)
	fakes := wflNewFakes()
	wflRefuseTaskFor(fakes, "billing/prep")
	wflRegisterAll(env, fakes, w28aParkCollab())
	wflSignalAt(env, 2*time.Second, "stage-transition", wflDoneStage("warehouse", "prep", 1))
	steps(env)
	wflSignalAt(env, 40*time.Second, "instance-cancel", adminSignalWire{AdminUserID: "admin-9", RecordVersion: 4})

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	return fakes, status
}

func w28aForwardBilling(env *testsuite.TestWorkflowEnvironment, at time.Duration) {
	wflSignalAt(env, at, "instance-force-forward", adminSignalWire{
		AdminUserID: "admin-1", Initiator: domain.InitiatorAdmin, TargetDeptID: "billing", TargetNodeKey: "billing/prep", RecordVersion: 1,
	})
}

func TestExecute_PauseOwnedBySafetyNetIsNeverReplaced(t *testing.T) {
	t.Run("running", func(t *testing.T) {
		warns := &w28aWarns{}
		env := w28aNewEnv(warns)
		fakes := wflNewFakes()
		wflRegisterAll(env, fakes, wflOneDept("sales", dsl.StageDef{Type: "prep"}))
		wflSignalAt(env, 2*time.Second, "instance-pause", w28aPause(domain.InitiatorSafetyNet, "admin-1", 5))
		wflSignalAt(env, 3*time.Second, "instance-pause", w28aPause(domain.InitiatorSafetyNet, "admin-2", 6))
		wflSignalAt(env, 4*time.Second, "instance-pause", w28aPause(domain.InitiatorAdmin, "admin-3", 7))
		wflSignalAt(env, 5*time.Second, "instance-pause", w28aPause(domain.InitiatorOOO, "admin-4", 8))
		wflSignalAt(env, 6*time.Second, "instance-resume", w28aPause(domain.InitiatorOOO, "admin-4", 9))
		var stillPaused wfengine.WorkflowStatusQuery
		wflQueryStatusAt(t, env, 7*time.Second, &stillPaused)
		wflSignalAt(env, 8*time.Second, "instance-resume", w28aPause(domain.InitiatorSafetyNet, "admin-5", 10))
		wflSignalAt(env, 10*time.Second, "stage-transition", wflDoneStage("sales", "prep", 1))

		status, err := wflStart(t, env, fakes, "")

		require.NoError(t, err)
		assert.Equal(t, domain.InstanceStatusCompleted, status)
		assert.Equal(t, domain.InstanceStatusPaused, stillPaused.Status)
		pauses := wflInputs[port.PauseInstanceInput](fakes, port.ActivityPauseInstance)
		require.Len(t, pauses, 1)
		assert.Equal(t, "admin-1", pauses[0].AdminUserID)
		assert.Equal(t, domain.InitiatorSafetyNet, pauses[0].Initiator)
		resumes := wflInputs[port.ResumeInstanceInput](fakes, port.ActivityResumeInstance)
		require.Len(t, resumes, 1, "only the safety net, the owner, lifted its pause")
		assert.Equal(t, domain.InitiatorSafetyNet, resumes[0].Initiator)
		assert.Equal(t, wflTenant, resumes[0].TenantID)
		assert.Equal(t, wflInstance, resumes[0].InstanceID)
		assert.Len(t, warns.containing("dropping admin signal"), 3, "the three later pauses are dropped")
	})

	t.Run("parked", func(t *testing.T) {
		warns := &w28aWarns{}
		fakes, status := w28aParkRun(t, warns, func(env *testsuite.TestWorkflowEnvironment) {
			wflSignalAt(env, 5*time.Second, "instance-pause", w28aPause(domain.InitiatorSafetyNet, "admin-1", 5))
			wflSignalAt(env, 6*time.Second, "instance-pause", w28aPause(domain.InitiatorSafetyNet, "admin-2", 6))
			wflSignalAt(env, 7*time.Second, "instance-pause", w28aPause(domain.InitiatorAdmin, "admin-3", 7))
			w28aForwardBilling(env, 12*time.Second)
		})

		assert.Equal(t, domain.InstanceStatusTerminated, status)
		pauses := wflInputs[port.PauseInstanceInput](fakes, port.ActivityPauseInstance)
		require.Len(t, pauses, 1)
		assert.Equal(t, port.PauseInstanceInput{
			InstanceID: wflInstance, TenantID: wflTenant, AdminUserID: "admin-1", Initiator: domain.InitiatorSafetyNet, RecordVersion: 5,
		}, pauses[0], "the first safety net pause stays the held one")
		held := warns.containing("a pause is already held")
		require.Len(t, held, 2)
		for _, w := range held {
			assert.Equal(t, domain.InitiatorSafetyNet, w.field("paused_by"))
		}
	})
}

func TestExecute_ResumeInAParkThatHoldsNoPauseOrNotTheCallersPause_IsDroppedWithItsOwnWarning(t *testing.T) {
	warns := &w28aWarns{}
	fakes, status := w28aParkRun(t, warns, func(env *testsuite.TestWorkflowEnvironment) {
		wflSignalAt(env, 5*time.Second, "instance-resume", w28aPause(domain.InitiatorAdmin, "admin-1", 5))
		wflSignalAt(env, 6*time.Second, "instance-pause", w28aPause(domain.InitiatorTenantState, "", 6))
		wflSignalAt(env, 7*time.Second, "instance-resume", w28aPause(domain.InitiatorOOO, "admin-2", 7))
		w28aForwardBilling(env, 12*time.Second)
	})

	assert.Equal(t, domain.InstanceStatusTerminated, status)
	noPause := warns.containing("no pause is held")
	require.Len(t, noPause, 1, "the first resume found nothing to lift")
	notOwner := warns.containing("does not own the held pause")
	require.Len(t, notOwner, 1, "the second resume came from a non-owner")
	assert.Equal(t, domain.InitiatorOOO, notOwner[0].field("resume_initiator"))
	assert.Equal(t, domain.InitiatorTenantState, notOwner[0].field("paused_by"))
	assert.Zero(t, fakes.attemptsOf(port.ActivityResumeInstance), "neither resume reached the status write")
	pauses := wflInputs[port.PauseInstanceInput](fakes, port.ActivityPauseInstance)
	require.Len(t, pauses, 1, "the held pause survived both resumes and was applied when the park ended")
	assert.Equal(t, domain.InitiatorTenantState, pauses[0].Initiator)
}

func TestExecute_HeldPauseAppliedWhenTheParkEnds_CarriesTheHeldSignalsAdminVersionAndOwner(t *testing.T) {
	cases := []struct {
		name  string
		pause []adminSignalWire
		want  port.PauseInstanceInput
	}{
		{
			name:  "administrator pause",
			pause: []adminSignalWire{w28aPause(domain.InitiatorAdmin, "admin-7", 9)},
			want: port.PauseInstanceInput{
				InstanceID: wflInstance, TenantID: wflTenant, AdminUserID: "admin-7", Initiator: domain.InitiatorAdmin, RecordVersion: 9,
			},
		},
		{
			name: "safety net pause that took over an out of office pause",
			pause: []adminSignalWire{
				w28aPause(domain.InitiatorOOO, "admin-6", 8),
				w28aPause(domain.InitiatorSafetyNet, "admin-7", 10),
			},
			want: port.PauseInstanceInput{
				InstanceID: wflInstance, TenantID: wflTenant, AdminUserID: "admin-7", Initiator: domain.InitiatorSafetyNet, RecordVersion: 10,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fakes, status := w28aParkRun(t, &w28aWarns{}, func(env *testsuite.TestWorkflowEnvironment) {
				for i, p := range tc.pause {
					wflSignalAt(env, time.Duration(5+i)*time.Second, "instance-pause", p)
				}
				w28aForwardBilling(env, 12*time.Second)
			})

			assert.Equal(t, domain.InstanceStatusTerminated, status)
			pauses := wflInputs[port.PauseInstanceInput](fakes, port.ActivityPauseInstance)
			require.Len(t, pauses, 1)
			assert.Equal(t, tc.want, pauses[0])
		})
	}
}
