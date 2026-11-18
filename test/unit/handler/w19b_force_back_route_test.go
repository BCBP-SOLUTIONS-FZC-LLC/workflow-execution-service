package handler_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// w19bTaskListPage is the page size the service reads an instance's tasks with.
const w19bTaskListPage = 50

// addTasks stores count READY tasks for the instance and returns them in listing order.
func (r *w19bRig) addTasks(inst *domain.Instance, count int) []*domain.Task {
	tasks := make([]*domain.Task, count)
	for i := range tasks {
		tasks[i] = r.addTask(inst, domain.TaskStatusReady)
	}
	return tasks
}

func w19bAssertForceBackSignalled(t *testing.T, rig *w19bRig, inst *domain.Instance, recordVersion int64) {
	t.Helper()
	require.Len(t, rig.sdk.signals, 1, "one signal reaches the workflow")
	got := rig.sdk.signals[0]
	assert.Equal(t, inst.TemporalWorkflowID, got.workflowID)
	assert.Equal(t, port.SignalInstanceForceBack+":"+inst.ID.String(), got.channel)
	assert.Equal(t, w19bSignalBody{AdminUserID: testUserID.String(), RecordVersion: recordVersion}, w19bDecodeSignal(t, got.payload))
}

func TestForceBackRoute_ACompletedTaskBeyondTheFirstPage_IsFoundAndTheSignalGoesOut(t *testing.T) {
	cases := []struct {
		name          string
		total         int
		completedAt   int
		wantListCalls int
	}{
		{"the first task of the second page", 120, w19bTaskListPage, 2},
		{"the last task of the last page", 120, 119, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w19bNewRig(t)
			inst := rig.seed(domain.InstanceStatusRunning, 4)
			tasks := rig.addTasks(inst, tc.total)
			tasks[tc.completedAt].Status = domain.TaskStatusCompleted

			w := rig.forceBack(inst.ID, 4)

			require.Equal(t, http.StatusAccepted, w.Code, w.Body.String())
			require.Len(t, rig.tasks.lists, tc.wantListCalls, "the listing stops at the page that holds the completed task")
			for i, call := range rig.tasks.lists {
				assert.Equal(t, testTenantID, call.tenantID)
				assert.Equal(t, inst.ID, call.instanceID)
				assert.Equal(t, w19bTaskListPage, call.limit)
				if i == 0 {
					assert.Nil(t, call.after)
					continue
				}
				require.NotNil(t, call.after)
				assert.Equal(t, tasks[i*w19bTaskListPage-1].ID, call.after.ID, "each page starts after the last task of the one before")
			}
			w19bAssertForceBackSignalled(t, rig, inst, 4)
		})
	}

	t.Run("control: no completed task on any page is refused after every page was read", func(t *testing.T) {
		rig := w19bNewRig(t)
		inst := rig.seed(domain.InstanceStatusRunning, 4)
		rig.addTasks(inst, 120)

		w := rig.forceBack(inst.ID, 4)

		require.Equal(t, http.StatusConflict, w.Code, w.Body.String())
		assert.Equal(t, "FORCE_BACK_NO_SAVED_BRANCH", w19bProblemOf(t, w).Code)
		assert.Len(t, rig.tasks.lists, 3)
		assert.Empty(t, rig.sdk.signals)
	})
}

func TestForceBackRoute_ATargetNamedForARunningInstance_IsNotSentToTheWorkflow(t *testing.T) {
	rig := w19bNewRig(t)
	inst := rig.seed(domain.InstanceStatusRunning, 4)
	rig.addTask(inst, domain.TaskStatusCompleted)

	w := rig.forceBackTo(inst.ID, w19bTargetNode, 4)

	require.Equal(t, http.StatusAccepted, w.Code, w.Body.String())
	w19bAssertForceBackSignalled(t, rig, inst, 4)
}

func TestForceBackRoute_AFailedTaskListing_Returns500WithTheFixedDetailAndSignalsNothing(t *testing.T) {
	cases := []struct {
		name          string
		failOnList    int
		wantListCalls int
	}{
		{"the first page fails", 1, 1},
		{"a later page fails", 2, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w19bNewRig(t)
			inst := rig.seed(domain.InstanceStatusRunning, 4)
			tasks := rig.addTasks(inst, 120)
			tasks[w19bTaskListPage*2].Status = domain.TaskStatusCompleted
			rig.tasks.failOnList = tc.failOnList
			rig.tasks.listErr = fmt.Errorf("query tasks: %w", errW19bCauseMarker)

			w := rig.forceBack(inst.ID, 4)

			require.Equal(t, http.StatusInternalServerError, w.Code, w.Body.String())
			p := w19bProblemOf(t, w)
			assert.Equal(t, "INTERNAL_ERROR", p.Code)
			assert.Equal(t, "an unexpected error occurred", p.Detail)
			assert.NotContains(t, w.Body.String(), errW19bCauseMarker.Error())
			assert.Len(t, rig.tasks.lists, tc.wantListCalls)
			assert.Empty(t, rig.sdk.signals, "no signal goes out when the history could not be read")
		})
	}
}

func TestForceBackRoute_TemporalReportsTheWorkflowGone_Returns409InvalidInstanceState(t *testing.T) {
	cases := []struct {
		name   string
		status domain.InstanceStatus
		setup  func(rig *w19bRig, inst *domain.Instance)
	}{
		{"a running instance with a completed step", domain.InstanceStatusRunning, func(rig *w19bRig, inst *domain.Instance) {
			rig.addTask(inst, domain.TaskStatusCompleted)
		}},
		{"a degraded instance", domain.InstanceStatusDegraded, func(*w19bRig, *domain.Instance) {}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w19bNewRig(t)
			rig.sdk.signalErr = serviceerror.NewNotFound("workflow execution not found")
			inst := rig.seed(tc.status, 4)
			tc.setup(rig, inst)

			w := rig.forceBack(inst.ID, 4)

			require.Equal(t, http.StatusConflict, w.Code, w.Body.String())
			assert.Equal(t, "INVALID_INSTANCE_STATE", w19bProblemOf(t, w).Code)
			require.Len(t, rig.sdk.signals, 1, "the signal was tried once")
			assert.Equal(t, inst.TemporalWorkflowID, rig.sdk.signals[0].workflowID)
			assert.Equal(t, port.SignalInstanceForceBack+":"+inst.ID.String(), rig.sdk.signals[0].channel)
		})
	}
}

func TestForceBackRoute_AnInstanceTheCallersTenantDoesNotHave_Returns404AndSignalsNothing(t *testing.T) {
	foreignTenant := uuid.New()
	cases := []struct {
		name string
		seed func(rig *w19bRig) uuid.UUID
	}{
		{"an id no tenant has", func(*w19bRig) uuid.UUID { return uuid.New() }},
		{"an instance of another tenant", func(rig *w19bRig) uuid.UUID {
			inst := rig.seed(domain.InstanceStatusRunning, 4)
			inst.TenantID = foreignTenant
			rig.addTask(inst, domain.TaskStatusCompleted)
			return inst.ID
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w19bNewRig(t)
			id := tc.seed(rig)

			w := rig.forceBack(id, 4)

			require.Equal(t, http.StatusNotFound, w.Code, w.Body.String())
			assert.Equal(t, "INSTANCE_NOT_FOUND", w19bProblemOf(t, w).Code)
			assert.Equal(t, []w19bIDs{{testTenantID, id}}, rig.instances.gets, "the lookup is scoped to the caller's tenant")
			assert.Empty(t, rig.tasks.lists, "no task of the other tenant is read")
			assert.Empty(t, rig.sdk.signals)
		})
	}
}
