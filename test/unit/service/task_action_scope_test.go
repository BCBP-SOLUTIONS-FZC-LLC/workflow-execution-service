package service_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

func TestTaskService_Complete_ATaskOfAnotherTenantIsNotFound(t *testing.T) {
	w := cseNewTaskWorld()
	other := uuid.New()

	_, err := w.svc.Complete(context.Background(), other, w.task.ID, w.user, json.RawMessage(`{}`), w.task.RecordVersion)

	require.ErrorIs(t, err, port.ErrTaskNotFound)
	assert.Equal(t, []cseLookup{{Tenant: other, ID: w.task.ID}}, w.tasks.getCalls, "the task was looked up in the caller's tenant")
	assert.Empty(t, w.assignments.calls, "nothing else is read once the task is not found")
	assert.Empty(t, w.instances.calls)
	cseRequireNoWorkflowCalls(t, w.temporal)
}

func TestTaskService_Complete_AStaleVersionOnACompletedTaskIsAVersionConflict(t *testing.T) {
	w := cseNewTaskWorld()
	w.task.Status = domain.TaskStatusCompleted

	_, err := w.svc.Complete(context.Background(), w.tenant, w.task.ID, w.user, json.RawMessage(`{}`), w.task.RecordVersion-1)

	require.ErrorIs(t, err, port.ErrRecordVersionConflict)
	assert.NotErrorIs(t, err, port.ErrInvalidTaskState, "the version is checked before the state")
	assert.Empty(t, w.assignments.calls)
	cseRequireNoWorkflowCalls(t, w.temporal)
}

func TestTaskService_Complete_ATaskThatIsNotActiveIsRefusedWithTheRightVersion(t *testing.T) {
	for _, status := range []domain.TaskStatus{
		domain.TaskStatusCompleted, domain.TaskStatusDeferred, domain.TaskStatusFailed, domain.TaskStatusSuperseded,
	} {
		t.Run(string(status), func(t *testing.T) {
			w := cseNewTaskWorld()
			w.task.Status = status

			_, err := w.svc.Complete(context.Background(), w.tenant, w.task.ID, w.user, json.RawMessage(`{}`), w.task.RecordVersion)

			require.ErrorIs(t, err, port.ErrInvalidTaskState)
			assert.NotErrorIs(t, err, port.ErrRecordVersionConflict)
			assert.Empty(t, w.assignments.calls, "the assignee is not looked at for a task that is not active")
			cseRequireNoWorkflowCalls(t, w.temporal)
		})
	}

	t.Run("an in-progress task is completed", func(t *testing.T) {
		w := cseNewTaskWorld()
		w.task.Status = domain.TaskStatusInProgress

		_, err := w.svc.Complete(context.Background(), w.tenant, w.task.ID, w.user, json.RawMessage(`{}`), w.task.RecordVersion)

		require.NoError(t, err)
		require.Len(t, w.temporal.signals, 1)
		assert.Equal(t, w.instance.ID, w.temporal.signals[0].InstanceID)
	})
}

func TestTaskService_Defer_ATaskOfAnotherTenantOrAnUnknownTaskIsNotFound(t *testing.T) {
	for _, tc := range []struct {
		name   string
		tenant func(w *cseTaskWorld) uuid.UUID
		task   func(w *cseTaskWorld) uuid.UUID
	}{
		{"another tenant", func(*cseTaskWorld) uuid.UUID { return uuid.New() }, func(w *cseTaskWorld) uuid.UUID { return w.task.ID }},
		{"an unknown task", func(w *cseTaskWorld) uuid.UUID { return w.tenant }, func(*cseTaskWorld) uuid.UUID { return uuid.New() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := cseNewTaskWorld()
			w.withCompletedStep()
			tenant, taskID := tc.tenant(w), tc.task(w)

			_, err := w.svc.Defer(context.Background(), tenant, taskID, w.user, "reason", w.task.RecordVersion)

			require.ErrorIs(t, err, port.ErrTaskNotFound)
			assert.Equal(t, []cseLookup{{Tenant: tenant, ID: taskID}}, w.tasks.getCalls)
			assert.Empty(t, w.assignments.calls)
			assert.Empty(t, w.instances.calls)
			cseRequireNoWorkflowCalls(t, w.temporal)
		})
	}
}

func TestTaskService_Defer_ARepeatWithTheSameVersionAfterASuccessIsAVersionConflict(t *testing.T) {
	w := cseNewTaskWorld()
	w.withCompletedStep()
	version := w.task.RecordVersion
	// The workflow's part: a defer it accepts moves the task to a new version.
	w.temporal.onUpdate = func(uuid.UUID) error {
		w.task.RecordVersion++
		return nil
	}

	_, err := w.svc.Defer(context.Background(), w.tenant, w.task.ID, w.user, "not my department", version)
	require.NoError(t, err)
	require.Len(t, w.temporal.updates, 1)

	_, err = w.svc.Defer(context.Background(), w.tenant, w.task.ID, w.user, "not my department", version)

	require.ErrorIs(t, err, port.ErrRecordVersionConflict)
	assert.Len(t, w.temporal.updates, 1, "the repeat never reaches the workflow")
	assert.Empty(t, w.temporal.signals)
}

func TestTaskService_Defer_ReadingTheAssignmentsFailsAsAnInternalError(t *testing.T) {
	w := cseNewTaskWorld()
	w.withCompletedStep()
	w.assignments.err = fmt.Errorf("list assignments: %w", assert.AnError)

	_, err := w.svc.Defer(context.Background(), w.tenant, w.task.ID, w.user, "reason", w.task.RecordVersion)

	require.ErrorIs(t, err, assert.AnError)
	for _, refusal := range []error{port.ErrNotAssignee, port.ErrTaskNotFound, port.ErrInvalidTaskState, port.ErrRecordVersionConflict} {
		assert.NotErrorIs(t, err, refusal, "a database failure is not a refusal the caller can fix")
	}
	assert.Equal(t, []cseLookup{{Tenant: w.tenant, ID: w.task.ID}}, w.assignments.calls)
	assert.Empty(t, w.instances.calls)
	cseRequireNoWorkflowCalls(t, w.temporal)
}

func TestTaskService_Defer_TheInstanceBehindTheEligibilityCheckIsNotFound(t *testing.T) {
	w := cseNewTaskWorld()
	w.withCompletedStep()
	elig, defs := w.withEligibility(t, true, dsl.StageDef{NodeID: "review", Role: "reviewer"})
	w.instances.rows = nil

	_, err := w.svc.Defer(context.Background(), w.tenant, w.task.ID, w.user, "reason", w.task.RecordVersion)

	require.ErrorIs(t, err, port.ErrInstanceNotFound)
	assert.Equal(t, []cseLookup{{Tenant: w.tenant, ID: w.instance.ID}}, w.instances.calls, "the eligibility check's own lookup is the one that failed")
	assert.Empty(t, defs.calls, "no plan is read for an instance that is not there")
	assert.Empty(t, elig.calls)
	cseRequireNoWorkflowCalls(t, w.temporal)
}

func TestTaskService_Defer_ATaskWithNoStageInThePlanIsIneligible(t *testing.T) {
	w := cseNewTaskWorld()
	w.withCompletedStep()
	elig, defs := w.withEligibility(t, true, dsl.StageDef{NodeID: "somewhere-else", Role: "reviewer"})

	_, err := w.svc.Defer(context.Background(), w.tenant, w.task.ID, w.user, "reason", w.task.RecordVersion)

	require.ErrorIs(t, err, port.ErrAssigneeIneligible)
	assert.Equal(t, []cseLookup{{Tenant: w.tenant, ID: w.instance.WorkflowVersionID}}, defs.calls)
	assert.Empty(t, elig.calls, "with no stage there is no level to ask Org & Membership about")
	cseRequireNoWorkflowCalls(t, w.temporal)
}

func TestTaskService_Defer_AnUnavailableDefinitionServiceIsReturnedAsUnavailable(t *testing.T) {
	w := cseNewTaskWorld()
	w.withCompletedStep()
	elig, defs := w.withEligibility(t, true, dsl.StageDef{NodeID: "review", Role: "reviewer"})
	defs.resp = nil
	defs.err = fmt.Errorf("get compiled workflow: %w", port.ErrDefinitionUnavailable)

	_, err := w.svc.Defer(context.Background(), w.tenant, w.task.ID, w.user, "reason", w.task.RecordVersion)

	require.ErrorIs(t, err, port.ErrDefinitionUnavailable)
	assert.NotErrorIs(t, err, port.ErrAssigneeIneligible, "an unreadable plan is not an ineligible user")
	assert.Len(t, defs.calls, 1)
	assert.Empty(t, elig.calls)
	cseRequireNoWorkflowCalls(t, w.temporal)
}

func TestTaskService_Defer_WithoutAnEligibilityOrDefinitionsClientTheCheckIsSkipped(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, w *cseTaskWorld) (calls func() int)
	}{
		{"no eligibility client", func(t *testing.T, w *cseTaskWorld) func() int {
			_, defs := w.withEligibility(t, false, dsl.StageDef{NodeID: "review", Role: "reviewer"})
			w.svc.Eligibility = nil
			return func() int { return len(defs.calls) }
		}},
		{"no definitions client", func(t *testing.T, w *cseTaskWorld) func() int {
			elig, _ := w.withEligibility(t, false, dsl.StageDef{NodeID: "review", Role: "reviewer"})
			w.svc.Definitions = nil
			return func() int { return len(elig.calls) }
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := cseNewTaskWorld()
			w.withCompletedStep()
			collaboratorCalls := tc.setup(t, w)

			got, err := w.svc.Defer(context.Background(), w.tenant, w.task.ID, w.user, "reason", w.task.RecordVersion)

			require.NoError(t, err)
			assert.Equal(t, w.task.ID, got.ID)
			assert.Zero(t, collaboratorCalls(), "the remaining collaborator is not asked either")
			require.Len(t, w.temporal.updates, 1)
			assert.Equal(t, "stage-defer", w.temporal.updates[0].SignalName)
			assert.Equal(t, w.instance.ID, w.temporal.updates[0].InstanceID)
			assert.Equal(t, w.instance.TemporalWorkflowID, w.temporal.updates[0].TemporalWorkflowID)
		})
	}
}

func TestTaskService_Defer_TheTaskReadAfterTheUpdate(t *testing.T) {
	deferred := func(w *cseTaskWorld, readErr error) error {
		w.withCompletedStep()
		// The first read is the precheck's; the second one follows the update.
		w.tasks.getErrs = []error{nil, readErr}
		_, err := w.svc.Defer(context.Background(), w.tenant, w.task.ID, w.user, "reason", w.task.RecordVersion)
		return err
	}

	t.Run("a task that is gone is not found", func(t *testing.T) {
		w := cseNewTaskWorld()

		err := deferred(w, domain.ErrNotFound)

		require.ErrorIs(t, err, port.ErrTaskNotFound)
		assert.Len(t, w.temporal.updates, 1, "the workflow was already updated")
		assert.Len(t, w.tasks.getCalls, 2)
		assert.Equal(t, cseLookup{Tenant: w.tenant, ID: w.task.ID}, w.tasks.getCalls[1])
	})

	t.Run("any other read failure is returned as it is", func(t *testing.T) {
		w := cseNewTaskWorld()

		err := deferred(w, fmt.Errorf("read task: %w", assert.AnError))

		require.ErrorIs(t, err, assert.AnError)
		assert.NotErrorIs(t, err, port.ErrTaskNotFound)
		assert.Len(t, w.temporal.updates, 1)
	})
}

func TestTaskService_Reassign_AnAllModeTaskTakesTheLeadAsTheOldUser(t *testing.T) {
	setup := func() (w *cseTaskWorld, nonLead, lead uuid.UUID) {
		w = cseNewTaskWorld()
		w.task.AssigneeMode = "all"
		nonLead, lead = uuid.New(), uuid.New()
		// The non-lead is listed first: the old user is the lead, not the first row.
		w.assignments.rows = []*domain.TaskAssignment{
			{ID: uuid.New(), TenantID: w.tenant, TaskID: w.task.ID, UserID: nonLead, IsActive: true},
			{ID: uuid.New(), TenantID: w.tenant, TaskID: w.task.ID, UserID: lead, IsActive: true, IsLead: true},
		}
		return w, nonLead, lead
	}

	t.Run("a new user equal to the lead is a no-op", func(t *testing.T) {
		w, _, lead := setup()

		_, err := w.svc.Reassign(context.Background(), w.tenant, w.task.ID, uuid.New(), lead, w.task.RecordVersion)

		require.ErrorIs(t, err, port.ErrOverrideNoOp)
		assert.Empty(t, w.instances.calls, "a no-op is refused before the instance is read")
		cseRequireNoWorkflowCalls(t, w.temporal)
	})

	t.Run("a new user equal to a non-lead assignee replaces the lead", func(t *testing.T) {
		w, nonLead, lead := setup()
		admin := uuid.New()

		_, err := w.svc.Reassign(context.Background(), w.tenant, w.task.ID, admin, nonLead, w.task.RecordVersion)

		require.NoError(t, err)
		require.Len(t, w.temporal.signals, 1)
		assert.Equal(t, port.SignalInstanceReassign, w.temporal.signals[0].SignalName)
		assert.Equal(t, w.instance.ID, w.temporal.signals[0].InstanceID)
		raw, err := json.Marshal(w.temporal.signals[0].Payload)
		require.NoError(t, err)
		assert.JSONEq(t, fmt.Sprintf(
			`{"TaskID":%q,"OldUserID":%q,"NewUserID":%q,"AdminUserID":%q,"RecordVersion":%d}`,
			w.task.ID, lead, nonLead, admin, w.task.RecordVersion), string(raw))
	})
}
