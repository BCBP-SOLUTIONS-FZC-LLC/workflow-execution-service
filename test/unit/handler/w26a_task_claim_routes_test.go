package handler_test

import (
	"errors"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

var errW26aDatabase = errors.New("w26a: connection to 10.9.8.7:5432 lost, password=hunter2")

func w26aRequireNoLeak(t *testing.T, body string) {
	t.Helper()
	for _, secret := range []string{"hunter2", "10.9.8.7", "w26a:"} {
		assert.NotContains(t, body, secret, "an internal error text never reaches the caller")
	}
}

func TestGetTask_ARepositoryFailureOrAnotherTenantsTask_AnswersWithTheMappedProblem(t *testing.T) {
	cases := []struct {
		name       string
		admin      bool
		setup      func(rig *w26aRig)
		wantStatus int
		wantCode   string
		wantLists  int
	}{
		{
			name: "the assignment lookup fails for an assignee", setup: func(rig *w26aRig) {
				rig.assign(testUserID)
				rig.assignments.listErr = errW26aDatabase
			},
			wantStatus: http.StatusInternalServerError, wantCode: "INTERNAL_ERROR", wantLists: 1,
		},
		{
			name: "the assignment lookup fails for a caller who is not an assignee", setup: func(rig *w26aRig) {
				rig.assign(uuid.New())
				rig.assignments.listErr = errW26aDatabase
			},
			wantStatus: http.StatusInternalServerError, wantCode: "INTERNAL_ERROR", wantLists: 1,
		},
		{
			name: "the assignment lookup fails for an admin", admin: true, setup: func(rig *w26aRig) {
				rig.assignments.listErr = errW26aDatabase
			},
			wantStatus: http.StatusInternalServerError, wantCode: "INTERNAL_ERROR", wantLists: 1,
		},
		{
			name: "the task belongs to another tenant and the caller is an admin", admin: true, setup: func(rig *w26aRig) {
				rig.task.TenantID = uuid.New()
				rig.assign(testUserID)
			},
			wantStatus: http.StatusNotFound, wantCode: "TASK_NOT_FOUND", wantLists: 0,
		},
		{
			name: "the task belongs to another tenant and the caller is a member", setup: func(rig *w26aRig) {
				rig.task.TenantID = uuid.New()
			},
			wantStatus: http.StatusNotFound, wantCode: "TASK_NOT_FOUND", wantLists: 0,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w26aNewRig(t, "single")
			tc.setup(rig)
			httpReq := req(http.MethodGet, rig.taskPath(""), nil)
			if tc.admin {
				httpReq = asAdmin(httpReq)
			}

			rec := do(rig.router(), httpReq)

			p := w26aProblemOf(t, rec, tc.wantStatus)
			assert.Equal(t, tc.wantCode, p.Code)
			w26aRequireNoLeak(t, rec.Body.String())
			if tc.wantStatus == http.StatusInternalServerError {
				assert.Equal(t, w26aInternalDetail, p.Detail)
			}
			assert.Equal(t, []w26aLookup{{testTenantID, testTaskID}}, rig.tasks.reads, "the task is read once, in the caller's tenant")
			assert.Len(t, rig.assignments.lists, tc.wantLists)
			for _, l := range rig.assignments.lists {
				assert.Equal(t, w26aLookup{testTenantID, testTaskID}, l)
			}
		})
	}
}

func TestClaimTask_Refused_AnswersWithTheMappedProblemAndChangesNothing(t *testing.T) {
	cases := []struct {
		name         string
		admin        bool
		setup        func(rig *w26aRig)
		wantStatus   int
		wantCode     string
		wantType     string
		wantListed   int
		wantEligible int
	}{
		{
			name: "a caller who is not an assignee and nobody is lead yet", setup: func(rig *w26aRig) {
				rig.assign(uuid.New())
				rig.assign(uuid.New())
			},
			wantStatus: http.StatusForbidden, wantCode: "NOT_ASSIGNEE", wantType: "https://errors.bcbp.io/execution/not-assignee", wantListed: 1,
		},
		{
			name: "a caller whose assignment was vacated", setup: func(rig *w26aRig) {
				rig.assign(testUserID).IsActive = false
				rig.assign(uuid.New())
			},
			wantStatus: http.StatusForbidden, wantCode: "NOT_ASSIGNEE", wantType: "https://errors.bcbp.io/execution/not-assignee", wantListed: 1,
		},
		{
			name: "an admin who is not an assignee has no bypass", admin: true, setup: func(rig *w26aRig) {
				rig.assign(uuid.New())
			},
			wantStatus: http.StatusForbidden, wantCode: "NOT_ASSIGNEE", wantType: "https://errors.bcbp.io/execution/not-assignee", wantListed: 1,
		},
		{
			name: "a second assignee after another assignee became lead", setup: func(rig *w26aRig) {
				rig.assign(uuid.New()).IsLead = true
				rig.assign(testUserID)
			},
			wantStatus: http.StatusConflict, wantCode: "TASK_ALREADY_CLAIMED", wantType: "https://errors.bcbp.io/execution/task-already-claimed", wantListed: 1,
		},
		{
			name: "an assignee who is no longer eligible", setup: func(rig *w26aRig) {
				rig.assign(testUserID)
				rig.assign(uuid.New())
				rig.eligibility.eligible = false
			},
			wantStatus: http.StatusUnprocessableEntity, wantCode: "ASSIGNEE_INELIGIBLE", wantType: "https://errors.bcbp.io/execution/assignee-ineligible",
			wantListed: 1, wantEligible: 1,
		},
		{
			name: "the task read fails with a database error", setup: func(rig *w26aRig) {
				rig.assign(testUserID)
				rig.tasks.getErr = errW26aDatabase
			},
			wantStatus: http.StatusInternalServerError, wantCode: "INTERNAL_ERROR", wantType: "https://errors.bcbp.io/execution/internal-error",
		},
		{
			name: "the assignment listing fails with a database error", setup: func(rig *w26aRig) {
				rig.assign(testUserID)
				rig.assignments.listErr = errW26aDatabase
			},
			wantStatus: http.StatusInternalServerError, wantCode: "INTERNAL_ERROR", wantType: "https://errors.bcbp.io/execution/internal-error", wantListed: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w26aNewRig(t, "all")
			rig.withPlanCheck = true
			tc.setup(rig)
			leadsBefore := w26aLeads(rig)

			rec := rig.post("claim", map[string]any{"record_version": w26aTaskVersion}, tc.admin)

			p := w26aProblemOf(t, rec, tc.wantStatus)
			assert.Equal(t, tc.wantCode, p.Code)
			assert.Equal(t, tc.wantType, p.Type)
			w26aRequireNoLeak(t, rec.Body.String())
			if tc.wantStatus == http.StatusInternalServerError {
				assert.Equal(t, w26aInternalDetail, p.Detail)
			}
			assert.Equal(t, []w26aLookup{{testTenantID, testTaskID}}, rig.tasks.reads, "the task is read once, in the caller's tenant")
			assert.Len(t, rig.assignments.lists, tc.wantListed)
			for _, l := range rig.assignments.lists {
				assert.Equal(t, w26aLookup{testTenantID, testTaskID}, l)
			}
			require.Len(t, rig.eligibility.calls, tc.wantEligible)
			for _, c := range rig.eligibility.calls {
				assert.Equal(t, w26aEligibilityCall{testTenantID, testUserID, rig.deptID, "approver"}, c, "the caller is checked for the stage's role in the task's department")
			}
			assert.Equal(t, leadsBefore, w26aLeads(rig), "no assignment changed hands")
			rig.requireTaskUntouched(t)
			rig.requireNoWorkflowEffect(t)
		})
	}
}

func w26aLeads(rig *w26aRig) []bool {
	out := make([]bool, len(rig.assignments.rows))
	for i, a := range rig.assignments.rows {
		out[i] = a.IsLead
	}
	return out
}

func TestClaimTask_ByAnActiveAssignee_Returns202WithTheClaimedTaskAndTheTaskReadsBackClaimed(t *testing.T) {
	rig := w26aNewRig(t, "all")
	rig.withPlanCheck = true
	caller := rig.assign(testUserID)
	other := rig.assign(uuid.New())
	before := obstest.Sum(t, "workflow_execution_service_task_signal_duration_seconds", map[string]string{"operation": "claim"})
	router := rig.router()

	rec := do(router, req(http.MethodPost, rig.taskPath("claim"), map[string]any{"record_version": w26aTaskVersion}))

	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	var claimed struct {
		ID            uuid.UUID `json:"id"`
		Status        string    `json:"status"`
		RecordVersion int64     `json:"record_version"`
		AssigneeCount *int      `json:"assignee_count"`
	}
	decodeJSON(t, rec.Body, &claimed)
	assert.Equal(t, testTaskID, claimed.ID)
	assert.Equal(t, "READY", claimed.Status)
	assert.EqualValues(t, w26aTaskVersion+1, claimed.RecordVersion, "the claim moved the task version by one")
	require.NotNil(t, claimed.AssigneeCount)
	assert.Equal(t, 2, *claimed.AssigneeCount)
	assert.Equal(t, []w26aClaim{{testTenantID, testTaskID, caller.ID, w26aTaskVersion}}, rig.assignments.claims, "the caller's own assignment is claimed at the version the caller sent")
	assert.Equal(t, []w26aEligibilityCall{{testTenantID, testUserID, rig.deptID, "approver"}}, rig.eligibility.calls)
	assert.Equal(t, []w26aLookup{{testTenantID, rig.versionID}}, rig.definitions.calls)
	rig.requireNoWorkflowEffect(t)
	after := obstest.Sum(t, "workflow_execution_service_task_signal_duration_seconds", map[string]string{"operation": "claim"})
	assert.Equal(t, before+1, after, "one claim latency sample is recorded under the claim label")

	get := do(router, req(http.MethodGet, rig.taskPath(""), nil))

	require.Equal(t, http.StatusOK, get.Code, get.Body.String())
	var detail struct {
		RecordVersion int64 `json:"record_version"`
		Assignments   []struct {
			ID     uuid.UUID `json:"id"`
			UserID uuid.UUID `json:"user_id"`
			IsLead bool      `json:"is_lead"`
		} `json:"assignments"`
	}
	decodeJSON(t, get.Body, &detail)
	assert.EqualValues(t, w26aTaskVersion+1, detail.RecordVersion)
	require.Len(t, detail.Assignments, 2)
	for _, a := range detail.Assignments {
		switch a.ID {
		case caller.ID:
			assert.True(t, a.IsLead, "the caller is lead")
		case other.ID:
			assert.False(t, a.IsLead, "the other assignee is not lead")
		default:
			t.Fatalf("unexpected assignment %s", a.ID)
		}
	}
}
