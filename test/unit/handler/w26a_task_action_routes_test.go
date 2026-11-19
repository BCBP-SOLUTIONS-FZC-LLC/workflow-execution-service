package handler_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	adapterhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

const (
	w26aUpstreamMetric = "workflow_execution_service_upstream_dependency_errors_total"
	w26aConflictMetric = "workflow_execution_service_record_version_conflict_total"
	w26aSignalMetric   = "workflow_execution_service_task_signal_duration_seconds"
)

func w26aUpstream(t *testing.T, dependency string) float64 {
	t.Helper()
	return obstest.Sum(t, w26aUpstreamMetric, map[string]string{"dependency": dependency})
}

func w26aConflicts(t *testing.T, table string) float64 {
	t.Helper()
	return obstest.Sum(t, w26aConflictMetric, map[string]string{"table": table})
}

func TestCompleteTask_ByAnEligibleAssignee_Returns202AndTheSignalGoesToTheWorkflow(t *testing.T) {
	rig := w26aNewRig(t, "single")
	rig.withPlanCheck = true
	rig.assign(testUserID)
	before := obstest.Sum(t, w26aSignalMetric, map[string]string{"operation": "complete"})

	rec := rig.post("complete", map[string]any{"record_version": w26aTaskVersion, "result_json": map[string]any{"approved": true}}, false)

	require.Equal(t, http.StatusAccepted, rec.Code, rec.Body.String())
	var body struct {
		ID     uuid.UUID `json:"id"`
		Status string    `json:"status"`
	}
	decodeJSON(t, rec.Body, &body)
	assert.Equal(t, testTaskID, body.ID)
	assert.Equal(t, "READY", body.Status, "the answer is the task as it was acted on")
	assert.Equal(t, []w26aEligibilityCall{{testTenantID, testUserID, rig.deptID, "approver"}}, rig.eligibility.calls,
		"the caller is checked for the stage's role in the task's department")
	assert.Equal(t, []w26aLookup{{testTenantID, rig.versionID}}, rig.definitions.calls, "the plan is read for the instance's version")
	require.Len(t, rig.temporal.signals, 1)
	sent := rig.temporal.signals[0]
	assert.Equal(t, "tenant:subject:key", sent.workflowID)
	assert.Equal(t, testInstID, sent.instanceID)
	assert.Equal(t, "stage-transition", sent.name)
	var wire struct {
		DeptID        string
		NodeID        string
		UserID        string
		ResultJSON    string
		RecordVersion int64
	}
	w26aDecodePayload(t, sent.payload, &wire)
	assert.Equal(t, "finance", wire.DeptID)
	assert.Equal(t, "review", wire.NodeID)
	assert.Equal(t, testUserID.String(), wire.UserID)
	assert.JSONEq(t, `{"approved":true}`, wire.ResultJSON)
	assert.EqualValues(t, w26aTaskVersion, wire.RecordVersion)
	assert.Equal(t, before+1, obstest.Sum(t, w26aSignalMetric, map[string]string{"operation": "complete"}))
}

func TestCompleteTask_Refused_AnswersWithTheMappedProblemAndSendsNoSignal(t *testing.T) {
	cases := []struct {
		name         string
		withPlan     bool
		sentVersion  int64
		setup        func(rig *w26aRig)
		wantStatus   int
		wantCode     string
		wantIAM      float64
		wantConflict float64
		wantDefCalls int
		wantInstRead int
	}{
		{
			name: "the eligibility read is unavailable", withPlan: true, sentVersion: w26aTaskVersion,
			setup: func(rig *w26aRig) {
				rig.eligibility.err = fmt.Errorf("%w: iam answered 503", adapterhttp.ErrUpstreamUnavailable)
			},
			wantStatus: http.StatusServiceUnavailable, wantCode: "UPSTREAM_UNAVAILABLE", wantIAM: 1, wantDefCalls: 1, wantInstRead: 1,
		},
		{
			name: "the instance row is missing and no plan check is wired", sentVersion: w26aTaskVersion,
			setup:      func(rig *w26aRig) { rig.instances.rows = nil },
			wantStatus: http.StatusNotFound, wantCode: "INSTANCE_NOT_FOUND", wantInstRead: 1,
		},
		{
			name: "the instance row is missing and the plan check reads it first", withPlan: true, sentVersion: w26aTaskVersion,
			setup:      func(rig *w26aRig) { rig.instances.rows = nil },
			wantStatus: http.StatusNotFound, wantCode: "INSTANCE_NOT_FOUND", wantInstRead: 1,
		},
		{
			name: "the record version is behind the task's", sentVersion: w26aTaskVersion - 1,
			wantStatus: http.StatusConflict, wantCode: "RECORD_VERSION_CONFLICT", wantConflict: 1,
		},
		{
			name: "the record version is ahead of the task's", sentVersion: w26aTaskVersion + 1,
			wantStatus: http.StatusConflict, wantCode: "RECORD_VERSION_CONFLICT", wantConflict: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w26aNewRig(t, "single")
			rig.withPlanCheck = tc.withPlan
			rig.assign(testUserID)
			if tc.setup != nil {
				tc.setup(rig)
			}
			iamBefore, conflictBefore := w26aUpstream(t, "iam"), w26aConflicts(t, "workflow_task")

			rec := rig.post("complete", map[string]any{"record_version": tc.sentVersion}, false)

			p := w26aProblemOf(t, rec, tc.wantStatus)
			assert.NotContains(t, rec.Body.String(), "iam answered 503", "the upstream cause stays out of the answer")
			w26aRequireNoLeak(t, rec.Body.String())
			assert.Equal(t, tc.wantCode, p.Code)
			if tc.wantStatus == http.StatusServiceUnavailable {
				assert.Equal(t, w26aUpstreamType, p.Type)
			}
			assert.Equal(t, iamBefore+tc.wantIAM, w26aUpstream(t, "iam"))
			assert.Equal(t, conflictBefore+tc.wantConflict, w26aConflicts(t, "workflow_task"))
			assert.Equal(t, []w26aLookup{{testTenantID, testTaskID}}, rig.tasks.reads)
			assert.Len(t, rig.instances.reads, tc.wantInstRead)
			assert.Len(t, rig.definitions.calls, tc.wantDefCalls)
			for _, c := range rig.definitions.calls {
				assert.Equal(t, w26aLookup{testTenantID, rig.versionID}, c, "the plan is read for the caller's tenant and the instance's version")
			}
			if tc.wantConflict > 0 {
				assert.Empty(t, rig.assignments.lists, "a stale version is refused before the assignees are read")
			}
			rig.requireNoWorkflowEffect(t)
			rig.requireTaskUntouched(t)
		})
	}
}

func TestDeferTask_Refused_AnswersWithTheMappedProblemAndSendsNoUpdate(t *testing.T) {
	cases := []struct {
		name       string
		withPlan   bool
		setup      func(rig *w26aRig)
		wantStatus int
		wantCode   string
		wantIAM    float64
		wantReads  int
	}{
		{
			name: "a non-lead assignee of an all-assignee task", setup: func(rig *w26aRig) {
				rig.assign(uuid.New()).IsLead = true
				rig.assign(testUserID)
			},
			wantStatus: http.StatusForbidden, wantCode: "NOT_ASSIGNEE",
		},
		{
			name: "an all-assignee task nobody has claimed", setup: func(rig *w26aRig) {
				rig.assign(testUserID)
				rig.assign(uuid.New())
			},
			wantStatus: http.StatusForbidden, wantCode: "NOT_ASSIGNEE",
		},
		{
			name: "the eligibility read is unavailable", withPlan: true, setup: func(rig *w26aRig) {
				rig.assign(testUserID).IsLead = true
				rig.eligibility.err = fmt.Errorf("%w: iam answered 503", adapterhttp.ErrUpstreamUnavailable)
			},
			wantStatus: http.StatusServiceUnavailable, wantCode: "UPSTREAM_UNAVAILABLE", wantIAM: 1,
		},
		{
			name: "the instance row is missing", setup: func(rig *w26aRig) {
				rig.assign(testUserID).IsLead = true
				rig.instances.rows = nil
			},
			wantStatus: http.StatusNotFound, wantCode: "INSTANCE_NOT_FOUND", wantReads: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w26aNewRig(t, "all")
			rig.withPlanCheck = tc.withPlan
			rig.addCompletedStep()
			tc.setup(rig)
			iamBefore := w26aUpstream(t, "iam")

			rec := rig.post("defer", map[string]any{"record_version": w26aTaskVersion, "reason": "waiting for documents"}, false)

			p := w26aProblemOf(t, rec, tc.wantStatus)
			assert.NotContains(t, rec.Body.String(), "iam answered 503", "the upstream cause stays out of the answer")
			w26aRequireNoLeak(t, rec.Body.String())
			assert.Equal(t, tc.wantCode, p.Code)
			if tc.wantStatus == http.StatusServiceUnavailable {
				assert.Equal(t, w26aUpstreamType, p.Type)
			}
			assert.Equal(t, iamBefore+tc.wantIAM, w26aUpstream(t, "iam"))
			assert.Equal(t, []w26aLookup{{testTenantID, testTaskID}}, rig.tasks.reads)
			assert.Empty(t, rig.tasks.lists, "the instance's history is never read for a refused defer")
			assert.Len(t, rig.instances.reads, tc.wantReads+w26aPlanCheckInstanceReads(tc.withPlan))
			rig.requireNoWorkflowEffect(t)
			assert.EqualValues(t, w26aTaskVersion, rig.task.RecordVersion)
		})
	}
}

// w26aPlanCheckInstanceReads is the instance read the eligibility check makes before any later read.
func w26aPlanCheckInstanceReads(withPlan bool) int {
	if withPlan {
		return 1
	}
	return 0
}

func TestDeferTask_ByTheLeadOfAnAllAssigneeTask_Returns200WithTheDeferredTask(t *testing.T) {
	rig := w26aNewRig(t, "all")
	rig.withPlanCheck = true
	rig.addCompletedStep()
	lead := rig.assign(testUserID)
	lead.IsLead = true
	rig.assign(uuid.New())
	rig.temporal.onUpdate = func() { // the workflow's part: the task is deferred and its version moves by two
		rig.task.Status = domain.TaskStatusDeferred
		rig.task.RecordVersion += 2
	}
	before := obstest.Sum(t, w26aSignalMetric, map[string]string{"operation": "defer"})

	rec := rig.post("defer", map[string]any{"record_version": w26aTaskVersion, "reason": "waiting for documents"}, false)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var body struct {
		ID            uuid.UUID `json:"id"`
		Status        string    `json:"status"`
		RecordVersion int64     `json:"record_version"`
	}
	decodeJSON(t, rec.Body, &body)
	assert.Equal(t, testTaskID, body.ID)
	assert.Equal(t, "DEFERRED", body.Status)
	assert.EqualValues(t, w26aTaskVersion+2, body.RecordVersion)
	require.Len(t, rig.temporal.updates, 1)
	sent := rig.temporal.updates[0]
	assert.Equal(t, "tenant:subject:key", sent.workflowID)
	assert.Equal(t, testInstID, sent.instanceID)
	assert.Equal(t, "stage-defer", sent.name)
	var wire struct {
		DeptID        string
		FromStage     string
		Reason        string
		UserID        string
		RecordVersion int64
		TaskID        string
		AssignmentID  string
	}
	w26aDecodePayload(t, sent.payload, &wire)
	assert.Equal(t, "finance", wire.DeptID)
	assert.Equal(t, "review", wire.FromStage)
	assert.Equal(t, "waiting for documents", wire.Reason)
	assert.Equal(t, testUserID.String(), wire.UserID)
	assert.EqualValues(t, w26aTaskVersion, wire.RecordVersion)
	assert.Equal(t, testTaskID.String(), wire.TaskID)
	assert.Equal(t, lead.ID.String(), wire.AssignmentID, "the lead's own assignment is the one that is closed")
	assert.Empty(t, rig.temporal.signals)
	assert.Equal(t, before+1, obstest.Sum(t, w26aSignalMetric, map[string]string{"operation": "defer"}))
}

func TestTaskAction_TheReadBackFailsAfterTheChangeWasMade_AnswersWithTheCommittedVersion(t *testing.T) {
	cases := []struct {
		name       string
		op         string
		mode       string
		setup      func(rig *w26aRig)
		wantStatus int
		wantTask   string
		wantStored int64
	}{
		{
			name: "a claim moves the version by one", op: "claim", mode: "all", wantStatus: http.StatusAccepted, wantTask: "READY",
			setup:      func(rig *w26aRig) { rig.assign(testUserID) },
			wantStored: w26aTaskVersion + 1,
		},
		{
			name: "a defer marks the task deferred and moves the version by two", op: "defer", mode: "all", wantStatus: http.StatusOK, wantTask: "DEFERRED",
			setup: func(rig *w26aRig) {
				rig.assign(testUserID).IsLead = true
				rig.addCompletedStep()
			},
			wantStored: w26aTaskVersion + 2,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w26aNewRig(t, tc.mode)
			tc.setup(rig)
			rig.tasks.getErr = errW26aDatabase
			rig.tasks.failReadsAfter = 1 // the precheck read succeeds, every read after the change fails

			rec := rig.post(tc.op, map[string]any{"record_version": w26aTaskVersion}, false)

			require.Equal(t, tc.wantStatus, rec.Code, rec.Body.String())
			w26aRequireNoLeak(t, rec.Body.String())
			var body struct {
				ID            uuid.UUID `json:"id"`
				Status        string    `json:"status"`
				RecordVersion int64     `json:"record_version"`
			}
			decodeJSON(t, rec.Body, &body)
			assert.Equal(t, testTaskID, body.ID)
			assert.Equal(t, tc.wantTask, body.Status)
			assert.Equal(t, tc.wantStored, body.RecordVersion, "the answer carries the version the service worked out for the committed change")
			assert.Len(t, rig.tasks.reads, 3, "one read before the change and two failed read-backs")
		})
	}
}

func TestReassignTask_Failures_AnswerWithTheMappedProblemAndSignalNothingUnlessTheSignalFails(t *testing.T) {
	cases := []struct {
		name           string
		withPlan       bool
		setup          func(rig *w26aRig)
		wantStatus     int
		wantCode       string
		wantIAM        float64
		wantDefinition float64
		wantEligible   int
		wantSignals    int
		wantInstReads  int
	}{
		{
			name: "the eligibility read is unavailable", withPlan: true,
			setup: func(rig *w26aRig) {
				rig.eligibility.err = fmt.Errorf("%w: iam answered 503", adapterhttp.ErrUpstreamUnavailable)
			},
			wantStatus: http.StatusServiceUnavailable, wantCode: "UPSTREAM_UNAVAILABLE", wantIAM: 1, wantEligible: 1, wantInstReads: 1,
		},
		{
			name: "the compiled plan cannot be loaded from the Definition Service", withPlan: true,
			setup: func(rig *w26aRig) {
				rig.definitions.resp = nil
				rig.definitions.err = fmt.Errorf("%w: connection refused", port.ErrDefinitionUnavailable)
			},
			wantStatus: http.StatusServiceUnavailable, wantCode: "UPSTREAM_UNAVAILABLE", wantDefinition: 1, wantInstReads: 1,
		},
		{
			name: "the workflow engine cannot take the signal",
			setup: func(rig *w26aRig) {
				rig.temporal.signalErr = fmt.Errorf("signal: %w: dial tcp 10.1.2.3:7233", port.ErrTemporalUnavailable)
			},
			wantStatus: http.StatusServiceUnavailable, wantCode: "UPSTREAM_UNAVAILABLE", wantSignals: 1, wantInstReads: 1,
		},
		{
			name:       "the signal fails for any other reason",
			setup:      func(rig *w26aRig) { rig.temporal.signalErr = errW26aDatabase },
			wantStatus: http.StatusInternalServerError, wantCode: "INTERNAL_ERROR", wantSignals: 1, wantInstReads: 1,
		},
		{
			name:       "the instance read fails with a database error",
			setup:      func(rig *w26aRig) { rig.instances.getErr = errW26aDatabase },
			wantStatus: http.StatusInternalServerError, wantCode: "INTERNAL_ERROR", wantInstReads: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w26aNewRig(t, "single")
			rig.withPlanCheck = tc.withPlan
			holder := rig.assign(uuid.New())
			tc.setup(rig)
			newUser := uuid.New()
			iamBefore, defBefore := w26aUpstream(t, "iam"), w26aUpstream(t, "definition_service")

			rec := rig.post("reassign", map[string]any{"new_user_id": newUser, "record_version": w26aTaskVersion}, true)

			p := w26aProblemOf(t, rec, tc.wantStatus)
			assert.Equal(t, tc.wantCode, p.Code)
			w26aRequireNoLeak(t, rec.Body.String())
			assert.NotContains(t, rec.Body.String(), "10.1.2.3")
			assert.NotContains(t, rec.Body.String(), "iam answered 503", "the upstream cause stays out of the answer")
			assert.NotContains(t, rec.Body.String(), "connection refused", "the upstream cause stays out of the answer")
			if tc.wantStatus == http.StatusServiceUnavailable {
				assert.Equal(t, w26aUpstreamType, p.Type)
			}
			if tc.wantStatus == http.StatusInternalServerError {
				assert.Equal(t, w26aInternalDetail, p.Detail)
			}
			assert.Equal(t, iamBefore+tc.wantIAM, w26aUpstream(t, "iam"))
			assert.Equal(t, defBefore+tc.wantDefinition, w26aUpstream(t, "definition_service"))
			assert.Equal(t, []w26aLookup{{testTenantID, testTaskID}}, rig.tasks.reads)
			assert.Equal(t, []w26aLookup{{testTenantID, testTaskID}}, rig.assignments.lists)
			assert.Len(t, rig.instances.reads, tc.wantInstReads)
			require.Len(t, rig.eligibility.calls, tc.wantEligible)
			for _, c := range rig.eligibility.calls {
				assert.Equal(t, w26aEligibilityCall{testTenantID, newUser, rig.deptID, "approver"}, c, "the new assignee is the one checked")
			}
			wantPlanReads := 0
			if tc.withPlan {
				wantPlanReads = 1
			}
			require.Len(t, rig.definitions.calls, wantPlanReads)
			for _, c := range rig.definitions.calls {
				assert.Equal(t, w26aLookup{testTenantID, rig.versionID}, c, "the plan is read for the caller's tenant and the instance's version")
			}
			require.Len(t, rig.temporal.signals, tc.wantSignals)
			for _, s := range rig.temporal.signals {
				assert.Equal(t, "instance-reassign", s.name)
				var wire struct {
					TaskID        string
					OldUserID     string
					NewUserID     string
					AdminUserID   string
					RecordVersion int64
				}
				w26aDecodePayload(t, s.payload, &wire)
				assert.Equal(t, testTaskID.String(), wire.TaskID)
				assert.Equal(t, holder.UserID.String(), wire.OldUserID)
				assert.Equal(t, newUser.String(), wire.NewUserID)
				assert.Equal(t, testUserID.String(), wire.AdminUserID)
				assert.EqualValues(t, w26aTaskVersion, wire.RecordVersion)
			}
			assert.EqualValues(t, w26aTaskVersion, rig.task.RecordVersion)
		})
	}
}

func TestReassignTask_ATaskThatIsMissingOrAnotherTenants_Returns404AndReachesNothingElse(t *testing.T) {
	cases := []struct {
		name  string
		setup func(rig *w26aRig)
	}{
		{name: "the task id does not exist", setup: func(rig *w26aRig) { rig.tasks.rows = nil }},
		{name: "the task belongs to another tenant", setup: func(rig *w26aRig) { rig.task.TenantID = uuid.New() }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w26aNewRig(t, "single")
			rig.withPlanCheck = true
			rig.assign(uuid.New())
			tc.setup(rig)

			rec := rig.post("reassign", map[string]any{"new_user_id": uuid.New(), "record_version": w26aTaskVersion}, true)

			p := w26aProblemOf(t, rec, http.StatusNotFound)
			assert.Equal(t, "TASK_NOT_FOUND", p.Code)
			assert.Equal(t, []w26aLookup{{testTenantID, testTaskID}}, rig.tasks.reads, "the task is looked up once, in the caller's tenant")
			assert.Empty(t, rig.assignments.lists, "the assignees are never listed")
			assert.Empty(t, rig.instances.reads, "the instance is never read")
			assert.Empty(t, rig.eligibility.calls)
			rig.requireNoWorkflowEffect(t)
		})
	}
}
