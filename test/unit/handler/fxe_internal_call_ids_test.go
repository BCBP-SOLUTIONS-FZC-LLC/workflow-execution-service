package handler_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
)

const (
	fxeReasonRequired = "is required"
	fxeReasonNotValid = "is not valid"
	fxeAllZero        = "00000000-0000-0000-0000-000000000000"
)

type fxeInternalCase struct {
	name      string
	method    string
	target    string
	body      string
	wantField string
}

type fxeInternalRig struct {
	guard  *w15bArchiveGuard
	pauser *w15bUserTaskPauser
	client *w15bImpactClient
}

func fxeServe(t *testing.T, tc fxeInternalCase) (fxeInternalRig, w15bProblem) {
	t.Helper()
	rig := fxeInternalRig{guard: &w15bArchiveGuard{count: 1}, pauser: &w15bUserTaskPauser{}, client: &w15bImpactClient{}}
	router := w15bInternalRouter(handler.New(handler.Services{
		ArchiveGuard: rig.guard, UserTaskPauser: rig.pauser, WorkflowClient: rig.client,
	}))
	contentType := ""
	if tc.body != "" {
		contentType = "application/json"
	}
	w := w15bServe(router, tc.method, tc.target, contentType, tc.body)
	return rig, w15bDecodeProblem(t, w, http.StatusBadRequest)
}

func (r fxeInternalRig) assertNothingCalled(t *testing.T) {
	t.Helper()
	assert.Empty(t, r.guard.calls, "a refused request must not reach the archive guard")
	assert.Empty(t, r.pauser.calls, "a refused request must not pause any task")
	assert.Empty(t, r.client.calls, "a refused request must not reach the workflow client")
}

func fxeActive(workflowID, query string) string {
	target := w15bActivePath(workflowID)
	if query != "" {
		target += "?" + query
	}
	return target
}

func fxePausePath(userID string) string {
	return "/api/v1/internal/users/" + url.PathEscape(userID) + "/pause-tasks"
}

func fxeImpact(q url.Values) string {
	return "/api/v1/internal/workflows/delegate-impact?" + q.Encode()
}

func fxeImpactQuery(overrides map[string]string) url.Values {
	q := url.Values{"tenant_id": {uuid.NewString()}, "delegate_user_id": {uuid.NewString()}, "delegation_id": {uuid.NewString()}}
	for k, v := range overrides {
		q.Set(k, v)
	}
	return q
}

func fxeImpactWithout(key string) url.Values {
	q := fxeImpactQuery(nil)
	q.Del(key)
	return q
}

func assertSingleInvalidParam(t *testing.T, p w15bProblem, name, reason string) {
	t.Helper()
	assert.Equal(t, "BAD_REQUEST", p.Code)
	assert.Equal(t, w15bBadRequestType, p.Type)
	require.Len(t, p.InvalidParams, 1, "the refusal names exactly the offending field")
	assert.Equal(t, w15bInvalidParam{Name: name, Reason: reason}, p.InvalidParams[0])
}

func TestInternalCalls_AllZeroID_RefusedNamingTheField(t *testing.T) {
	t.Parallel()

	tenant, user := uuid.NewString(), uuid.NewString()
	paused := `{"tenant_id":"` + tenant + `"`
	cases := []fxeInternalCase{
		{"active-instances tenant_id", http.MethodGet, fxeActive(uuid.NewString(), "tenant_id="+fxeAllZero), "", "tenant_id"},
		{"pause-tasks user_id", http.MethodPost, fxePausePath(fxeAllZero), paused + `}`, "user_id"},
		{"pause-tasks tenant_id", http.MethodPost, fxePausePath(user), `{"tenant_id":"` + fxeAllZero + `"}`, "tenant_id"},
		{"pause-tasks department_id", http.MethodPost, fxePausePath(user), paused + `,"department_id":"` + fxeAllZero + `"}`, "department_id"},
		{"delegate-impact tenant_id", http.MethodGet, fxeImpact(fxeImpactQuery(map[string]string{"tenant_id": fxeAllZero})), "", "tenant_id"},
		{"delegate-impact delegate_user_id", http.MethodGet, fxeImpact(fxeImpactQuery(map[string]string{"delegate_user_id": fxeAllZero})), "", "delegate_user_id"},
		{"delegate-impact delegation_id", http.MethodGet, fxeImpact(fxeImpactQuery(map[string]string{"delegation_id": fxeAllZero})), "", "delegation_id"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rig, got := fxeServe(t, tc)

			assertSingleInvalidParam(t, got, tc.wantField, fxeReasonNotValid)
			rig.assertNothingCalled(t)
		})
	}
}

func TestInternalCalls_AbsentField_ReasonIsRequired(t *testing.T) {
	t.Parallel()

	user := uuid.NewString()
	cases := []fxeInternalCase{
		{"active-instances tenant_id", http.MethodGet, fxeActive(uuid.NewString(), ""), "", "tenant_id"},
		{"pause-tasks tenant_id absent", http.MethodPost, fxePausePath(user), `{}`, "tenant_id"},
		{"pause-tasks tenant_id null", http.MethodPost, fxePausePath(user), `{"tenant_id":null}`, "tenant_id"},
		{"delegate-impact tenant_id", http.MethodGet, fxeImpact(fxeImpactWithout("tenant_id")), "", "tenant_id"},
		{"delegate-impact delegate_user_id", http.MethodGet, fxeImpact(fxeImpactWithout("delegate_user_id")), "", "delegate_user_id"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rig, got := fxeServe(t, tc)

			assertSingleInvalidParam(t, got, tc.wantField, fxeReasonRequired)
			rig.assertNothingCalled(t)
		})
	}
}

func TestInternalCalls_MalformedField_ReasonIsNotValid(t *testing.T) {
	t.Parallel()

	user := uuid.NewString()
	cases := []fxeInternalCase{
		{"active-instances tenant_id not a UUID", http.MethodGet, fxeActive(uuid.NewString(), "tenant_id=nope"), "", "tenant_id"},
		{"active-instances tenant_id empty", http.MethodGet, fxeActive(uuid.NewString(), "tenant_id="), "", "tenant_id"},
		{"pause-tasks user_id not a UUID", http.MethodPost, fxePausePath("nope"), `{"tenant_id":"` + uuid.NewString() + `"}`, "user_id"},
		{"pause-tasks tenant_id not a UUID", http.MethodPost, fxePausePath(user), `{"tenant_id":"nope"}`, "tenant_id"},
		{"delegate-impact tenant_id not a UUID", http.MethodGet, fxeImpact(fxeImpactQuery(map[string]string{"tenant_id": "nope"})), "", "tenant_id"},
		{"delegate-impact tenant_id empty", http.MethodGet, fxeImpact(fxeImpactQuery(map[string]string{"tenant_id": ""})), "", "tenant_id"},
		{"delegate-impact delegate_user_id not a UUID", http.MethodGet, fxeImpact(fxeImpactQuery(map[string]string{"delegate_user_id": "12345"})), "", "delegate_user_id"},
		{"delegate-impact delegation_id not a UUID", http.MethodGet, fxeImpact(fxeImpactQuery(map[string]string{"delegation_id": "nope"})), "", "delegation_id"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rig, got := fxeServe(t, tc)

			assertSingleInvalidParam(t, got, tc.wantField, fxeReasonNotValid)
			rig.assertNothingCalled(t)
		})
	}
}

func TestInternalCalls_ValidIDs_StillAccepted(t *testing.T) {
	t.Parallel()

	tenant, user, workflow, delegate, delegation, department := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	guard, pauser, client := &w15bArchiveGuard{count: 2}, &w15bUserTaskPauser{}, &w15bImpactClient{}
	router := w15bInternalRouter(handler.New(handler.Services{ArchiveGuard: guard, UserTaskPauser: pauser, WorkflowClient: client}))

	active := w15bServe(router, http.MethodGet, fxeActive(workflow.String(), "tenant_id="+tenant.String()), "", "")
	pause := w15bServe(router, http.MethodPost, fxePausePath(user.String()), "application/json",
		`{"tenant_id":"`+tenant.String()+`","department_id":"`+department.String()+`"}`)
	impact := w15bServe(router, http.MethodGet, fxeImpact(url.Values{
		"tenant_id": {tenant.String()}, "delegate_user_id": {delegate.String()}, "delegation_id": {delegation.String()},
	}), "", "")

	assert.Equal(t, http.StatusOK, active.Code, active.Body.String())
	assert.Equal(t, http.StatusNoContent, pause.Code, pause.Body.String())
	assert.Equal(t, http.StatusOK, impact.Code, impact.Body.String())
	assert.Equal(t, []w15bGuardCall{{tenant, workflow}}, guard.calls)
	require.Len(t, pauser.calls, 1)
	assert.Equal(t, tenant, pauser.calls[0].tenantID)
	assert.Equal(t, user, pauser.calls[0].userID)
	require.NotNil(t, pauser.calls[0].departmentID)
	assert.Equal(t, department, *pauser.calls[0].departmentID)
	require.Len(t, client.calls, 1)
	assert.Equal(t, tenant, client.calls[0].TenantID)
	assert.Equal(t, delegate, client.calls[0].DelegateUserID)
	require.NotNil(t, client.calls[0].DelegationID)
	assert.Equal(t, delegation, *client.calls[0].DelegationID)
}

func TestInternalCalls_PauseTasksAllZeroTenantAndDepartment_ListsBothFields(t *testing.T) {
	t.Parallel()

	tc := fxeInternalCase{
		name: "both", method: http.MethodPost, target: fxePausePath(uuid.NewString()),
		body: `{"tenant_id":"` + fxeAllZero + `","department_id":"` + fxeAllZero + `"}`,
	}

	rig, got := fxeServe(t, tc)

	assert.Equal(t, "BAD_REQUEST", got.Code)
	assert.Equal(t, []w15bInvalidParam{
		{Name: "tenant_id", Reason: fxeReasonNotValid},
		{Name: "department_id", Reason: fxeReasonNotValid},
	}, got.InvalidParams)
	rig.assertNothingCalled(t)
}
