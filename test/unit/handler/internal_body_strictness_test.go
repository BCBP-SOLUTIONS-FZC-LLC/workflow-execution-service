package handler_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

const internalReassignPath = "/api/v1/internal/workflows/reassign-delegate"

func internalReassignRig(t *testing.T) (*[]port.ReassignDelegateInput, func(body string) (int, hhaProblem)) {
	var calls []port.ReassignDelegateInput
	fake := &fakeWorkflowClient{reassignDelegate: func(_ context.Context, in port.ReassignDelegateInput) (int, error) {
		calls = append(calls, in)
		return 1, nil
	}}
	router := hhaInternalRouter(handler.New(handler.Services{WorkflowClient: fake}))
	return &calls, func(body string) (int, hhaProblem) {
		w := hhaPostJSON(router, internalReassignPath, body)
		var p hhaProblem
		if w.Code != http.StatusOK {
			p = hhaDecodeProblem(t, w, w.Code)
		}
		return w.Code, p
	}
}

func reassignBody(tenantKey string, tenant, oldID, newID uuid.UUID) string {
	return `{"` + tenantKey + `":"` + tenant.String() + `","old_delegate_id":"` + oldID.String() + `","new_delegate_id":"` + newID.String() + `"}`
}

func TestInternalBody_ASecondJSONValueAfterTheFirstIsRefused(t *testing.T) {
	calls, send := internalReassignRig(t)
	body := reassignBody("tenant_id", testTenantID, uuid.New(), uuid.New())

	for name, trailing := range map[string]string{"object": `{"x":1}`, "garbage": `junk`, "scalar": `7`} {
		t.Run(name, func(t *testing.T) {
			status, p := send(body + trailing)

			require.Equal(t, http.StatusBadRequest, status)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			assert.Equal(t, "request body has data after its JSON value", p.Detail)
		})
	}
	assert.Empty(t, *calls, "a body with trailing data must not be processed")
}

func TestInternalBody_AFieldNameInTheWrongCaseIsNotBoundToTheField(t *testing.T) {
	calls, send := internalReassignRig(t)

	status, p := send(reassignBody("Tenant_ID", testTenantID, uuid.New(), uuid.New()))

	require.Equal(t, http.StatusBadRequest, status)
	assert.Equal(t, "BAD_REQUEST", p.Code)
	assert.Empty(t, *calls, "Tenant_ID must not bind as tenant_id")
}

func TestInternalBody_AWrongCaseSiblingCannotOverrideTheExactField(t *testing.T) {
	calls, send := internalReassignRig(t)
	oldID, newID := uuid.New(), uuid.New()
	body := `{"tenant_id":"` + testTenantID.String() + `","TENANT_ID":"` + uuid.New().String() +
		`","old_delegate_id":"` + oldID.String() + `","new_delegate_id":"` + newID.String() + `"}`

	status, _ := send(body)

	assert.Equal(t, http.StatusBadRequest, status)
	assert.Empty(t, *calls)
}

func TestInternalBody_AFieldTheServiceDoesNotKnowIsStillIgnored(t *testing.T) {
	calls, send := internalReassignRig(t)
	oldID, newID := uuid.New(), uuid.New()
	body := `{"tenant_id":"` + testTenantID.String() + `","old_delegate_id":"` + oldID.String() +
		`","new_delegate_id":"` + newID.String() + `","added_by_a_newer_sender":{"nested":true}}`

	status, _ := send(body)

	require.Equal(t, http.StatusOK, status)
	require.Len(t, *calls, 1)
	assert.Equal(t, port.ReassignDelegateInput{TenantID: testTenantID, OldDelegateID: oldID, NewDelegateID: newID}, (*calls)[0])
}

func TestInternalEvents_ASecondJSONValueAfterTheEnvelopeIsRefused(t *testing.T) {
	fakes := newEventsFakes()
	router := hhaInternalRouter(newEventsHandler(fakes))
	envelope := `{"id":"` + uuid.NewString() + `","type":"DelegationStarted","tenant_id":"` + testTenantID.String() + `","time":"2026-03-01T08:00:00Z","data":{}}`

	w := hhaPostJSON(router, "/api/v1/internal/events/delegation", envelope+`{"id":"other"}`)

	p := hhaDecodeProblem(t, w, http.StatusBadRequest)
	assert.Equal(t, "request body has data after its JSON value", p.Detail)
	assert.Empty(t, fakes.processedEvents.seen, "a refused envelope is not recorded as processed")
}

func TestInternalEvents_AMiscasedEnvelopeKeyIsNotBoundToTheField(t *testing.T) {
	fakes := newEventsFakes()
	router := hhaInternalRouter(newEventsHandler(fakes))
	envelope := `{"id":"` + uuid.NewString() + `","Type":"DelegationStarted","tenant_id":"` + testTenantID.String() + `","time":"2026-03-01T08:00:00Z","data":{}}`

	w := hhaPostJSON(router, "/api/v1/internal/events/delegation", envelope)

	hhaDecodeProblem(t, w, http.StatusBadRequest)
	assert.Empty(t, fakes.processedEvents.seen, "a refused envelope is not recorded as processed")
}

func TestFailConnectorTask_AFieldTheServiceDoesNotKnowIsIgnored(t *testing.T) {
	tasks := &hhaConnectorTasks{}
	router := hhaInternalRouter(handler.New(handler.Services{ConnectorTasks: tasks}))
	taskID := uuid.New()

	w := hhaPostJSON(router, hhaConnectorPath(taskID, "fail"), `{"tenant_id":"`+testTenantID.String()+`","error_class":"permanent","retry":true}`)

	require.Equal(t, http.StatusNoContent, w.Code, w.Body.String())
	require.Len(t, tasks.failCalls, 1)
	assert.Equal(t, hhaFailCall{tenantID: testTenantID, taskID: taskID, errorClass: "permanent"}, tasks.failCalls[0])
}
