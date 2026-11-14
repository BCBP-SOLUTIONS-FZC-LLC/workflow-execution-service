package handler_test

import (
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
)

func hhaConnectorPath(taskID uuid.UUID, action string) string {
	return "/api/v1/internal/connector-tasks/" + taskID.String() + "/" + action
}

// A tenant_id that is not a UUID string is refused with a 400; the detail
// wording for it is deliberately not asserted.
func TestCompleteConnectorTask_TenantIDNotAUUID_Returns400(t *testing.T) {
	tasks := &hhaConnectorTasks{}
	router := hhaInternalRouter(handler.New(handler.Services{ConnectorTasks: tasks}))

	w := hhaPostJSON(router, hhaConnectorPath(uuid.New(), "complete"), `{"tenant_id":"not-a-uuid","output":{}}`)

	p := hhaDecodeProblem(t, w, http.StatusBadRequest)
	assert.Equal(t, "BAD_REQUEST", p.Code)
	hhaAssertNoInternalText(t, w)
	assert.Empty(t, tasks.completeCalls, "a refused body must not complete the task")
}

func TestCompleteConnectorTask_OutputNotAnObject_Returns400(t *testing.T) {
	cases := []struct{ name, output string }{
		{"string", `"done"`},
		{"number", `7`},
		{"array", `["a"]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tasks := &hhaConnectorTasks{}
			router := hhaInternalRouter(handler.New(handler.Services{ConnectorTasks: tasks}))

			w := hhaPostJSON(router, hhaConnectorPath(uuid.New(), "complete"),
				`{"tenant_id":"`+uuid.NewString()+`","output":`+tc.output+`}`)

			p := hhaDecodeProblem(t, w, http.StatusBadRequest)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			assert.Equal(t, "request body has a field of the wrong type", p.Detail)
			require.Len(t, p.InvalidParams, 1)
			assert.Equal(t, "output", p.InvalidParams[0].Name)
			assert.Equal(t, "has the wrong type", p.InvalidParams[0].Reason)
			hhaAssertNoInternalText(t, w)
			assert.Empty(t, tasks.completeCalls, "a refused body must not complete the task")
		})
	}
}

func TestFailConnectorTask_NilOrNullTenantID_Returns400(t *testing.T) {
	cases := []struct{ name, tenant string }{
		{"nil uuid", `"00000000-0000-0000-0000-000000000000"`},
		{"null", `null`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tasks := &hhaConnectorTasks{}
			router := hhaInternalRouter(handler.New(handler.Services{ConnectorTasks: tasks}))

			w := hhaPostJSON(router, hhaConnectorPath(uuid.New(), "fail"),
				`{"tenant_id":`+tc.tenant+`,"error_class":"permanent"}`)

			p := hhaDecodeProblem(t, w, http.StatusBadRequest)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			require.Len(t, p.InvalidParams, 1)
			assert.Equal(t, "tenant_id", p.InvalidParams[0].Name)
			assert.Equal(t, "is required", p.InvalidParams[0].Reason)
			hhaAssertNoInternalText(t, w)
			assert.Empty(t, tasks.failCalls, "a refused body must not fail the task")
		})
	}
}

func TestFailConnectorTask_BodyNotJSON_Returns400(t *testing.T) {
	cases := []struct{ name, body string }{
		{"invalid json", `{"tenant_id":`},
		{"not an object start", `tenant_id=1`},
		{"empty body", ``},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tasks := &hhaConnectorTasks{}
			router := hhaInternalRouter(handler.New(handler.Services{ConnectorTasks: tasks}))

			w := hhaPostJSON(router, hhaConnectorPath(uuid.New(), "fail"), tc.body)

			p := hhaDecodeProblem(t, w, http.StatusBadRequest)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			assert.Equal(t, "request body is not valid JSON", p.Detail)
			assert.Empty(t, p.InvalidParams)
			hhaAssertNoInternalText(t, w)
			assert.Empty(t, tasks.failCalls, "a refused body must not fail the task")
		})
	}
}

func TestFailConnectorTask_WrongJSONTypes_Returns400(t *testing.T) {
	tenant := `"tenant_id":"` + uuid.NewString() + `"`
	cases := []struct {
		name, body, wantField string
	}{
		{"error_class as a number", `{` + tenant + `,"error_class":5}`, "error_class"},
		{"output as a string", `{` + tenant + `,"error_class":"permanent","output":"oops"}`, "output"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tasks := &hhaConnectorTasks{}
			router := hhaInternalRouter(handler.New(handler.Services{ConnectorTasks: tasks}))

			w := hhaPostJSON(router, hhaConnectorPath(uuid.New(), "fail"), tc.body)

			p := hhaDecodeProblem(t, w, http.StatusBadRequest)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			require.Len(t, p.InvalidParams, 1)
			assert.Equal(t, tc.wantField, p.InvalidParams[0].Name)
			assert.Equal(t, "has the wrong type", p.InvalidParams[0].Reason)
			hhaAssertNoInternalText(t, w)
			assert.Empty(t, tasks.failCalls, "a refused body must not fail the task")
		})
	}
}
