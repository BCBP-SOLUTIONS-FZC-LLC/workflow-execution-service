package handler_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

const (
	valfixNotJSONDetail = "request body is not valid JSON"
	valfixInstancesPath = "/api/v1/instances"
	valfixRecordVersion = `"record_version":-1`
)

type valfixCalls struct {
	n int
}

// valfixRecordingServices fakes every service a public route can reach and
// counts each call, so a refused request can be shown to have reached none.
func valfixRecordingServices(calls *valfixCalls) (*fakeTaskService, *fakeInstanceService) {
	task := newTestTask()
	tasks := &fakeTaskService{
		claim: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, int64) (*port.Task, error) {
			calls.n++
			return task, nil
		},
		complete: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, json.RawMessage, int64) (*port.Task, error) {
			calls.n++
			return task, nil
		},
		deferTask: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string, int64) (*port.Task, error) {
			calls.n++
			return task, nil
		},
		reassign: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, uuid.UUID, int64) (*port.Task, error) {
			calls.n++
			return task, nil
		},
		overrideAssignee: func(_ context.Context, in port.AssigneeOverrideInput) (*port.AssigneeOverride, error) {
			calls.n++
			return &port.AssigneeOverride{Reason: in.Reason}, nil
		},
	}
	instances := &fakeInstanceService{
		start: func(context.Context, port.StartInstanceInput) (*port.Instance, error) {
			calls.n++
			return &port.Instance{}, nil
		},
		pause: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string, int64) error {
			calls.n++
			return nil
		},
		resume: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, int64) error {
			calls.n++
			return nil
		},
		cancel: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string, int64) error {
			calls.n++
			return nil
		},
		terminate: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string) error {
			calls.n++
			return nil
		},
		forceForward: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string, int64) error {
			calls.n++
			return nil
		},
		forceBack: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string, int64) error {
			calls.n++
			return nil
		},
	}
	return tasks, instances
}

func valfixPublicHandler(calls *valfixCalls) *handler.Handler {
	tasks, instances := valfixRecordingServices(calls)
	return handler.New(handler.Services{Tasks: tasks, Instances: instances, Eligibility: &fakeEligibilityChecker{}})
}

func valfixPost(path, body string) *http.Request {
	return hreRawRequest(http.MethodPost, path, body)
}

func valfixParamNames(p hreProblem) []string {
	names := make([]string, len(p.InvalidParams))
	for i, param := range p.InvalidParams {
		names[i] = param.Name
	}
	return names
}

func valfixAssertRefused(t *testing.T, w *httptest.ResponseRecorder, calls *valfixCalls, field, reason string) {
	t.Helper()
	require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
	p := hreDecodeProblem(t, w)
	assert.Equal(t, "BAD_REQUEST", p.Code)
	assert.NotEqual(t, valfixNotJSONDetail, p.Detail)
	assert.Contains(t, p.InvalidParams, hreParam{Name: field, Reason: reason})
	hreAssertNoInternalText(t, w)
	assert.Zero(t, calls.n, "a refused request must not reach a service")
}

func TestValfix_StartInstance_NonUUIDValuesNameTheField(t *testing.T) {
	good := uuid.NewString()
	cases := []struct {
		name  string
		body  string
		field string
	}{
		{"version id", `{"subject_id":"s1","business_key":"bk","workflow_version_id":"not-a-uuid"}`, "workflow_version_id"},
		{"override map user", `{"subject_id":"s1","business_key":"bk","workflow_version_id":"` + good + `","override_map":{"finance/review":"nope"}}`, "override_map.finance/review"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls valfixCalls
			router := newRouter(valfixPublicHandler(&calls))

			w := do(router, valfixPost(valfixInstancesPath, tc.body))

			valfixAssertRefused(t, w, &calls, tc.field, "is not valid")
		})
	}
}

func TestValfix_StartInstance_EveryBadValueIsNamed(t *testing.T) {
	var calls valfixCalls
	router := newRouter(valfixPublicHandler(&calls))
	body := `{"subject_id":"s1","business_key":"bk","workflow_version_id":"x","override_map":{"a":"y","b":"` + uuid.NewString() + `"}}`

	w := do(router, valfixPost(valfixInstancesPath, body))

	require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
	p := hreDecodeProblem(t, w)
	assert.ElementsMatch(t, []string{"workflow_version_id", "override_map.a"}, valfixParamNames(p))
	assert.Zero(t, calls.n)
}

func TestValfix_UnknownFieldStillWinsOverBadUUID(t *testing.T) {
	var calls valfixCalls
	router := newRouter(valfixPublicHandler(&calls))

	w := do(router, valfixPost(valfixInstancesPath, `{"workflow_version_id":"x","surprise":1}`))

	require.Equal(t, http.StatusBadRequest, w.Code)
	p := hreDecodeProblem(t, w)
	assert.Equal(t, []string{"surprise"}, valfixParamNames(p))
}

func TestValfix_PublicRoutes_NonUUIDUserIDNamesTheField(t *testing.T) {
	cases := []struct{ name, path, body string }{
		{"reassign task", hreTaskPath(testTaskID.String(), "reassign"), `{"new_user_id":"abc","record_version":1}`},
		{"override node", overridePath(testInstID, "review"), `{"new_user_id":"abc","record_version":4}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls valfixCalls
			router := newRouter(valfixPublicHandler(&calls))

			w := do(router, valfixPost(tc.path, tc.body))

			valfixAssertRefused(t, w, &calls, "new_user_id", "is not valid")
		})
	}
}

func valfixIDBody(fields []string, bad string) string {
	parts := make([]string, len(fields))
	for i, f := range fields {
		value := uuid.NewString()
		if f == bad {
			value = "zzz"
		}
		parts[i] = `"` + f + `":"` + value + `"`
	}
	return "{" + strings.Join(parts, ",") + "}"
}

func TestValfix_InternalBodies_NonUUIDIDsNameTheField(t *testing.T) {
	routes := []struct {
		name, path string
		fields     []string
	}{
		{"reassign delegate", "/api/v1/internal/workflows/reassign-delegate", []string{"tenant_id", "old_delegate_id", "new_delegate_id", "delegation_id"}},
		{"cancel by delegate", "/api/v1/internal/workflows/cancel-by-delegate", []string{"tenant_id", "delegate_user_id", "delegation_id"}},
		{"pause user tasks", "/api/v1/internal/users/" + uuid.NewString() + "/pause-tasks", []string{"tenant_id", "department_id"}},
		{"complete connector task", "/api/v1/internal/connector-tasks/" + uuid.NewString() + "/complete", []string{"tenant_id"}},
	}
	for _, route := range routes {
		for _, field := range route.fields {
			t.Run(route.name+" "+field, func(t *testing.T) {
				wc := &hhaWorkflowClient{}
				pauser := &hhaUserTaskPauser{}
				connector := &hhaConnectorTasks{}
				router := hhaInternalRouter(handler.New(handler.Services{WorkflowClient: wc, UserTaskPauser: pauser, ConnectorTasks: connector}))

				w := hhaPostJSON(router, route.path, valfixIDBody(route.fields, field))

				p := hhaDecodeProblem(t, w, http.StatusBadRequest)
				assert.Equal(t, "BAD_REQUEST", p.Code)
				assert.NotEqual(t, valfixNotJSONDetail, p.Detail)
				assert.Equal(t, []hhaInvalidParam{{Name: field, Reason: "is not valid"}}, p.InvalidParams)
				hhaAssertNoInternalText(t, w)
				assert.Empty(t, wc.reassignCalls)
				assert.Empty(t, wc.cancelCalls)
				assert.Empty(t, pauser.calls)
				assert.Empty(t, connector.completeCalls)
			})
		}
	}
}

func TestValfix_FailConnectorTask_NonUUIDTenantNamesTheField(t *testing.T) {
	connector := &hhaConnectorTasks{}
	router := hhaInternalRouter(handler.New(handler.Services{ConnectorTasks: connector}))

	w := hhaPostJSON(router, hhaConnectorPath(uuid.New(), "fail"), `{"tenant_id":"zzz","error_class":"x"}`)

	p := hhaDecodeProblem(t, w, http.StatusBadRequest)
	assert.Equal(t, []hhaInvalidParam{{Name: "tenant_id", Reason: "is not valid"}}, p.InvalidParams)
	assert.Empty(t, connector.failCalls)
}

func TestValfix_PathIDsNotUUID_NameThePathParameter(t *testing.T) {
	cases := []struct {
		name, method, path, param string
	}{
		{"instance pause", http.MethodPost, valfixInstancesPath + "/abc/pause", "id"},
		{"instance cancel", http.MethodPost, valfixInstancesPath + "/abc/cancel", "id"},
		{"instance get", http.MethodGet, valfixInstancesPath + "/abc", "id"},
		{"task claim", http.MethodPost, "/api/v1/tasks/abc/claim", "id"},
		{"task get", http.MethodGet, "/api/v1/tasks/abc", "id"},
		{"node override", http.MethodPost, valfixInstancesPath + "/abc/nodes/review/override", "id"},
		{"outside answer", http.MethodPost, valfixInstancesPath + "/abc/nodes/review/complete", "id"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls valfixCalls
			router := newRouter(valfixPublicHandler(&calls))
			r := hreRawRequest(tc.method, tc.path, `{"record_version":1,"reason":"r"}`)

			w := do(router, r)

			require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
			p := hreDecodeProblem(t, w)
			assert.Equal(t, "invalid "+tc.param+" path parameter", p.Detail)
			assert.Equal(t, []hreParam{{Name: tc.param, Reason: "is not valid"}}, p.InvalidParams)
			assert.Zero(t, calls.n)
		})
	}
}

func TestValfix_InternalPathIDsNotUUID_NameThePathParameter(t *testing.T) {
	cases := []struct{ name, method, path, param string }{
		{"active instances", http.MethodGet, "/api/v1/internal/workflows/abc/active-instances?tenant_id=" + uuid.NewString(), "workflow_id"},
		{"pause tasks", http.MethodPost, "/api/v1/internal/users/abc/pause-tasks", "user_id"},
		{"connector complete", http.MethodPost, "/api/v1/internal/connector-tasks/abc/complete", "id"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			router := hhaInternalRouter(handler.New(handler.Services{
				ArchiveGuard: &hhaArchiveGuard{}, UserTaskPauser: &hhaUserTaskPauser{}, ConnectorTasks: &hhaConnectorTasks{},
			}))

			w := hhaServe(router, tc.method, tc.path, "application/json", `{"tenant_id":"`+uuid.NewString()+`"}`)

			p := hhaDecodeProblem(t, w, http.StatusBadRequest)
			assert.Equal(t, []hhaInvalidParam{{Name: tc.param, Reason: "is not valid"}}, p.InvalidParams)
		})
	}
}

func TestValfix_RecordVersionBelowOne_Refused(t *testing.T) {
	user := uuid.NewString()
	routes := []struct{ name, path, body string }{
		{"claim", hreTaskPath(testTaskID.String(), "claim"), `{` + valfixRecordVersion + `}`},
		{"complete", hreTaskPath(testTaskID.String(), "complete"), `{` + valfixRecordVersion + `}`},
		{"defer", hreTaskPath(testTaskID.String(), "defer"), `{` + valfixRecordVersion + `}`},
		{"reassign", hreTaskPath(testTaskID.String(), "reassign"), `{"new_user_id":"` + user + `",` + valfixRecordVersion + `}`},
		{"override", overridePath(testInstID, "review"), `{"new_user_id":"` + user + `",` + valfixRecordVersion + `}`},
		{"pause", instancePath(testInstID) + "/pause", `{` + valfixRecordVersion + `}`},
		{"resume", instancePath(testInstID) + "/resume", `{` + valfixRecordVersion + `}`},
		{"cancel", instancePath(testInstID) + "/cancel", `{"reason":"why",` + valfixRecordVersion + `}`},
		{"force forward", instancePath(testInstID) + "/force-forward", `{"target_node_key":"n",` + valfixRecordVersion + `}`},
		{"force back", instancePath(testInstID) + "/force-back", `{` + valfixRecordVersion + `}`},
	}
	for _, rt := range routes {
		t.Run(rt.name, func(t *testing.T) {
			var calls valfixCalls
			router := newRouter(valfixPublicHandler(&calls))

			w := do(router, valfixPost(rt.path, rt.body))

			valfixAssertRefused(t, w, &calls, "record_version", "is not valid")
		})
	}
}

func TestValfix_BlankReasonRefusedOnCancelAndTerminate(t *testing.T) {
	routes := []struct{ name, path, extra string }{
		{"cancel", instancePath(testInstID) + "/cancel", `,"record_version":1`},
		{"terminate", instancePath(testInstID) + "/terminate", ``},
	}
	for _, rt := range routes {
		for _, reason := range []string{"   ", "\t\n ", " "} {
			t.Run(rt.name+" "+strings.TrimSpace(reason), func(t *testing.T) {
				var calls valfixCalls
				router := newRouter(valfixPublicHandler(&calls))

				w := do(router, valfixPost(rt.path, `{"reason":"`+strings.NewReplacer("\t", `\t`, "\n", `\n`).Replace(reason)+`"`+rt.extra+`}`))

				valfixAssertRefused(t, w, &calls, "reason", "is not valid")
			})
		}
		t.Run(rt.name+" real text still accepted", func(t *testing.T) {
			var calls valfixCalls
			router := newRouter(valfixPublicHandler(&calls))

			w := do(router, valfixPost(rt.path, `{"reason":"  needs a human  "`+rt.extra+`}`))

			assert.Less(t, w.Code, 300, w.Body.String())
			assert.Equal(t, 1, calls.n)
		})
	}
}

func TestValfix_OverrideReason_EmptyOrBlankRefusedOmittedAndNullAccepted(t *testing.T) {
	user := uuid.NewString()
	base := `"new_user_id":"` + user + `","record_version":4`
	for _, reason := range []string{`""`, `"   "`} {
		t.Run("refused "+reason, func(t *testing.T) {
			var calls valfixCalls
			router := newRouter(valfixPublicHandler(&calls))

			w := do(router, valfixPost(overridePath(testInstID, "review"), `{`+base+`,"reason":`+reason+`}`))

			valfixAssertRefused(t, w, &calls, "reason", "is not valid")
		})
	}
	for name, body := range map[string]string{"omitted": `{` + base + `}`, "null": `{` + base + `,"reason":null}`} {
		t.Run("accepted "+name, func(t *testing.T) {
			var gotReason []string
			tasks := &fakeTaskService{
				overrideAssignee: func(_ context.Context, in port.AssigneeOverrideInput) (*port.AssigneeOverride, error) {
					gotReason = append(gotReason, in.Reason)
					return &port.AssigneeOverride{}, nil
				},
			}
			router := newRouter(newHandler(tasks, &fakeEligibilityChecker{}))

			w := do(router, valfixPost(overridePath(testInstID, "review"), body))

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			assert.Equal(t, []string{""}, gotReason)
		})
	}
}

func TestValfix_ActiveInstances_AllZeroIDsRefused(t *testing.T) {
	zero := uuid.Nil.String()
	cases := []struct{ name, workflow, tenant, field string }{
		{"tenant", uuid.NewString(), zero, "tenant_id"},
		{"workflow", zero, uuid.NewString(), "workflow_id"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			guard := &hhaArchiveGuard{}
			router := hhaInternalRouter(handler.New(handler.Services{ArchiveGuard: guard}))

			w := hhaServe(router, http.MethodGet, hhaActiveInstancesPath(uuid.MustParse(tc.workflow))+"?tenant_id="+tc.tenant, "", "")

			p := hhaDecodeProblem(t, w, http.StatusBadRequest)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			assert.Equal(t, []hhaInvalidParam{{Name: tc.field, Reason: "is not valid"}}, p.InvalidParams)
			assert.Empty(t, guard.calls, "a malformed request must not reach the archive guard")
		})
	}
}

// valfixOutsideRequest sends the body with the framing given: a known length,
// or an unknown one as a chunked client would.
func valfixOutsideRequest(path, body string, chunked bool) *http.Request {
	r := httptest.NewRequest(http.MethodPost, path, io.NopCloser(bytes.NewReader([]byte(body))))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("x-tenant-id", testTenantID.String())
	r.Header.Set("x-user-id", testUserID.String())
	r.Header.Set("x-tenant-roles", "tenant_admin")
	if chunked {
		r.ContentLength = -1
	} else {
		r.ContentLength = int64(len(body))
	}
	return r
}

func TestValfix_OutsideAnswer_EmptyOrBlankBodyCountsAsAbsent(t *testing.T) {
	routes := map[string]string{
		"complete": completePath("review"),
		"fire":     firePath("review", "Boundary_1"),
	}
	bodies := map[string]string{"empty": "", "spaces": "   ", "newlines": "\r\n\t \n"}
	for routeName, path := range routes {
		for bodyName, body := range bodies {
			for _, chunked := range []bool{false, true} {
				name := routeName + " " + bodyName
				if chunked {
					name += " chunked"
				}
				t.Run(name, func(t *testing.T) {
					var got answered
					router := newRouter(newInstanceHandler(recordingInstances(t, &got, nil)))

					w := do(router, valfixOutsideRequest(path, body, chunked))

					require.Equal(t, http.StatusAccepted, w.Code, w.Body.String())
					assert.Equal(t, 1, got.calls)
					assert.Empty(t, got.variables)
				})
			}
		}
	}
}

func TestValfix_OutsideAnswer_NonEmptyNonJSONBodyStillRefused(t *testing.T) {
	for _, chunked := range []bool{false, true} {
		var got answered
		router := newRouter(newInstanceHandler(recordingInstances(t, &got, nil)))

		w := do(router, valfixOutsideRequest(completePath("review"), `{bad`, chunked))

		require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
		assert.Zero(t, got.calls)
	}
}

func TestValfix_OverrideNodeKey_NULOrInvalidUTF8Refused(t *testing.T) {
	cases := map[string]string{"nul byte": "review%00x", "invalid utf-8": "review%ff"}
	for name, node := range cases {
		t.Run(name, func(t *testing.T) {
			var calls valfixCalls
			router := newRouter(valfixPublicHandler(&calls))

			w := do(router, valfixPost(overridePath(testInstID, node), `{"new_user_id":"`+uuid.NewString()+`","record_version":4}`))

			valfixAssertRefused(t, w, &calls, "node_key", "is not valid")
		})
	}
}
