package contract_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-gincommon/pkg/gincommon"

	httpadapter "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/contract"
)

const publicSpec = "../../docs/swagger/openapi.yaml"

type instanceFake struct {
	port.InstanceService
	instance *port.Instance
	tasks    []*port.Task
	events   []*port.WorkflowEvent
}

func (f instanceFake) Start(context.Context, port.StartInstanceInput) (*port.Instance, error) {
	return f.instance, nil
}

func (f instanceFake) List(context.Context, uuid.UUID, port.ReadScope, port.InstanceFilter, port.Page) (port.PageResult[*port.Instance], error) {
	return port.PageResult[*port.Instance]{Items: []*port.Instance{f.instance}}, nil
}

func (f instanceFake) Get(context.Context, uuid.UUID, uuid.UUID, port.ReadScope) (*port.Instance, []*port.Task, error) {
	return f.instance, f.tasks, nil
}

func (instanceFake) Pause(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string, int64) error {
	return nil
}
func (instanceFake) Resume(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, int64) error { return nil }
func (instanceFake) Cancel(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string, int64) error {
	return nil
}
func (instanceFake) Terminate(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string) error {
	return nil
}

func (f instanceFake) ListEvents(context.Context, uuid.UUID, uuid.UUID, port.ReadScope, port.Page) (port.PageResult[*port.WorkflowEvent], error) {
	return port.PageResult[*port.WorkflowEvent]{Items: f.events}, nil
}

type taskFake struct {
	port.TaskService
	task        *port.Task
	assignments []*port.TaskAssignment
}

func (f taskFake) List(context.Context, uuid.UUID, port.ReadScope, port.TaskFilter, port.Page) (port.PageResult[*port.Task], error) {
	return port.PageResult[*port.Task]{Items: []*port.Task{f.task}}, nil
}

func (f taskFake) Get(context.Context, uuid.UUID, uuid.UUID, port.ReadScope) (*port.Task, []*port.TaskAssignment, error) {
	return f.task, f.assignments, nil
}

func (f taskFake) Claim(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, int64) (*port.Task, error) {
	return f.task, nil
}

func (f taskFake) Complete(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, json.RawMessage, int64) (*port.Task, error) {
	return f.task, nil
}

func (f taskFake) Defer(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string, int64) (*port.Task, error) {
	return f.task, nil
}

func (taskFake) ActiveByUser(context.Context, uuid.UUID, uuid.UUID, port.Page) (port.PageResult[*port.ActiveUserTask], error) {
	return port.PageResult[*port.ActiveUserTask]{Items: []*port.ActiveUserTask{{TaskID: uuid.New(), WorkflowInstanceID: uuid.New(), NodeKey: "eng/Review", UserID: uuid.New(), DepartmentID: uuid.New(), Status: port.TaskStatusReady, RecordVersion: 1}}}, nil
}

func TestPublicContract_Instances_And_Tasks(t *testing.T) {
	spec := contract.Load(t, publicSpec)
	gin.SetMode(gin.TestMode)
	at := time.Now().UTC()
	tenant, user := uuid.New(), uuid.New()
	instance := &port.Instance{
		ID: uuid.New(), TenantID: tenant, WorkflowID: uuid.New(), WorkflowVersionID: uuid.New(), SubjectID: "T-1", BusinessKey: "bk",
		TemporalWorkflowID: "wf", TemporalRunID: "run", Status: port.InstanceStatusRunning, CurrentNodeKeys: []string{"eng/Review"}, SavedNodeKeys: []string{},
		ContextJSON: json.RawMessage(`{}`), OverrideMap: json.RawMessage(`{}`), TaskQueue: "wf-queue-default", StartedByUserID: user,
		StartedAt: &at, RecordVersion: 2, CreatedAt: at, UpdatedAt: at,
	}
	task := &port.Task{
		ID: uuid.New(), TenantID: tenant, WorkflowInstanceID: instance.ID, NodeKey: "eng/Review", TaskType: "review", DepartmentID: uuid.New(),
		RequiredLevel: "reviewer", Status: port.TaskStatusReady, RecordVersion: 3, AssigneeMode: "single", AssigneeCount: 1,
		ExtrasJSON: json.RawMessage(`{}`), CreatedAt: at, UpdatedAt: at,
	}
	assignment := &port.TaskAssignment{ID: uuid.New(), TenantID: tenant, TaskID: task.ID, UserID: user, IsLead: true, IsActive: true, AssignedAt: &at, UpdatedAt: at}
	eventTask := task.ID
	events := []*port.WorkflowEvent{{ID: uuid.New(), WorkflowInstanceID: instance.ID, TaskID: &eventTask, TenantID: tenant, EventType: port.EventTaskCreated, PayloadJSON: json.RawMessage(`{}`), OccurredAt: at}}

	router := httpadapter.NewRouter(httpadapter.RouterConfig{
		GinConfig: gincommon.Config{ServiceName: "workflow-execution-service", BuildVersion: "test", Domain: "workflow", Environment: "test"}, AppEnv: "test", InternalAPIToken: "internal-secret",
		Handler: handler.New(handler.Services{
			Instances: instanceFake{instance: instance, tasks: []*port.Task{task}, events: events},
			Tasks:     taskFake{task: task, assignments: []*port.TaskAssignment{assignment}},
			Cache:     nil, IdempotencyTTL: time.Minute,
		}),
		DB: upDB{}, Cache: upPinger{}, Temporal: upPinger{},
	}).Handler()
	call := func(method, path, body string) *http.Request {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		req.Header.Set("x-tenant-id", tenant.String())
		req.Header.Set("x-user-id", user.String())
		req.Header.Set("x-tenant-roles", "tenant_admin")
		req.Header.Set("x-departments", uuid.NewString()+":reviewer")
		return req
	}
	inst := "/api/v1/instances/" + instance.ID.String()
	tsk := "/api/v1/tasks/" + task.ID.String()
	for _, tc := range []struct {
		name string
		req  *http.Request
		want int
	}{
		{"list instances", call(http.MethodGet, "/api/v1/instances", ""), http.StatusOK},
		{"get an instance", call(http.MethodGet, inst, ""), http.StatusOK},
		{"list instance events", call(http.MethodGet, inst+"/events", ""), http.StatusOK},
		{"pause", call(http.MethodPost, inst+"/pause", `{"reason":"audit","record_version":2}`), http.StatusAccepted},
		{"resume", call(http.MethodPost, inst+"/resume", `{"record_version":2}`), http.StatusAccepted},
		{"cancel", call(http.MethodPost, inst+"/cancel", `{"reason":"no longer needed","record_version":2}`), http.StatusAccepted},
		{"terminate", call(http.MethodPost, inst+"/terminate", `{"reason":"abort"}`), http.StatusOK},
		{"list tasks", call(http.MethodGet, "/api/v1/tasks", ""), http.StatusOK},
		{"get a task", call(http.MethodGet, tsk, ""), http.StatusOK},
		{"claim a task", call(http.MethodPost, tsk+"/claim", `{"record_version":3}`), http.StatusAccepted},
		{"complete a task", call(http.MethodPost, tsk+"/complete", `{"record_version":3,"result_json":{}}`), http.StatusAccepted},
		{"defer a task", call(http.MethodPost, tsk+"/defer", `{"reason":"waiting","record_version":3}`), http.StatusAccepted},
		{"active tasks of a user", call(http.MethodGet, "/api/v1/workflows/active-by-user?user_id="+user.String(), ""), http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec.Request(t, tc.req)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, tc.req)
			if w.Code != tc.want {
				t.Fatalf("status %d, want %d: %s", w.Code, tc.want, w.Body.String())
			}
			spec.Response(t, tc.req, w.Code, w.Header(), w.Body.Bytes())
			spec.Undeclared(t, tc.req, w.Code, w.Body.Bytes())
		})
	}
}

func TestPublicContract_BodyRoutesDeclareTheirRefusals(t *testing.T) {
	spec := contract.Load(t, publicSpec)
	gin.SetMode(gin.TestMode)
	router := httpadapter.NewRouter(httpadapter.RouterConfig{
		GinConfig: gincommon.Config{ServiceName: "workflow-execution-service", BuildVersion: "test", Domain: "workflow", Environment: "test"}, AppEnv: "test", InternalAPIToken: "internal-secret",
		Handler: handler.New(handler.Services{IdempotencyTTL: time.Minute}),
		DB:      upDB{}, Cache: upPinger{}, Temporal: upPinger{},
	}).Handler()
	id := regexp.MustCompile(`\{[^}]+\}`)
	for _, op := range spec.Operations() {
		if !op.HasBody {
			continue
		}
		path := id.ReplaceAllString(op.Path, uuid.NewString())
		for _, tc := range []struct {
			name, contentType, body string
			want                    int
		}{
			{"wrong media type", "text/plain", `{}`, http.StatusUnsupportedMediaType},
			{"oversized body", "application/json", `{"reason":"` + strings.Repeat("x", 11<<20) + `"}`, http.StatusRequestEntityTooLarge},
		} {
			t.Run(op.Method+" "+op.Path+" "+tc.name, func(t *testing.T) {
				req := httptest.NewRequest(op.Method, path, strings.NewReader(tc.body))
				req.Header.Set("Content-Type", tc.contentType)
				req.Header.Set("x-tenant-id", uuid.NewString())
				req.Header.Set("x-user-id", uuid.NewString())
				req.Header.Set("x-tenant-roles", "tenant_admin")
				req.Header.Set("x-departments", uuid.NewString()+":reviewer")
				w := httptest.NewRecorder()
				router.ServeHTTP(w, req)
				if w.Code != tc.want {
					t.Fatalf("status %d, want %d: %.200s", w.Code, tc.want, w.Body.String())
				}
				spec.Response(t, req, w.Code, w.Header(), w.Body.Bytes())
				spec.Undeclared(t, req, w.Code, w.Body.Bytes())
			})
		}
	}
}
