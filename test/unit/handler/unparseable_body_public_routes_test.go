package handler_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func hrdTaskAndInstanceHandler(tasks *fakeTaskService, instances *fakeInstanceService) *handler.Handler {
	return handler.New(handler.Services{Tasks: tasks, Instances: instances, Eligibility: &fakeEligibilityChecker{}})
}

func TestStartInstance_InvalidEmptyOrBlankBody_Returns400(t *testing.T) {
	cases := []struct{ name, body string }{
		{"truncated object", `{`},
		{"empty body", ``},
		{"whitespace only", "   "},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			started := false
			router := newRouter(newInstanceHandler(&fakeInstanceService{
				start: func(context.Context, port.StartInstanceInput) (*port.Instance, error) {
					started = true
					return &port.Instance{}, nil
				},
			}))

			w := do(router, hrdPublicRequest(http.MethodPost, instancesPath(), tc.body, false))

			p := hrdDecodeProblem(t, w, http.StatusBadRequest)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			assert.Equal(t, "request body is not valid JSON", p.Detail)
			assert.Empty(t, p.InvalidParams)
			hrdAssertNoInternalText(t, w)
			assert.False(t, started, "a refused body must not start an instance")
		})
	}
}

func TestPublicRoutes_UnparseableJSONBody_Returns400WithFixedDetail(t *testing.T) {
	bodies := []struct{ name, body string }{
		{"truncated value", `{"record_version":`},
		{"bare word", `nonsense`},
	}
	routes := []struct {
		name, path string
		admin      bool
	}{
		{"claim a task", "/api/v1/tasks/" + testTaskID.String() + "/claim", false},
		{"terminate an instance", instancePath(testInstID) + "/terminate", true},
	}
	for _, route := range routes {
		for _, b := range bodies {
			t.Run(route.name+" with "+b.name, func(t *testing.T) {
				reached := false
				tasks := &fakeTaskService{claim: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, int64) (*port.Task, error) {
					reached = true
					return newTestTask(), nil
				}}
				instances := &fakeInstanceService{terminate: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string) error {
					reached = true
					return nil
				}}
				router := newRouter(hrdTaskAndInstanceHandler(tasks, instances))

				w := do(router, hrdPublicRequest(http.MethodPost, route.path, b.body, route.admin))

				p := hrdDecodeProblem(t, w, http.StatusBadRequest)
				assert.Equal(t, "BAD_REQUEST", p.Code)
				assert.Equal(t, "request body is not valid JSON", p.Detail)
				assert.Empty(t, p.InvalidParams)
				hrdAssertNoInternalText(t, w)
				assert.False(t, reached, "a refused body must not reach the service")
			})
		}
	}
}
