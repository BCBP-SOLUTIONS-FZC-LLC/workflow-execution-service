package handler_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func fxcTaskAnswering(task *port.Task) *fakeTaskService {
	return &fakeTaskService{
		claim: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, int64) (*port.Task, error) { return task, nil },
		complete: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, json.RawMessage, int64) (*port.Task, error) {
			return task, nil
		},
		deferTask: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string, int64) (*port.Task, error) {
			return task, nil
		},
		reassign: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, uuid.UUID, int64) (*port.Task, error) {
			return task, nil
		},
	}
}

func TestTaskActionAnswers_LeaveOutTheAssigneeCountOnlyWhenItIsUnavailable(t *testing.T) {
	routes := map[string]struct {
		path  string
		body  map[string]any
		admin bool
		code  int
	}{
		"claim":    {"claim", map[string]any{"record_version": 4}, false, http.StatusAccepted},
		"complete": {"complete", map[string]any{"result_json": map[string]any{"ok": true}, "record_version": 4}, false, http.StatusAccepted},
		"defer":    {"defer", map[string]any{"reason": "backlog", "record_version": 4}, false, http.StatusOK},
		"reassign": {"reassign", map[string]any{"new_user_id": uuid.New(), "record_version": 4}, true, http.StatusAccepted},
	}
	for name, rt := range routes {
		for _, unavailable := range []bool{true, false} {
			t.Run(name, func(t *testing.T) {
				task := newTestTask()
				task.AssigneeCountUnavailable = unavailable
				router := newRouter(newHandler(fxcTaskAnswering(task), &fakeEligibilityChecker{}))
				r := req(http.MethodPost, "/api/v1/tasks/"+testTaskID.String()+"/"+rt.path, rt.body)
				if rt.admin {
					r = asAdmin(r)
				}

				w := do(router, r)

				require.Equal(t, rt.code, w.Code)
				var body map[string]any
				decodeJSON(t, w.Body, &body)
				count, present := body["assignee_count"]
				if unavailable {
					assert.False(t, present, "the count is left out, not reported as 0")
					return
				}
				assert.True(t, present)
				assert.EqualValues(t, 0, count, "a known count of zero is still sent")
			})
		}
	}
}
