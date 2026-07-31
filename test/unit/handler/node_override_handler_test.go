package handler_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	adapterhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func overridePath(instanceID uuid.UUID, node string) string {
	return "/api/v1/instances/" + instanceID.String() + "/nodes/" + node + "/override"
}

func TestOverrideNodeAssignee_Success(t *testing.T) {
	newUser := uuid.New()
	deptID := uuid.New()
	previousUser := uuid.New()

	var eligibilityCalledWith struct {
		newUserID, deptID, actorID uuid.UUID
		level                      string
	}
	fake := &fakeTaskService{
		getByNode: func(_ context.Context, tenantID, instanceID uuid.UUID, nodeKey string) (*port.Task, error) {
			assert.Equal(t, testTenantID, tenantID)
			assert.Equal(t, testInstID, instanceID)
			assert.Equal(t, "review_finance", nodeKey)
			return &port.Task{
				ID: testTaskID, WorkflowInstanceID: testInstID, NodeKey: "review_finance",
				DepartmentID: deptID, RequiredLevel: "reviewer", Status: port.TaskStatusReady, RecordVersion: 4,
			}, nil
		},
		overrideAssignee: func(_ context.Context, in port.AssigneeOverrideInput) (*port.AssigneeOverride, error) {
			assert.Equal(t, newUser, in.NewUserID)
			assert.Equal(t, int64(4), in.RecordVersion)
			return &port.AssigneeOverride{
				WorkflowInstanceID: testInstID, NodeKey: "review_finance",
				PreviousUserID: previousUser, NewUserID: newUser, ActorUserID: testUserID, RecordVersion: 5,
			}, nil
		},
		signalReassign: func(_ context.Context, tenantID, taskID, actorUserID, prevUserID, newUserID uuid.UUID, recordVersion int64) error {
			assert.Equal(t, testTaskID, taskID)
			assert.Equal(t, previousUser, prevUserID)
			assert.Equal(t, newUser, newUserID)
			assert.Equal(t, int64(4), recordVersion, "signal carries the record_version validated at step 1, not the post-persist one")
			return nil
		},
	}
	elig := &fakeEligibilityChecker{
		check: func(_ context.Context, newUserID, departmentID uuid.UUID, level string, actorID uuid.UUID) (bool, error) {
			eligibilityCalledWith.newUserID = newUserID
			eligibilityCalledWith.deptID = departmentID
			eligibilityCalledWith.level = level
			eligibilityCalledWith.actorID = actorID
			return true, nil
		},
	}
	router := newRouter(newHandler(fake, elig))

	w := do(router, req(http.MethodPost, overridePath(testInstID, "review_finance"), map[string]any{
		"new_user_id": newUser, "reason": "primary reviewer on leave", "record_version": 4,
	}))

	require.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, newUser, eligibilityCalledWith.newUserID)
	assert.Equal(t, deptID, eligibilityCalledWith.deptID)
	assert.Equal(t, "reviewer", eligibilityCalledWith.level)
	assert.Equal(t, testUserID, eligibilityCalledWith.actorID)

	var body struct {
		PreviousUserID uuid.UUID `json:"previous_user_id"`
		NewUserID      uuid.UUID `json:"new_user_id"`
		RecordVersion  int64     `json:"record_version"`
	}
	decodeJSON(t, w.Body, &body)
	assert.Equal(t, previousUser, body.PreviousUserID)
	assert.Equal(t, newUser, body.NewUserID)
}

func TestOverrideNodeAssignee_AlreadyResolved(t *testing.T) {
	fake := &fakeTaskService{
		getByNode: func(context.Context, uuid.UUID, uuid.UUID, string) (*port.Task, error) {
			return &port.Task{Status: port.TaskStatusCompleted, RecordVersion: 4}, nil
		},
		overrideAssignee: func(context.Context, port.AssigneeOverrideInput) (*port.AssigneeOverride, error) {
			t.Fatal("must not persist when the node has already resolved")
			return nil, nil
		},
	}
	elig := &fakeEligibilityChecker{
		check: func(context.Context, uuid.UUID, uuid.UUID, string, uuid.UUID) (bool, error) {
			t.Fatal("must not call eligibility when step 1 already rejects")
			return false, nil
		},
	}
	router := newRouter(newHandler(fake, elig))

	w := do(router, req(http.MethodPost, overridePath(testInstID, "review_finance"), map[string]any{
		"new_user_id": uuid.New(), "record_version": 4,
	}))

	require.Equal(t, http.StatusConflict, w.Code)
	var body struct {
		Code string `json:"code"`
	}
	decodeJSON(t, w.Body, &body)
	assert.Equal(t, "NODE_ALREADY_RESOLVED", body.Code)
}

func TestOverrideNodeAssignee_StaleRecordVersion(t *testing.T) {
	fake := &fakeTaskService{
		getByNode: func(context.Context, uuid.UUID, uuid.UUID, string) (*port.Task, error) {
			return &port.Task{Status: port.TaskStatusReady, RecordVersion: 9}, nil
		},
	}
	elig := &fakeEligibilityChecker{
		check: func(context.Context, uuid.UUID, uuid.UUID, string, uuid.UUID) (bool, error) {
			t.Fatal("must not call eligibility on a stale record_version")
			return false, nil
		},
	}
	router := newRouter(newHandler(fake, elig))

	w := do(router, req(http.MethodPost, overridePath(testInstID, "review_finance"), map[string]any{
		"new_user_id": uuid.New(), "record_version": 4,
	}))

	require.Equal(t, http.StatusConflict, w.Code)
	var body struct {
		Code string `json:"code"`
	}
	decodeJSON(t, w.Body, &body)
	assert.Equal(t, "RECORD_VERSION_CONFLICT", body.Code)
}

func TestOverrideNodeAssignee_Ineligible(t *testing.T) {
	var persisted bool
	fake := &fakeTaskService{
		getByNode: func(context.Context, uuid.UUID, uuid.UUID, string) (*port.Task, error) {
			return &port.Task{Status: port.TaskStatusReady, RecordVersion: 4}, nil
		},
		overrideAssignee: func(context.Context, port.AssigneeOverrideInput) (*port.AssigneeOverride, error) {
			persisted = true
			return nil, nil
		},
	}
	elig := &fakeEligibilityChecker{
		check: func(context.Context, uuid.UUID, uuid.UUID, string, uuid.UUID) (bool, error) {
			return false, nil
		},
	}
	router := newRouter(newHandler(fake, elig))

	w := do(router, req(http.MethodPost, overridePath(testInstID, "review_finance"), map[string]any{
		"new_user_id": uuid.New(), "record_version": 4,
	}))

	require.Equal(t, http.StatusUnprocessableEntity, w.Code)
	assert.False(t, persisted, "an ineligible request must leave zero rows persisted")
	var body struct {
		Code string `json:"code"`
	}
	decodeJSON(t, w.Body, &body)
	assert.Equal(t, "ASSIGNEE_INELIGIBLE", body.Code)
}

func TestOverrideNodeAssignee_UpstreamUnavailable(t *testing.T) {
	fake := &fakeTaskService{
		getByNode: func(context.Context, uuid.UUID, uuid.UUID, string) (*port.Task, error) {
			return &port.Task{Status: port.TaskStatusReady, RecordVersion: 4}, nil
		},
	}
	elig := &fakeEligibilityChecker{
		check: func(context.Context, uuid.UUID, uuid.UUID, string, uuid.UUID) (bool, error) {
			return false, fmt.Errorf("wrapped: %w", adapterhttp.ErrUpstreamUnavailable)
		},
	}
	router := newRouter(newHandler(fake, elig))

	w := do(router, req(http.MethodPost, overridePath(testInstID, "review_finance"), map[string]any{
		"new_user_id": uuid.New(), "record_version": 4,
	}))

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
}

// TestOverrideNodeAssignee_PersistConflict covers the documented double-
// override race (LLD §5.4): both requests pass step 1 and step 2, but only
// the first to persist wins — the loser's persist fails on its now-stale
// record_version.
func TestOverrideNodeAssignee_PersistConflict(t *testing.T) {
	fake := &fakeTaskService{
		getByNode: func(context.Context, uuid.UUID, uuid.UUID, string) (*port.Task, error) {
			return &port.Task{Status: port.TaskStatusReady, RecordVersion: 4}, nil
		},
		overrideAssignee: func(context.Context, port.AssigneeOverrideInput) (*port.AssigneeOverride, error) {
			return nil, port.ErrRecordVersionConflict
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodPost, overridePath(testInstID, "review_finance"), map[string]any{
		"new_user_id": uuid.New(), "record_version": 4,
	}))

	assert.Equal(t, http.StatusConflict, w.Code)
}
