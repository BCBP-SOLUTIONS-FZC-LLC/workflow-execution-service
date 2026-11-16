package handler_test

import (
	"context"
	"fmt"
	"net/http"
	"strings"
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
	previousUser := uuid.New()

	fake := &fakeTaskService{
		overrideAssignee: func(_ context.Context, in port.AssigneeOverrideInput) (*port.AssigneeOverride, error) {
			assert.Equal(t, testTenantID, in.TenantID)
			assert.Equal(t, testInstID, in.InstanceID)
			assert.Equal(t, "review_finance", in.NodeKey)
			assert.Equal(t, newUser, in.NewUserID)
			assert.Equal(t, testUserID, in.ActorUserID)
			assert.Equal(t, int64(4), in.RecordVersion)
			return &port.AssigneeOverride{
				WorkflowInstanceID: testInstID, NodeKey: "review_finance",
				PreviousUserID: &previousUser, NewUserID: newUser, ActorUserID: testUserID, RecordVersion: 5,
			}, nil
		},
	}
	elig := &fakeEligibilityChecker{
		check: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string) (bool, error) {
			t.Fatal("the service asks eligibility once, before it stores anything; the handler does not")
			return false, nil
		},
	}
	router := newRouter(newHandler(fake, elig))

	w := do(router, asAdmin(req(http.MethodPost, overridePath(testInstID, "review_finance"), map[string]any{
		"new_user_id": newUser, "reason": "primary reviewer on leave", "record_version": 4,
	})))

	require.Equal(t, http.StatusOK, w.Code)

	var body struct {
		PreviousUserID *uuid.UUID `json:"previous_user_id"`
		NewUserID      uuid.UUID  `json:"new_user_id"`
		RecordVersion  int64      `json:"record_version"`
	}
	decodeJSON(t, w.Body, &body)
	require.NotNil(t, body.PreviousUserID)
	assert.Equal(t, previousUser, *body.PreviousUserID)
	assert.Equal(t, newUser, body.NewUserID)
}

func TestOverrideNodeAssignee_AlreadyResolved(t *testing.T) {
	fake := &fakeTaskService{
		overrideAssignee: func(context.Context, port.AssigneeOverrideInput) (*port.AssigneeOverride, error) {
			return nil, port.ErrNodeAlreadyResolved
		},
	}
	elig := &fakeEligibilityChecker{
		check: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string) (bool, error) {
			t.Fatal("must not call eligibility when step 1 already rejects")
			return false, nil
		},
	}
	router := newRouter(newHandler(fake, elig))

	w := do(router, asAdmin(req(http.MethodPost, overridePath(testInstID, "review_finance"), map[string]any{
		"new_user_id": uuid.New(), "record_version": 4,
	})))

	require.Equal(t, http.StatusConflict, w.Code)
	var body struct {
		Code string `json:"code"`
	}
	decodeJSON(t, w.Body, &body)
	assert.Equal(t, "NODE_ALREADY_RESOLVED", body.Code)
}

func TestOverrideNodeAssignee_StaleRecordVersion(t *testing.T) {
	fake := &fakeTaskService{
		overrideAssignee: func(context.Context, port.AssigneeOverrideInput) (*port.AssigneeOverride, error) {
			return nil, port.ErrRecordVersionConflict
		},
	}
	elig := &fakeEligibilityChecker{
		check: func(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string) (bool, error) {
			t.Fatal("must not call eligibility on a stale record_version")
			return false, nil
		},
	}
	router := newRouter(newHandler(fake, elig))

	w := do(router, asAdmin(req(http.MethodPost, overridePath(testInstID, "review_finance"), map[string]any{
		"new_user_id": uuid.New(), "record_version": 4,
	})))

	require.Equal(t, http.StatusConflict, w.Code)
	var body struct {
		Code string `json:"code"`
	}
	decodeJSON(t, w.Body, &body)
	assert.Equal(t, "RECORD_VERSION_CONFLICT", body.Code)
}

func TestOverrideNodeAssignee_Ineligible(t *testing.T) {
	fake := &fakeTaskService{
		overrideAssignee: func(context.Context, port.AssigneeOverrideInput) (*port.AssigneeOverride, error) {
			return nil, port.ErrAssigneeIneligible
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, asAdmin(req(http.MethodPost, overridePath(testInstID, "review_finance"), map[string]any{
		"new_user_id": uuid.New(), "record_version": 4,
	})))

	require.Equal(t, http.StatusUnprocessableEntity, w.Code)
	var body struct {
		Code string `json:"code"`
	}
	decodeJSON(t, w.Body, &body)
	assert.Equal(t, "ASSIGNEE_INELIGIBLE", body.Code)
}

func TestOverrideNodeAssignee_UpstreamUnavailable(t *testing.T) {
	fake := &fakeTaskService{
		overrideAssignee: func(context.Context, port.AssigneeOverrideInput) (*port.AssigneeOverride, error) {
			return nil, fmt.Errorf("wrapped: %w", adapterhttp.ErrUpstreamUnavailable)
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, asAdmin(req(http.MethodPost, overridePath(testInstID, "review_finance"), map[string]any{
		"new_user_id": uuid.New(), "record_version": 4,
	})))

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
}

// TestOverrideNodeAssignee_PersistConflict covers the documented double-
// override race (LLD §5.4): both requests pass step 1 and step 2, but only
// the first to persist wins — the loser's persist fails on its now-stale
// record_version.
func TestOverrideNodeAssignee_PersistConflict(t *testing.T) {
	fake := &fakeTaskService{
		overrideAssignee: func(context.Context, port.AssigneeOverrideInput) (*port.AssigneeOverride, error) {
			return nil, port.ErrRecordVersionConflict
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, asAdmin(req(http.MethodPost, overridePath(testInstID, "review_finance"), map[string]any{
		"new_user_id": uuid.New(), "record_version": 4,
	})))

	assert.Equal(t, http.StatusConflict, w.Code)
}

func TestOverrideNodeAssignee_NotAdmin_Forbidden(t *testing.T) {
	fake := &fakeTaskService{}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, req(http.MethodPost, overridePath(testInstID, "review_finance"), map[string]any{
		"new_user_id": uuid.New(), "record_version": 4,
	}))

	require.Equal(t, http.StatusForbidden, w.Code)
	var body struct {
		Code string `json:"code"`
	}
	decodeJSON(t, w.Body, &body)
	assert.Equal(t, "FORBIDDEN", body.Code)
}

func TestOverrideNodeAssignee_ReasonTooLong_400(t *testing.T) {
	fake := &fakeTaskService{}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, asAdmin(req(http.MethodPost, overridePath(testInstID, "review_finance"), map[string]any{
		"new_user_id": uuid.New(), "record_version": 4, "reason": strings.Repeat("a", 501),
	})))

	assert.Equal(t, http.StatusBadRequest, w.Code)
}
