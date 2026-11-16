package handler_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestOverrideNodeAssignee_NoOneReplaced_AnswerCarriesNullPreviousUser(t *testing.T) {
	newUser := uuid.New()
	fake := &fakeTaskService{
		overrideAssignee: func(context.Context, port.AssigneeOverrideInput) (*port.AssigneeOverride, error) {
			return &port.AssigneeOverride{
				WorkflowInstanceID: testInstID, NodeKey: "review_finance",
				NewUserID: newUser, ActorUserID: testUserID, RecordVersion: 5,
			}, nil
		},
	}
	router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

	w := do(router, asAdmin(req(http.MethodPost, overridePath(testInstID, "review_finance"), map[string]any{
		"new_user_id": newUser, "reason": "nobody held it", "record_version": 4,
	})))

	require.Equal(t, http.StatusOK, w.Code)
	var body map[string]any
	decodeJSON(t, w.Body, &body)
	value, present := body["previous_user_id"]
	assert.True(t, present, "the field is present and null, not left out")
	assert.Nil(t, value)
}
