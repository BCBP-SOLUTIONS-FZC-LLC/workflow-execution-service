package handler_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompleteOutsideWait_AMessageBoundaryKeyIsPassedOnLikeAReceiveTaskKey(t *testing.T) {
	var got answered
	router := newRouter(newInstanceHandler(recordingInstances(t, &got, nil)))

	w := do(router, asAdmin(req(http.MethodPost, completePath("finance%2FReview%23timeout"), map[string]any{"variables": map[string]any{"late": true}})))

	require.Equal(t, http.StatusAccepted, w.Code, w.Body.String())
	assert.Equal(t, "finance/Review#timeout", got.key)
	assert.Equal(t, map[string]any{"late": true}, got.variables)
	assert.Equal(t, 1, got.calls)
}
