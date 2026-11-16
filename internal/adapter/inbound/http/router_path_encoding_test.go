package http_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRouter_EncodedSlashInTaskID_ReachesTheHandlerAndIsRejectedAs400(t *testing.T) {
	rig := hrfNewRig(t, "test")

	w := rig.do(http.MethodGet, "/api/v1/tasks/a%2Fb", hrfAuthHeaders())

	require.Equal(t, http.StatusBadRequest, w.Code, "the route must match with the slash kept inside :id, not 404")
	body := hrfBody(t, w)
	assert.Equal(t, "BAD_REQUEST", body["code"])
	assert.Equal(t, "invalid id path parameter", body["detail"])
	assert.Equal(t, "/api/v1/tasks/a/b", body["instance"])
}
