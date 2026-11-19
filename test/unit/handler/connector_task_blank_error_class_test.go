package handler_test

import (
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
)

func TestFailConnectorTask_BlankErrorClass_Returns400(t *testing.T) {
	cases := []struct{ name, errorClass string }{
		{"spaces", `"   "`},
		{"tab and newline", `"\t\n"`},
		{"empty", `""`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tasks := &hhaConnectorTasks{}
			router := hhaInternalRouter(handler.New(handler.Services{ConnectorTasks: tasks}))

			w := hhaPostJSON(router, hhaConnectorPath(uuid.New(), "fail"),
				`{"tenant_id":"`+uuid.NewString()+`","error_class":`+tc.errorClass+`}`)

			p := hhaDecodeProblem(t, w, http.StatusBadRequest)
			assert.Equal(t, "BAD_REQUEST", p.Code)
			require.Len(t, p.InvalidParams, 1)
			assert.Equal(t, "error_class", p.InvalidParams[0].Name)
			assert.Empty(t, tasks.failCalls, "a refused body must not fail the task")
		})
	}
}
