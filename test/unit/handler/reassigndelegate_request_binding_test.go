package handler_test

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

const hrdReassignDelegatePath = "/api/v1/internal/workflows/reassign-delegate"

func hrdRecordingReassignClient(calls *[]port.ReassignDelegateInput) *fakeWorkflowClient {
	return &fakeWorkflowClient{reassignDelegate: func(_ context.Context, in port.ReassignDelegateInput) (int, error) {
		*calls = append(*calls, in)
		return 0, nil
	}}
}

func TestReassignDelegate_TextPlainContentType_Returns415(t *testing.T) {
	var calls []port.ReassignDelegateInput
	router := newInternalRouter(handler.New(handler.Services{WorkflowClient: hrdRecordingReassignClient(&calls)}))
	body, err := json.Marshal(map[string]any{
		"tenant_id": uuid.New(), "old_delegate_id": uuid.New(), "new_delegate_id": uuid.New(),
	})
	require.NoError(t, err)

	w := hrdInternalPost(router, hrdReassignDelegatePath, "text/plain", string(body))

	p := hrdDecodeProblem(t, w, http.StatusUnsupportedMediaType)
	assert.Equal(t, "UNSUPPORTED_MEDIA_TYPE", p.Code)
	assert.Equal(t, "request body must be application/json", p.Detail)
	assert.Empty(t, calls, "a valid body under the wrong content type must not be processed")
}

func TestReassignDelegate_IDOfWrongJSONType_Returns400NamingTheField(t *testing.T) {
	fields := []string{"tenant_id", "old_delegate_id", "new_delegate_id", "delegation_id"}
	values := []struct{ name, json string }{
		{"number", `5`},
		{"bool", `true`},
		{"object", `{}`},
		{"array", `[]`},
	}
	for _, field := range fields {
		for _, v := range values {
			t.Run(field+" as "+v.name, func(t *testing.T) {
				var calls []port.ReassignDelegateInput
				router := newInternalRouter(handler.New(handler.Services{WorkflowClient: hrdRecordingReassignClient(&calls)}))
				body := map[string]json.RawMessage{
					"tenant_id":       json.RawMessage(`"` + uuid.NewString() + `"`),
					"old_delegate_id": json.RawMessage(`"` + uuid.NewString() + `"`),
					"new_delegate_id": json.RawMessage(`"` + uuid.NewString() + `"`),
					"delegation_id":   json.RawMessage(`"` + uuid.NewString() + `"`),
				}
				body[field] = json.RawMessage(v.json)
				raw, err := json.Marshal(body)
				require.NoError(t, err)

				w := hrdInternalPost(router, hrdReassignDelegatePath, "application/json", string(raw))

				p := hrdDecodeProblem(t, w, http.StatusBadRequest)
				assert.Equal(t, "BAD_REQUEST", p.Code)
				assert.Equal(t, []hrdParam{{Name: field, Reason: "has the wrong type"}}, p.InvalidParams)
				hrdAssertNoInternalText(t, w)
				assert.Empty(t, calls, "a refused body must not reach the workflow client")
			})
		}
	}
}
