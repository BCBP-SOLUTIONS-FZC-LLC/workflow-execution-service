package handler_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestStartInstance_ANulCharacterInTheBody_Returns400BeforeTheService(t *testing.T) {
	tests := []struct {
		name string
		body map[string]any
	}{
		{"subject_id", map[string]any{"subject_id": "SU\x00BJ", "business_key": "TND-001"}},
		{"business_key", map[string]any{"subject_id": "SUBJ-1", "business_key": "TN\x00D"}},
		{"nested context value", map[string]any{
			"subject_id": "SUBJ-1", "business_key": "TND-001", "context_json": map[string]any{"k": []string{"a\x00b"}},
		}},
		{"context key", map[string]any{
			"subject_id": "SUBJ-1", "business_key": "TND-001", "context_json": map[string]any{"a\x00b": 1},
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			router := newRouter(newInstanceHandler(&fakeInstanceService{
				start: func(context.Context, port.StartInstanceInput) (*port.Instance, error) {
					called = true
					return nil, nil
				},
			}))
			tc.body["workflow_version_id"] = uuid.New()

			w := do(router, req(http.MethodPost, instancesPath(), tc.body))

			require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
			assert.Equal(t, "BAD_REQUEST", hhbDecodeProblem(t, w).Code)
			assert.False(t, called, "a refused body must not reach the service")
		})
	}
}

func TestStartInstance_TheTextOfANulEscape_IsNotANulCharacter(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	var got port.StartInstanceInput
	router := newRouter(newInstanceHandler(&fakeInstanceService{
		start: func(_ context.Context, in port.StartInstanceInput) (*port.Instance, error) {
			got = in
			return &port.Instance{
				ID: testInstID, TenantID: in.TenantID, Status: port.InstanceStatusRunning,
				BusinessKey: in.BusinessKey, StartedByUserID: in.StartedByUserID,
				StartedAt: &now, RecordVersion: 1, CreatedAt: now, UpdatedAt: now,
			}, nil
		},
	}))

	w := do(router, req(http.MethodPost, instancesPath(), hhbStartBody("subject_id", `a\u0000b`)))

	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
	assert.Equal(t, `a\u0000b`, got.SubjectID)
}
