package handler_test

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func hhbStartBody(field, value string) map[string]any {
	body := map[string]any{
		"subject_id":          "SUBJ-1",
		"business_key":        "TND-001",
		"workflow_version_id": uuid.New(),
	}
	body[field] = value
	return body
}

func TestStartInstance_SubjectAndBusinessKeyOutsideLengthBounds_Returns400(t *testing.T) {
	cases := []struct {
		name  string
		value string
	}{
		{"one character", "x"},
		{"sixty five characters", strings.Repeat("x", 65)},
		{"sixty five multi-byte characters", strings.Repeat("é", 65)},
	}
	for _, field := range []string{"subject_id", "business_key"} {
		for _, tc := range cases {
			t.Run(field+"/"+tc.name, func(t *testing.T) {
				called := false
				router := newRouter(newInstanceHandler(&fakeInstanceService{
					start: func(context.Context, port.StartInstanceInput) (*port.Instance, error) {
						called = true
						return nil, nil
					},
				}))

				w := do(router, req(http.MethodPost, instancesPath(), hhbStartBody(field, tc.value)))

				require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
				p := hhbDecodeProblem(t, w)
				assert.Equal(t, "BAD_REQUEST", p.Code)
				assert.Equal(t, hhbBadRequestType, p.Type)
				require.Len(t, p.InvalidParams, 1)
				assert.Equal(t, field, p.InvalidParams[0].Name)
				assert.Equal(t, "is not valid", p.InvalidParams[0].Reason)
				assert.False(t, called, "a refused body must not reach the service")
			})
		}
	}
}

func TestStartInstance_SubjectAndBusinessKeyAtLengthBounds_Returns201(t *testing.T) {
	cases := []struct {
		name  string
		value string
	}{
		{"two characters", "ab"},
		{"sixty four characters", strings.Repeat("x", 64)},
		{"two spaces", "  "},
		{"two multi-byte characters", "éé"},
		{"sixty four multi-byte characters", strings.Repeat("日", 64)},
	}
	for _, field := range []string{"subject_id", "business_key"} {
		for _, tc := range cases {
			t.Run(field+"/"+tc.name, func(t *testing.T) {
				now := time.Now().UTC().Truncate(time.Second)
				var got port.StartInstanceInput
				calls := 0
				router := newRouter(newInstanceHandler(&fakeInstanceService{
					start: func(_ context.Context, in port.StartInstanceInput) (*port.Instance, error) {
						calls++
						got = in
						return &port.Instance{
							ID: testInstID, TenantID: in.TenantID, Status: port.InstanceStatusRunning,
							BusinessKey: in.BusinessKey, StartedByUserID: in.StartedByUserID,
							StartedAt: &now, RecordVersion: 1, CreatedAt: now, UpdatedAt: now,
						}, nil
					},
				}))

				w := do(router, req(http.MethodPost, instancesPath(), hhbStartBody(field, tc.value)))

				require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
				require.Equal(t, 1, calls)
				assert.Equal(t, testTenantID, got.TenantID)
				assert.Equal(t, testUserID, got.StartedByUserID)
				if field == "subject_id" {
					assert.Equal(t, tc.value, got.SubjectID, "the value must reach the service untrimmed")
					assert.Equal(t, "TND-001", got.BusinessKey)
				} else {
					assert.Equal(t, tc.value, got.BusinessKey, "the value must reach the service untrimmed")
					assert.Equal(t, "SUBJ-1", got.SubjectID)
				}
			})
		}
	}
}
