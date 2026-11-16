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

type hreActiveByUserCall struct {
	tenantID uuid.UUID
	userID   uuid.UUID
}

func TestListActiveByUser_RepeatedUserIDParameter_FirstValueWins(t *testing.T) {
	first := uuid.MustParse("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	second := uuid.MustParse("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
	cases := []struct {
		name  string
		query string
		want  uuid.UUID
	}{
		{name: "A then B", query: "user_id=" + first.String() + "&user_id=" + second.String(), want: first},
		{name: "B then A", query: "user_id=" + second.String() + "&user_id=" + first.String(), want: second},
		{name: "valid then malformed", query: "user_id=" + first.String() + "&user_id=not-a-uuid", want: first},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls []hreActiveByUserCall
			fake := &fakeTaskService{
				activeByUser: func(_ context.Context, tenantID, userID uuid.UUID, _ port.Page) (port.PageResult[*port.ActiveUserTask], error) {
					calls = append(calls, hreActiveByUserCall{tenantID: tenantID, userID: userID})
					return port.PageResult[*port.ActiveUserTask]{}, nil
				},
			}
			router := newRouter(newHandler(fake, &fakeEligibilityChecker{}))

			w := do(router, asAdmin(req(http.MethodGet, "/api/v1/workflows/active-by-user?"+tc.query, nil)))

			require.Equal(t, http.StatusOK, w.Code, "body: %s", w.Body.String())
			assert.Equal(t, []hreActiveByUserCall{{tenantID: testTenantID, userID: tc.want}}, calls)
			var body struct {
				Items []map[string]any `json:"items"`
			}
			decodeJSON(t, w.Body, &body)
			assert.NotNil(t, body.Items)
			assert.Empty(t, body.Items)
		})
	}
}
