package handler_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type hreOutsideAnswerCall struct {
	tenantID   uuid.UUID
	instanceID uuid.UUID
	key        string
}

func hreOutsideAnswerFake() (*fakeInstanceService, *[]hreOutsideAnswerCall) {
	var calls []hreOutsideAnswerCall
	fake := &fakeInstanceService{answerOutsideWait: func(_ context.Context, tenantID, instanceID uuid.UUID, _ port.ReadScope, key string, _ map[string]any) error {
		calls = append(calls, hreOutsideAnswerCall{tenantID: tenantID, instanceID: instanceID, key: key})
		return nil
	}}
	return fake, &calls
}

func TestAnswerOutsideWait_UnusableCallerIdentity_Returns500(t *testing.T) {
	routes := []struct {
		name    string
		pattern string
		path    string
		handle  func(*handler.Handler) gin.HandlerFunc
	}{
		{name: "complete", pattern: "/api/v1/instances/:id/nodes/:node/complete", path: completePath("Receive_Bid"),
			handle: func(h *handler.Handler) gin.HandlerFunc { return h.CompleteOutsideWait }},
		{name: "boundary fire", pattern: "/api/v1/instances/:id/nodes/:node/boundaries/:boundary/fire", path: firePath("Receive_Bid", "Timeout"),
			handle: func(h *handler.Handler) gin.HandlerFunc { return h.FireOutsideBoundary }},
	}
	for _, route := range routes {
		for _, tc := range hreIdentityCases() {
			t.Run(route.name+"/"+tc.name, func(t *testing.T) {
				fake, calls := hreOutsideAnswerFake()
				h := newInstanceHandler(fake)
				router := newRouter(h)
				if tc.bare {
					router = hreBareEngine(http.MethodPost, route.pattern, route.handle(h))
				}
				r := hreRawRequest(http.MethodPost, route.path, `{"variables":{"bid":"42"}}`)
				if tc.header != "" {
					r.Header.Set(tc.header, tc.bad)
				}

				w := do(router, r)

				require.Equal(t, http.StatusInternalServerError, w.Code)
				prob := hreDecodeProblem(t, w)
				assert.Equal(t, "INTERNAL_ERROR", prob.Code)
				assert.Equal(t, tc.wantDetail, prob.Detail)
				assert.Empty(t, prob.InvalidParams)
				assert.Empty(t, *calls, "an unusable identity must not reach the service")
				assert.NotContains(t, w.Body.String(), "not-a-uuid", "the rejected header value must not be echoed")
				hreAssertNoInternalText(t, w)
			})
		}
	}
}
