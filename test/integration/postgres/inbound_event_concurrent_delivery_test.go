//go:build integration

package postgres_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/eventbus"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

// iecOverlapSafetyNet holds the first applier until a second one arrives, or the wait runs out,
// so two deliveries that are both allowed to apply really overlap.
type iecOverlapSafetyNet struct {
	applied atomic.Int32
	entered chan struct{}
	wait    time.Duration
}

func (s *iecOverlapSafetyNet) VacateAssignments(_ context.Context, _ port.UserDeletedInput) error {
	if s.applied.Add(1) == 1 {
		select {
		case <-s.entered:
		case <-time.After(s.wait):
		}
		return nil
	}
	close(s.entered)
	return nil
}

func TestUserDeletedEvent_TwoConcurrentDeliveriesOfOneEventApplyOnce(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	inbox, err := eventbus.NewInbox(appPool, port.EventConsumers...)
	require.NoError(t, err)
	safetyNet := &iecOverlapSafetyNet{entered: make(chan struct{}), wait: time.Second}
	h := handler.New(handler.Services{ProcessedEvents: inbox, UserSafetyNet: safetyNet})
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.POST("/events/user-profile", h.HandleUserProfileEvents)
	tenantID, userID, eventID := uuid.New(), uuid.New(), uuid.New()
	body := `{"id":"` + eventID.String() + `","type":"UserDeleted","tenant_id":"` + tenantID.String() +
		`","time":"2030-01-02T03:04:05Z","data":{"user_id":"` + userID.String() + `","deleted_at":"2030-01-02T03:04:05Z"}}`

	var codes [2]int
	var wg sync.WaitGroup
	for i := range codes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := httptest.NewRequest(http.MethodPost, "/events/user-profile", strings.NewReader(body))
			r.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			router.ServeHTTP(w, r)
			codes[i] = w.Code
		}()
	}
	wg.Wait()

	assert.Equal(t, [2]int{http.StatusOK, http.StatusOK}, codes, "a duplicate is acknowledged")
	assert.EqualValues(t, 1, safetyNet.applied.Load(), "the event's effects ran once")
	processed, err := inbox.IsProcessed(context.Background(), eventID, port.ConsumerUser)
	require.NoError(t, err)
	assert.True(t, processed)
}
