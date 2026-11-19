package handler_test

import (
	"context"
	"errors"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func fx11bUserDeleted(eventID uuid.UUID) map[string]any {
	return envelope("UserDeleted", eventID, testTenantID, time.Now(), map[string]any{
		"user_id": testUserID.String(), "deleted_at": time.Now().Format(time.RFC3339),
	})
}

func TestFx11bIsProcessedFailureAnswers503AndSkipsEffects(t *testing.T) {
	fakes := newEventsFakes()
	var called atomic.Int32
	fakes.userSafetyNet.vacateAssignments = func(context.Context, port.UserDeletedInput) error {
		called.Add(1)
		return nil
	}
	h := newEventsHandlerWithProcessedEvents(fakes, &erroringProcessedEventRepository{isProcessedErr: errors.New("db down")})

	w := postEvent(newInternalRouter(h), fx11bUserDeleted(uuid.New()))

	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
	assert.Zero(t, called.Load(), "effects must not run when the dedup lookup failed")
}

func TestFx11bMarkProcessedFailureAnswers503(t *testing.T) {
	fakes := newEventsFakes()
	fakes.userSafetyNet.vacateAssignments = func(context.Context, port.UserDeletedInput) error { return nil }
	h := newEventsHandlerWithProcessedEvents(fakes, &erroringProcessedEventRepository{recordErr: errors.New("db down")})

	w := postEvent(newInternalRouter(h), fx11bUserDeleted(uuid.New()))

	assert.Equal(t, http.StatusServiceUnavailable, w.Code, "an unrecorded delivery must be redelivered, not acknowledged")
}
