package handler_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	adapterhttp "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/http"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type fxfRecordingResolver struct {
	err     error
	tenants []uuid.UUID
}

func (r *fxfRecordingResolver) AutomationPrincipal(_ context.Context, tenantID uuid.UUID) (port.AutomationPrincipal, error) {
	r.tenants = append(r.tenants, tenantID)
	return port.AutomationPrincipal{}, r.err
}

type fxfProblem struct {
	Status int    `json:"status"`
	Code   string `json:"code"`
	Title  string `json:"title"`
	Detail string `json:"detail"`
	Type   string `json:"type"`
}

func fxfPostTaskCreated(t *testing.T, resolverErr error) (*fxfRecordingResolver, *fakeConnectorEventPublisher, *eventsFakes, uuid.UUID, int, string, fxfProblem) {
	t.Helper()
	fakes := newEventsFakes()
	publisher := &fakeConnectorEventPublisher{}
	resolver := &fxfRecordingResolver{err: resolverErr}
	router := newInternalRouter(newConnectorEventsHandlerWith(fakes, publisher, resolver))

	connectorType := "storage"
	eventID := uuid.New()
	w := postEvent(router, envelope("WorkflowTaskCreated", eventID, testTenantID, time.Now(), connectorTaskCreatedPayload(&connectorType)))

	var p fxfProblem
	_ = json.Unmarshal(w.Body.Bytes(), &p)
	return resolver, publisher, fakes, eventID, w.Code, w.Body.String(), p
}

func TestWorkflowTaskEvent_NoUsablePrincipal_Answers422AndQueuesNothing(t *testing.T) {
	cases := map[string]error{
		"token service has no principal for the tenant": fmt.Errorf("token service answered 404 for tenant-secret-marker: %w", port.ErrNoAutomationPrincipal),
		"principal exists but is not active":            port.ErrNoAutomationPrincipal,
	}
	for name, resolverErr := range cases {
		t.Run(name, func(t *testing.T) {
			resolver, publisher, fakes, eventID, code, body, p := fxfPostTaskCreated(t, resolverErr)

			require.Equal(t, http.StatusUnprocessableEntity, code)
			assert.Equal(t, "NO_AUTOMATION_PRINCIPAL", p.Code)
			assert.Equal(t, http.StatusUnprocessableEntity, p.Status)
			assert.NotEmpty(t, p.Title)
			assert.Equal(t, port.ErrNoAutomationPrincipal.Error(), p.Detail)
			assert.NotContains(t, body, "tenant-secret-marker", "the cause text of a wrapped error is not shown to the caller")
			assert.Equal(t, []uuid.UUID{testTenantID}, resolver.tenants)
			assert.Empty(t, publisher.published, "no principal, so the job is not put on the stream")

			processed, err := fakes.processedEvents.IsProcessed(context.Background(), eventID, port.ConsumerConnector)
			require.NoError(t, err)
			assert.False(t, processed, "the event is not marked processed")
		})
	}
}

func TestWorkflowTaskEvent_TokenServiceOutage_StillAnswers503(t *testing.T) {
	_, publisher, fakes, eventID, code, _, p := fxfPostTaskCreated(t, fmt.Errorf("%w: token service: connection refused", adapterhttp.ErrUpstreamUnavailable))

	assert.Equal(t, http.StatusServiceUnavailable, code)
	assert.Equal(t, "UPSTREAM_UNAVAILABLE", p.Code)
	assert.Empty(t, publisher.published)
	processed, err := fakes.processedEvents.IsProcessed(context.Background(), eventID, port.ConsumerConnector)
	require.NoError(t, err)
	assert.False(t, processed)
}

func TestWorkflowTaskEvent_OtherResolverFailure_StillAnswers500(t *testing.T) {
	_, publisher, fakes, eventID, code, body, p := fxfPostTaskCreated(t, errors.New("unexpected-cause-marker"))

	assert.Equal(t, http.StatusInternalServerError, code)
	assert.Equal(t, "INTERNAL_ERROR", p.Code)
	assert.NotContains(t, body, "unexpected-cause-marker")
	assert.Empty(t, publisher.published)
	processed, err := fakes.processedEvents.IsProcessed(context.Background(), eventID, port.ConsumerConnector)
	require.NoError(t, err)
	assert.False(t, processed)
}
