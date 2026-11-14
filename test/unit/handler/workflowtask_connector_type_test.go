package handler_test

import (
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestHandleWorkflowTaskCreated_NoPrincipalClient_QueuesWithNilPrincipalAndEmptySub(t *testing.T) {
	fakes := newEventsFakes()
	publisher := &fakeConnectorEventPublisher{}
	router := newInternalRouter(handler.New(handler.Services{
		ProcessedEvents: fakes.processedEvents,
		Recency:         fakes.recency,
		Delegation:      fakes.delegation,
		TenantLifecycle: fakes.tenantLifecycle,
		UserSafetyNet:   fakes.userSafetyNet,
		OOOAvailability: fakes.oooAvailability,
		ConnectorEvents: publisher,
		Log:             fakes.log,
	}))
	eventID := uuid.New()
	connectorType := "storage"

	w := postEvent(router, envelope("WorkflowTaskCreated", eventID, testTenantID, time.Now(), connectorTaskCreatedPayload(&connectorType)))

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Len(t, publisher.published, 1)
	entry := publisher.published[0]
	assert.Equal(t, uuid.Nil, entry.PrincipalID)
	assert.Empty(t, entry.PrincipalSub)
	assert.Equal(t, eventID, entry.EventID)
	assert.Equal(t, testTenantID, entry.TenantID)
	assert.Equal(t, testTaskID, entry.TaskID)
	assert.Equal(t, testInstID, entry.InstanceID)
	done, err := fakes.processedEvents.IsProcessed(t.Context(), eventID, port.ConsumerConnector)
	require.NoError(t, err)
	assert.True(t, done, "the event is recorded as handled")
}

// A whitespace-only or unknown connector_type is failed by the connector worker with error class unknown_connector_type (cmd/connector-worker/dispatch.go), so queuing it unchanged does not leave the task stuck.
func TestHandleWorkflowTaskCreated_ConnectorTypeIsNotChecked_QueuedUnchanged(t *testing.T) {
	cases := []struct{ name, connectorType string }{
		{"whitespace only", "   "},
		{"not in the catalogue", "no-such-connector"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			fakes := newEventsFakes()
			publisher := &fakeConnectorEventPublisher{}
			router := newInternalRouter(newConnectorEventsHandler(fakes, publisher))
			eventID := uuid.New()
			connectorType := tc.connectorType

			w := postEvent(router, envelope("WorkflowTaskCreated", eventID, testTenantID, time.Now(), connectorTaskCreatedPayload(&connectorType)))

			require.Equal(t, http.StatusOK, w.Code, w.Body.String())
			require.Len(t, publisher.published, 1)
			assert.Equal(t, tc.connectorType, publisher.published[0].ConnectorType)
			assert.Equal(t, eventID, publisher.published[0].EventID)
			assert.Equal(t, testTaskID, publisher.published[0].TaskID)
		})
	}
}
