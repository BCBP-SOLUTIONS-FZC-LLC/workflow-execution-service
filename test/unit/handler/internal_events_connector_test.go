package handler_test

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type fakeConnectorEventPublisher struct {
	publishFunc func(context.Context, port.ConnectorTaskCreatedEvent) error
	published   []port.ConnectorTaskCreatedEvent
}

func (f *fakeConnectorEventPublisher) PublishTaskCreated(ctx context.Context, event port.ConnectorTaskCreatedEvent) error {
	f.published = append(f.published, event)
	if f.publishFunc != nil {
		return f.publishFunc(ctx, event)
	}
	return nil
}

func newConnectorEventsHandler(fakes *eventsFakes, publisher port.ConnectorEventPublisher) *handler.Handler {
	return handler.New(handler.Services{
		ProcessedEvents: fakes.processedEvents,
		Recency:         fakes.recency,
		Delegation:      fakes.delegation,
		TenantLifecycle: fakes.tenantLifecycle,
		UserSafetyNet:   fakes.userSafetyNet,
		OOOAvailability: fakes.oooAvailability,
		ConnectorEvents: publisher,
		Log:             fakes.log,
	})
}

func connectorTaskCreatedPayload(connectorType *string) map[string]any {
	return map[string]any{
		"workflow_instance_id": testInstID.String(),
		"task_id":              testTaskID.String(),
		"node_key":             "dept-1/nodeA",
		"department_id":        testTenantID.String(),
		"connector_type":       connectorType,
		"resolved_inputs":      map[string]any{"bucket": "b1"},
		"output_mapping":       []map[string]any{{"source": "contentRef", "target": "docRef"}},
	}
}

func TestHandleWorkflowTaskCreated_ConnectorTyped_Publishes(t *testing.T) {
	fakes := newEventsFakes()
	publisher := &fakeConnectorEventPublisher{}
	router := newInternalRouter(newConnectorEventsHandler(fakes, publisher))

	connectorType := "storage"
	w := postEvent(router, envelope("workflow.task.created", uuid.New(), testTenantID, time.Now(), connectorTaskCreatedPayload(&connectorType)))

	assert.Equal(t, http.StatusOK, w.Code)
	require.Len(t, publisher.published, 1)
	assert.Equal(t, testTaskID, publisher.published[0].TaskID)
	assert.Equal(t, "storage", publisher.published[0].ConnectorType)
	assert.Equal(t, "b1", publisher.published[0].ResolvedInputs["bucket"])
}

func TestHandleWorkflowTaskCreated_NonConnector_SkipsPublish(t *testing.T) {
	fakes := newEventsFakes()
	publisher := &fakeConnectorEventPublisher{}
	router := newInternalRouter(newConnectorEventsHandler(fakes, publisher))

	w := postEvent(router, envelope("workflow.task.created", uuid.New(), testTenantID, time.Now(), connectorTaskCreatedPayload(nil)))

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Empty(t, publisher.published)
}

func TestHandleWorkflowTaskCreated_NoPublisherConfigured_FailsOpen(t *testing.T) {
	fakes := newEventsFakes()
	router := newInternalRouter(newConnectorEventsHandler(fakes, nil))

	connectorType := "storage"
	w := postEvent(router, envelope("workflow.task.created", uuid.New(), testTenantID, time.Now(), connectorTaskCreatedPayload(&connectorType)))

	assert.Equal(t, http.StatusOK, w.Code)
}

func TestHandleWorkflowTaskCreated_PublisherError_Returns500(t *testing.T) {
	fakes := newEventsFakes()
	publisher := &fakeConnectorEventPublisher{publishFunc: func(context.Context, port.ConnectorTaskCreatedEvent) error {
		return errors.New("stream unreachable")
	}}
	router := newInternalRouter(newConnectorEventsHandler(fakes, publisher))

	connectorType := "storage"
	w := postEvent(router, envelope("workflow.task.created", uuid.New(), testTenantID, time.Now(), connectorTaskCreatedPayload(&connectorType)))

	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestHandleWorkflowTaskCreated_AlreadyProcessed_SkipsPublish(t *testing.T) {
	fakes := newEventsFakes()
	publisher := &fakeConnectorEventPublisher{}
	router := newInternalRouter(newConnectorEventsHandler(fakes, publisher))

	connectorType := "storage"
	eventID := uuid.New()
	body := connectorTaskCreatedPayload(&connectorType)

	w1 := postEvent(router, envelope("workflow.task.created", eventID, testTenantID, time.Now(), body))
	require.Equal(t, http.StatusOK, w1.Code)
	require.Len(t, publisher.published, 1)

	w2 := postEvent(router, envelope("workflow.task.created", eventID, testTenantID, time.Now(), body))
	assert.Equal(t, http.StatusOK, w2.Code)
	assert.Len(t, publisher.published, 1, "a replayed event must not publish a second time")
}

func TestHandleWorkflowTaskCreated_InvalidTaskID_Returns400(t *testing.T) {
	fakes := newEventsFakes()
	publisher := &fakeConnectorEventPublisher{}
	router := newInternalRouter(newConnectorEventsHandler(fakes, publisher))

	connectorType := "storage"
	body := connectorTaskCreatedPayload(&connectorType)
	body["task_id"] = "not-a-uuid"

	w := postEvent(router, envelope("workflow.task.created", uuid.New(), testTenantID, time.Now(), body))

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Empty(t, publisher.published)
}

func TestHandleWorkflowTaskCreated_MalformedPayload_Returns400(t *testing.T) {
	fakes := newEventsFakes()
	router := newInternalRouter(newConnectorEventsHandler(fakes, &fakeConnectorEventPublisher{}))

	w := do(router, internalReq(http.MethodPost, "/api/v1/internal/events", map[string]any{
		"id":        uuid.New().String(),
		"type":      "workflow.task.created",
		"tenant_id": testTenantID.String(),
		"time":      time.Now().Format(time.RFC3339),
		"data":      "not-an-object",
	}))

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleWorkflowTaskCreated_InvalidEventID_Returns400(t *testing.T) {
	fakes := newEventsFakes()
	router := newInternalRouter(newConnectorEventsHandler(fakes, &fakeConnectorEventPublisher{}))

	connectorType := "storage"
	body := envelope("workflow.task.created", uuid.New(), testTenantID, time.Now(), connectorTaskCreatedPayload(&connectorType))
	body["id"] = "not-a-uuid"

	w := do(router, internalReq(http.MethodPost, "/api/v1/internal/events", body))
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleWorkflowTaskCreated_InvalidTenantID_Returns400(t *testing.T) {
	fakes := newEventsFakes()
	router := newInternalRouter(newConnectorEventsHandler(fakes, &fakeConnectorEventPublisher{}))

	connectorType := "storage"
	body := envelope("workflow.task.created", uuid.New(), testTenantID, time.Now(), connectorTaskCreatedPayload(&connectorType))
	body["tenant_id"] = "not-a-uuid"

	w := do(router, internalReq(http.MethodPost, "/api/v1/internal/events", body))
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleWorkflowTaskCreated_InvalidInstanceID_Returns400(t *testing.T) {
	fakes := newEventsFakes()
	publisher := &fakeConnectorEventPublisher{}
	router := newInternalRouter(newConnectorEventsHandler(fakes, publisher))

	connectorType := "storage"
	body := connectorTaskCreatedPayload(&connectorType)
	body["workflow_instance_id"] = "not-a-uuid"

	w := postEvent(router, envelope("workflow.task.created", uuid.New(), testTenantID, time.Now(), body))

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Empty(t, publisher.published)
}

func TestHandleWorkflowTaskCreated_InvalidDepartmentID_Returns400(t *testing.T) {
	fakes := newEventsFakes()
	publisher := &fakeConnectorEventPublisher{}
	router := newInternalRouter(newConnectorEventsHandler(fakes, publisher))

	connectorType := "storage"
	body := connectorTaskCreatedPayload(&connectorType)
	body["department_id"] = "not-a-uuid"

	w := postEvent(router, envelope("workflow.task.created", uuid.New(), testTenantID, time.Now(), body))

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Empty(t, publisher.published)
}
