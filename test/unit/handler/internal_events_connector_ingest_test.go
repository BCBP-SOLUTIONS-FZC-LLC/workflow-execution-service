package handler_test

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type ingfixGate struct {
	dispatchable bool
	err          error
	tenants      []uuid.UUID
	tasks        []uuid.UUID
}

func (g *ingfixGate) Dispatchable(_ context.Context, tenantID, taskID uuid.UUID) (bool, error) {
	g.tenants = append(g.tenants, tenantID)
	g.tasks = append(g.tasks, taskID)
	return g.dispatchable, g.err
}

func ingfixRouter(fakes *eventsFakes, publisher port.ConnectorEventPublisher, gate port.ConnectorDispatchGate) *gin.Engine {
	return newInternalRouter(handler.New(handler.Services{
		AutomationPrincipals: fakePrincipalResolver{principal: port.AutomationPrincipal{ID: uuid.New(), Subject: "principal-sub"}},
		ProcessedEvents:      fakes.processedEvents,
		Recency:              fakes.recency,
		Delegation:           fakes.delegation,
		TenantLifecycle:      fakes.tenantLifecycle,
		UserSafetyNet:        fakes.userSafetyNet,
		OOOAvailability:      fakes.oooAvailability,
		ConnectorEvents:      publisher,
		ConnectorDispatch:    gate,
		Log:                  fakes.log,
	}))
}

func ingfixPayload() map[string]any {
	connectorType := "storage"
	return connectorTaskCreatedPayload(&connectorType)
}

func ingfixPost(router *gin.Engine, eventID uuid.UUID, body map[string]any) int {
	return do(router, internalReq(http.MethodPost, "/api/v1/internal/events/workflow-task",
		envelope("WorkflowTaskCreated", eventID, testTenantID, time.Now(), body))).Code
}

func TestIngestConnector_FinishedTaskOrEndedInstance_IsAcknowledgedWithoutQueuing(t *testing.T) {
	fakes := newEventsFakes()
	publisher := &fakeConnectorEventPublisher{}
	gate := &ingfixGate{dispatchable: false}
	router := ingfixRouter(fakes, publisher, gate)

	eventID := uuid.New()
	code := ingfixPost(router, eventID, ingfixPayload())

	assert.Equal(t, http.StatusOK, code)
	assert.Empty(t, publisher.published, "a task that is finished, or whose instance ended, is not queued")
	require.Len(t, gate.tasks, 1)
	assert.Equal(t, testTaskID, gate.tasks[0])
	assert.Equal(t, testTenantID, gate.tenants[0])

	processed, err := fakes.processedEvents.IsProcessed(context.Background(), eventID, port.ConsumerConnector)
	require.NoError(t, err)
	assert.True(t, processed, "the skipped event is marked processed so a redelivery is not re-evaluated")
}

func TestIngestConnector_OpenTaskOnLiveInstance_IsQueued(t *testing.T) {
	fakes := newEventsFakes()
	publisher := &fakeConnectorEventPublisher{}
	gate := &ingfixGate{dispatchable: true}
	router := ingfixRouter(fakes, publisher, gate)

	code := ingfixPost(router, uuid.New(), ingfixPayload())

	assert.Equal(t, http.StatusOK, code)
	require.Len(t, publisher.published, 1)
	assert.Equal(t, testTaskID, publisher.published[0].TaskID)
}

func TestIngestConnector_GateReadFailure_IsRetryableAndQueuesNothing(t *testing.T) {
	fakes := newEventsFakes()
	publisher := &fakeConnectorEventPublisher{}
	gate := &ingfixGate{err: errors.New("db down")}
	router := ingfixRouter(fakes, publisher, gate)

	eventID := uuid.New()
	code := ingfixPost(router, eventID, ingfixPayload())

	assert.Equal(t, http.StatusInternalServerError, code)
	assert.Empty(t, publisher.published)
	processed, err := fakes.processedEvents.IsProcessed(context.Background(), eventID, port.ConsumerConnector)
	require.NoError(t, err)
	assert.False(t, processed, "a failed read is redelivered, not recorded as handled")
}

func TestIngestConnector_NilDepartment_IsRefusedWith400(t *testing.T) {
	fakes := newEventsFakes()
	publisher := &fakeConnectorEventPublisher{}
	gate := &ingfixGate{dispatchable: true}
	router := ingfixRouter(fakes, publisher, gate)

	body := ingfixPayload()
	body["department_id"] = uuid.Nil.String()
	code := ingfixPost(router, uuid.New(), body)

	assert.Equal(t, http.StatusBadRequest, code)
	assert.Empty(t, publisher.published, "a connector task always belongs to a real department")
	assert.Empty(t, gate.tasks)
}

func TestIngestConnector_AbsentInputsAndMapping_AreEmptyObjectAndEmptyList(t *testing.T) {
	for name, drop := range map[string][]string{
		"absent":   {"resolved_inputs", "output_mapping"},
		"explicit": nil,
	} {
		t.Run(name, func(t *testing.T) {
			fakes := newEventsFakes()
			publisher := &fakeConnectorEventPublisher{}
			router := ingfixRouter(fakes, publisher, &ingfixGate{dispatchable: true})

			body := ingfixPayload()
			for _, key := range drop {
				delete(body, key)
			}
			if drop == nil {
				body["resolved_inputs"] = nil
				body["output_mapping"] = nil
			}
			code := ingfixPost(router, uuid.New(), body)

			require.Equal(t, http.StatusOK, code)
			require.Len(t, publisher.published, 1)
			assert.NotNil(t, publisher.published[0].ResolvedInputs, "the stream carries an object, never null")
			assert.Empty(t, publisher.published[0].ResolvedInputs)
			assert.NotNil(t, publisher.published[0].OutputMapping, "the stream carries a list, never null")
			assert.Empty(t, publisher.published[0].OutputMapping)
		})
	}
}
