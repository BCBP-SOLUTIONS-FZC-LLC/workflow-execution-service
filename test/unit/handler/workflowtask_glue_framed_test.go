package handler_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestHandleWorkflowTaskCreated_SchemaRegistryFrame_IsPublishedLikeAPlainPayload(t *testing.T) {
	connectorType := "storage"
	payload := connectorTaskCreatedPayload(&connectorType)
	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	eventID := uuid.New()
	principal := port.AutomationPrincipal{ID: uuid.New(), Subject: "principal-sub"}

	run := func(t *testing.T, data any, schemaID string) (*fakeConnectorEventPublisher, int) {
		t.Helper()
		fakes := newEventsFakes()
		publisher := &fakeConnectorEventPublisher{}
		router := newInternalRouter(newConnectorEventsHandlerWith(fakes, publisher, fakePrincipalResolver{principal: principal}))
		var body map[string]any
		if schemaID == "" {
			body = envelope("WorkflowTaskCreated", eventID, testTenantID, time.Now(), payload)
		} else {
			body = envelopeWithSchemaID("WorkflowTaskCreated", eventID, testTenantID, time.Now(), schemaID, data)
		}
		w := postEvent(router, body)
		return publisher, w.Code
	}

	plain, plainCode := run(t, nil, "")
	framed, framedCode := run(t, glueFrame(0x03, 0x00, raw), uuid.NewString())

	require.Equal(t, http.StatusOK, plainCode)
	require.Equal(t, http.StatusOK, framedCode)
	require.Len(t, plain.published, 1)
	require.Len(t, framed.published, 1)

	got := framed.published[0]
	assert.Equal(t, eventID, got.EventID)
	assert.Equal(t, testTenantID, got.TenantID)
	assert.Equal(t, testInstID, got.InstanceID)
	assert.Equal(t, testTaskID, got.TaskID)
	assert.Equal(t, "dept-1/nodeA", got.NodeKey)
	assert.Equal(t, "storage", got.ConnectorType)
	assert.Equal(t, "b1", got.ResolvedInputs["bucket"])
	assert.Equal(t, principal.ID, got.PrincipalID)
	require.Len(t, got.OutputMapping, 1)
	assert.Equal(t, plain.published[0], got, "a framed payload must be queued exactly like the plain one")
}
