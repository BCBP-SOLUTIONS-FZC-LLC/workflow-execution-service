package handler_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func glueFrame(version, compression byte, payload []byte) string {
	id := uuid.New()
	frame := append([]byte{version, compression}, id[:]...)
	return base64.StdEncoding.EncodeToString(append(frame, payload...))
}

func TestHandleInternalEvent_SchemaRegistryFrame_IsDecodedAsProductionWiresIt(t *testing.T) {
	fakes := newEventsFakes()
	var gotIn port.UserDeletedInput
	fakes.userSafetyNet.vacateAssignments = func(_ context.Context, in port.UserDeletedInput) error {
		gotIn = in
		return nil
	}
	body, err := json.Marshal(map[string]any{"user_id": testUserID.String(), "deleted_at": time.Now().Format(time.RFC3339)})
	require.NoError(t, err)
	router := newInternalRouter(newEventsHandler(fakes))

	w := postEvent(router, envelopeWithSchemaID("UserDeleted", uuid.New(), testTenantID, time.Now(), uuid.NewString(), glueFrame(0x03, 0x00, body)))

	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	assert.Equal(t, testUserID, gotIn.UserID, "a schema-registry framed payload must reach the handler decoded")
}

func TestHandleInternalEvent_SchemaRegistryFrame_UndecodableIs502(t *testing.T) {
	valid := []byte(`{"user_id":"` + testUserID.String() + `"}`)
	for name, framed := range map[string]any{
		"not base64":          "###",
		"not a string":        map[string]any{"not": "a string"},
		"wrong header":        glueFrame(0x01, 0x00, valid),
		"compressed":          glueFrame(0x03, 0x05, valid),
		"not json":            glueFrame(0x03, 0x00, []byte("\x00\x01binary")),
		"shorter than header": base64.StdEncoding.EncodeToString([]byte{0x03, 0x00}),
	} {
		t.Run(name, func(t *testing.T) {
			fakes := newEventsFakes()
			called := false
			fakes.userSafetyNet.vacateAssignments = func(context.Context, port.UserDeletedInput) error {
				called = true
				return nil
			}
			router := newInternalRouter(newEventsHandler(fakes))
			before := decodeFailedSampleCount(t, "UserDeleted")

			w := postEvent(router, envelopeWithSchemaID("UserDeleted", uuid.New(), testTenantID, time.Now(), uuid.NewString(), framed))

			assert.Equal(t, http.StatusBadGateway, w.Code, w.Body.String())
			assert.Equal(t, before+1, decodeFailedSampleCount(t, "UserDeleted"))
			assert.False(t, called)
		})
	}
}
