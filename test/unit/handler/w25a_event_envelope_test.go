package handler_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

const w25aIngestMetric = "workflow_execution_service_internal_events_ingest_total"

// w25aCountingInbox records every duplicate check, to show a refused envelope reached no handler.
type w25aCountingInbox struct{ checks int }

func (i *w25aCountingInbox) IsProcessed(context.Context, uuid.UUID, string) (bool, error) {
	i.checks++
	return false, nil
}
func (i *w25aCountingInbox) MarkProcessed(context.Context, uuid.UUID, string) error {
	i.checks++
	return nil
}
func (i *w25aCountingInbox) Prune(context.Context, time.Duration) (int64, error) { return 0, nil }

var _ port.ProcessedEventRepository = (*w25aCountingInbox)(nil)

// Every one of the four event routes shares one envelope check: an empty type or a zero timestamp is a 400 that
// counts as bad_payload under the unknown label, a non-JSON content type is a 415 counted the same way, and a
// schema-registry payload that cannot be decoded is a 502 EVENT_DECODE_FAILED counted as decode_failed.
func TestInternalEvents_EnvelopeRefusals_AreTheSameOnEveryRoute(t *testing.T) {
	obstest.Init(t)
	routes := []struct{ name, path, eventType string }{
		{"delegation", "/api/v1/internal/events/delegation", "DelegationStarted"},
		{"user profile", "/api/v1/internal/events/user-profile", "UserDeleted"},
		{"tenant", "/api/v1/internal/events/tenant", "TenantStateChanged"},
		{"workflow task", "/api/v1/internal/events/workflow-task", "WorkflowTaskCreated"},
	}
	at := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	refusals := []struct {
		name       string
		build      func(eventType string) (contentType, body string)
		wantStatus int
		wantCode   string
		wantDetail string
		wantMetric string
	}{
		{
			name: "empty type",
			build: func(string) (string, string) {
				return "application/json", w25aEnvelopeJSON(t, "", at, "")
			},
			wantStatus: http.StatusBadRequest, wantCode: "BAD_REQUEST", wantDetail: "missing event type", wantMetric: "bad_payload",
		},
		{
			name: "zero timestamp",
			build: func(eventType string) (string, string) {
				return "application/json", w25aEnvelopeJSON(t, eventType, time.Time{}, "")
			},
			wantStatus: http.StatusBadRequest, wantCode: "BAD_REQUEST", wantDetail: "missing or zero envelope timestamp", wantMetric: "bad_payload",
		},
		{
			name: "non-JSON content type",
			build: func(eventType string) (string, string) {
				return "text/plain", w25aEnvelopeJSON(t, eventType, at, "")
			},
			wantStatus: http.StatusUnsupportedMediaType, wantCode: "UNSUPPORTED_MEDIA_TYPE", wantDetail: "request body must be application/json", wantMetric: "bad_payload",
		},
		{
			name: "schema-registry payload that is not a base64 string",
			build: func(eventType string) (string, string) {
				return "application/json", w25aEnvelopeJSON(t, eventType, at, "schema-1")
			},
			wantStatus: http.StatusBadGateway, wantCode: "EVENT_DECODE_FAILED", wantDetail: "schema-registry payload is not a base64 JSON string", wantMetric: "decode_failed",
		},
	}
	for _, rt := range routes {
		for _, rf := range refusals {
			t.Run(rt.name+": "+rf.name, func(t *testing.T) {
				inbox := &w25aCountingInbox{}
				router := newInternalRouter(handler.New(handler.Services{ProcessedEvents: inbox}))
				labels := map[string]string{"event_type": "unknown", "result": rf.wantMetric}
				before := obstest.Sum(t, w25aIngestMetric, labels)
				contentType, body := rf.build(rt.eventType)
				r := httptest.NewRequest(http.MethodPost, rt.path, strings.NewReader(body))
				r.Header.Set("Content-Type", contentType)

				w := httptest.NewRecorder()
				router.ServeHTTP(w, r)

				p := w25aDecodeProblem(t, w, rf.wantStatus)
				assert.Equal(t, rf.wantCode, p.Code)
				assert.Equal(t, rf.wantDetail, p.Detail)
				assert.Equal(t, rt.path, p.Instance)
				assert.Equal(t, before+1, obstest.Sum(t, w25aIngestMetric, labels), "the refusal is counted once under the unknown label")
				assert.Zero(t, inbox.checks, "no handler ran for a refused envelope")
			})
		}
	}
}

// w25aEnvelopeJSON writes an envelope with the given type and timestamp; an empty-string payload schema leaves
// the payload an object, any other schema id sends a payload that is not a base64 string.
func w25aEnvelopeJSON(t *testing.T, eventType string, at time.Time, schemaID string) string {
	t.Helper()
	env := map[string]any{
		"id": uuid.NewString(), "tenant_id": uuid.NewString(), "type": eventType, "data": map[string]any{},
	}
	if !at.IsZero() {
		env["time"] = at.Format(time.RFC3339)
	}
	if schemaID != "" {
		env["dataschema"] = schemaID
	}
	b, err := json.Marshal(env)
	require.NoError(t, err)
	return string(b)
}
