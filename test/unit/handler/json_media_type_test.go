package handler_test

import (
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
)

func TestBindBody_ContentType_AcceptsOnlyTheJSONMediaType(t *testing.T) {
	cases := []struct {
		name        string
		contentType string
		wantStatus  int
	}{
		{"plain", "application/json", http.StatusNoContent},
		{"with charset", "application/json; charset=utf-8", http.StatusNoContent},
		{"upper case", "Application/JSON", http.StatusNoContent},
		{"empty header", "", http.StatusNoContent},
		{"longer subtype", "application/jsonfoo", http.StatusUnsupportedMediaType},
		{"structured suffix", "application/json+ld", http.StatusUnsupportedMediaType},
		{"text", "text/plain", http.StatusUnsupportedMediaType},
		{"unparsable", "application/json; =", http.StatusUnsupportedMediaType},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tasks := &hhaConnectorTasks{}
			router := hhaInternalRouter(handler.New(handler.Services{ConnectorTasks: tasks}))

			w := hhaServe(router, http.MethodPost, hhaConnectorPath(uuid.New(), "complete"),
				tc.contentType, `{"tenant_id":"`+uuid.NewString()+`"}`)

			assert.Equal(t, tc.wantStatus, w.Code, w.Body.String())
		})
	}
}
