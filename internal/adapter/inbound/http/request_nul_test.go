package http

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRouter_ANulCharacterInTheQueryOrThePathIsRefusedWith400(t *testing.T) {
	cases := []struct{ name, method, target string }{
		{"query value", http.MethodGet, "/api/v1/instances?subject_id=a%00b"},
		{"query name", http.MethodGet, "/api/v1/instances?subj%00ect=a"},
		{"path segment", http.MethodPost, "/api/v1/instances/6b0e8e0a-1c3a-4f5e-9d7a-2b4c6d8e0f11/nodes/a%00b/complete"},
		{"internal path segment", http.MethodPost, "/api/v1/internal/connector-tasks/a%00b/complete"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := testRouter(fakeDBPinger{}, fakePinger{}, fakePinger{})

			w := httptest.NewRecorder()
			r.ServeHTTP(w, httptest.NewRequest(tc.method, tc.target, nil))

			assert.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
			assert.Contains(t, w.Body.String(), "BAD_REQUEST")
		})
	}
}

func TestRouter_AQueryWithoutANulCharacterIsNotRefusedByTheCheck(t *testing.T) {
	r := testRouter(fakeDBPinger{}, fakePinger{}, fakePinger{})

	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/v1/instances?subject_id=a%20b", nil))

	assert.NotEqual(t, http.StatusBadRequest, w.Code, "an ordinary query reaches the identity check instead")
}
