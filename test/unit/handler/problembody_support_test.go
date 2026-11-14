package handler_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type hrdParam struct {
	Name   string `json:"name"`
	Reason string `json:"reason"`
}

type hrdProblemBody struct {
	Status        int        `json:"status"`
	Code          string     `json:"code"`
	Detail        string     `json:"detail"`
	Instance      string     `json:"instance"`
	InvalidParams []hrdParam `json:"invalid_params"`
}

// hrdDecodeProblem fails the test unless the response is a problem body with
// the given status, and returns it decoded.
func hrdDecodeProblem(t *testing.T, w *httptest.ResponseRecorder, status int) hrdProblemBody {
	t.Helper()
	require.Equal(t, status, w.Code, w.Body.String())
	assert.Equal(t, "application/problem+json", strings.Split(w.Header().Get("Content-Type"), ";")[0])
	var p hrdProblemBody
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &p), w.Body.String())
	assert.Equal(t, status, p.Status)
	return p
}

// hrdAssertNoInternalText fails when decoder, validator or runtime wording
// reached the response body.
func hrdAssertNoInternalText(t *testing.T, w *httptest.ResponseRecorder) {
	t.Helper()
	body := w.Body.String()
	for _, leak := range []string{
		"cannot unmarshal", "Go struct", "Go value", "uuid.UUID", "map[string]", "invalid UUID",
		"Key: '", "Field validation", "unexpected end", "invalid character", "EOF", "goroutine", "panic",
	} {
		assert.NotContains(t, body, leak)
	}
}

// hrdPublicRequest builds a request carrying the gateway identity headers
// with a raw (unmarshalled) body.
func hrdPublicRequest(method, target, rawBody string, admin bool) *http.Request {
	r := httptest.NewRequest(method, target, strings.NewReader(rawBody))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("x-tenant-id", testTenantID.String())
	r.Header.Set("x-user-id", testUserID.String())
	if admin {
		r.Header.Set("x-tenant-roles", "tenant_admin")
	}
	return r
}

// hrdInternalPost posts a raw body to a route that carries no gateway identity.
func hrdInternalPost(router *gin.Engine, target, contentType, rawBody string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, target, strings.NewReader(rawBody))
	if contentType != "" {
		r.Header.Set("Content-Type", contentType)
	}
	w := httptest.NewRecorder()
	router.ServeHTTP(w, r)
	return w
}
