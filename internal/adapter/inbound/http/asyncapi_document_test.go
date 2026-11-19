package http

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

func serveAsyncAPI(t *testing.T, h gin.HandlerFunc) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET("/asyncapi", h)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/asyncapi", nil))
	return w
}

const cyclicSpecHead = `
asyncapi: "3.0.0"
info:
  title: cyclic
  version: "1"
components:
  messages:
    Started:
      name: Started
      payload:
        $ref: "#/components/schemas/Entry"
  schemas:
`

func TestAsyncAPIHandler_ASchemaThatReferencesItselfAnswersTheProblemAndKeepsTheProcessAlive(t *testing.T) {
	specs := map[string]string{
		"a ref to itself": cyclicSpecHead + `
    Entry:
      $ref: "#/components/schemas/Entry"
`,
		"two refs in a ring": cyclicSpecHead + `
    Entry:
      $ref: "#/components/schemas/Other"
    Other:
      $ref: "#/components/schemas/Entry"
`,
		"an allOf that includes itself": cyclicSpecHead + `
    Entry:
      allOf:
        - $ref: "#/components/schemas/Other"
    Other:
      allOf:
        - $ref: "#/components/schemas/Entry"
`,
	}
	for name, spec := range specs {
		t.Run(name, func(t *testing.T) {
			w := serveAsyncAPI(t, asyncAPIHandlerFor([]byte(spec), "dev"))

			require.Equal(t, http.StatusInternalServerError, w.Code)
			var problem map[string]any
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &problem))
			assert.Equal(t, "INTERNAL_ERROR", problem["code"])
			assert.False(t, strings.Contains(w.Body.String(), "Entry"), "the problem does not name its cause")
		})
	}
}

func TestAsyncAPIHandler_AnAcyclicSpecWithSharedSchemasStillRenders(t *testing.T) {
	spec := cyclicSpecHead + `
    Entry:
      allOf:
        - $ref: "#/components/schemas/Base"
        - $ref: "#/components/schemas/Extra"
    Base:
      type: object
    Extra:
      allOf:
        - $ref: "#/components/schemas/Base"
`
	w := serveAsyncAPI(t, asyncAPIHandlerFor([]byte(spec), "dev"))

	require.Equal(t, http.StatusOK, w.Code)
}

func TestAsyncAPIHandler_TheServedDocumentDoesNotDependOnTheWorkingDirectory(t *testing.T) {
	t.Chdir(t.TempDir())

	w := serveAsyncAPI(t, AsyncAPIHandler("dev"))

	require.Equal(t, http.StatusOK, w.Code)
	assert.Contains(t, w.Body.String(), "workflow-execution-service events")
	assert.Contains(t, w.Body.String(), `id="msg-WorkflowInstanceStarted"`)
}
