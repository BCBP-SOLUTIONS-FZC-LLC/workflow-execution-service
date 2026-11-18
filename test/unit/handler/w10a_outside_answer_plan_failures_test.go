package handler_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/inbound/http/handler"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

// w10aCallToMissingPlan is a plan of the current schema whose only step calls a plan that is not in it,
// so expanding the calls fails.
func w10aCallToMissingPlan() string {
	return fmt.Sprintf(`{"main_plan":"main","schema_version":%d,"plans":[{"name":"main","departments":[],"execution":{"steps":[{"call_plan":{"node_id":"CA_1","plan":"Nowhere"}}]}}]}`, dsl.CurrentSchemaVersion)
}

// w10aOutsideWorld serves the real instance service over fakes: one running instance, an empty plan cache
// and a Definition Service client whose answer the test chooses.
func w10aOutsideWorld() (*w10aInstances, *w10aTemporal, *w10aDefinitions, uuid.UUID, *gin.Engine) {
	versionID := uuid.New()
	instances := &w10aInstances{rows: []*domain.Instance{{
		ID: testInstID, TenantID: testTenantID, WorkflowVersionID: versionID,
		TemporalWorkflowID: "tenant:subject:key", Status: domain.InstanceStatusRunning,
	}}}
	temporal := &w10aTemporal{}
	definitions := &w10aDefinitions{}
	svc := &service.InstanceService{Instances: instances, Temporal: temporal, Definitions: definitions}
	return instances, temporal, definitions, versionID, newRouter(handler.New(handler.Services{Instances: svc}))
}

func TestCompleteOutsideWait_CalledModulesCannotBeExpanded_Returns409VersionInvalid(t *testing.T) {
	instances, temporal, definitions, versionID, router := w10aOutsideWorld()
	definitions.resp = &port.CompiledWorkflow{VersionID: versionID, Status: "PUBLISHED", IsValid: true, CompiledPlanJSON: w10aCallToMissingPlan()}

	rec := do(router, asAdmin(req(http.MethodPost, completePath("finance%2FReceive_Bid"), nil)))

	require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
	var body problemBody
	decodeJSON(t, rec.Body, &body)
	assert.Equal(t, "VERSION_INVALID", body.Code)
	assert.Equal(t, []w10aRead{{testTenantID, testInstID}}, instances.reads)
	assert.Equal(t, []w10aRead{{testTenantID, versionID}}, definitions.calls, "the plan was read once, for this tenant and the instance's version")
	assert.Empty(t, temporal.queries, "the workflow is not asked what it waits for")
	assert.Empty(t, temporal.signals, "nothing is signalled")
}

func TestCompleteOutsideWait_CompiledPlanNotCachedAndDefinitionsFails_Returns503UpstreamUnavailable(t *testing.T) {
	obstest.Init(t)
	cases := []struct {
		name string
		err  error
	}{
		{name: "the Definition Service is down", err: fmt.Errorf("%w: %w", port.ErrDefinitionUnavailable, errW10aDefinitions)},
		{name: "the Definition Service rejects the version", err: fmt.Errorf("%w: status 404", port.ErrDefinitionRejected)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, temporal, definitions, versionID, router := w10aOutsideWorld()
			definitions.err = tc.err
			before := obstest.Sum(t, w10aUpstreamMetric, map[string]string{"dependency": "definition_service"})

			rec := do(router, asAdmin(req(http.MethodPost, completePath("finance%2FReceive_Bid"), nil)))

			require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
			var body problemBody
			decodeJSON(t, rec.Body, &body)
			assert.Equal(t, "UPSTREAM_UNAVAILABLE", body.Code)
			assert.Equal(t, before+1, obstest.Sum(t, w10aUpstreamMetric, map[string]string{"dependency": "definition_service"}))
			assert.Equal(t, []w10aRead{{testTenantID, versionID}}, definitions.calls, "the plan was asked for once, for this tenant and the instance's version")
			assert.Empty(t, temporal.queries)
			assert.Empty(t, temporal.signals)
		})
	}
}
