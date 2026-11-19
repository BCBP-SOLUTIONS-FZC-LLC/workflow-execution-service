package handler_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

const w18aWaitKey = "finance/Receive_Bid"

func w18aReceivePlan(t *testing.T) string {
	t.Helper()
	return w18aPlanJSON(t, dsl.StageDef{Type: "receive_task", NodeID: "Receive_Bid", Extras: map[string]string{"message": "bid-received"}})
}

func TestFireOutsideBoundary_UnknownKeyOnAFinishedOrPausedInstance_Returns404WaitNotFound(t *testing.T) {
	cases := []struct {
		name   string
		status domain.InstanceStatus
	}{
		{"a finished instance", domain.InstanceStatusCompleted},
		{"a paused instance", domain.InstanceStatusPaused},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			world := w18aNewWorld(t, w18aReceivePlan(t))
			world.seedInstance(tc.status)
			world.temporal.waiting = []string{"finance/Receive_Bid#Boundary_Missing"}

			rec := do(world.router, asAdmin(req(http.MethodPost, firePath("finance%2FReceive_Bid", "Boundary_Missing"), nil)))

			require.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())
			var body problemBody
			decodeJSON(t, rec.Body, &body)
			assert.Equal(t, "WAIT_NOT_FOUND", body.Code, "the unknown key is reported, not the instance state")
			assert.Equal(t, []w18aRead{{testTenantID, testInstID}}, world.instances.reads)
			assert.Equal(t, []w18aRead{{testTenantID, world.versionID}}, world.definitions.calls)
			assert.Empty(t, world.temporal.queries, "the workflow is not asked")
			assert.Empty(t, world.temporal.signals)
		})
	}
}

func TestCompleteOutsideWait_PlanOverTheStageBudget_Returns409VersionInvalid(t *testing.T) {
	world := w18aNewWorld(t, w18aOverBudgetPlanJSON(t))
	world.seedInstance(domain.InstanceStatusRunning)
	world.temporal.waiting = []string{"finance/T0"}

	rec := do(world.router, asAdmin(req(http.MethodPost, completePath("finance%2FT0"), nil)))

	require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
	var body problemBody
	decodeJSON(t, rec.Body, &body)
	assert.Equal(t, "VERSION_INVALID", body.Code)
	assert.Equal(t, []w18aRead{{testTenantID, testInstID}}, world.instances.reads)
	assert.Equal(t, []w18aRead{{testTenantID, world.versionID}}, world.definitions.calls)
	assert.Empty(t, world.temporal.queries, "the workflow is not asked about a broken plan")
	assert.Empty(t, world.temporal.signals)
}

func TestCompleteOutsideWait_SignalFails_AnswersByTheKindOfFailure(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		wantStatus int
		wantCode   string
		wantDetail string
		hidesCause bool
	}{
		{
			name:       "the cluster cannot take the signal",
			err:        fmt.Errorf("%w: %w", port.ErrTemporalUnavailable, errW18aTemporal),
			wantStatus: http.StatusServiceUnavailable, wantCode: "UPSTREAM_UNAVAILABLE",
			wantDetail: "the workflow engine could not take the request; retry it", hidesCause: true,
		},
		{
			name:       "the workflow is gone",
			err:        fmt.Errorf("%w: %w", port.ErrWorkflowNotFound, errW18aTemporal),
			wantStatus: http.StatusConflict, wantCode: "INVALID_INSTANCE_STATE",
		},
		{
			name:       "any other failure",
			err:        errW18aTemporal,
			wantStatus: http.StatusInternalServerError, wantCode: "INTERNAL_ERROR",
			wantDetail: "an unexpected error occurred", hidesCause: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			world := w18aNewWorld(t, w18aReceivePlan(t))
			world.seedInstance(domain.InstanceStatusRunning)
			world.temporal.waiting = []string{w18aWaitKey}
			world.temporal.signalErr = tc.err

			rec := do(world.router, asAdmin(req(http.MethodPost, completePath("finance%2FReceive_Bid"), map[string]any{"variables": map[string]any{"bid": "42"}})))

			require.Equal(t, tc.wantStatus, rec.Code, rec.Body.String())
			var body problemBody
			decodeJSON(t, rec.Body, &body)
			assert.Equal(t, tc.wantCode, body.Code)
			if tc.hidesCause {
				assert.Equal(t, tc.wantDetail, body.Detail)
				assert.NotContains(t, rec.Body.String(), "10.9.8.7", "the cluster address is not echoed")
				assert.NotContains(t, rec.Body.String(), "w18a temporal", "the cause is not echoed")
			}
			assert.Equal(t, []string{"tenant:subject:w18a " + port.QueryWaitingExternal}, world.temporal.queries, "the workflow was asked what it waits for, once")
			require.Len(t, world.temporal.signals, 1, "the signal was tried once")
			got := world.temporal.signals[0]
			assert.Equal(t, "tenant:subject:w18a", got.workflowID)
			assert.Equal(t, testInstID, got.instanceID)
			assert.Equal(t, port.SignalInstanceExternalAnswer, got.name)
			assert.JSONEq(t, `{"Key":"finance/Receive_Bid","Variables":{"bid":"42"}}`, string(got.payload))
		})
	}
}
