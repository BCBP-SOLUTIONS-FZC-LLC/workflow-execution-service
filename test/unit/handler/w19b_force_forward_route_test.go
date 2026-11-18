package handler_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

const (
	w19bUpstreamMetric = "workflow_execution_service_upstream_dependency_errors_total"
	w19bConflictMetric = "workflow_execution_service_record_version_conflict_total"
)

var errW19bCauseMarker = errors.New("cause-marker-7f3")

// w19bPlanWithoutTarget is a valid plan that has no node the tests force to, so a request it serves is refused
// with a different problem than one served from the plan the tests send to.
func w19bPlanWithoutTarget(t *testing.T) string {
	t.Helper()
	return w19bCollabJSON(t, dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{{
			Name: "main", TaskQueue: "wf-queue-default",
			Departments: []dsl.DepartmentDef{{ID: "legal", Label: "Legal", Stages: []dsl.StageDef{
				{Type: "receive_task", NodeID: "Receive_Contract", Extras: map[string]string{"message": "contract-received"}},
			}}},
			Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{Sequential: []string{"legal"}}}},
		}},
	})
}

func w19bAssertForceForwardSignalled(t *testing.T, rig *w19bRig, inst *domain.Instance, recordVersion int64) {
	t.Helper()
	require.Len(t, rig.sdk.signals, 1, "one signal reaches the workflow")
	got := rig.sdk.signals[0]
	assert.Equal(t, inst.TemporalWorkflowID, got.workflowID)
	assert.Equal(t, port.SignalInstanceForceFwd+":"+inst.ID.String(), got.channel)
	assert.Equal(t, w19bSignalBody{AdminUserID: testUserID.String(), TargetNodeKey: w19bTargetNode, RecordVersion: recordVersion}, w19bDecodeSignal(t, got.payload))
}

func TestForceForwardRoute_APlanWhoseCallsCannotBeExpanded_Returns409VersionInvalid(t *testing.T) {
	cases := []struct {
		name string
		plan func(t *testing.T) string
	}{
		{"a call to a plan the collaboration does not hold", func(t *testing.T) string {
			return w19bCollabJSON(t, dsl.CompiledCollaboration{MainPlan: "main", Plans: []*dsl.CompiledPlan{{
				Name: "main", TaskQueue: "wf-queue-default",
				Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{CallPlan: &dsl.CallPlanStep{NodeID: "Call_Ghost", Plan: "ghost"}}}},
			}}})
		}},
		{"a plan that calls itself", func(t *testing.T) string {
			return w19bCollabJSON(t, dsl.CompiledCollaboration{MainPlan: "main", Plans: []*dsl.CompiledPlan{{
				Name: "main", TaskQueue: "wf-queue-default",
				Execution: dsl.ExecutionPlan{Steps: []dsl.ExecutionStep{{CallPlan: &dsl.CallPlanStep{NodeID: "Call_Self", Plan: "main"}}}},
			}}})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w19bNewRig(t)
			rig.defs.planJSON = tc.plan(t)
			inst := rig.seed(domain.InstanceStatusRunning, 2)

			w := rig.forceForward(inst.ID, 2)

			require.Equal(t, http.StatusConflict, w.Code, w.Body.String())
			assert.Equal(t, "VERSION_INVALID", w19bProblemOf(t, w).Code)
			assert.Equal(t, []w19bPlanRead{{testTenantID, inst.WorkflowVersionID}}, rig.defs.reads, "the plan of the instance's own version is read for the caller's tenant")
			assert.Empty(t, rig.sdk.signals, "no workflow is signalled")
		})
	}
}

func TestForceForwardRoute_APlanInTheCache_IsServedWithoutTheDefinitionService(t *testing.T) {
	rig := w19bNewRig(t)
	inst := rig.seed(domain.InstanceStatusRunning, 2)
	key := w19bPlanCacheKey(testTenantID, inst.WorkflowVersionID)
	rig.cache.entries[key] = w19bCachedPlan(t, inst.WorkflowVersionID, w19bPlanJSON(t))
	rig.defs.planJSON = w19bPlanWithoutTarget(t)

	w := rig.forceForward(inst.ID, 2)

	require.Equal(t, http.StatusAccepted, w.Code, w.Body.String())
	assert.Equal(t, []string{key}, rig.cache.gets, "the entry is read under the tenant and version it belongs to")
	assert.Empty(t, rig.defs.reads, "the Definition Service is not called")
	assert.Empty(t, rig.cache.sets, "a hit writes nothing back")
	w19bAssertForceForwardSignalled(t, rig, inst, 2)
}

func TestForceForwardRoute_ACacheThatCannotServeThePlan_FallsBackToTheDefinitionServiceAndRewritesTheEntry(t *testing.T) {
	cases := []struct {
		name  string
		setup func(rig *w19bRig, key string)
	}{
		{"the cache read fails", func(rig *w19bRig, _ string) { rig.cache.getErr = errW19bCauseMarker }},
		{"the entry is not JSON", func(rig *w19bRig, key string) { rig.cache.entries[key] = "not json at all" }},
		{"the entry is JSON but no compiled plan", func(rig *w19bRig, key string) { rig.cache.entries[key] = "{}" }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w19bNewRig(t)
			inst := rig.seed(domain.InstanceStatusRunning, 2)
			key := w19bPlanCacheKey(testTenantID, inst.WorkflowVersionID)
			tc.setup(rig, key)

			w := rig.forceForward(inst.ID, 2)

			require.Equal(t, http.StatusAccepted, w.Code, w.Body.String())
			assert.Equal(t, []w19bPlanRead{{testTenantID, inst.WorkflowVersionID}}, rig.defs.reads)
			assert.GreaterOrEqual(t, rig.log.warns, 1, "the fallback is logged")
			require.Len(t, rig.cache.sets, 1, "the plan read from the Definition Service is written back")
			assert.Equal(t, key, rig.cache.sets[0].key)
			var stored port.CompiledWorkflow
			require.NoError(t, json.Unmarshal([]byte(rig.cache.sets[0].value), &stored))
			assert.Equal(t, rig.defs.planJSON, stored.CompiledPlanJSON, "the entry now holds the live plan")
			assert.Equal(t, inst.WorkflowVersionID, stored.VersionID)
			w19bAssertForceForwardSignalled(t, rig, inst, 2)
		})
	}
}

func TestForceForwardRoute_ACacheMiss_ReadsTheDefinitionServiceWritesTheEntryBackAndTheNextCallIsServedFromIt(t *testing.T) {
	rig := w19bNewRig(t)
	inst := rig.seed(domain.InstanceStatusRunning, 2)
	key := w19bPlanCacheKey(testTenantID, inst.WorkflowVersionID)

	w := rig.forceForward(inst.ID, 2)

	require.Equal(t, http.StatusAccepted, w.Code, w.Body.String())
	assert.Equal(t, []w19bPlanRead{{testTenantID, inst.WorkflowVersionID}}, rig.defs.reads)
	require.Len(t, rig.cache.sets, 1)
	assert.Equal(t, key, rig.cache.sets[0].key)
	assert.Positive(t, rig.cache.sets[0].ttl, "an entry with no lifetime would never be refreshed")
	w19bAssertForceForwardSignalled(t, rig, inst, 2)

	again := rig.forceForward(inst.ID, 2)

	require.Equal(t, http.StatusAccepted, again.Code, again.Body.String())
	assert.Len(t, rig.defs.reads, 1, "the second call does not read the Definition Service again")
	assert.Len(t, rig.cache.sets, 1)
	assert.Len(t, rig.sdk.signals, 2)
}

func TestForceForwardRoute_AFailedCacheWrite_DoesNotFailTheCall(t *testing.T) {
	rig := w19bNewRig(t)
	rig.cache.setErr = errW19bCauseMarker
	inst := rig.seed(domain.InstanceStatusRunning, 2)

	w := rig.forceForward(inst.ID, 2)

	require.Equal(t, http.StatusAccepted, w.Code, w.Body.String())
	assert.Len(t, rig.cache.sets, 1, "the write was tried")
	assert.GreaterOrEqual(t, rig.log.warns, 1, "the failed write is logged")
	w19bAssertForceForwardSignalled(t, rig, inst, 2)
}

func TestForceForwardRoute_TheDefinitionServiceCannotServeThePlan_Returns503AndSignalsNothing(t *testing.T) {
	cases := []struct {
		name string
		err  error
	}{
		{"unavailable", fmt.Errorf("get compiled workflow: %w: %w", port.ErrDefinitionUnavailable, errW19bCauseMarker)},
		{"rejected", fmt.Errorf("get compiled workflow: %w: %w", port.ErrDefinitionRejected, errW19bCauseMarker)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w19bNewRig(t)
			rig.defs.err = tc.err
			inst := rig.seed(domain.InstanceStatusRunning, 2)
			before := obstest.Sum(t, w19bUpstreamMetric, map[string]string{"dependency": "definition_service"})

			w := rig.forceForward(inst.ID, 2)

			require.Equal(t, http.StatusServiceUnavailable, w.Code, w.Body.String())
			p := w19bProblemOf(t, w)
			assert.Equal(t, "UPSTREAM_UNAVAILABLE", p.Code)
			assert.NotEmpty(t, p.Detail)
			assert.NotContains(t, w.Body.String(), errW19bCauseMarker.Error(), "the cause is logged, not returned")
			assert.Equal(t, []w19bPlanRead{{testTenantID, inst.WorkflowVersionID}}, rig.defs.reads)
			assert.Empty(t, rig.sdk.signals)
			assert.Empty(t, rig.cache.sets, "nothing is cached for a plan that was not read")
			assert.Equal(t, before+1, obstest.Sum(t, w19bUpstreamMetric, map[string]string{"dependency": "definition_service"}), "one failure is counted against the Definition Service")
		})
	}
}

func TestForceForwardRoute_TemporalReportsTheWorkflowGone_Returns409InvalidInstanceState(t *testing.T) {
	rig := w19bNewRig(t)
	rig.sdk.signalErr = serviceerror.NewNotFound("workflow execution not found")
	inst := rig.seed(domain.InstanceStatusRunning, 2)

	w := rig.forceForward(inst.ID, 2)

	require.Equal(t, http.StatusConflict, w.Code, w.Body.String())
	p := w19bProblemOf(t, w)
	assert.Equal(t, "INVALID_INSTANCE_STATE", p.Code)
	assert.Equal(t, port.ErrWorkflowNotFound.Error(), p.Detail)
	assert.NotContains(t, w.Body.String(), port.SignalInstanceForceFwd, "the signal channel is not echoed")
	require.Len(t, rig.sdk.signals, 1, "the signal was tried once")
	assert.Equal(t, inst.TemporalWorkflowID, rig.sdk.signals[0].workflowID)
	assert.Equal(t, port.SignalInstanceForceFwd+":"+inst.ID.String(), rig.sdk.signals[0].channel)
}

func TestForceForwardRoute_AFailedInstanceRead_Returns500WithTheFixedDetail(t *testing.T) {
	rig := w19bNewRig(t)
	rig.instances.getErr = fmt.Errorf("query instance: %w", errW19bCauseMarker)
	id := uuid.New()

	w := rig.forceForward(id, 2)

	require.Equal(t, http.StatusInternalServerError, w.Code, w.Body.String())
	p := w19bProblemOf(t, w)
	assert.Equal(t, "INTERNAL_ERROR", p.Code)
	assert.Equal(t, "an unexpected error occurred", p.Detail)
	assert.NotContains(t, w.Body.String(), errW19bCauseMarker.Error())
	assert.Equal(t, []w19bIDs{{testTenantID, id}}, rig.instances.gets)
	assert.Empty(t, rig.defs.reads, "the plan is not read for an instance that could not be read")
	assert.Empty(t, rig.sdk.signals)
}

func TestForceForwardRoute_AnInstanceTheCallersTenantDoesNotHave_Returns404AndSignalsNothing(t *testing.T) {
	foreignTenant := uuid.New()
	cases := []struct {
		name string
		seed func(rig *w19bRig) uuid.UUID
	}{
		{"an id no tenant has", func(*w19bRig) uuid.UUID { return uuid.New() }},
		{"an instance of another tenant", func(rig *w19bRig) uuid.UUID {
			inst := rig.seed(domain.InstanceStatusRunning, 2)
			inst.TenantID = foreignTenant
			return inst.ID
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w19bNewRig(t)
			id := tc.seed(rig)

			w := rig.forceForward(id, 2)

			require.Equal(t, http.StatusNotFound, w.Code, w.Body.String())
			assert.Equal(t, "INSTANCE_NOT_FOUND", w19bProblemOf(t, w).Code)
			assert.Equal(t, []w19bIDs{{testTenantID, id}}, rig.instances.gets, "the lookup is scoped to the caller's tenant")
			assert.Empty(t, rig.defs.reads)
			assert.Empty(t, rig.sdk.signals)
		})
	}
}

func TestForceForwardRoute_ARecordVersionOtherThanTheCurrentOne_Returns409AndCountsOneConflict(t *testing.T) {
	const current = int64(5)
	cases := []struct {
		name    string
		version int64
	}{
		{"a lower version", current - 1},
		{"the next version", current + 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w19bNewRig(t)
			inst := rig.seed(domain.InstanceStatusRunning, current)
			labels := map[string]string{"table": "workflow_instance"}
			before := obstest.Sum(t, w19bConflictMetric, labels)

			w := rig.forceForward(inst.ID, tc.version)

			require.Equal(t, http.StatusConflict, w.Code, w.Body.String())
			assert.Equal(t, "RECORD_VERSION_CONFLICT", w19bProblemOf(t, w).Code)
			assert.Equal(t, before+1, obstest.Sum(t, w19bConflictMetric, labels), "one conflict is counted for the instance table")
			assert.Empty(t, rig.defs.reads, "the version is checked before the plan is read")
			assert.Empty(t, rig.sdk.signals)
		})
	}

	t.Run("control: the current version signals the workflow and counts no conflict", func(t *testing.T) {
		rig := w19bNewRig(t)
		inst := rig.seed(domain.InstanceStatusRunning, current)
		labels := map[string]string{"table": "workflow_instance"}
		before := obstest.Sum(t, w19bConflictMetric, labels)

		w := rig.forceForward(inst.ID, current)

		require.Equal(t, http.StatusAccepted, w.Code, w.Body.String())
		assert.Equal(t, before, obstest.Sum(t, w19bConflictMetric, labels))
		w19bAssertForceForwardSignalled(t, rig, inst, current)
	})
}
