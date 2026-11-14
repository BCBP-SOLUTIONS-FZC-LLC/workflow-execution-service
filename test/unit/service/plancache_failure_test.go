package service_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// csaEligibility records the tenant, user, department and level of every read.
type csaEligibilityCall struct {
	tenantID, userID, departmentID uuid.UUID
	level                          string
}

type csaEligibility struct {
	calls []csaEligibilityCall
}

func (e *csaEligibility) CheckEligibility(_ context.Context, tenantID, newUserID, departmentID uuid.UUID, requiredLevel string) (bool, error) {
	e.calls = append(e.calls, csaEligibilityCall{tenantID, newUserID, departmentID, requiredLevel})
	return true, nil
}

func (e *csaEligibility) CheckEligibilityBatch(context.Context, uuid.UUID, []port.EligibilityCheckRequest) ([]port.EligibilityResult, error) {
	return nil, nil
}

func TestWorkflowClient_ReassignDelegate_APlanThatCannotBeReadIsReadAgainForTheNextAssignment(t *testing.T) {
	obstest.Init(t)
	const missesMetric = "workflow_execution_service_wf_cache_misses_total"
	const hitsMetric = "workflow_execution_service_wf_cache_hits_total"
	financeDept := uuid.New()
	good := func(versionID uuid.UUID) *port.CompiledWorkflow {
		return publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSONWithDept(t,
			dsl.DepartmentDef{ID: "finance", IAMDepartmentID: financeDept.String(), Stages: []dsl.StageDef{{Type: "userTask", NodeID: "review", Role: "reviewer"}}}))
	}
	withMainPlan := func(versionID uuid.UUID, mainPlan string) *port.CompiledWorkflow {
		collab := dsl.CompiledCollaboration{MainPlan: mainPlan, Plans: []*dsl.CompiledPlan{{Name: "main", TaskQueue: "q"}}}
		raw, err := json.Marshal(collab)
		require.NoError(t, err)
		return publishedCompiledWorkflow(uuid.New(), versionID, string(raw))
	}

	failures := []struct {
		name   string
		answer func(versionID uuid.UUID) (*port.CompiledWorkflow, error)
		cause  string
	}{
		{"the fetch fails", func(uuid.UUID) (*port.CompiledWorkflow, error) { return nil, assert.AnError }, assert.AnError.Error()},
		{"the plan cannot be decoded", func(v uuid.UUID) (*port.CompiledWorkflow, error) {
			return publishedCompiledWorkflow(uuid.New(), v, "not json"), nil
		}, "unmarshal compiled plan"},
		{"the main plan is missing", func(v uuid.UUID) (*port.CompiledWorkflow, error) { return withMainPlan(v, "ghost"), nil }, `main plan "ghost" not found in compiled collaboration`},
	}
	for _, tc := range failures {
		t.Run(tc.name+", and the failure is not remembered", func(t *testing.T) {
			h := csaNewClientHarness()
			log := &fakeLogger{}
			h.svc.Log = log
			eligibility := &csaEligibility{}
			h.svc.Eligibility = eligibility
			tenantID, oldDelegate, newDelegate, versionID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
			definitions := &csaDefinitions{answer: func(int) (*port.CompiledWorkflow, error) { return tc.answer(versionID) }}
			h.svc.Definitions = definitions
			var held []csaHeld
			instanceID := uuid.New()
			for i := 0; i < 2; i++ {
				held = append(held, h.seedOnInstance(tenantID, oldDelegate, instanceID, "", domain.TaskStatusReady, domain.InstanceStatusRunning))
				held[i].instance.WorkflowVersionID = versionID
				held[i].task.NodeKey = "finance/review"
				held[i].task.DepartmentID = financeDept
			}
			missesBefore, hitsBefore := obstest.Sum(t, missesMetric, nil), obstest.Sum(t, hitsMetric, nil)

			count, err := h.svc.ReassignDelegate(context.Background(), port.ReassignDelegateInput{TenantID: tenantID, OldDelegateID: oldDelegate, NewDelegateID: newDelegate})

			require.NoError(t, err, "an unreadable plan holds the row, it does not fail the call")
			assert.Zero(t, count)
			assert.Equal(t, []csaIDCall{{tenantID, versionID}, {tenantID, versionID}}, definitions.reads, "each assignment reads the version again")
			assert.Equal(t, 2.0, obstest.Sum(t, missesMetric, nil)-missesBefore, "every failed attempt is a miss")
			assert.Zero(t, obstest.Sum(t, hitsMetric, nil)-hitsBefore, "a failure is never served as a hit")
			unreadable := 0
			for i, msg := range log.warnCalls {
				if msg == "compiled plan unreadable" {
					unreadable++
					assert.Equal(t, versionID, log.warnFields[i]["version_id"])
					assert.Contains(t, log.warnFields[i]["error"], tc.cause)
				}
			}
			assert.Equal(t, 2, unreadable, "each attempt logs the unreadable plan with its cause")
			assert.Empty(t, eligibility.calls)
			for _, hd := range held {
				assert.True(t, hd.assignment.IsActive)
			}
		})
	}

	t.Run("a read that succeeds after a failure is kept for the rest of the call", func(t *testing.T) {
		h := csaNewClientHarness()
		eligibility := &csaEligibility{}
		h.svc.Eligibility = eligibility
		tenantID, oldDelegate, newDelegate, versionID := uuid.New(), uuid.New(), uuid.New(), uuid.New()
		definitions := &csaDefinitions{answer: func(call int) (*port.CompiledWorkflow, error) {
			if call == 1 {
				return nil, assert.AnError
			}
			return good(versionID), nil
		}}
		h.svc.Definitions = definitions
		instanceID := uuid.New()
		var held []csaHeld
		for i := 0; i < 3; i++ {
			held = append(held, h.seedOnInstance(tenantID, oldDelegate, instanceID, "", domain.TaskStatusReady, domain.InstanceStatusRunning))
			held[i].instance.WorkflowVersionID = versionID
			held[i].task.NodeKey = "finance/review"
			held[i].task.DepartmentID = financeDept
		}
		missesBefore, hitsBefore := obstest.Sum(t, missesMetric, nil), obstest.Sum(t, hitsMetric, nil)

		count, err := h.svc.ReassignDelegate(context.Background(), port.ReassignDelegateInput{TenantID: tenantID, OldDelegateID: oldDelegate, NewDelegateID: newDelegate})

		require.NoError(t, err)
		assert.Equal(t, 2, count, "the first assignment was held by the failure, the other two were moved")
		assert.Len(t, definitions.reads, 2, "one failed read and one that succeeded; the third assignment is served from memory")
		assert.Equal(t, 2.0, obstest.Sum(t, missesMetric, nil)-missesBefore)
		assert.Equal(t, 1.0, obstest.Sum(t, hitsMetric, nil)-hitsBefore)
		assert.True(t, held[0].assignment.IsActive)
		assert.False(t, held[1].assignment.IsActive)
		assert.False(t, held[2].assignment.IsActive)
		require.Len(t, eligibility.calls, 1)
		assert.Equal(t, tenantID, eligibility.calls[0].tenantID)
		assert.Equal(t, newDelegate, eligibility.calls[0].userID)
		assert.Equal(t, "reviewer", eligibility.calls[0].level)
		assert.Equal(t, financeDept, eligibility.calls[0].departmentID)
	})
}
