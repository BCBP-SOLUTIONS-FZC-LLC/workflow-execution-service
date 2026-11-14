package service_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

// cseOutsideWorld is an instance service over one instance, a Definition
// Service that serves planJSON for its version, and a workflow that records
// what it is asked.
type cseOutsideWorld struct {
	svc       *service.InstanceService
	instances *cseInstanceRepo
	defs      *cseDefinitions
	temporal  *cseTemporal
	instance  *domain.Instance
}

func cseNewOutsideWorld(planJSON string) *cseOutsideWorld {
	inst := &domain.Instance{
		ID: uuid.New(), TenantID: uuid.New(), WorkflowVersionID: uuid.New(),
		Status: domain.InstanceStatusRunning, TemporalWorkflowID: "tenant:subject:key",
	}
	w := &cseOutsideWorld{
		instances: &cseInstanceRepo{rows: []*domain.Instance{inst}},
		defs:      &cseDefinitions{resp: publishedCompiledWorkflow(uuid.New(), inst.WorkflowVersionID, planJSON)},
		temporal:  &cseTemporal{},
		instance:  inst,
	}
	w.svc = &service.InstanceService{Instances: w.instances, Definitions: w.defs, Temporal: w.temporal}
	return w
}

func (w *cseOutsideWorld) answer(key string) error {
	return w.svc.AnswerOutsideWait(context.Background(), w.instance.TenantID, w.instance.ID, adminScope(), key, nil)
}

func (w *cseOutsideWorld) requireUntouchedWorkflow(t *testing.T) {
	t.Helper()
	assert.Empty(t, w.temporal.queries, "the workflow is not asked")
	assert.Empty(t, w.temporal.signals, "nothing is signalled")
}

func TestAnswerOutsideWait_AMainPlanThatNamesNoPlanHasNoWaits(t *testing.T) {
	// The schema version decides whether the stored plan is expanded first.
	for _, schemaVersion := range []int{0, dsl.CurrentSchemaVersion} {
		t.Run(fmt.Sprintf("schema version %d", schemaVersion), func(t *testing.T) {
			raw, err := json.Marshal(dsl.CompiledCollaboration{
				MainPlan:      "MainPlan",
				SchemaVersion: schemaVersion,
				Plans: []*dsl.CompiledPlan{{
					Name:        "other",
					Departments: []dsl.DepartmentDef{{ID: "finance", Stages: []dsl.StageDef{{Type: "receive_task", NodeID: "Receive_Bid", Extras: map[string]string{"message": "bid-received"}}}}},
				}},
			})
			require.NoError(t, err)
			w := cseNewOutsideWorld(string(raw))

			err = w.answer(outsideKey)

			require.ErrorIs(t, err, port.ErrWaitNotFound)
			w.requireUntouchedWorkflow(t)
			assert.Equal(t, []cseLookup{{Tenant: w.instance.TenantID, ID: w.instance.WorkflowVersionID}}, w.defs.calls)
		})
	}
}

func TestAnswerOutsideWait_ADatabaseFailureOnTheInstanceLookupIsAnInternalError(t *testing.T) {
	w := cseNewOutsideWorld(compiledPlanJSON(t,
		dsl.StageDef{Type: "receive_task", NodeID: "Receive_Bid", Extras: map[string]string{"message": "bid-received"}}))
	w.instances.err = fmt.Errorf("get instance: %w", assert.AnError)

	err := w.answer(outsideKey)

	require.ErrorIs(t, err, assert.AnError)
	assert.NotErrorIs(t, err, port.ErrInstanceNotFound, "a database failure is not a missing instance")
	assert.Equal(t, []cseLookup{{Tenant: w.instance.TenantID, ID: w.instance.ID}}, w.instances.calls)
	assert.Empty(t, w.defs.calls, "no plan is read")
	w.requireUntouchedWorkflow(t)
}
