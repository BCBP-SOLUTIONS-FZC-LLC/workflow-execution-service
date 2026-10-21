package service

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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl/dsltest"
)

func TestDeptUUID_PrefersIAMDepartmentID(t *testing.T) {
	iamDeptID := uuid.New()
	got := deptUUID(&dsl.DepartmentDef{ID: "sales", IAMDepartmentID: iamDeptID.String()})
	assert.Equal(t, iamDeptID, got)
}

func TestDeptUUID_NilWithoutIAMDepartmentID(t *testing.T) {
	got := deptUUID(&dsl.DepartmentDef{ID: "sales"})
	assert.Equal(t, uuid.Nil, got)
}

func plainPlan(dept dsl.DepartmentDef) *dsl.CompiledPlan {
	return &dsl.CompiledPlan{Departments: []dsl.DepartmentDef{dept}}
}

func TestRequiredLevelForTask_MatchesTaskWithRealIAMDepartmentID(t *testing.T) {
	stage := dsl.StageDef{Type: "approve", NodeID: "n1", Role: "sales_rep"}
	iamDeptID := uuid.New()
	dept := dsl.DepartmentDef{ID: "sales", IAMDepartmentID: iamDeptID.String(), Stages: []dsl.StageDef{stage}}
	task := &domain.Task{DepartmentID: iamDeptID, NodeKey: "sales/n1"}

	role, ok := requiredLevelForTask(plainPlan(dept), task)
	assert.True(t, ok)
	assert.Equal(t, "sales_rep", role)
}

func TestNoopLogger(t *testing.T) {
	var l noopLogger
	l.Debug("msg", nil)
	l.Info("msg", nil)
	l.Warn("msg", nil)
	l.Error("msg", nil)
}

type goldenDefinitions struct{}

func (goldenDefinitions) GetCompiledWorkflow(context.Context, uuid.UUID, uuid.UUID) (*port.CompiledWorkflow, error) {
	return &port.CompiledWorkflow{Status: "PUBLISHED", IsValid: true, CompiledPlanJSON: string(dsltest.LibraryCalls())}, nil
}

func TestCompiledPlanCache_CountsMissesThenHits(t *testing.T) {
	obstest.Init(t)
	const hits, misses = "workflow_execution_service_wf_cache_hits_total", "workflow_execution_service_wf_cache_misses_total"
	cache := newCompiledPlanCache(goldenDefinitions{}, noopLogger{})
	tenantID, versionID := uuid.New(), uuid.New()
	hitsBefore, missesBefore := obstest.Sum(t, hits, nil), obstest.Sum(t, misses, nil)

	_, err := cache.mainPlan(context.Background(), tenantID, versionID)
	require.NoError(t, err)
	assert.Equal(t, missesBefore+1, obstest.Sum(t, misses, nil), "the first read of a version is a miss")
	assert.Equal(t, hitsBefore, obstest.Sum(t, hits, nil))

	_, err = cache.mainPlan(context.Background(), tenantID, versionID)
	require.NoError(t, err)
	assert.Equal(t, hitsBefore+1, obstest.Sum(t, hits, nil), "the second read is a hit")
	assert.Equal(t, missesBefore+1, obstest.Sum(t, misses, nil))
}

// A module task's required level is found through its call-scoped node key,
// which exists only once the plan's calls are expanded.
func TestCompiledPlanCache_ExpandsLibraryCalls(t *testing.T) {
	plan, err := newCompiledPlanCache(goldenDefinitions{}, noopLogger{}).mainPlan(context.Background(), uuid.New(), uuid.New())
	require.NoError(t, err)
	task := &domain.Task{
		DepartmentID: uuid.MustParse("92fd92cf-4b2c-58dc-bf98-f3d0af729876"),
		NodeKey:      "CA_Ops::Review_Check::Process_Check/Check_Task",
	}
	level, ok := requiredLevelForTask(plan, task)
	assert.True(t, ok, "no stage for the module task")
	assert.Equal(t, "approver", level)
}

type sizedDefinitions struct{ stages int }

func (d sizedDefinitions) GetCompiledWorkflow(context.Context, uuid.UUID, uuid.UUID) (*port.CompiledWorkflow, error) {
	stages := make([]dsl.StageDef, d.stages)
	for i := range stages {
		stages[i] = dsl.StageDef{Type: "approve", NodeID: fmt.Sprintf("T%d", i)}
	}
	collab := dsl.CompiledCollaboration{
		MainPlan:      "main",
		SchemaVersion: dsl.CurrentSchemaVersion,
		Plans:         []*dsl.CompiledPlan{{Name: "main", Departments: []dsl.DepartmentDef{{ID: "Ops", Stages: stages}}}},
	}
	raw, err := json.Marshal(collab)
	if err != nil {
		return nil, fmt.Errorf("marshal sized plan: %w", err)
	}
	return &port.CompiledWorkflow{Status: "PUBLISHED", IsValid: true, CompiledPlanJSON: string(raw)}, nil
}

func TestCompiledPlanCache_PlanAtTheStageBudgetLoads(t *testing.T) {
	_, err := newCompiledPlanCache(sizedDefinitions{stages: domain.MaxPlanStages}, noopLogger{}).mainPlan(context.Background(), uuid.New(), uuid.New())
	require.NoError(t, err)
}

func TestCompiledPlanCache_PlanOverTheStageBudgetIsAnInvalidVersion(t *testing.T) {
	_, err := newCompiledPlanCache(sizedDefinitions{stages: domain.MaxPlanStages + 1}, noopLogger{}).mainPlan(context.Background(), uuid.New(), uuid.New())
	require.ErrorIs(t, err, port.ErrVersionInvalid)
	assert.ErrorIs(t, err, dsl.ErrPlanTooLarge)
}
