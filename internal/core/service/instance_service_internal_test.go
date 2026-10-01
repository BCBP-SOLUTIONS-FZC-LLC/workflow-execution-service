package service

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl/dsltest"
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
	assert.Equal(t, "checker", level)
}
