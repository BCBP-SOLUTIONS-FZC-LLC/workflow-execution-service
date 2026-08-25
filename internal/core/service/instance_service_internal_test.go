package service

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
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
