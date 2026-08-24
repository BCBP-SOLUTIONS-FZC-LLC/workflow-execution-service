package service

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

func TestDeptUUID_PrefersIAMDepartmentID(t *testing.T) {
	iamDeptID := uuid.New()
	got := deptUUID(&dsl.DepartmentDef{ID: "sales", IAMDepartmentID: iamDeptID.String()})
	assert.Equal(t, iamDeptID, got)
}

func TestDeptUUID_FallsBackWithoutIAMDepartmentID(t *testing.T) {
	got := deptUUID(&dsl.DepartmentDef{ID: "sales"})
	assert.NotEqual(t, uuid.Nil, got)
	assert.Equal(t, got, deptUUID(&dsl.DepartmentDef{ID: "sales"}), "must be stable for the same deptID")
}

// TestNoopLogger only confirms the zero-value fallback never panics — real
// behavior (falling back to it when Log is nil) is exercised via
// test/unit/service's InstanceService tests.
func TestNoopLogger(t *testing.T) {
	var l noopLogger
	l.Debug("msg", nil)
	l.Info("msg", nil)
	l.Warn("msg", nil)
	l.Error("msg", nil)
}
