package service

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestReadScopeFilter_DepartmentsAndCaller(t *testing.T) {
	callerID := uuid.New()
	deptA, deptB := uuid.New(), uuid.New()

	cases := []struct {
		name      string
		scope     port.ReadScope
		wantDepts []uuid.UUID
	}{
		{"no departments gives an empty list, not nil", port.ReadScope{CallerUserID: callerID}, []uuid.UUID{}},
		{"an empty department list gives an empty list, not nil", port.ReadScope{CallerUserID: callerID, Departments: []port.DepartmentRole{}}, []uuid.UUID{}},
		{
			"each department id is kept in order and the role is dropped",
			port.ReadScope{CallerUserID: callerID, Departments: []port.DepartmentRole{{DepartmentID: deptA, Role: "approver"}, {DepartmentID: deptB, Role: "reviewer"}}},
			[]uuid.UUID{deptA, deptB},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			filter := readScopeFilter(tc.scope)

			require.NotNil(t, filter)
			require.NotNil(t, filter.DepartmentIDs, "a nil list would read as no department filter to the repositories")
			assert.Equal(t, tc.wantDepts, filter.DepartmentIDs)
			assert.Equal(t, callerID, filter.CallerUserID)
		})
	}
}
