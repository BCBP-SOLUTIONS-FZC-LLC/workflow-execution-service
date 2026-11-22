package port_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func w9aActivityNames() map[string]string {
	return map[string]string{
		"ActivityGetCompiledPlan":      port.ActivityGetCompiledPlan,
		"ActivityCreateTask":           port.ActivityCreateTask,
		"ActivityUpdateInstanceNodes":  port.ActivityUpdateInstanceNodes,
		"ActivityCompleteAssignment":   port.ActivityCompleteAssignment,
		"ActivityDeferTask":            port.ActivityDeferTask,
		"ActivityUpdateInstanceStatus": port.ActivityUpdateInstanceStatus,
		"ActivityRecordForceRoute":     port.ActivityRecordForceRoute,
		"ActivityRecordSLAWarning":     port.ActivityRecordSLAWarning,
		"ActivityRecordSLABreach":      port.ActivityRecordSLABreach,
		"ActivityPauseInstance":        port.ActivityPauseInstance,
		"ActivityResumeInstance":       port.ActivityResumeInstance,
		"ActivityCancelInstance":       port.ActivityCancelInstance,
		"ActivityReassignAssignment":   port.ActivityReassignAssignment,
		"ActivityUpdateTaskStatus":     port.ActivityUpdateTaskStatus,
	}
}

// w9aDeclaredActivityConstants reads activities.go and returns every Activity* string constant it declares, so a
// constant added to the file without joining the checked set fails the count.
func w9aDeclaredActivityConstants(t *testing.T) map[string]string {
	t.Helper()
	file, err := parser.ParseFile(token.NewFileSet(), "../../../internal/core/port/activities.go", nil, 0)
	require.NoError(t, err)
	declared := map[string]string{}
	for _, decl := range file.Decls {
		gen, ok := decl.(*ast.GenDecl)
		if !ok || gen.Tok != token.CONST {
			continue
		}
		for _, spec := range gen.Specs {
			vs := spec.(*ast.ValueSpec)
			for i, name := range vs.Names {
				if !strings.HasPrefix(name.Name, "Activity") || i >= len(vs.Values) {
					continue
				}
				lit, ok := vs.Values[i].(*ast.BasicLit)
				require.True(t, ok, "%s must be a string literal", name.Name)
				value, err := strconv.Unquote(lit.Value)
				require.NoError(t, err)
				declared[name.Name] = value
			}
		}
	}
	return declared
}

func TestActivityNames_AreFourteenNonEmptyDistinctNamesEndingInActivity(t *testing.T) {
	names := w9aActivityNames()
	declared := w9aDeclaredActivityConstants(t)

	cases := []struct {
		name  string
		check func(t *testing.T)
	}{
		{name: "the checked set is every Activity constant the file declares", check: func(t *testing.T) {
			assert.Len(t, declared, 14)
			assert.Equal(t, declared, names)
		}},
		{name: "every name is non-empty and ends with Activity", check: func(t *testing.T) {
			for constName, value := range names {
				assert.NotEmpty(t, value, constName)
				assert.True(t, strings.HasSuffix(value, "Activity"), "%s = %q", constName, value)
			}
		}},
		{name: "no two constants share a name", check: func(t *testing.T) {
			seen := map[string]string{}
			for constName, value := range names {
				other, dup := seen[value]
				assert.False(t, dup, "%s and %s are both %q", constName, other, value)
				seen[value] = constName
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, tc.check)
	}
}

func TestActivityNames_KeepTheirTemporalWireStrings(t *testing.T) {
	cases := []struct {
		constName string
		want      string
	}{
		{"ActivityGetCompiledPlan", "GetCompiledPlanActivity"},
		{"ActivityCreateTask", "CreateTaskActivity"},
		{"ActivityUpdateInstanceNodes", "UpdateInstanceNodesActivity"},
		{"ActivityCompleteAssignment", "CompleteAssignmentActivity"},
		{"ActivityDeferTask", "DeferTaskActivity"},
		{"ActivityUpdateInstanceStatus", "UpdateInstanceStatusActivity"},
		{"ActivityRecordForceRoute", "RecordForceRouteActivity"},
		{"ActivityRecordSLAWarning", "RecordSLAWarningActivity"},
		{"ActivityRecordSLABreach", "RecordSLABreachActivity"},
		{"ActivityPauseInstance", "PauseInstanceActivity"},
		{"ActivityResumeInstance", "ResumeInstanceActivity"},
		{"ActivityCancelInstance", "CancelInstanceActivity"},
		{"ActivityReassignAssignment", "ReassignAssignmentActivity"},
		{"ActivityUpdateTaskStatus", "UpdateTaskStatusActivity"},
	}
	names := w9aActivityNames()
	require.Len(t, cases, len(names), "a new activity needs its wire string pinned here")
	for _, tc := range cases {
		t.Run(tc.constName, func(t *testing.T) {
			got, ok := names[tc.constName]
			require.True(t, ok, "%s is not in the checked set", tc.constName)
			assert.Equal(t, tc.want, got, "running workflows address this activity by this string")
		})
	}
}
