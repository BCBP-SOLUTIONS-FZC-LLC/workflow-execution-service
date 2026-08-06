package workflow

import (
	"testing"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

func TestEvaluateCondition(t *testing.T) {
	tests := []struct {
		name       string
		expr       string
		resultJSON string
		want       bool
		wantErr    bool
	}{
		{name: "equality match", expr: `decision == "approved"`, resultJSON: `{"decision":"approved"}`, want: true},
		{name: "equality mismatch", expr: `decision == "approved"`, resultJSON: `{"decision":"rejected"}`, want: false},
		{name: "inequality match", expr: `decision != "rejected"`, resultJSON: `{"decision":"approved"}`, want: true},
		{name: "inequality mismatch", expr: `decision != "approved"`, resultJSON: `{"decision":"approved"}`, want: false},
		{name: "missing field compares as nil", expr: `decision == "approved"`, resultJSON: `{}`, want: false},
		{name: "empty result_json", expr: `decision == "approved"`, resultJSON: ``, want: false},
		{name: "unsupported operator", expr: `decision > "approved"`, resultJSON: `{"decision":"approved"}`, wantErr: true},
		{name: "invalid result_json", expr: `decision == "approved"`, resultJSON: `not json`, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := evaluateCondition(tt.expr, tt.resultJSON)
			if (err != nil) != tt.wantErr {
				t.Fatalf("evaluateCondition() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil && got != tt.want {
				t.Errorf("evaluateCondition() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSelectBranch(t *testing.T) {
	tests := []struct {
		name       string
		branches   []dsl.ExclusiveBranch
		resultJSON string
		wantTarget string
		wantNil    bool
	}{
		{
			name: "first true branch wins",
			branches: []dsl.ExclusiveBranch{
				{ConditionExpression: `decision == "approved"`, Target: "shipping"},
				{ConditionExpression: "", Target: "rework"},
			},
			resultJSON: `{"decision":"approved"}`,
			wantTarget: "shipping",
		},
		{
			name: "falls back to implicit else",
			branches: []dsl.ExclusiveBranch{
				{ConditionExpression: `decision == "approved"`, Target: "shipping"},
				{ConditionExpression: "", Target: "rework"},
			},
			resultJSON: `{"decision":"rejected"}`,
			wantTarget: "rework",
		},
		{
			name: "no match and no implicit else",
			branches: []dsl.ExclusiveBranch{
				{ConditionExpression: `decision == "approved"`, Target: "shipping"},
			},
			resultJSON: `{"decision":"rejected"}`,
			wantNil:    true,
		},
		{
			name: "does not evaluate branches after the winner",
			branches: []dsl.ExclusiveBranch{
				{ConditionExpression: `decision == "approved"`, Target: "first"},
				{ConditionExpression: `decision == "approved"`, Target: "second"},
			},
			resultJSON: `{"decision":"approved"}`,
			wantTarget: "first",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := selectBranch(tt.branches, tt.resultJSON)
			if err != nil {
				t.Fatalf("selectBranch() unexpected error: %v", err)
			}
			if tt.wantNil {
				if got != nil {
					t.Fatalf("selectBranch() = %+v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Fatalf("selectBranch() = nil, want Target %q", tt.wantTarget)
			}
			if got.Target != tt.wantTarget {
				t.Errorf("selectBranch().Target = %q, want %q", got.Target, tt.wantTarget)
			}
		})
	}
}
