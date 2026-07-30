package workflow

import (
	"encoding/json"
	"testing"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

func TestApplyIOMapping(t *testing.T) {
	tests := []struct {
		name        string
		contextJSON string
		mapping     *dsl.IOMapping
		want        map[string]any
	}{
		{
			name:        "nil mapping is a no-op",
			contextJSON: `{"a":1}`,
			mapping:     nil,
			want:        map[string]any{"a": float64(1)},
		},
		{
			name:        "empty inputs is a no-op",
			contextJSON: `{"a":1}`,
			mapping:     &dsl.IOMapping{},
			want:        map[string]any{"a": float64(1)},
		},
		{
			name:        "department remapping copies source to target",
			contextJSON: `{"dept_id":"legal"}`,
			mapping:     &dsl.IOMapping{Inputs: []dsl.IOVar{{Source: "dept_id", Target: "target_dept_id"}}},
			want:        map[string]any{"dept_id": "legal", "target_dept_id": "legal"},
		},
		{
			name:        "empty context with an input still produces a key",
			contextJSON: "",
			mapping:     &dsl.IOMapping{Inputs: []dsl.IOVar{{Source: "missing", Target: "target"}}},
			want:        map[string]any{"target": nil},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := applyIOMapping(tt.contextJSON, tt.mapping)
			if err != nil {
				t.Fatalf("applyIOMapping() unexpected error: %v", err)
			}
			var gotMap map[string]any
			if err := json.Unmarshal([]byte(got), &gotMap); err != nil {
				t.Fatalf("applyIOMapping() produced invalid JSON: %v", err)
			}
			if len(gotMap) != len(tt.want) {
				t.Fatalf("applyIOMapping() = %v, want %v", gotMap, tt.want)
			}
			for k, v := range tt.want {
				if gotMap[k] != v {
					t.Errorf("applyIOMapping()[%q] = %v, want %v", k, gotMap[k], v)
				}
			}
		})
	}
}

func TestApplyIOMappingInvalidContext(t *testing.T) {
	_, err := applyIOMapping("not json", &dsl.IOMapping{Inputs: []dsl.IOVar{{Source: "a", Target: "b"}}})
	if err == nil {
		t.Fatal("applyIOMapping() with invalid context_json: want error, got nil")
	}
}
