package workflow

import (
	"reflect"
	"testing"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
)

func TestNodeHistoryPopToDept(t *testing.T) {
	tests := []struct {
		name       string
		stack      []domain.NodeKey
		dept       string
		wantPopped []domain.NodeKey
		wantLeft   []domain.NodeKey
	}{
		{
			name:       "pops the department's latest run and everything above it",
			stack:      []domain.NodeKey{"x/x1", "a/a1", "a/a2", "b/b1"},
			dept:       "a",
			wantPopped: []domain.NodeKey{"a/a1", "a/a2", "b/b1"},
			wantLeft:   []domain.NodeKey{"x/x1"},
		},
		{
			name:       "stops at an earlier visit separated by another department",
			stack:      []domain.NodeKey{"a/a1", "x/x1", "a/a1"},
			dept:       "a",
			wantPopped: []domain.NodeKey{"a/a1"},
			wantLeft:   []domain.NodeKey{"a/a1", "x/x1"},
		},
		{
			name:     "leaves the history alone when the department never completed a node",
			stack:    []domain.NodeKey{"x/x1", "b/b1"},
			dept:     "a",
			wantLeft: []domain.NodeKey{"x/x1", "b/b1"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newNodeHistory()
			for _, k := range tc.stack {
				h.Push(k)
			}
			popped := h.PopToDept(tc.dept)
			if !reflect.DeepEqual(popped, tc.wantPopped) {
				t.Errorf("popped = %v, want %v", popped, tc.wantPopped)
			}
			if !reflect.DeepEqual(h.stack, tc.wantLeft) {
				t.Errorf("left = %v, want %v", h.stack, tc.wantLeft)
			}
		})
	}
}
