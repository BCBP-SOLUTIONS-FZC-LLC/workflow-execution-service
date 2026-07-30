package workflow

import (
	"reflect"
	"testing"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
)

func TestNodeHistoryPushPeek(t *testing.T) {
	h := newNodeHistory()
	if got := h.Peek(); got != "" {
		t.Fatalf("Peek() on empty history = %q, want empty", got)
	}
	h.Push("dept-a:prep")
	h.Push("dept-a:review")
	if got := h.Peek(); got != "dept-a:review" {
		t.Fatalf("Peek() = %q, want dept-a:review", got)
	}
}

func TestNodeHistoryPopTo(t *testing.T) {
	tests := []struct {
		name       string
		pushed     []domain.NodeKey
		target     domain.NodeKey
		wantPopped []domain.NodeKey
		wantPeek   domain.NodeKey
	}{
		{
			name:       "pops down to and including target",
			pushed:     []domain.NodeKey{"a:1", "a:2", "a:3", "a:4"},
			target:     "a:2",
			wantPopped: []domain.NodeKey{"a:2", "a:3", "a:4"},
			wantPeek:   "a:1",
		},
		{
			name:       "target is the top entry",
			pushed:     []domain.NodeKey{"a:1", "a:2"},
			target:     "a:2",
			wantPopped: []domain.NodeKey{"a:2"},
			wantPeek:   "a:1",
		},
		{
			name:       "target not found pops everything",
			pushed:     []domain.NodeKey{"a:1", "a:2"},
			target:     "does-not-exist",
			wantPopped: []domain.NodeKey{"a:1", "a:2"},
			wantPeek:   "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newNodeHistory()
			for _, k := range tt.pushed {
				h.Push(k)
			}
			popped := h.PopTo(tt.target)
			if !reflect.DeepEqual(popped, tt.wantPopped) {
				t.Errorf("PopTo() popped = %v, want %v", popped, tt.wantPopped)
			}
			if got := h.Peek(); got != tt.wantPeek {
				t.Errorf("Peek() after PopTo() = %q, want %q", got, tt.wantPeek)
			}
		})
	}
}

func TestNodeHistoryPreForkEntry(t *testing.T) {
	h := newNodeHistory()
	h.Push("dept-a:prep")
	h.Push("dept-a:review")
	// PreForkEntry is the top of history at the moment a parallel gateway
	// forks — i.e. whatever was last completed before the fork, not any
	// branch's own in-flight position (LLD §2.7 point 1).
	if got := h.PreForkEntry(); got != "dept-a:review" {
		t.Fatalf("PreForkEntry() = %q, want dept-a:review", got)
	}
}
