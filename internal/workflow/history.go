package workflow

import "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"

// nodeHistory is the append-only completedNodes stack backing force-back /
// force-forward semantics (LLD §2.7). It is workflow-local state, not
// directly Query-visible — a get-workflow-status query would read through a
// snapshot method rather than touching the stack (that query handler is a
// sibling task's concern).
type nodeHistory struct {
	stack []domain.NodeKey
}

func newNodeHistory() *nodeHistory {
	return &nodeHistory{}
}

// Push records a newly completed node.
func (h *nodeHistory) Push(key domain.NodeKey) {
	h.stack = append(h.stack, key)
}

// Peek returns the most recently completed node, or "" if history is empty.
func (h *nodeHistory) Peek() domain.NodeKey {
	if len(h.stack) == 0 {
		return ""
	}
	return h.stack[len(h.stack)-1]
}

// PopTo pops entries down to and including target, returning the popped span
// in the order they were originally pushed (oldest first). Callers feed this
// span to messageBuffer.ResetSpan to drop only the unconsumed sends that
// fell within the rewound range. If target is not found, the entire stack is
// popped.
func (h *nodeHistory) PopTo(target domain.NodeKey) []domain.NodeKey {
	idx := -1
	for i := len(h.stack) - 1; i >= 0; i-- {
		if h.stack[i] == target {
			idx = i
			break
		}
	}
	if idx == -1 {
		popped := append([]domain.NodeKey(nil), h.stack...)
		h.stack = nil
		return popped
	}
	// Copy rather than reslice: h.stack[idx:] would share the backing array
	// with the truncated h.stack[:idx], so a later Push could silently
	// overwrite entries the caller is still holding.
	popped := append([]domain.NodeKey(nil), h.stack[idx:]...)
	h.stack = h.stack[:idx]
	return popped
}

// PreForkEntry returns the node key recorded immediately before a parallel
// gateway forks — i.e. the current top of history at the moment Parallel
// dispatch begins. Force-back during an active parallel gateway pops to
// this entry, never to any single branch's in-flight position (LLD §2.7
// point 1).
func (h *nodeHistory) PreForkEntry() domain.NodeKey {
	return h.Peek()
}
