package workflow

import "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"

type nodeHistory struct {
	stack []domain.NodeKey
}

func newNodeHistory() *nodeHistory {
	return &nodeHistory{}
}

func (h *nodeHistory) Push(key domain.NodeKey) {
	h.stack = append(h.stack, key)
}

func (h *nodeHistory) Peek() domain.NodeKey {
	if len(h.stack) == 0 {
		return ""
	}
	return h.stack[len(h.stack)-1]
}

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
	popped := append([]domain.NodeKey(nil), h.stack[idx:]...)
	h.stack = h.stack[:idx]
	return popped
}

func (h *nodeHistory) PreForkEntry() domain.NodeKey {
	return h.Peek()
}

func (h *nodeHistory) PopOne() (target domain.NodeKey, popped []domain.NodeKey, ok bool) {
	top := h.Peek()
	if top == "" {
		return "", nil, false
	}
	return top, h.PopTo(top), true
}
