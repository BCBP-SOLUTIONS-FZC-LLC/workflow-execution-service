package workflow

import (
	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
)

type messageBuffer struct {
	buf     map[string][]domain.NodeKey
	blocked map[string][]wf.Channel
}

func newMessageBuffer() *messageBuffer {
	return &messageBuffer{
		buf:     make(map[string][]domain.NodeKey),
		blocked: make(map[string][]wf.Channel),
	}
}

func (b *messageBuffer) Send(ctx wf.Context, messageName string, node domain.NodeKey) {
	if queue := b.blocked[messageName]; len(queue) > 0 {
		queue[0].Send(ctx, node)
		b.blocked[messageName] = queue[1:]
		return
	}
	b.buf[messageName] = append(b.buf[messageName], node)
}

func (b *messageBuffer) waitChannel(ctx wf.Context, messageName string) (ch wf.Channel, cancel func()) {
	if entries := b.buf[messageName]; len(entries) > 0 {
		node := entries[0]
		b.buf[messageName] = entries[1:]
		ch := wf.NewBufferedChannel(ctx, 1)
		ch.Send(ctx, node)
		return ch, func() {}
	}
	ch = wf.NewBufferedChannel(ctx, 1)
	b.blocked[messageName] = append(b.blocked[messageName], ch)
	return ch, func() {
		queue := b.blocked[messageName]
		for i, c := range queue {
			if c == ch {
				b.blocked[messageName] = append(queue[:i], queue[i+1:]...)
				return
			}
		}
	}
}

func (b *messageBuffer) Receive(ctx wf.Context, messageName string) domain.NodeKey {
	ch, _ := b.waitChannel(ctx, messageName)
	var node domain.NodeKey
	ch.Receive(ctx, &node)
	return node
}

func (b *messageBuffer) ResetSpan(span []domain.NodeKey) {
	if len(span) == 0 {
		return
	}
	inSpan := make(map[domain.NodeKey]bool, len(span))
	for _, k := range span {
		inSpan[k] = true
	}
	for name, entries := range b.buf {
		kept := entries[:0]
		for _, e := range entries {
			if !inSpan[e] {
				kept = append(kept, e)
			}
		}
		b.buf[name] = kept
	}
}
