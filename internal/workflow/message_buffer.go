package workflow

import (
	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
)

// messageBuffer implements the intra-pool message buffer (LLD §2.4's 5-point
// algorithm): an instance-wide, FIFO, node-keyed record of unconsumed
// send_task fires, shared across every Parallel branch and inline SubWorkflow
// recursion so a sibling's receive_task is never blocked forever on a
// message a different branch already sent.
//
// No mutex is needed: Temporal workflow code runs on a single-threaded
// cooperative scheduler, so this is replay-safe by construction.
//
// CompiledCollaboration.Messages plays no role here — it is diagram-only.
// Real correlation runs entirely through StageDef.Extras["message"].
type messageBuffer struct {
	buf map[string][]domain.NodeKey
	// blocked holds, per message name, a FIFO queue of channels currently
	// waiting for a delivery — a queue rather than a single channel because
	// more than one receive_task (or a receive_task racing a message
	// boundary) can legitimately wait on the same name at once; aliasing
	// them onto one channel would leave every waiter but the first
	// deadlocked forever.
	blocked map[string][]wf.Channel
}

func newMessageBuffer() *messageBuffer {
	return &messageBuffer{
		buf:     make(map[string][]domain.NodeKey),
		blocked: make(map[string][]wf.Channel),
	}
}

// Send fires a send_task from node. If a waiter is currently blocked on this
// message name, the oldest one is delivered to directly (skipping the
// buffer); otherwise node is appended to the buffer and Send returns
// immediately (fire-and-forget) — LLD §2.4 point 2.
func (b *messageBuffer) Send(ctx wf.Context, messageName string, node domain.NodeKey) {
	if queue := b.blocked[messageName]; len(queue) > 0 {
		queue[0].Send(ctx, node)
		b.blocked[messageName] = queue[1:]
		return
	}
	b.buf[messageName] = append(b.buf[messageName], node)
}

// waitChannel returns a channel that already carries the oldest unconsumed
// entry for messageName if one exists (FIFO pop, LLD §2.4 point 3), or a
// fresh channel queued behind any other waiter on the same name that a
// later Send will deliver to in order. cancel must be called if the caller
// stops waiting without the channel ever firing (e.g. a boundary event
// whose host resolved first) — otherwise the abandoned channel stays queued
// and could steal a delivery meant for a later, still-live waiter.
func (b *messageBuffer) waitChannel(ctx wf.Context, messageName string) (ch wf.Channel, cancel func()) {
	if entries := b.buf[messageName]; len(entries) > 0 {
		node := entries[0]
		b.buf[messageName] = entries[1:]
		ch := wf.NewBufferedChannel(ctx, 1)
		ch.Send(ctx, node)
		return ch, func() { /* already delivered from the buffer, nothing to cancel */ }
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

// Receive resolves a receive_task, blocking until a matching send_task fires
// (LLD §2.4 point 3).
func (b *messageBuffer) Receive(ctx wf.Context, messageName string) domain.NodeKey {
	ch, _ := b.waitChannel(ctx, messageName)
	var node domain.NodeKey
	ch.Receive(ctx, &node)
	return node
}

// ResetSpan removes, from every message name's buffer, only the unconsumed
// entries whose firing node key appears in span — the reset a force-back or
// DEGRADED respawn applies when it rewinds a branch past a send_task's node.
// Consumed entries are already gone and a sibling's entries are never
// touched (LLD §2.4 point 5).
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
