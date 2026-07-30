package workflow

import (
	"testing"

	"go.temporal.io/sdk/testsuite"
	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
)

// TestMessageBufferResetSpan exercises the pure, context-free part of
// messageBuffer (LLD §2.4 point 5): only unconsumed entries whose node key
// falls within span are removed, never a sibling's, and never an already-
// consumed entry (which is simply absent from buf already).
func TestMessageBufferResetSpan(t *testing.T) {
	b := newMessageBuffer()
	b.buf["order-ready"] = []domain.NodeKey{"branch-a:send", "branch-b:send"}
	b.buf["invoice-sent"] = []domain.NodeKey{"branch-a:send2"}

	b.ResetSpan([]domain.NodeKey{"branch-a:send"})

	if got := b.buf["order-ready"]; len(got) != 1 || got[0] != "branch-b:send" {
		t.Errorf("ResetSpan() left order-ready = %v, want only branch-b:send", got)
	}
	if got := b.buf["invoice-sent"]; len(got) != 1 || got[0] != "branch-a:send2" {
		t.Errorf("ResetSpan() should not touch entries outside span, got %v", got)
	}
}

func TestMessageBufferResetSpanEmptySpanIsNoOp(t *testing.T) {
	b := newMessageBuffer()
	b.buf["order-ready"] = []domain.NodeKey{"branch-a:send"}
	b.ResetSpan(nil)
	if got := b.buf["order-ready"]; len(got) != 1 {
		t.Errorf("ResetSpan(nil) mutated buffer: %v", got)
	}
}

// TestMessageBufferSendThenReceive drives the Temporal-context-dependent
// Send/Receive pair inside a minimal ad-hoc workflow via
// testsuite.WorkflowTestSuite, since both require a real workflow.Context
// for channel creation.
func TestMessageBufferSendThenReceive(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	var got domain.NodeKey
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		b := newMessageBuffer()
		// send_task fires before receive_task is blocked: LLD §2.4 point 2
		// — buffered, delivered on the later Receive.
		b.Send(ctx, "order-ready", "sender:send")
		got = b.Receive(ctx, "order-ready")
		return nil
	})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	if got != "sender:send" {
		t.Errorf("Receive() = %q, want sender:send", got)
	}
}

// TestMessageBufferTwoConcurrentWaitersOnSameName guards against aliasing
// two simultaneous waiters on the same message name onto a single shared
// channel — every waiter but the first would deadlock forever, since a
// Temporal channel delivers to exactly one blocked receiver. Both must
// resolve, each with its own send.
func TestMessageBufferTwoConcurrentWaitersOnSameName(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	var got1, got2 domain.NodeKey
	var done1, done2 bool
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		b := newMessageBuffer()
		wf.Go(ctx, func(gctx wf.Context) {
			got1 = b.Receive(gctx, "m")
			done1 = true
		})
		wf.Go(ctx, func(gctx wf.Context) {
			got2 = b.Receive(gctx, "m")
			done2 = true
		})
		// Yield so both Receive calls register as waiters before either Send.
		if err := wf.Sleep(ctx, 0); err != nil {
			return err
		}
		b.Send(ctx, "m", "sender-a:send")
		b.Send(ctx, "m", "sender-b:send")
		return wf.Sleep(ctx, 0)
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	if !done1 || !done2 {
		t.Fatalf("both waiters must resolve, got done1=%v done2=%v", done1, done2)
	}
	got := map[domain.NodeKey]bool{got1: true, got2: true}
	if !got["sender-a:send"] || !got["sender-b:send"] {
		t.Errorf("waiters resolved to (%q, %q), want one each of sender-a:send/sender-b:send", got1, got2)
	}
}

// TestMessageBufferFIFOOrdering verifies multiple unconsumed sends for the
// same message name pop oldest-first (LLD §2.4 point 3), and that the
// receiver may be a different branch than either sender — the cross-
// sibling correlation the instance-wide scope exists to enable.
func TestMessageBufferFIFOOrdering(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	var first, second domain.NodeKey
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		b := newMessageBuffer()
		b.Send(ctx, "m", "branch-a:send")
		b.Send(ctx, "m", "branch-b:send")
		first = b.Receive(ctx, "m")
		second = b.Receive(ctx, "m")
		return nil
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	if first != "branch-a:send" || second != "branch-b:send" {
		t.Errorf("FIFO order = (%q, %q), want (branch-a:send, branch-b:send)", first, second)
	}
}

// TestMessageBufferReceiveBlocksUntilSend verifies a receive_task reached
// before any send blocks on a fresh channel that a later Send resolves,
// rather than erroring or returning immediately (LLD §2.4 point 3, else
// branch).
func TestMessageBufferReceiveBlocksUntilSend(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	var got domain.NodeKey
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		b := newMessageBuffer()
		wf.Go(ctx, func(gctx wf.Context) {
			b.Send(gctx, "m", "sender:send")
		})
		got = b.Receive(ctx, "m")
		return nil
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	if got != "sender:send" {
		t.Errorf("Receive() = %q, want sender:send", got)
	}
}
