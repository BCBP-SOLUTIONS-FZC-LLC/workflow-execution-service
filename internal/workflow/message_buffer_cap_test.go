package workflow

import (
	"fmt"
	"strings"
	"testing"

	"go.temporal.io/sdk/testsuite"
	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
)

func TestMessageBufferDropsTheOldestOverTheCap(t *testing.T) {
	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	var buffered int
	var first domain.NodeKey
	env.ExecuteWorkflow(func(ctx wf.Context) error {
		b := newMessageBuffer()
		for i := 0; i < maxBufferedMessages+1; i++ {
			b.Send(ctx, "m", domain.NodeKey(fmt.Sprintf("sender-%d:send", i)))
		}
		buffered = len(b.buf["m"])
		first = b.Receive(ctx, "m")
		return nil
	})

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow returned error: %v", err)
	}
	if buffered != maxBufferedMessages {
		t.Errorf("buffered = %d, want %d", buffered, maxBufferedMessages)
	}
	if first != "sender-1:send" {
		t.Errorf("oldest kept = %q, want sender-1:send", first)
	}
}

func TestValidateMessageName(t *testing.T) {
	tests := []struct {
		name    string
		message string
		wantErr bool
	}{
		{"plain name", "order-ready", false},
		{"longest allowed", strings.Repeat("a", maxMessageNameLength), false},
		{"empty", "", true},
		{"blank", "   ", true},
		{"oversized", strings.Repeat("a", maxMessageNameLength+1), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := validateMessageName(tc.message)
			if (err != nil) != tc.wantErr {
				t.Errorf("validateMessageName(%q) error = %v, wantErr %v", tc.message, err, tc.wantErr)
			}
		})
	}
}
