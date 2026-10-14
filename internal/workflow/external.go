package workflow

import (
	"encoding/json"
	"fmt"
	"slices"

	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

const SignalInstanceExternalAnswer = "instance-external-answer"

type externalAnswerSignal struct {
	Key       string
	Variables map[string]any
}

func externalWaitKeys(collab *dsl.CompiledCollaboration) map[string]bool {
	keys := map[string]bool{}
	if collab == nil {
		return keys
	}
	for _, w := range dsl.ExternalWaits(collab) {
		keys[w.Key] = true
	}
	return keys
}

func (in *interpreter) waitExternal(ctx wf.Context, key string) (ch wf.Channel, cancel func()) {
	ch = wf.NewBufferedChannel(ctx, 1)
	in.externalWaiting[key] = ch
	return ch, func() {
		if in.externalWaiting[key] == ch {
			delete(in.externalWaiting, key)
		}
	}
}

func (in *interpreter) handleExternalAnswer(ctx wf.Context, sig externalAnswerSignal) {
	logger := wf.GetLogger(ctx)
	if err := validateSignal(in.status, SignalInstanceExternalAnswer); err != nil {
		logger.Warn("dropping external answer", "key", sig.Key, "error", err)
		return
	}
	ch, ok := in.externalWaiting[sig.Key]
	if !ok {
		logger.Warn("dropping external answer: nothing is waiting there", "key", sig.Key)
		return
	}
	if err := in.mergeVariables(sig.Variables); err != nil {
		logger.Warn("dropping external answer", "key", sig.Key, "error", err)
		return
	}
	delete(in.externalWaiting, sig.Key)
	ch.Send(ctx, domain.NodeKey(sig.Key))
}

func (in *interpreter) mergeVariables(vars map[string]any) error {
	if len(vars) == 0 {
		return nil
	}
	current := map[string]any{}
	if in.contextJSON != "" {
		if err := json.Unmarshal([]byte(in.contextJSON), &current); err != nil {
			return fmt.Errorf("workflow: parsing context_json: %w", err)
		}
	}
	for k, v := range vars {
		current[k] = v
	}
	merged, err := json.Marshal(current)
	if err != nil {
		return fmt.Errorf("workflow: marshaling context_json: %w", err)
	}
	in.contextJSON = string(merged)
	return nil
}

func (in *interpreter) mergeResult(resultJSON string) error {
	if resultJSON == "" {
		return nil
	}
	var vars map[string]any
	if err := json.Unmarshal([]byte(resultJSON), &vars); err != nil {
		return nil
	}
	return in.mergeVariables(vars)
}

func (in *interpreter) registerWaitingExternalQuery(ctx wf.Context) error {
	err := wf.SetQueryHandler(ctx, QueryWaitingExternal, func() ([]string, error) {
		keys := make([]string, 0, len(in.externalWaiting))
		for k := range in.externalWaiting {
			keys = append(keys, k)
		}
		slices.Sort(keys)
		return keys, nil
	})
	if err != nil {
		return fmt.Errorf("workflow: registering %s query handler: %w", QueryWaitingExternal, err)
	}
	return nil
}

func (in *interpreter) receive(ctx wf.Context, nodeKey domain.NodeKey, messageName string) {
	if in.external[string(nodeKey)] && getVersion(ctx, externalWaitChangeID) != wf.DefaultVersion {
		ch, cancel := in.waitExternal(ctx, string(nodeKey))
		defer cancel()
		var answered domain.NodeKey
		ch.Receive(ctx, &answered)
		return
	}
	in.msgBuf.Receive(ctx, messageName)
}

func (in *interpreter) addMessageBoundary(ctx wf.Context, sel wf.Selector, host string, m dsl.MessagePath, onFire func(boundaryFire)) (cancel func()) {
	key := dsl.BoundaryWaitKey(host, m.BoundaryID)
	if m.BoundaryID != "" && in.external[key] && getVersion(ctx, externalWaitChangeID) != wf.DefaultVersion {
		ch, cancelWait := in.waitExternal(ctx, key)
		sel.AddReceive(ch, func(c wf.ReceiveChannel, more bool) {
			var answered domain.NodeKey
			c.Receive(ctx, &answered)
			onFire(boundaryFire{Kind: "message", Interrupting: m.Interrupting, TargetDept: m.TargetDept})
		})
		return cancelWait
	}
	return addMessageCase(ctx, sel, in.msgBuf, m.MessageName, m.Interrupting, m.TargetDept, onFire)
}
