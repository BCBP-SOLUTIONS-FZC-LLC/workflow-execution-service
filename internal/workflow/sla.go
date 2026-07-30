package workflow

import (
	"time"

	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// slaTimerParams bundles addSLATimers' identifying fields — a struct rather
// than individual parameters to stay under the project's function-arity
// lint threshold.
type slaTimerParams struct {
	TenantID     string
	InstanceID   string
	TaskID       string
	NodeKey      domain.NodeKey
	DueDate      string
	FollowUpDate string
}

// addSLATimers registers a Selector case per non-empty DueDate/FollowUpDate
// (raw ISO-8601, unparsed by the compiler — FEEL out of scope, same
// permanent limit as the exclusive-gateway comparator) alongside the
// stage's own resolution case, on the SAME sel the caller re-Selects in a
// loop — that loop is what "loops back to the wait" after a timer fires
// (LLD §3.4); this function only registers cases, it doesn't loop itself.
// cancel must run once the host's own resolution wins the race. A branch
// respawned after DEGRADED calls this fresh — it never resumes a cancelled
// timer's remaining duration.
func addSLATimers(ctx wf.Context, sel wf.Selector, p slaTimerParams) (cancel func()) {
	var cancels []func()

	register := func(raw string, onFire func(wf.Context)) {
		if raw == "" {
			return
		}
		at, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			return // unparsable date is treated as "no SLA timer", not an error
		}
		waitDur := at.Sub(wf.Now(ctx))
		if waitDur < 0 {
			waitDur = 0
		}
		timerCtx, cancelTimer := wf.WithCancel(ctx)
		cancels = append(cancels, cancelTimer)
		future := wf.NewTimer(timerCtx, waitDur)
		sel.AddFuture(future, func(f wf.Future) {
			if f.Get(timerCtx, nil) != nil {
				return // cancelled, not fired
			}
			onFire(ctx)
		})
	}

	register(p.DueDate, func(c wf.Context) {
		_ = recordSLABreach(c, port.RecordSLABreachInput{InstanceID: p.InstanceID, TenantID: p.TenantID, TaskID: p.TaskID, NodeKey: p.NodeKey})
	})
	register(p.FollowUpDate, func(c wf.Context) {
		_ = recordSLAWarning(c, port.RecordSLAWarningInput{InstanceID: p.InstanceID, TenantID: p.TenantID, TaskID: p.TaskID, NodeKey: p.NodeKey})
	})

	return func() {
		for _, c := range cancels {
			c()
		}
	}
}
