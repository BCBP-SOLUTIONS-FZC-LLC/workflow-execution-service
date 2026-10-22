package workflow

import (
	"time"

	wf "go.temporal.io/sdk/workflow"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

type slaTimerParams struct {
	TenantID     string
	InstanceID   string
	TaskID       string
	NodeKey      domain.NodeKey
	DueDate      string
	FollowUpDate string
}

func addSLATimers(ctx wf.Context, sel wf.Selector, p slaTimerParams) (cancel func()) {
	var cancels []func()

	register := func(raw string, onFire func(wf.Context)) {
		if raw == "" {
			return
		}
		at, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			wf.GetLogger(ctx).Warn("SLA timer has an unparseable date, disabling it", "date", raw, "error", err)
			return
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
				return
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
