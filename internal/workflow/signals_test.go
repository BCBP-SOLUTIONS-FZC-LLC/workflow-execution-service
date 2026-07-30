package workflow

import (
	"testing"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
)

func TestValidateSignal(t *testing.T) {
	tests := []struct {
		name    string
		status  domain.InstanceStatus
		signal  string
		wantErr bool
	}{
		{name: "stage-transition while running is allowed", status: domain.InstanceStatusRunning, signal: SignalStageTransition, wantErr: false},
		{name: "stage-transition while DEGRADED is allowed (a respawned or never-failed branch's own task)", status: domain.InstanceStatusDegraded, signal: SignalStageTransition, wantErr: false},
		{name: "stage-transition while paused is rejected", status: domain.InstanceStatusPaused, signal: SignalStageTransition, wantErr: true},
		{name: "instance-pause while running is allowed", status: domain.InstanceStatusRunning, signal: SignalInstancePause, wantErr: false},
		{
			// This is LLD §7.2 test #5's unit-level mechanism: a DEGRADED
			// instance rejects instance-pause at signal validation, before
			// the DEGRADED park loop's Selector — which registers no case
			// for instance-pause at all — ever sees it.
			name:    "instance-pause while DEGRADED is rejected",
			status:  domain.InstanceStatusDegraded,
			signal:  SignalInstancePause,
			wantErr: true,
		},
		{name: "instance-resume while paused is allowed", status: domain.InstanceStatusPaused, signal: SignalInstanceResume, wantErr: false},
		{name: "instance-resume while running is rejected", status: domain.InstanceStatusRunning, signal: SignalInstanceResume, wantErr: true},
		{name: "instance-cancel while running is allowed", status: domain.InstanceStatusRunning, signal: SignalInstanceCancel, wantErr: false},
		{name: "instance-cancel while paused is allowed", status: domain.InstanceStatusPaused, signal: SignalInstanceCancel, wantErr: false},
		{name: "instance-cancel while DEGRADED is allowed", status: domain.InstanceStatusDegraded, signal: SignalInstanceCancel, wantErr: false},
		{name: "instance-cancel while completed is rejected", status: domain.InstanceStatusCompleted, signal: SignalInstanceCancel, wantErr: true},
		{name: "instance-force-forward while DEGRADED is allowed", status: domain.InstanceStatusDegraded, signal: SignalInstanceForceFwd, wantErr: false},
		{name: "instance-force-forward while paused is rejected", status: domain.InstanceStatusPaused, signal: SignalInstanceForceFwd, wantErr: true},
		{name: "instance-force-back while running is allowed", status: domain.InstanceStatusRunning, signal: SignalInstanceForceBack, wantErr: false},
		{name: "unknown signal is rejected", status: domain.InstanceStatusRunning, signal: "not-a-real-signal", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSignal(tt.status, tt.signal)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateSignal(%s, %s) error = %v, wantErr %v", tt.status, tt.signal, err, tt.wantErr)
			}
		})
	}
}
