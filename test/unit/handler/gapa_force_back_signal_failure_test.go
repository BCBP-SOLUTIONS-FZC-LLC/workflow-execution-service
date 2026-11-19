package handler_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestForceBackRoute_TemporalCannotTakeTheSignal_Returns503WithTheFixedDetailAndChangesNothing(t *testing.T) {
	cases := []struct {
		name       string
		status     domain.InstanceStatus
		target     string
		wantTarget string
		setup      func(rig *w19bRig, inst *domain.Instance)
	}{
		{"a running instance with a completed step", domain.InstanceStatusRunning, "", "", func(rig *w19bRig, inst *domain.Instance) {
			rig.addTask(inst, domain.TaskStatusCompleted)
		}},
		{"a degraded instance", domain.InstanceStatusDegraded, w19bTargetNode, w19bTargetNode, func(*w19bRig, *domain.Instance) {}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w19bNewRig(t)
			rig.sdk.signalErr = serviceerror.NewUnavailable(fmt.Sprintf("frontend down: %s", errW19bCauseMarker))
			inst := rig.seed(tc.status, 4)
			tc.setup(rig, inst)

			w := rig.forceBackTo(inst.ID, tc.target, 4)

			require.Equal(t, http.StatusServiceUnavailable, w.Code, w.Body.String())
			p := w19bProblemOf(t, w)
			assert.Equal(t, "UPSTREAM_UNAVAILABLE", p.Code)
			assert.Equal(t, "the workflow engine could not take the request; retry it", p.Detail)
			assert.NotContains(t, w.Body.String(), errW19bCauseMarker.Error(), "the cause stays out of the answer")
			require.Len(t, rig.sdk.signals, 1, "the signal was tried once")
			assert.Equal(t, "wf-"+inst.ID.String(), rig.sdk.signals[0].workflowID)
			assert.Empty(t, rig.sdk.signals[0].runID, "the signal targets the latest run")
			assert.Equal(t, port.SignalInstanceForceBack+":"+inst.ID.String(), rig.sdk.signals[0].channel)
			assert.Equal(t, w19bSignalBody{AdminUserID: testUserID.String(), TargetNodeKey: tc.wantTarget, RecordVersion: 4}, w19bDecodeSignal(t, rig.sdk.signals[0].payload))
			assert.Empty(t, rig.instances.writes, "no instance state is written")
			assert.Empty(t, rig.outbox.events, "no event is queued")
		})
	}
}
