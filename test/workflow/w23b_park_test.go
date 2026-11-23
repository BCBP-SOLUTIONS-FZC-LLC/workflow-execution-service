package workflow_test

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestExecute_TwoBranchesFailInOnePark_FirstDegradedWriteNamesBothDepartments(t *testing.T) {
	env := w23bNewEnv()
	fakes := wflNewFakes()
	collab := parallelCollaboration()
	warehouseIAM, billingIAM := uuid.New(), uuid.New()
	collab.Plans[0].Departments[0].IAMDepartmentID = warehouseIAM.String()
	collab.Plans[0].Departments[1].IAMDepartmentID = billingIAM.String()
	fakes.failFor = func(_ string, in any) error {
		if v, ok := in.(port.CreateTaskInput); ok {
			return wflRefuse("create " + string(v.NodeKey))
		}
		return nil
	}
	wflRegisterAll(env, fakes, collab)
	wflSignalAt(env, 4*time.Second, "instance-force-forward", adminSignalWire{AdminUserID: "admin-1", TargetNodeKey: "warehouse/prep", RecordVersion: 1})
	wflSignalAt(env, 6*time.Second, "instance-force-forward", adminSignalWire{AdminUserID: "admin-1", TargetNodeKey: "billing/prep", RecordVersion: 2})

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	var degraded []port.UpdateInstanceStatusInput
	for _, w := range wflInputs[port.UpdateInstanceStatusInput](fakes, port.ActivityUpdateInstanceStatus) {
		if w.Status == domain.InstanceStatusDegraded {
			degraded = append(degraded, w)
		}
	}
	require.NotEmpty(t, degraded)
	first := degraded[0]
	assert.Equal(t, wflTenant, first.TenantID)
	assert.Equal(t, wflInstance, first.InstanceID)
	var depts []uuid.UUID
	for _, b := range first.FailedBranches {
		depts = append(depts, b.DepartmentID)
	}
	assert.ElementsMatch(t, []uuid.UUID{warehouseIAM, billingIAM}, depts, "the one park records every failed department")
}

// w23bSequence records the order in which the instance's activities ran.
type w23bSequence struct {
	mu     sync.Mutex
	events []string
}

func (s *w23bSequence) add(event string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, event)
}

func (s *w23bSequence) snapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.events...)
}

func w23bFailBranchesTwo(fakes *wflFakes, seq *w23bSequence) {
	fakes.failFor = func(_ string, in any) error {
		switch v := in.(type) {
		case port.CreateTaskInput:
			if v.NodeKey == "x2/prep" || v.NodeKey == "y2/prep" {
				return wflRefuse("create " + string(v.NodeKey))
			}
		case port.RecordForceRouteInput:
			seq.add(fmt.Sprintf("route:%s", v.TargetNodeID))
		case port.PauseInstanceInput:
			seq.add("pause:" + v.Initiator)
		case port.ResumeInstanceInput:
			seq.add("resume:" + v.Initiator)
		case port.UpdateInstanceStatusInput:
			seq.add("status:" + string(v.Status))
		}
		return nil
	}
}

func TestExecute_PauseDuringTwoOpenParks_IsHeldUntilTheLastParkCloses(t *testing.T) {
	env := w23bNewEnv()
	fakes := wflNewFakes()
	seq := &w23bSequence{}
	w23bFailBranchesTwo(fakes, seq)
	wflRegisterAll(env, fakes, twoInnerForksCollaboration())
	wflSignalsAt(env, 2*time.Second,
		wflSignal{"stage-transition", wflDoneStage("x1", "prep", 1)},
		wflSignal{"stage-transition", wflDoneStage("y1", "prep", 1)},
	)
	wflSignalAt(env, 4*time.Second, "instance-pause", adminSignalWire{AdminUserID: "admin-1", Initiator: domain.InitiatorAdmin, RecordVersion: 1})
	wflSignalAt(env, 6*time.Second, "instance-force-forward", adminSignalWire{AdminUserID: "admin-1", TargetNodeKey: "y2/prep", RecordVersion: 2})
	wflSignalAt(env, 8*time.Second, "instance-force-forward", adminSignalWire{AdminUserID: "admin-1", TargetNodeKey: "x2/prep", RecordVersion: 3})
	wflSignalAt(env, 12*time.Second, "instance-resume", adminSignalWire{AdminUserID: "admin-1", Initiator: domain.InitiatorAdmin, RecordVersion: 4})

	status, err := wflStart(t, env, fakes, "")

	require.NoError(t, err)
	assert.Equal(t, domain.InstanceStatusCompleted, status)
	events := seq.snapshot()
	indexOf := func(event string) int {
		for i, e := range events {
			if e == event {
				return i
			}
		}
		return -1
	}
	pauseAt, firstClose, lastClose := indexOf("pause:admin"), indexOf("route:y2/prep"), indexOf("route:x2/prep")
	require.NotEqual(t, -1, pauseAt, "the held pause is applied in the end: %v", events)
	require.NotEqual(t, -1, firstClose, "%v", events)
	require.NotEqual(t, -1, lastClose, "%v", events)
	assert.Less(t, lastClose, pauseAt, "the pause waited for the last park, not the first one to close: %v", events)
	pauses := 0
	for _, e := range events {
		if e == "pause:admin" {
			pauses++
		}
	}
	assert.Equal(t, 1, pauses, "%v", events)
}
