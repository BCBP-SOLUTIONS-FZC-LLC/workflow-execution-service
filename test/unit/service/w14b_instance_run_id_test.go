package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var errW14bRunIDWrite = errors.New("w14b database unavailable")

type w14bRunIDRig struct {
	svcStart  func() (*port.Instance, error)
	log       *fakeLogger
	starts    *[]port.StartWorkflowInput
	instances *fakeInstanceRepo
	tenantID  uuid.UUID
}

func w14bNewRunIDRig(t *testing.T) *w14bRunIDRig {
	t.Helper()
	svc, instances, _, _, _, temporal, definitions, _ := newInstanceServiceHarness()
	log := &fakeLogger{}
	svc.Log = log
	versionID, tenantID, caller := uuid.New(), uuid.New(), uuid.New()
	definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))
	var starts []port.StartWorkflowInput
	temporal.startFunc = func(_ context.Context, in port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
		starts = append(starts, in)
		return port.StartWorkflowOutput{TemporalWorkflowID: in.TemporalWorkflowID, TemporalRunID: "run-1"}, nil
	}
	return &w14bRunIDRig{
		svcStart: func() (*port.Instance, error) {
			return svc.Start(context.Background(), port.StartInstanceInput{
				TenantID: tenantID, WorkflowVersionID: versionID, SubjectID: "SUBJ-1", BusinessKey: "TND-w14b", StartedByUserID: caller,
			})
		},
		log: log, starts: &starts, instances: instances, tenantID: tenantID,
	}
}

func TestInstanceServiceStart_RunIdWriteFailureIsAWarningAndARepeatStartLaunchesAgain(t *testing.T) {
	t.Parallel()
	rig := w14bNewRunIDRig(t)
	rig.instances.runIDErr = errW14bRunIDWrite

	first, err := rig.svcStart()

	require.NoError(t, err, "the workflow started, so the start succeeds")
	assert.Equal(t, port.InstanceStatusRunning, first.Status)
	require.Len(t, rig.log.warnFields, 1, "the failure is logged once, as a warning")
	warn := rig.log.warnFields[0]
	assert.Equal(t, first.ID, warn["instance_id"])
	assert.Equal(t, rig.tenantID, warn["tenant_id"])
	assert.Equal(t, errW14bRunIDWrite.Error(), warn["error"])
	assert.Empty(t, rig.log.errorCalls, "nothing is logged at error level")
	assert.Empty(t, rig.instances.byID[first.ID].TemporalRunID, "the run id was not stored")
	require.Len(t, *rig.starts, 1)

	rig.instances.runIDErr = nil
	again, err := rig.svcStart()

	require.NoError(t, err)
	assert.Equal(t, first.ID, again.ID, "the same request finds the same instance")
	require.Len(t, *rig.starts, 2, "a row without a run id is launched again")
	assert.Equal(t, (*rig.starts)[0].TemporalWorkflowID, (*rig.starts)[1].TemporalWorkflowID, "under the same workflow id, so the server returns the running execution")
	assert.Equal(t, first.ID, (*rig.starts)[1].InstanceID)
	assert.Equal(t, rig.tenantID, (*rig.starts)[1].TenantID)
	assert.Equal(t, "run-1", rig.instances.byID[first.ID].TemporalRunID, "the second launch records the run id")
	assert.Len(t, rig.instances.byID, 1)

	third, err := rig.svcStart()

	require.NoError(t, err)
	assert.Equal(t, first.ID, third.ID)
	assert.Len(t, *rig.starts, 2, "once the run id is stored a repeat start launches nothing")
}
