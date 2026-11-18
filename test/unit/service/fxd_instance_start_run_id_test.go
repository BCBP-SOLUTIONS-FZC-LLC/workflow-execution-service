package service_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInstanceServiceStart_RunIdWriteFailureStillAnswersWithTheRunIdTheWorkflowStartedUnder(t *testing.T) {
	t.Parallel()
	rig := w14bNewRunIDRig(t)
	rig.instances.runIDErr = errW14bRunIDWrite

	got, err := rig.svcStart()

	require.NoError(t, err)
	assert.Equal(t, "run-1", got.TemporalRunID, "the answer carries the run id the started workflow returned")
	assert.Empty(t, rig.instances.byID[got.ID].TemporalRunID, "the run id was not stored")
	require.Len(t, rig.log.warnFields, 1, "the failure is still logged once, as a warning")
	assert.Equal(t, errW14bRunIDWrite.Error(), rig.log.warnFields[0]["error"])
	assert.Empty(t, rig.log.errorCalls)
}

func TestInstanceServiceStart_RunIdIsInTheAnswerWhenItWasRecorded(t *testing.T) {
	t.Parallel()
	rig := w14bNewRunIDRig(t)

	got, err := rig.svcStart()

	require.NoError(t, err)
	assert.Equal(t, "run-1", got.TemporalRunID)
	assert.Equal(t, "run-1", rig.instances.byID[got.ID].TemporalRunID)
	assert.Empty(t, rig.log.warnFields)
}
