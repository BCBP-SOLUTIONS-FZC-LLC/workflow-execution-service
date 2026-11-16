package service_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTaskOverride_NobodyHoldsTheTask_RecordAndAnswerCarryNoPreviousUser(t *testing.T) {
	h := newTaHarness(t, "single")
	h.vacateAll()
	newUser := uuid.New()

	got, err := h.svc.OverrideAssignee(context.Background(), h.overrideInput(newUser))

	require.NoError(t, err)
	assert.Nil(t, got.PreviousUserID, "the answer says no one was replaced")
	require.Len(t, h.overrides.creates, 1)
	assert.Nil(t, h.overrides.creates[0].Override.PreviousUserID, "the stored record says no one was replaced")
}

func TestTaskOverride_SomeoneHoldsTheTask_RecordAndAnswerCarryThePreviousUser(t *testing.T) {
	h := newTaHarness(t, "single")

	got, err := h.svc.OverrideAssignee(context.Background(), h.overrideInput(uuid.New()))

	require.NoError(t, err)
	require.NotNil(t, got.PreviousUserID)
	require.Len(t, h.overrides.creates, 1)
	require.NotNil(t, h.overrides.creates[0].Override.PreviousUserID)
	assert.Equal(t, *got.PreviousUserID, *h.overrides.creates[0].Override.PreviousUserID)
}
