//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTaskAssignmentRepo_CreateRecordsWhenTheAssigneeWasAssigned(t *testing.T) {
	explicit := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	tests := []struct {
		name       string
		assignedAt *time.Time
		wantExact  bool
	}{
		{name: "no time given: the insert time is recorded"},
		{name: "a time given is kept", assignedAt: &explicit, wantExact: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := newClaimedAtWorld(t)
			a := newTaskAssignment(w.tenantID, w.task.ID, uuid.New())
			a.AssignedAt = tc.assignedAt
			before := time.Now().Add(-time.Second)

			require.NoError(t, w.assignments.Create(context.Background(), a))

			stored, err := w.assignments.GetByID(context.Background(), w.tenantID, a.ID)
			require.NoError(t, err)
			require.NotNil(t, stored.AssignedAt, "an assignment always says when it was made")
			if tc.wantExact {
				assert.True(t, explicit.Equal(*stored.AssignedAt))
				return
			}
			assert.True(t, stored.AssignedAt.After(before))
		})
	}
}
