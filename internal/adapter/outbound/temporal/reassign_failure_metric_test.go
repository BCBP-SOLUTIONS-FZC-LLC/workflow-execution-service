package temporal_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/observability/obstest"
)

func TestReassignAssignment_PermanentFailureIsCounted(t *testing.T) {
	taskID, newUser := uuid.New(), uuid.New()
	task := &domain.Task{ID: taskID, WorkflowInstanceID: uuid.New(), RecordVersion: 1}
	valid := port.ReassignAssignmentInput{
		TaskID: taskID.String(), TenantID: uuid.NewString(),
		NewUserID: newUser.String(), AdminUserID: uuid.NewString(), RecordVersion: 1,
	}
	cases := []struct {
		name     string
		mutate   func(*port.ReassignAssignmentInput)
		wantFail bool
	}{
		{"reassigned", func(*port.ReassignAssignmentInput) {}, false},
		{"malformed tenant id", func(in *port.ReassignAssignmentInput) { in.TenantID = "x" }, true},
		{"malformed new user id", func(in *port.ReassignAssignmentInput) { in.NewUserID = "x" }, true},
		{"unknown task", func(in *port.ReassignAssignmentInput) { in.TaskID = uuid.NewString() }, true},
		{"stale record version", func(in *port.ReassignAssignmentInput) { in.RecordVersion = 7 }, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			obstest.Init(t)
			deps, _ := newAssignmentTestDeps(newFakeTaskRepo(task), newFakeAssignmentRepo())
			in := valid
			tc.mutate(&in)
			before := testutil.ToFloat64(observability.ReassignFailuresTotal)

			err := deps.ReassignAssignment(context.Background(), in)

			want := before
			if tc.wantFail {
				assert.Error(t, err)
				want++
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, want, testutil.ToFloat64(observability.ReassignFailuresTotal))
		})
	}
}
