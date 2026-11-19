package temporal_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestRecordSLAEvents_ATaskWithoutTheDateIsANonRetryableValidationErrorAndRecordsNothing(t *testing.T) {
	date := time.Date(2026, 4, 2, 10, 30, 0, 0, time.UTC)
	cases := []struct {
		name   string
		due    *time.Time
		follow *time.Time
		record func(deps interface {
			RecordSLAWarning(context.Context, port.RecordSLAWarningInput) error
			RecordSLABreach(context.Context, port.RecordSLABreachInput) error
		}, tenant, instance, task string) error
	}{
		{"warning without follow-up date", &date, nil, func(d interface {
			RecordSLAWarning(context.Context, port.RecordSLAWarningInput) error
			RecordSLABreach(context.Context, port.RecordSLABreachInput) error
		}, tenant, instance, task string) error {
			return d.RecordSLAWarning(context.Background(), port.RecordSLAWarningInput{TenantID: tenant, InstanceID: instance, TaskID: task})
		}},
		{"breach without due date", nil, &date, func(d interface {
			RecordSLAWarning(context.Context, port.RecordSLAWarningInput) error
			RecordSLABreach(context.Context, port.RecordSLABreachInput) error
		}, tenant, instance, task string) error {
			return d.RecordSLABreach(context.Background(), port.RecordSLABreachInput{TenantID: tenant, InstanceID: instance, TaskID: task})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rig := w22aNewRig()
			tenant := uuid.New()
			inst, task, _ := w22aSeedSLATask(rig, tenant, tc.due, tc.follow)

			err := tc.record(w22aNewDeps(t, rig), tenant.String(), inst.ID.String(), task.ID.String())

			require.Error(t, err)
			requireValidationError(t, err)
			assert.Empty(t, rig.outbox, "no event with a zero date is recorded")
		})
	}
}
