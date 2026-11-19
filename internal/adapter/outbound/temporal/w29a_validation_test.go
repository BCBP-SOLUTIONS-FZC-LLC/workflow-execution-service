package temporal_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"

	outboundtemporal "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

const w29aNotAUUID = "not-a-uuid"

// w29aRequireRefusedID checks that the activity ended with a Temporal
// application error of type ValidationError that is not retried, whose cause
// is the parse error of the named field, before any transaction or repository
// call.
func w29aRequireRefusedID(t *testing.T, rig *w25bRig, err error, field string) {
	t.Helper()
	require.Error(t, err)
	var appErr *temporal.ApplicationError
	require.ErrorAs(t, err, &appErr)
	assert.Equal(t, "ValidationError", appErr.Type())
	assert.True(t, appErr.NonRetryable(), "Temporal must not retry a malformed id")
	_, parseErr := uuid.Parse(w29aNotAUUID)
	require.Error(t, parseErr)
	assert.Equal(t, "parse "+field+": "+parseErr.Error(), appErr.Message())
	cause := appErr.Unwrap()
	require.Error(t, cause, "the parse error travels as the cause")
	assert.Equal(t, appErr.Message(), cause.Error())
	assert.Equal(t, parseErr.Error(), errors.Unwrap(cause).Error())
	assert.Empty(t, rig.txOpened, "no transaction is opened for a malformed id")
	assert.Empty(t, rig.calls, "no repository is touched for a malformed id")
	assert.Empty(t, rig.outbox)
}

func TestDeferTask_AnIdThatIsNotAUUID_IsANonRetryableValidationErrorWithTheParseErrorAsCause(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		field  string
		mutate func(in *port.DeferTaskInput)
	}{
		{name: "tenant id", field: "tenant_id", mutate: func(in *port.DeferTaskInput) { in.TenantID = w29aNotAUUID }},
		{name: "task id", field: "task_id", mutate: func(in *port.DeferTaskInput) { in.TaskID = w29aNotAUUID }},
		{name: "assignment id", field: "assignment_id", mutate: func(in *port.DeferTaskInput) { in.AssignmentID = w29aNotAUUID }},
		{name: "user id", field: "user_id", mutate: func(in *port.DeferTaskInput) { in.UserID = w29aNotAUUID }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rig := w25bNewRig()
			in := port.DeferTaskInput{TenantID: uuid.NewString(), TaskID: uuid.NewString(), AssignmentID: uuid.NewString(), UserID: uuid.NewString()}
			tc.mutate(&in)

			err := w25bNewDeps(t, rig).DeferTask(context.Background(), in)

			w29aRequireRefusedID(t, rig, err, tc.field)
		})
	}
}

func TestPauseAndResumeInstance_TenantOrInstanceIdThatIsNotAUUID_IsANonRetryableValidationError(t *testing.T) {
	t.Parallel()

	activities := []struct {
		name string
		run  func(d *outboundtemporal.Deps, tenant, instance string) error
	}{
		{name: "pause", run: func(d *outboundtemporal.Deps, tenant, instance string) error {
			return d.PauseInstance(context.Background(), port.PauseInstanceInput{
				TenantID: tenant, InstanceID: instance, AdminUserID: uuid.NewString(), Initiator: domain.InitiatorAdmin, RecordVersion: 3,
			})
		}},
		{name: "resume", run: func(d *outboundtemporal.Deps, tenant, instance string) error {
			return d.ResumeInstance(context.Background(), port.ResumeInstanceInput{
				TenantID: tenant, InstanceID: instance, AdminUserID: uuid.NewString(), Initiator: domain.InitiatorAdmin, RecordVersion: 3,
			})
		}},
	}
	fields := []struct {
		name  string
		field string
		ids   func() (tenant, instance string)
	}{
		{name: "tenant id", field: "tenant_id", ids: func() (string, string) { return w29aNotAUUID, uuid.NewString() }},
		{name: "instance id", field: "instance_id", ids: func() (string, string) { return uuid.NewString(), w29aNotAUUID }},
	}
	for _, a := range activities {
		for _, f := range fields {
			t.Run(a.name+" with a bad "+f.name, func(t *testing.T) {
				t.Parallel()
				rig := w25bNewRig()
				tenant, instance := f.ids()

				err := a.run(w25bNewDeps(t, rig), tenant, instance)

				w29aRequireRefusedID(t, rig, err, f.field)
			})
		}
	}
}

func TestCancelInstance_AnIdThatIsNotAUUID_IsANonRetryableValidationErrorWithTheParseErrorAsCause(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		field  string
		mutate func(in *port.CancelInstanceInput)
	}{
		{name: "tenant id", field: "tenant_id", mutate: func(in *port.CancelInstanceInput) { in.TenantID = w29aNotAUUID }},
		{name: "instance id", field: "instance_id", mutate: func(in *port.CancelInstanceInput) { in.InstanceID = w29aNotAUUID }},
		{name: "admin user id", field: "admin_user_id", mutate: func(in *port.CancelInstanceInput) { in.AdminUserID = w29aNotAUUID }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rig := w25bNewRig()
			in := port.CancelInstanceInput{TenantID: uuid.NewString(), InstanceID: uuid.NewString(), AdminUserID: uuid.NewString()}
			tc.mutate(&in)

			err := w25bNewDeps(t, rig).CancelInstance(context.Background(), in)

			w29aRequireRefusedID(t, rig, err, tc.field)
		})
	}
}
