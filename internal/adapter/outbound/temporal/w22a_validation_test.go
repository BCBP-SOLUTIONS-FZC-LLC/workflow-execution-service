package temporal_test

import (
	"context"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"

	outboundtemporal "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// w22aBadIDCase is one input with a single malformed id: the field it must be
// reported under, and the activity call that carries it.
type w22aBadIDCase struct {
	name  string
	field string
	run   func(d *outboundtemporal.Deps) error
}

// w22aRequireBadIDsRefused runs every case against a fresh rig and checks the
// activity ends with a non-retryable ValidationError naming the field, before
// it opens a transaction or reads or writes anything.
func w22aRequireBadIDsRefused(t *testing.T, cases []w22aBadIDCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			rig := w22aNewRig()
			err := tc.run(w22aNewDeps(t, rig))

			w22aRequireNonRetryable(t, err, "ValidationError")
			var appErr *temporal.ApplicationError
			require.ErrorAs(t, err, &appErr)
			assert.True(t, strings.HasPrefix(appErr.Message(), "parse "+tc.field+":"), "message %q does not name %s", appErr.Message(), tc.field)
			assert.Empty(t, rig.txOpened, "a transaction was opened for malformed input")
			assert.Empty(t, rig.calls, "the repositories were touched for malformed input")
			assert.Empty(t, rig.outbox)
		})
	}
}

func TestCompleteAssignment_NonUUIDIDs_NonRetryableValidationErrorNamingTheField(t *testing.T) {
	t.Parallel()
	w22aRequireBadIDsRefused(t, []w22aBadIDCase{
		{name: "tenant_id", field: "tenant_id", run: func(d *outboundtemporal.Deps) error {
			_, err := d.CompleteAssignment(context.Background(), port.CompleteAssignmentInput{TenantID: "bad", TaskID: uuid.NewString(), UserID: uuid.NewString()})
			return err
		}},
		{name: "task_id", field: "task_id", run: func(d *outboundtemporal.Deps) error {
			_, err := d.CompleteAssignment(context.Background(), port.CompleteAssignmentInput{TenantID: uuid.NewString(), TaskID: "bad", UserID: uuid.NewString()})
			return err
		}},
		{name: "user_id", field: "user_id", run: func(d *outboundtemporal.Deps) error {
			_, err := d.CompleteAssignment(context.Background(), port.CompleteAssignmentInput{TenantID: uuid.NewString(), TaskID: uuid.NewString(), UserID: "bad"})
			return err
		}},
	})
}

// An empty user_id is a valid input, not a malformed one: the activity looks
// the task up and stops there.
func TestCompleteAssignment_EmptyUserID_IsNotAValidationError(t *testing.T) {
	t.Parallel()
	rig := w22aNewRig()
	tenant := uuid.New()
	task := rig.addTask(tenant, rig.addInstance(tenant, domain.InstanceStatusRunning), "sales/review", domain.TaskStatusReady)

	out, err := w22aNewDeps(t, rig).CompleteAssignment(context.Background(), port.CompleteAssignmentInput{TenantID: tenant.String(), TaskID: task.ID.String()})

	require.NoError(t, err)
	assert.False(t, out.AllDone)
	assert.Equal(t, []string{"Tasks.GetByID"}, rig.ops())
	assert.Equal(t, task.ID, rig.calls[0].ID)
	assert.Empty(t, rig.outbox)
}

func TestDeferTask_NonUUIDIDs_NonRetryableValidationErrorNamingTheField(t *testing.T) {
	t.Parallel()
	valid := port.DeferTaskInput{TenantID: uuid.NewString(), TaskID: uuid.NewString(), AssignmentID: uuid.NewString(), UserID: uuid.NewString()}
	withBad := func(mutate func(in *port.DeferTaskInput)) func(d *outboundtemporal.Deps) error {
		return func(d *outboundtemporal.Deps) error {
			in := valid
			mutate(&in)
			return d.DeferTask(context.Background(), in)
		}
	}
	w22aRequireBadIDsRefused(t, []w22aBadIDCase{
		{name: "tenant_id", field: "tenant_id", run: withBad(func(in *port.DeferTaskInput) { in.TenantID = "bad" })},
		{name: "task_id", field: "task_id", run: withBad(func(in *port.DeferTaskInput) { in.TaskID = "bad" })},
		{name: "assignment_id", field: "assignment_id", run: withBad(func(in *port.DeferTaskInput) { in.AssignmentID = "bad" })},
		{name: "user_id", field: "user_id", run: withBad(func(in *port.DeferTaskInput) { in.UserID = "bad" })},
	})
}

func TestReassignAssignment_NonUUIDIDs_NonRetryableValidationErrorNamingTheField(t *testing.T) {
	t.Parallel()
	valid := port.ReassignAssignmentInput{
		TenantID: uuid.NewString(), TaskID: uuid.NewString(), OldUserID: uuid.NewString(),
		NewUserID: uuid.NewString(), AdminUserID: uuid.NewString(),
	}
	withBad := func(mutate func(in *port.ReassignAssignmentInput)) func(d *outboundtemporal.Deps) error {
		return func(d *outboundtemporal.Deps) error {
			in := valid
			mutate(&in)
			return d.ReassignAssignment(context.Background(), in)
		}
	}
	w22aRequireBadIDsRefused(t, []w22aBadIDCase{
		{name: "tenant_id", field: "tenant_id", run: withBad(func(in *port.ReassignAssignmentInput) { in.TenantID = "bad" })},
		{name: "task_id", field: "task_id", run: withBad(func(in *port.ReassignAssignmentInput) { in.TaskID = "bad" })},
		{name: "old_user_id", field: "old_user_id", run: withBad(func(in *port.ReassignAssignmentInput) { in.OldUserID = "bad" })},
		{name: "new_user_id", field: "new_user_id", run: withBad(func(in *port.ReassignAssignmentInput) { in.NewUserID = "bad" })},
		{name: "admin_user_id", field: "admin_user_id", run: withBad(func(in *port.ReassignAssignmentInput) { in.AdminUserID = "bad" })},
	})
}

// An empty old_user_id means nobody held the task, so it passes validation:
// the activity goes on to read the task, which does not exist here.
func TestReassignAssignment_EmptyOldUserID_IsNotAValidationError(t *testing.T) {
	t.Parallel()
	rig := w22aNewRig()
	tenant, missingTask := uuid.New(), uuid.New()

	err := w22aNewDeps(t, rig).ReassignAssignment(context.Background(), port.ReassignAssignmentInput{
		TenantID: tenant.String(), TaskID: missingTask.String(), OldUserID: "",
		NewUserID: uuid.NewString(), AdminUserID: uuid.NewString(),
	})

	w22aRequireNonRetryable(t, err, "NotFoundError")
	assert.Equal(t, []string{"Tasks.GetByID"}, rig.ops())
	assert.Equal(t, missingTask, rig.calls[0].ID)
}

func TestUpdateTaskStatus_NonUUIDIDs_NonRetryableValidationErrorNamingTheField(t *testing.T) {
	t.Parallel()
	w22aRequireBadIDsRefused(t, []w22aBadIDCase{
		{name: "tenant_id", field: "tenant_id", run: func(d *outboundtemporal.Deps) error {
			return d.UpdateTaskStatus(context.Background(), port.UpdateTaskStatusInput{TenantID: "bad", TaskID: uuid.NewString(), Status: domain.TaskStatusFailed})
		}},
		{name: "task_id", field: "task_id", run: func(d *outboundtemporal.Deps) error {
			return d.UpdateTaskStatus(context.Background(), port.UpdateTaskStatusInput{TenantID: uuid.NewString(), TaskID: "bad", Status: domain.TaskStatusFailed})
		}},
	})
}

func TestRecordForceRoute_NonUUIDIDs_NonRetryableValidationErrorNamingTheField(t *testing.T) {
	t.Parallel()
	valid := port.RecordForceRouteInput{
		TenantID: uuid.NewString(), InstanceID: uuid.NewString(), AdminUserID: uuid.NewString(),
		OldNodeKeys: []domain.NodeKey{"sales/review"}, TargetNodeID: "sales/approve",
	}
	withBad := func(mutate func(in *port.RecordForceRouteInput)) func(d *outboundtemporal.Deps) error {
		return func(d *outboundtemporal.Deps) error {
			in := valid
			mutate(&in)
			return d.RecordForceRoute(context.Background(), in)
		}
	}
	w22aRequireBadIDsRefused(t, []w22aBadIDCase{
		{name: "tenant_id", field: "tenant_id", run: withBad(func(in *port.RecordForceRouteInput) { in.TenantID = "bad" })},
		{name: "instance_id", field: "instance_id", run: withBad(func(in *port.RecordForceRouteInput) { in.InstanceID = "bad" })},
		{name: "admin_user_id", field: "admin_user_id", run: withBad(func(in *port.RecordForceRouteInput) { in.AdminUserID = "bad" })},
	})
}

// Both audit events go through one shared path, so a malformed id must be
// refused the same way for the warning and the breach.
func TestRecordSLAEvents_NonUUIDIDs_NonRetryableValidationErrorNamingTheField(t *testing.T) {
	t.Parallel()
	type input struct{ tenant, instance, task string }
	good := func() input { return input{uuid.NewString(), uuid.NewString(), uuid.NewString()} }
	calls := map[string]func(d *outboundtemporal.Deps, in input) error{
		"warning": func(d *outboundtemporal.Deps, in input) error {
			return d.RecordSLAWarning(context.Background(), port.RecordSLAWarningInput{TenantID: in.tenant, InstanceID: in.instance, TaskID: in.task})
		},
		"breach": func(d *outboundtemporal.Deps, in input) error {
			return d.RecordSLABreach(context.Background(), port.RecordSLABreachInput{TenantID: in.tenant, InstanceID: in.instance, TaskID: in.task})
		},
	}
	breakers := map[string]func(in *input){
		"tenant_id":   func(in *input) { in.tenant = "bad" },
		"instance_id": func(in *input) { in.instance = "bad" },
		"task_id":     func(in *input) { in.task = "bad" },
	}
	var cases []w22aBadIDCase
	for callName, call := range calls {
		for field, breakIt := range breakers {
			cases = append(cases, w22aBadIDCase{name: callName + "/" + field, field: field, run: func(d *outboundtemporal.Deps) error {
				in := good()
				breakIt(&in)
				return call(d, in)
			}})
		}
	}
	w22aRequireBadIDsRefused(t, cases)
}

// Pause, resume and the status update share their id checks with each other
// only in part, so each activity is held to the same refusal.
func TestInstanceActivities_NonUUIDIDs_NonRetryableValidationErrorNamingTheField(t *testing.T) {
	t.Parallel()
	type input struct{ tenant, instance string }
	calls := map[string]func(d *outboundtemporal.Deps, in input) error{
		"update": func(d *outboundtemporal.Deps, in input) error {
			return d.UpdateInstanceStatus(context.Background(), port.UpdateInstanceStatusInput{TenantID: in.tenant, InstanceID: in.instance, Status: domain.InstanceStatusCompleted})
		},
		"pause": func(d *outboundtemporal.Deps, in input) error {
			return d.PauseInstance(context.Background(), port.PauseInstanceInput{TenantID: in.tenant, InstanceID: in.instance, Initiator: domain.InitiatorAdmin})
		},
		"resume": func(d *outboundtemporal.Deps, in input) error {
			return d.ResumeInstance(context.Background(), port.ResumeInstanceInput{TenantID: in.tenant, InstanceID: in.instance, Initiator: domain.InitiatorAdmin})
		},
	}
	var cases []w22aBadIDCase
	for callName, call := range calls {
		cases = append(cases,
			w22aBadIDCase{name: callName + "/tenant_id", field: "tenant_id", run: func(d *outboundtemporal.Deps) error {
				return call(d, input{tenant: "bad", instance: uuid.NewString()})
			}},
			w22aBadIDCase{name: callName + "/instance_id", field: "instance_id", run: func(d *outboundtemporal.Deps) error {
				return call(d, input{tenant: uuid.NewString(), instance: "bad"})
			}},
		)
	}
	w22aRequireBadIDsRefused(t, cases)
}
