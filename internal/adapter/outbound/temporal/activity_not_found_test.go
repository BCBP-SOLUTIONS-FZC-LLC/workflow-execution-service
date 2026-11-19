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

func TestActivities_AMissingRowIsANonRetryableNotFoundError(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		run  func(d *outboundtemporal.Deps, tenant string) error
	}{
		{"CreateTask", func(d *outboundtemporal.Deps, tenant string) error {
			_, err := d.CreateTask(context.Background(), port.CreateTaskInput{
				InstanceID: uuid.NewString(), TenantID: tenant, NodeKey: "sales/review", CompiledNode: []byte(`{}`),
			})
			return err
		}},
		{"UpdateInstanceStatus", func(d *outboundtemporal.Deps, tenant string) error {
			return d.UpdateInstanceStatus(context.Background(), port.UpdateInstanceStatusInput{InstanceID: uuid.NewString(), TenantID: tenant, Status: domain.InstanceStatusCompleted})
		}},
		{"UpdateInstanceNodes", func(d *outboundtemporal.Deps, tenant string) error {
			return d.UpdateInstanceNodes(context.Background(), port.UpdateInstanceNodesInput{InstanceID: uuid.NewString(), TenantID: tenant, NodeKeys: []domain.NodeKey{"sales/review"}})
		}},
		{"PauseInstance", func(d *outboundtemporal.Deps, tenant string) error {
			return d.PauseInstance(context.Background(), port.PauseInstanceInput{InstanceID: uuid.NewString(), TenantID: tenant})
		}},
		{"ResumeInstance", func(d *outboundtemporal.Deps, tenant string) error {
			return d.ResumeInstance(context.Background(), port.ResumeInstanceInput{InstanceID: uuid.NewString(), TenantID: tenant})
		}},
		{"CancelInstance", func(d *outboundtemporal.Deps, tenant string) error {
			return d.CancelInstance(context.Background(), port.CancelInstanceInput{InstanceID: uuid.NewString(), TenantID: tenant, AdminUserID: uuid.NewString()})
		}},
		{"CompleteAssignment", func(d *outboundtemporal.Deps, tenant string) error {
			_, err := d.CompleteAssignment(context.Background(), port.CompleteAssignmentInput{TaskID: uuid.NewString(), TenantID: tenant, UserID: uuid.NewString()})
			return err
		}},
		{"DeferTask", func(d *outboundtemporal.Deps, tenant string) error {
			return d.DeferTask(context.Background(), port.DeferTaskInput{
				TaskID: uuid.NewString(), TenantID: tenant, UserID: uuid.NewString(), AssignmentID: uuid.NewString(),
			})
		}},
		{"ReassignAssignment", func(d *outboundtemporal.Deps, tenant string) error {
			return d.ReassignAssignment(context.Background(), port.ReassignAssignmentInput{
				TaskID: uuid.NewString(), TenantID: tenant, OldUserID: uuid.NewString(), NewUserID: uuid.NewString(), AdminUserID: uuid.NewString(),
			})
		}},
		{"UpdateTaskStatus", func(d *outboundtemporal.Deps, tenant string) error {
			return d.UpdateTaskStatus(context.Background(), port.UpdateTaskStatusInput{TaskID: uuid.NewString(), TenantID: tenant, Status: domain.TaskStatusFailed})
		}},
		{"RecordForceRoute", func(d *outboundtemporal.Deps, tenant string) error {
			return d.RecordForceRoute(context.Background(), port.RecordForceRouteInput{
				InstanceID: uuid.NewString(), TenantID: tenant, AdminUserID: uuid.NewString(),
			})
		}},
		{"RecordSLAWarning", func(d *outboundtemporal.Deps, tenant string) error {
			return d.RecordSLAWarning(context.Background(), port.RecordSLAWarningInput{InstanceID: uuid.NewString(), TenantID: tenant, TaskID: uuid.NewString()})
		}},
		{"RecordSLABreach", func(d *outboundtemporal.Deps, tenant string) error {
			return d.RecordSLABreach(context.Background(), port.RecordSLABreachInput{InstanceID: uuid.NewString(), TenantID: tenant, TaskID: uuid.NewString()})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			world := wkactNewWorld()
			deps, _ := wkactNewDeps(t, world)

			err := tc.run(deps, uuid.NewString())

			require.Error(t, err)
			var appErr *temporal.ApplicationError
			require.ErrorAs(t, err, &appErr)
			assert.Equal(t, "NotFoundError", appErr.Type())
			assert.True(t, appErr.NonRetryable(), "the error itself carries the non-retryable mark")
			assert.ErrorIs(t, err, domain.ErrNotFound)
		})
	}
}

func TestActivities_AnInstanceOfAnotherTenantIsNotFoundAndNotRetried(t *testing.T) {
	t.Parallel()
	world := wkactNewWorld()
	deps, _ := wkactNewDeps(t, world)
	owner := uuid.New()
	inst := world.addInstance(owner, domain.InstanceStatusRunning)

	err := deps.UpdateInstanceStatus(context.Background(), port.UpdateInstanceStatusInput{
		InstanceID: inst.ID.String(), TenantID: uuid.NewString(), Status: domain.InstanceStatusCompleted,
	})

	var appErr *temporal.ApplicationError
	require.ErrorAs(t, err, &appErr)
	assert.Equal(t, "NotFoundError", appErr.Type())
	assert.Equal(t, domain.InstanceStatusRunning, world.instances[inst.ID].Status, "the other tenant's instance is untouched")
}

func TestActivities_ADatabaseFailureStaysRetryable(t *testing.T) {
	t.Parallel()
	world := wkactNewWorld()
	deps, _ := wkactNewDeps(t, world)
	tenant := uuid.New()
	inst := world.addInstance(tenant, domain.InstanceStatusRunning)
	world.failures["Instances.GetByID"] = errWkactBoom

	err := deps.UpdateInstanceStatus(context.Background(), port.UpdateInstanceStatusInput{
		InstanceID: inst.ID.String(), TenantID: tenant.String(), Status: domain.InstanceStatusCompleted,
	})

	require.ErrorIs(t, err, errWkactBoom)
	var appErr *temporal.ApplicationError
	assert.False(t, errors.As(err, &appErr), "an infrastructure error is a plain error, so Temporal retries it")
}
