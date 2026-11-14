package temporal_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	outboundtemporal "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

func TestActivities_EveryTransactionOpensWithTheInputTenantOnTheContext(t *testing.T) {
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
		{"ClaimAssignment", func(d *outboundtemporal.Deps, tenant string) error {
			return d.ClaimAssignment(context.Background(), port.ClaimAssignmentInput{AssignmentID: uuid.NewString(), TenantID: tenant})
		}},
		{"CompleteAssignment", func(d *outboundtemporal.Deps, tenant string) error {
			_, err := d.CompleteAssignment(context.Background(), port.CompleteAssignmentInput{TaskID: uuid.NewString(), TenantID: tenant})
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
		{"UpdateInstanceStatus", func(d *outboundtemporal.Deps, tenant string) error {
			return d.UpdateInstanceStatus(context.Background(), port.UpdateInstanceStatusInput{InstanceID: uuid.NewString(), TenantID: tenant, Status: domain.InstanceStatusCompleted})
		}},
		{"RecordForceRoute", func(d *outboundtemporal.Deps, tenant string) error {
			return d.RecordForceRoute(context.Background(), port.RecordForceRouteInput{InstanceID: uuid.NewString(), TenantID: tenant, AdminUserID: uuid.NewString()})
		}},
		{"RecordSLAWarning", func(d *outboundtemporal.Deps, tenant string) error {
			return d.RecordSLAWarning(context.Background(), port.RecordSLAWarningInput{InstanceID: uuid.NewString(), TenantID: tenant, TaskID: uuid.NewString()})
		}},
		{"RecordSLABreach", func(d *outboundtemporal.Deps, tenant string) error {
			return d.RecordSLABreach(context.Background(), port.RecordSLABreachInput{InstanceID: uuid.NewString(), TenantID: tenant, TaskID: uuid.NewString()})
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
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			world := wkactNewWorld()
			deps, tx := wkactNewDeps(t, world)
			tx.skip = true
			tenant := uuid.New()

			require.NoError(t, tc.run(deps, tenant.String()))

			require.Len(t, tx.opened, 1, "exactly one transaction is opened")
			assert.True(t, tx.opened[0].GUCSet, "the tenant GUC must already be on the context RunInTx receives")
			assert.Equal(t, tenant.String(), tx.opened[0].Tenant)
		})
	}
}
