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

// wkactEmitterCase runs one activity against a world holding a single task with
// no active assignment, and names the event it must emit and the array fields
// of that event's payload.
type wkactEmitterCase struct {
	name      string
	eventType string
	run       func(d *outboundtemporal.Deps, tenant uuid.UUID, inst *domain.Instance, task *domain.Task) error
	arrays    map[string]string
}

func TestEmitters_NoActiveAssignees_SendEmptyArraysNeverNull(t *testing.T) {
	t.Parallel()

	empty := map[string]string{"assignee_user_ids": "[]"}
	cases := []wkactEmitterCase{
		{
			name: "superseded", eventType: domain.EventWorkflowTaskSuperseded, arrays: empty,
			run: func(d *outboundtemporal.Deps, tenant uuid.UUID, _ *domain.Instance, task *domain.Task) error {
				return d.UpdateTaskStatus(context.Background(), port.UpdateTaskStatusInput{
					TaskID: task.ID.String(), TenantID: tenant.String(), Status: domain.TaskStatusSuperseded,
				})
			},
		},
		{
			name: "failed by stage failure", eventType: domain.EventWorkflowTaskFailed, arrays: empty,
			run: func(d *outboundtemporal.Deps, tenant uuid.UUID, _ *domain.Instance, task *domain.Task) error {
				return d.UpdateTaskStatus(context.Background(), port.UpdateTaskStatusInput{
					TaskID: task.ID.String(), TenantID: tenant.String(), Status: domain.TaskStatusFailed, ErrorClass: "timeout",
				})
			},
		},
		{
			name: "failed by instance cancel", eventType: domain.EventWorkflowTaskFailed, arrays: empty,
			run: func(d *outboundtemporal.Deps, tenant uuid.UUID, inst *domain.Instance, _ *domain.Task) error {
				return d.CancelInstance(context.Background(), port.CancelInstanceInput{
					InstanceID: inst.ID.String(), TenantID: tenant.String(), AdminUserID: uuid.NewString(),
				})
			},
		},
		{
			name: "sla warning", eventType: domain.EventWorkflowTaskSLAWarning, arrays: empty,
			run: func(d *outboundtemporal.Deps, tenant uuid.UUID, inst *domain.Instance, task *domain.Task) error {
				return d.RecordSLAWarning(context.Background(), port.RecordSLAWarningInput{
					InstanceID: inst.ID.String(), TenantID: tenant.String(), TaskID: task.ID.String(),
				})
			},
		},
		{
			name: "sla breach", eventType: domain.EventWorkflowTaskSLABreached, arrays: empty,
			run: func(d *outboundtemporal.Deps, tenant uuid.UUID, inst *domain.Instance, task *domain.Task) error {
				return d.RecordSLABreach(context.Background(), port.RecordSLABreachInput{
					InstanceID: inst.ID.String(), TenantID: tenant.String(), TaskID: task.ID.String(),
				})
			},
		},
		{
			name: "force route with no bypassed node", eventType: domain.EventWorkflowInstanceForceRouted,
			arrays: map[string]string{"from_node_keys": "[]"},
			run: func(d *outboundtemporal.Deps, tenant uuid.UUID, inst *domain.Instance, _ *domain.Task) error {
				return d.RecordForceRoute(context.Background(), port.RecordForceRouteInput{
					InstanceID: inst.ID.String(), TenantID: tenant.String(), TargetNodeID: "sales/approve", AdminUserID: uuid.NewString(),
				})
			},
		},
		{
			name: "force route supersedes a task nobody holds", eventType: domain.EventWorkflowTaskSuperseded, arrays: empty,
			run: func(d *outboundtemporal.Deps, tenant uuid.UUID, inst *domain.Instance, task *domain.Task) error {
				return d.RecordForceRoute(context.Background(), port.RecordForceRouteInput{
					InstanceID: inst.ID.String(), TenantID: tenant.String(), OldNodeKeys: []domain.NodeKey{domain.NodeKey(task.NodeKey)},
					TargetNodeID: "sales/approve", AdminUserID: uuid.NewString(),
				})
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { t.Parallel(); wkactRunEmitterCase(t, tc) })
	}

	t.Run("claimed", func(t *testing.T) {
		t.Parallel()
		world := wkactNewWorld()
		deps, _ := wkactNewDeps(t, world)
		tenant := uuid.New()
		inst := world.addInstance(tenant, domain.InstanceStatusRunning)
		task := world.addTask(tenant, inst, "sales/review", domain.TaskStatusReady)
		task.RecordVersion = 3
		vacated := world.addAssignment(tenant, task, false)

		require.NoError(t, deps.ClaimAssignment(context.Background(), port.ClaimAssignmentInput{
			AssignmentID: vacated.ID.String(), TenantID: tenant.String(),
		}))

		evs := world.eventsOfType(domain.EventWorkflowTaskClaimed)
		require.Len(t, evs, 1)
		assert.JSONEq(t, "[]", string(wkactPayloadFields(t, evs[0])["assignee_user_ids"]))
		leads := world.callsTo("Assignments.SetLead")
		require.Len(t, leads, 1)
		assert.Equal(t, int64(3), leads[0].Version, "the lead is set at the task version read in the transaction")
		world.requireAllCallsForTenant(t, tenant)
	})
}

func wkactRunEmitterCase(t *testing.T, tc wkactEmitterCase) {
	t.Helper()
	world := wkactNewWorld()
	deps, _ := wkactNewDeps(t, world)
	tenant := uuid.New()
	inst := world.addInstance(tenant, domain.InstanceStatusRunning)
	task := world.addTask(tenant, inst, "sales/review", domain.TaskStatusReady)

	require.NoError(t, tc.run(deps, tenant, inst, task))

	evs := world.eventsOfType(tc.eventType)
	require.Len(t, evs, 1, "one %s event", tc.eventType)
	fields := wkactPayloadFields(t, evs[0])
	for field, want := range tc.arrays {
		require.Contains(t, fields, field)
		assert.JSONEq(t, want, string(fields[field]), field)
	}
	world.requireAllCallsForTenant(t, tenant)
}

func TestEmitters_SingleAssigneeEvents_SendOneElementArray(t *testing.T) {
	t.Parallel()

	user := uuid.New()
	cases := []struct {
		name      string
		eventType string
		run       func(d *outboundtemporal.Deps, tenant uuid.UUID, inst *domain.Instance, task *domain.Task, a *domain.TaskAssignment) error
	}{
		{"completed", domain.EventWorkflowTaskCompleted, func(d *outboundtemporal.Deps, tenant uuid.UUID, _ *domain.Instance, task *domain.Task, a *domain.TaskAssignment) error {
			_, err := d.CompleteAssignment(context.Background(), port.CompleteAssignmentInput{
				TaskID: task.ID.String(), TenantID: tenant.String(), UserID: a.UserID.String(), ResultJSON: `{}`,
			})
			return err
		}},
		{"deferred", domain.EventWorkflowTaskDeferred, func(d *outboundtemporal.Deps, tenant uuid.UUID, _ *domain.Instance, task *domain.Task, a *domain.TaskAssignment) error {
			return d.DeferTask(context.Background(), port.DeferTaskInput{
				TaskID: task.ID.String(), TenantID: tenant.String(), UserID: a.UserID.String(), AssignmentID: a.ID.String(),
				Reason: "waiting", DeferredToNodeKey: "sales/later",
			})
		}},
		{"reassigned", domain.EventWorkflowTaskReassigned, func(d *outboundtemporal.Deps, tenant uuid.UUID, _ *domain.Instance, task *domain.Task, a *domain.TaskAssignment) error {
			return d.ReassignAssignment(context.Background(), port.ReassignAssignmentInput{
				TaskID: task.ID.String(), TenantID: tenant.String(), OldUserID: a.UserID.String(), NewUserID: user.String(),
				AdminUserID: uuid.NewString(), RecordVersion: task.RecordVersion,
			})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			world := wkactNewWorld()
			deps, _ := wkactNewDeps(t, world)
			tenant := uuid.New()
			inst := world.addInstance(tenant, domain.InstanceStatusRunning)
			task := world.addTask(tenant, inst, "sales/review", domain.TaskStatusReady)
			assignment := world.addAssignment(tenant, task, true)

			require.NoError(t, tc.run(deps, tenant, inst, task, assignment))

			evs := world.eventsOfType(tc.eventType)
			require.Len(t, evs, 1)
			got := string(wkactPayloadFields(t, evs[0])["assignee_user_ids"])
			wantUser := assignment.UserID
			if tc.name == "reassigned" {
				wantUser = user
			}
			assert.JSONEq(t, `["`+wantUser.String()+`"]`, got)
			world.requireAllCallsForTenant(t, tenant)
		})
	}
}
