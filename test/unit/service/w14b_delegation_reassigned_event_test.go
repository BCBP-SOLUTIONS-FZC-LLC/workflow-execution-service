package service_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

func TestDelegationReconciler_MovedRowEventNamesTheTaskTheNewHolderAndTheDelegation(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		// holder and receiver pick the delegator or the delegate as the old and new holder of the row.
		reverse bool
	}{
		{name: "a start hands the row to the delegate"},
		{name: "an end hands the row back to the delegator", reverse: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			svc, instances, tasks, assignments, outbox, _, definitions, _ := newDelegationReconcilerHarness()
			tenantID, delegator, delegate, delegationID, deptID := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
			oldHolder, newHolder := delegator, delegate
			if tc.reverse {
				oldHolder, newHolder = delegate, delegator
			}
			inst, task, held := seedDelegationFixture(instances, tasks, assignments, definitions, tenantID, oldHolder, "biz-w14b", dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer"}, t)
			definitions.resp = publishedCompiledWorkflow(uuid.New(), inst.WorkflowVersionID, compiledPlanJSONWithDept(t, dsl.DepartmentDef{
				ID: "finance", IAMDepartmentID: deptID.String(), Stages: []dsl.StageDef{{Type: "userTask", NodeID: "review", Role: "reviewer"}},
			}))
			task.DepartmentID = deptID
			if tc.reverse {
				held.Reason = "delegation:" + delegationID.String()
			}

			var err error
			if tc.reverse {
				err = svc.Reverse(context.Background(), port.DelegationReversalInput{
					TenantID: tenantID, DelegationID: delegationID, DelegatorID: delegator, DelegateID: delegate, EndedReason: "expired",
				})
			} else {
				err = svc.Reroute(context.Background(), port.DelegationRerouteInput{
					TenantID: tenantID, DelegationID: delegationID, DelegatorID: delegator, DelegateID: delegate, Scope: "all",
				})
			}
			require.NoError(t, err)

			require.Len(t, outbox.enqueued, 1)
			env := outbox.enqueued[0]
			assert.Equal(t, domain.EventWorkflowTaskReassigned, env.Type)
			assert.Equal(t, "instances/"+inst.ID.String()+"/tasks/"+task.ID.String(), env.Subject)
			assert.Equal(t, tenantID.String(), env.TenantID)
			assert.Empty(t, env.Actor, "a delegation move has no acting user")
			assert.Equal(t, "1", env.SchemaVersion)

			var payload struct {
				WorkflowInstanceID uuid.UUID   `json:"workflow_instance_id"`
				TaskID             uuid.UUID   `json:"task_id"`
				NodeKey            string      `json:"node_key"`
				DepartmentID       uuid.UUID   `json:"department_id"`
				AssigneeUserIDs    []uuid.UUID `json:"assignee_user_ids"`
				OldUserID          uuid.UUID   `json:"old_user_id"`
				NewUserID          uuid.UUID   `json:"new_user_id"`
				Initiator          string      `json:"initiator"`
				DelegationID       uuid.UUID   `json:"delegation_id"`
			}
			require.NoError(t, json.Unmarshal(env.Payload, &payload))
			assert.Equal(t, inst.ID, payload.WorkflowInstanceID)
			assert.Equal(t, task.ID, payload.TaskID)
			assert.Equal(t, task.NodeKey, payload.NodeKey)
			assert.Equal(t, deptID, payload.DepartmentID)
			assert.Equal(t, []uuid.UUID{newHolder}, payload.AssigneeUserIDs, "the assignees are only the new holder")
			assert.Equal(t, oldHolder, payload.OldUserID)
			assert.Equal(t, newHolder, payload.NewUserID)
			assert.Equal(t, "delegation", payload.Initiator)
			assert.Equal(t, delegationID, payload.DelegationID)
		})
	}
}
