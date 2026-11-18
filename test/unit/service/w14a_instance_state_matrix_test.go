package service_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

const (
	w14aWaitKey       = "finance/Receive_Bid"
	w14aRecordVersion = int64(3)
)

type w14aInstanceRead struct {
	tenantID, instanceID uuid.UUID
}

// w14aInstances records the tenant and id of every instance read.
type w14aInstances struct {
	port.InstanceRepository
	reads []w14aInstanceRead
}

func (r *w14aInstances) GetByID(ctx context.Context, tenantID, id uuid.UUID) (*domain.Instance, error) {
	r.reads = append(r.reads, w14aInstanceRead{tenantID, id})
	return r.InstanceRepository.GetByID(ctx, tenantID, id)
}

type w14aStateRig struct {
	svc       *service.InstanceService
	temporal  *fakeTemporalClient
	instances *w14aInstances
	tasks     *w14aTasks
	plans     *w14aDefinitions
	inst      *domain.Instance
	actor     uuid.UUID
}

// newW14aStateRig is an instance in status with a one-stage plan (finance/review) and an outside wait
// (finance/Receive_Bid) the workflow reports as waiting, and one completed task so a force-back has a step to return to.
func newW14aStateRig(t *testing.T, status domain.InstanceStatus) *w14aStateRig {
	t.Helper()
	svc, instances, tasks, _, _, temporal, definitions, _ := newInstanceServiceHarness()
	tenantID, versionID := uuid.New(), uuid.New()
	definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t,
		dsl.StageDef{Type: "userTask", NodeID: "review"},
		dsl.StageDef{Type: "receive_task", NodeID: "Receive_Bid", Extras: map[string]string{"message": "bid-received"}},
	))
	inst := &domain.Instance{
		ID: uuid.New(), TenantID: tenantID, WorkflowVersionID: versionID, Status: status,
		RecordVersion: w14aRecordVersion, TemporalWorkflowID: "tenant:biz",
	}
	instances.byID[inst.ID] = inst
	done := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: inst.ID, NodeKey: "finance/review", Status: domain.TaskStatusCompleted}
	tasks.byID[done.ID] = done
	temporal.queryFunc = func(_ context.Context, workflowID, queryType string, out any) error {
		assert.Equal(t, inst.TemporalWorkflowID, workflowID)
		assert.Equal(t, port.QueryWaitingExternal, queryType)
		return json.Unmarshal([]byte(`["`+w14aWaitKey+`"]`), out)
	}
	recorder := &w14aInstances{InstanceRepository: instances}
	taskReads := &w14aTasks{TaskRepository: tasks}
	planReads := &w14aDefinitions{DefinitionServiceClient: definitions}
	svc.Instances, svc.Tasks, svc.Definitions = recorder, taskReads, planReads
	return &w14aStateRig{svc: svc, temporal: temporal, instances: recorder, tasks: taskReads, plans: planReads, inst: inst, actor: uuid.New()}
}

// requireReadsForThisInstance checks that every plan fetch and task listing the call made names this instance's
// tenant, version and id, and that the ones the call must make were made.
func (r *w14aStateRig) requireReadsForThisInstance(t *testing.T, wantPlan, wantTasks bool) {
	t.Helper()
	if wantPlan {
		require.NotEmpty(t, r.plans.reads, "the call reads the plan")
	}
	for _, read := range r.plans.reads {
		assert.Equal(t, w14aPlanRead{r.inst.TenantID, r.inst.WorkflowVersionID}, read)
	}
	if wantTasks {
		require.NotEmpty(t, r.tasks.listings, "the call lists the instance's tasks")
	}
	for _, listing := range r.tasks.listings {
		assert.Equal(t, w14aTaskListing{r.inst.TenantID, r.inst.ID}, listing)
	}
}

func (r *w14aStateRig) requireReadOnlyThisInstance(t *testing.T) {
	t.Helper()
	require.NotEmpty(t, r.instances.reads)
	for _, read := range r.instances.reads {
		assert.Equal(t, w14aInstanceRead{r.inst.TenantID, r.inst.ID}, read)
	}
}

type w14aSignalOp struct {
	name       string
	signalName string
	allowed    []domain.InstanceStatus
	call       func(ctx context.Context, r *w14aStateRig) error
	// readsPlan is true when an allowed call reads the plan; listsTasks says, for a status, whether it lists tasks.
	readsPlan  bool
	listsTasks func(domain.InstanceStatus) bool
}

func w14aNever(domain.InstanceStatus) bool { return false }

func w14aWhenRunning(status domain.InstanceStatus) bool {
	return status == domain.InstanceStatusRunning
}

func w14aSignalOps() []w14aSignalOp {
	rv := w14aRecordVersion
	return []w14aSignalOp{
		{"pause", port.SignalInstancePause, []domain.InstanceStatus{domain.InstanceStatusRunning, domain.InstanceStatusDegraded},
			func(ctx context.Context, r *w14aStateRig) error {
				return r.svc.Pause(ctx, r.inst.TenantID, r.inst.ID, r.actor, "why", rv)
			}, false, w14aNever},
		{"resume", port.SignalInstanceResume, []domain.InstanceStatus{domain.InstanceStatusPaused, domain.InstanceStatusDegraded},
			func(ctx context.Context, r *w14aStateRig) error {
				return r.svc.Resume(ctx, r.inst.TenantID, r.inst.ID, r.actor, rv)
			}, false, w14aNever},
		{"cancel", port.SignalInstanceCancel, []domain.InstanceStatus{domain.InstanceStatusRunning, domain.InstanceStatusPaused, domain.InstanceStatusDegraded},
			func(ctx context.Context, r *w14aStateRig) error {
				return r.svc.Cancel(ctx, r.inst.TenantID, r.inst.ID, r.actor, "why", rv)
			}, false, w14aNever},
		{"force-forward", port.SignalInstanceForceFwd, []domain.InstanceStatus{domain.InstanceStatusRunning, domain.InstanceStatusDegraded},
			func(ctx context.Context, r *w14aStateRig) error {
				return r.svc.ForceForward(ctx, r.inst.TenantID, r.inst.ID, r.actor, "finance/review", rv)
			}, true, w14aWhenRunning},
		{"force-back", port.SignalInstanceForceBack, []domain.InstanceStatus{domain.InstanceStatusRunning, domain.InstanceStatusDegraded},
			func(ctx context.Context, r *w14aStateRig) error {
				return r.svc.ForceBack(ctx, r.inst.TenantID, r.inst.ID, r.actor, "finance/review", rv)
			}, false, w14aWhenRunning},
		{"outside answer", port.SignalInstanceExternalAnswer, []domain.InstanceStatus{domain.InstanceStatusRunning, domain.InstanceStatusDegraded},
			func(ctx context.Context, r *w14aStateRig) error {
				return r.svc.AnswerOutsideWait(ctx, r.inst.TenantID, r.inst.ID, port.ReadScope{CallerUserID: r.actor, IsAdmin: true}, w14aWaitKey, nil)
			}, true, w14aNever},
	}
}

func TestInstanceService_SignalsByStatus_EachSignalIsAllowedOnlyFromItsStates(t *testing.T) {
	statuses := []domain.InstanceStatus{domain.InstanceStatusRunning, domain.InstanceStatusPaused, domain.InstanceStatusDegraded}
	for _, op := range w14aSignalOps() {
		for _, status := range statuses {
			allowed := false
			for _, a := range op.allowed {
				allowed = allowed || a == status
			}
			t.Run(op.name+" from "+string(status), func(t *testing.T) {
				rig := newW14aStateRig(t, status)

				err := op.call(context.Background(), rig)

				rig.requireReadOnlyThisInstance(t)
				if !allowed {
					require.ErrorIs(t, err, port.ErrInvalidInstanceState)
					assert.Empty(t, rig.temporal.signals, "a refused signal is never sent")
					rig.requireReadsForThisInstance(t, false, false)
					return
				}
				require.NoError(t, err)
				rig.requireReadsForThisInstance(t, op.readsPlan, op.listsTasks(status))
				require.Len(t, rig.temporal.signals, 1)
				assert.Equal(t, op.signalName, rig.temporal.signals[0].SignalName)
				assert.Equal(t, rig.inst.ID, rig.temporal.signals[0].InstanceID)
				assert.Equal(t, rig.inst.TemporalWorkflowID, rig.temporal.signals[0].TemporalWorkflowID)
			})
		}
	}
}

func TestInstanceService_SignalsOnAFinishedInstance_AreRefusedAsAlreadyTerminal(t *testing.T) {
	finished := []domain.InstanceStatus{domain.InstanceStatusCompleted, domain.InstanceStatusTerminated, domain.InstanceStatusFailed}
	for _, op := range w14aSignalOps() {
		for _, status := range finished {
			t.Run(op.name+" on a "+string(status)+" instance", func(t *testing.T) {
				rig := newW14aStateRig(t, status)

				err := op.call(context.Background(), rig)

				require.ErrorIs(t, err, port.ErrInstanceAlreadyTerminal)
				assert.NotErrorIs(t, err, port.ErrInvalidInstanceState, "a finished instance is not merely in the wrong state")
				assert.Empty(t, rig.temporal.signals)
				rig.requireReadOnlyThisInstance(t)
				rig.requireReadsForThisInstance(t, false, false)
			})
		}
	}
}
