package temporal_test

import (
	"context"
	"testing"

	"github.com/google/uuid"

	outboundtemporal "github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/temporal"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

// w25bScene is one in-progress task with a lead and a co-assignee, plus the
// inputs of every activity that works on it.
type w25bScene struct {
	rig        *w25bRig
	deps       *outboundtemporal.Deps
	tenant     uuid.UUID
	admin      uuid.UUID
	newUser    uuid.UUID
	inst       *domain.Instance
	task       *domain.Task
	version    int64
	leadUser   uuid.UUID
	helperUser uuid.UUID
	lead       *domain.TaskAssignment
	helper     *domain.TaskAssignment
}

// w25bNewScene builds the scene. With withInstance false the instance row is
// absent, as when it was deleted after the task was created.
func w25bNewScene(t *testing.T, withInstance bool) *w25bScene {
	t.Helper()
	rig := w25bNewRig()
	s := &w25bScene{rig: rig, deps: w25bNewDeps(t, rig), tenant: uuid.New(), admin: uuid.New(), newUser: uuid.New(), leadUser: uuid.New(), helperUser: uuid.New()}
	s.inst = rig.addInstance(s.tenant, domain.InstanceStatusRunning)
	s.task = rig.addTask(s.tenant, s.inst, "sales/review", domain.TaskStatusInProgress)
	s.version = s.task.RecordVersion
	s.lead = rig.addAssignment(s.tenant, s.task, s.leadUser, true, true)
	s.helper = rig.addAssignment(s.tenant, s.task, s.helperUser, false, true)
	if !withInstance {
		delete(rig.instances, s.inst.ID)
	}
	return s
}

func (s *w25bScene) claim() error {
	return s.deps.ClaimAssignment(context.Background(), port.ClaimAssignmentInput{
		TenantID: s.tenant.String(), AssignmentID: s.helper.ID.String(), UserID: s.helperUser.String(),
	})
}

func (s *w25bScene) complete() error {
	_, err := s.deps.CompleteAssignment(context.Background(), port.CompleteAssignmentInput{
		TenantID: s.tenant.String(), TaskID: s.task.ID.String(), UserID: s.leadUser.String(), ResultJSON: `{"ok":true}`,
	})
	return err
}

func (s *w25bScene) deferTask() error {
	return s.deps.DeferTask(context.Background(), port.DeferTaskInput{
		TenantID: s.tenant.String(), TaskID: s.task.ID.String(), UserID: s.leadUser.String(), AssignmentID: s.lead.ID.String(),
		Reason: "needs rework", DeferredToNodeKey: "sales/draft",
	})
}

func (s *w25bScene) reassignInput(newUser uuid.UUID) port.ReassignAssignmentInput {
	return port.ReassignAssignmentInput{
		TenantID: s.tenant.String(), TaskID: s.task.ID.String(), OldUserID: s.leadUser.String(), NewUserID: newUser.String(),
		AdminUserID: s.admin.String(), RecordVersion: s.version,
	}
}

func (s *w25bScene) reassignToNewUser() error {
	return s.deps.ReassignAssignment(context.Background(), s.reassignInput(s.newUser))
}

func (s *w25bScene) reassignToHolder() error {
	return s.deps.ReassignAssignment(context.Background(), s.reassignInput(s.helperUser))
}

func (s *w25bScene) slaWarning() error {
	return s.deps.RecordSLAWarning(context.Background(), port.RecordSLAWarningInput{
		TenantID: s.tenant.String(), InstanceID: s.inst.ID.String(), TaskID: s.task.ID.String(), NodeKey: "sales/review",
	})
}

func (s *w25bScene) slaBreach() error {
	return s.deps.RecordSLABreach(context.Background(), port.RecordSLABreachInput{
		TenantID: s.tenant.String(), InstanceID: s.inst.ID.String(), TaskID: s.task.ID.String(), NodeKey: "sales/review",
	})
}

func (s *w25bScene) cancelInstance() error {
	return s.deps.CancelInstance(context.Background(), port.CancelInstanceInput{
		TenantID: s.tenant.String(), InstanceID: s.inst.ID.String(), AdminUserID: s.admin.String(),
	})
}

func (s *w25bScene) setInstanceStatus(status domain.InstanceStatus) error {
	return s.deps.UpdateInstanceStatus(context.Background(), port.UpdateInstanceStatusInput{
		TenantID: s.tenant.String(), InstanceID: s.inst.ID.String(), Status: status,
	})
}

func (s *w25bScene) pauseInstance() error {
	return s.deps.PauseInstance(context.Background(), port.PauseInstanceInput{
		TenantID: s.tenant.String(), InstanceID: s.inst.ID.String(), AdminUserID: s.admin.String(), Initiator: domain.InitiatorAdmin,
	})
}

func (s *w25bScene) resumeInstance() error {
	return s.deps.ResumeInstance(context.Background(), port.ResumeInstanceInput{
		TenantID: s.tenant.String(), InstanceID: s.inst.ID.String(), AdminUserID: s.admin.String(), Initiator: domain.InitiatorAdmin,
	})
}

func (s *w25bScene) forceRoute() error {
	return s.deps.RecordForceRoute(context.Background(), port.RecordForceRouteInput{
		TenantID: s.tenant.String(), InstanceID: s.inst.ID.String(), OldNodeKeys: []domain.NodeKey{domain.NodeKey(s.task.NodeKey)},
		TargetNodeID: "sales/approve", Direction: domain.ForceRouteDirectionForward, AdminUserID: s.admin.String(), RecordVersion: 5,
	})
}

func (s *w25bScene) setStatus(status domain.TaskStatus) error {
	return s.deps.UpdateTaskStatus(context.Background(), port.UpdateTaskStatusInput{
		TenantID: s.tenant.String(), TaskID: s.task.ID.String(), Status: status,
	})
}
