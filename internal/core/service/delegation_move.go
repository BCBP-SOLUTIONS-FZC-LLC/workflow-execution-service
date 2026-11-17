package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

const (
	delegationTagPrefix      = "delegation:"
	delegationChainSeparator = ";"
)

// A delegated assignment's reason is the chain of delegations that carried the
// work to its holder, oldest first, each hop written
// "delegation:<delegation id>:<delegator id>". A reason without such a hop was
// not created by a delegation.
func delegationHops(reason string) []string {
	var hops []string
	for _, segment := range strings.Split(reason, delegationChainSeparator) {
		if strings.HasPrefix(segment, delegationTagPrefix) {
			hops = append(hops, segment)
		}
	}
	return hops
}

func isDelegated(reason string) bool {
	return len(delegationHops(reason)) > 0
}

func reasonHasDelegation(reason, tag string) bool {
	return hopIndex(delegationHops(reason), tag) >= 0
}

func hopIndex(hops []string, tag string) int {
	for i, hop := range hops {
		if strings.HasPrefix(hop, tag) {
			return i
		}
	}
	return -1
}

// reasonBeforeDelegation is the chain of delegations that preceded the named
// one, which is what a row returned to that delegation's delegator carries.
func reasonBeforeDelegation(reason, tag string) string {
	hops := delegationHops(reason)
	i := hopIndex(hops, tag)
	if i < 0 {
		return ""
	}
	return strings.Join(hops[:i], delegationChainSeparator)
}

func delegationHop(delegationID, delegatorID uuid.UUID) string {
	return delegationReasonTag(delegationID) + ":" + delegatorID.String()
}

func appendDelegationHop(reason string, hop string) string {
	return strings.Join(append(delegationHops(reason), hop), delegationChainSeparator)
}

// parseDelegationHop returns the delegation and the user the hop took the work
// from; a hop written without a delegator has none.
func parseDelegationHop(hop string) (delegationID, delegatorID uuid.UUID, ok bool) {
	parts := strings.Split(hop, ":")
	if len(parts) != 3 {
		return uuid.Nil, uuid.Nil, false
	}
	delegationID, err := uuid.Parse(parts[1])
	if err != nil {
		return uuid.Nil, uuid.Nil, false
	}
	delegatorID, err = uuid.Parse(parts[2])
	return delegationID, delegatorID, err == nil
}

// hopToUnwind is the hop a cancel takes back: the named delegation's, or the most
// recent one when none is named.
func hopToUnwind(reason string, delegationID *uuid.UUID) (string, bool) {
	hops := delegationHops(reason)
	if len(hops) == 0 {
		return "", false
	}
	if delegationID == nil {
		return hops[len(hops)-1], true
	}
	i := hopIndex(hops, delegationReasonTag(*delegationID))
	if i < 0 {
		return "", false
	}
	return hops[i], true
}

func activeAssignmentByID(active []*domain.TaskAssignment, id uuid.UUID) bool {
	for _, a := range active {
		if a.ID == id {
			return true
		}
	}
	return false
}

func activeAssignmentOfUser(active []*domain.TaskAssignment, userID uuid.UUID) *domain.TaskAssignment {
	for _, a := range active {
		if a.UserID == userID {
			return a
		}
	}
	return nil
}

// vacateIfActive vacates the assignment inside the caller's transaction, after
// re-reading the task's active assignments there. The re-read is not locked, so
// the vacate itself only matches a row that is still active: an assignee who
// completes the task in between ends the row first. It returns the task's
// active assignments and false, with nothing written, when the assignment has
// already ended.
func vacateIfActive(ctx context.Context, repo port.TaskAssignmentRepository, tenantID uuid.UUID, a *domain.TaskAssignment) ([]*domain.TaskAssignment, bool, error) {
	active, err := repo.ListActiveByTask(ctx, tenantID, a.TaskID)
	if err != nil {
		return nil, false, err
	}
	if !activeAssignmentByID(active, a.ID) {
		return nil, false, nil
	}
	if _, err := repo.VacateActive(ctx, tenantID, a.ID); err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil, false, nil
		}
		return nil, false, err
	}
	return active, true, nil
}

// handOver gives the vacated assignment's task to newUserID inside the caller's
// transaction. A user who already holds the task keeps that one row, and
// inherits the lead flag; inserting a second row would be a unique violation,
// which aborts the whole transaction. The lead move is stamped with the task's
// version as read here, inside the transaction: the caller's own read came earlier,
// and a claim or completion in between would fail the whole move. Callers never
// hand a task to the user who just lost it: that move changes nothing, and the
// vacate would take the task away.
func handOver(ctx context.Context, repo port.TaskAssignmentRepository, tasks port.TaskRepository, tenantID uuid.UUID, vacated *domain.TaskAssignment, active []*domain.TaskAssignment, task *domain.Task, newUserID uuid.UUID, reason string) error {
	if existing := activeAssignmentOfUser(active, newUserID); existing != nil {
		if !vacated.IsLead {
			return nil
		}
		current, err := tasks.GetByID(ctx, tenantID, task.ID)
		if err != nil {
			return err
		}
		_, err = repo.SetLead(ctx, tenantID, task.ID, existing.ID, current.RecordVersion)
		return err
	}
	assignedAt := time.Now().UTC()
	return repo.Create(ctx, &domain.TaskAssignment{
		ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: newUserID,
		Reason: reason, IsLead: vacated.IsLead, AssignedAt: &assignedAt,
	})
}

// enqueueTaskReassigned records the WorkflowTaskReassigned event of a delegation
// move in the caller's transaction.
func enqueueTaskReassigned(ctx context.Context, outbox port.OutboxRepository, validator port.EventValidator, inst *domain.Instance, task *domain.Task, oldUserID, newUserID uuid.UUID, delegationID *uuid.UUID) error {
	core := domain.CommonCore{WorkflowInstanceID: inst.ID, SubjectID: inst.SubjectID, BusinessKey: inst.BusinessKey, WorkflowVersionID: inst.WorkflowVersionID}
	taskCore := domain.TaskScopedCore{TaskID: task.ID, NodeKey: task.NodeKey, DepartmentID: task.DepartmentID, AssigneeUserIDs: []uuid.UUID{newUserID}}
	payload := domain.NewWorkflowTaskReassignedPayload(core, taskCore, &oldUserID, newUserID, domain.ReassignInitiatorDelegation, delegationID)
	subject := "instances/" + inst.ID.String() + "/tasks/" + task.ID.String()
	envelope, err := BuildEnvelope(ctx, validator, domain.EventWorkflowTaskReassigned, inst.TenantID.String(), subject, "", payload)
	if err != nil {
		return err
	}
	return outbox.Enqueue(ctx, envelope)
}

// recordWorkflowSignal records a signal for the instance in the caller's
// transaction; the outbox relay delivers it to Temporal after the commit.
func recordWorkflowSignal(ctx context.Context, outbox port.WorkflowSignalOutbox, inst *domain.Instance, name string, wire any) error {
	payload, err := json.Marshal(wire)
	if err != nil {
		return fmt.Errorf("marshal %s signal: %w", name, err)
	}
	return outbox.EnqueueSignal(ctx, port.WorkflowSignal{
		TenantID: inst.TenantID, InstanceID: inst.ID, TemporalWorkflowID: inst.TemporalWorkflowID,
		Name: name, Payload: payload,
	})
}
