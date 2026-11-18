package service_test

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

var (
	errW11bTaggedList   = errors.New("w11b: listing the tagged assignments failed")
	errW11bEligibility  = errors.New("w11b: eligibility batch failed")
	errW11bEligibleItem = errors.New("w11b: eligibility of one pair failed")
	errW11bAvailability = errors.New("w11b: availability read failed")
)

type w11bTaggedListCall struct {
	tenantID uuid.UUID
	tag      string
}

// w11bTaggedListing records every listing by reason tag and can fail it.
type w11bTaggedListing struct {
	*fakeAssignmentRepo
	err   error
	calls []w11bTaggedListCall
}

func (r *w11bTaggedListing) ListActiveByReasonTag(ctx context.Context, tenantID uuid.UUID, tag string) ([]*domain.TaskAssignment, error) {
	r.calls = append(r.calls, w11bTaggedListCall{tenantID: tenantID, tag: tag})
	if r.err != nil {
		return nil, r.err
	}
	return r.fakeAssignmentRepo.ListActiveByReasonTag(ctx, tenantID, tag)
}

// w11bAvailabilityReads records every availability read and can fail it.
type w11bAvailabilityReads struct {
	*fakeUserAvailability
	err   error
	reads []w11bUserListCall
}

func (a *w11bAvailabilityReads) Get(ctx context.Context, tenantID, userID uuid.UUID) (string, bool, error) {
	a.reads = append(a.reads, w11bUserListCall{tenantID: tenantID, userID: userID})
	if a.err != nil {
		return "", false, a.err
	}
	return a.fakeUserAvailability.Get(ctx, tenantID, userID)
}

func TestDelegationReroute_DepartmentScope_OnlyATaskOfAPlanDepartmentMoves(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		nodeKey   string
		wantMoved bool
	}{
		{name: "the task's department is not in the plan", nodeKey: "legal/reviewer"},
		{name: "control: the task's department is the plan's department", nodeKey: "finance/reviewer", wantMoved: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := dlgfixNewRecon()
			tx := &dlgfixTx{repo: h.assignments, outbox: h.outbox}
			h.svc.Transactor = tx
			ended := newFakeEndedDelegations()
			h.svc.Ended = ended
			tasks := &w11bRecordingTasks{fakeTaskRepo: h.tasks}
			instances := &w11bReadFailInstances{fakeInstanceRepo: h.instances, failing: map[uuid.UUID]error{}}
			assignments := &w11bListingAssignments{fakeAssignmentRepo: h.assignments}
			h.svc.Tasks, h.svc.Instances, h.svc.Assignments = tasks, instances, assignments
			var eligibilityTenants []uuid.UUID
			var asked []port.EligibilityCheckRequest
			h.eligibility.batchCheck = func(_ context.Context, tenantID uuid.UUID, reqs []port.EligibilityCheckRequest) ([]port.EligibilityResult, error) {
				eligibilityTenants = append(eligibilityTenants, tenantID)
				asked = append(asked, reqs...)
				answers := make([]port.EligibilityResult, len(reqs))
				for i := range answers {
					answers[i].Eligible = true
				}
				return answers, nil
			}
			h.eligibility.check = func(_ context.Context, tenantID, _, _ uuid.UUID, _ string) (bool, error) {
				eligibilityTenants = append(eligibilityTenants, tenantID)
				return true, nil
			}
			row := h.seed(t, "reviewer")
			scopeID := uuid.New()
			h.definitions.byVersion[row.versionID] = publishedCompiledWorkflow(uuid.New(), row.versionID, compiledPlanJSONWithDept(t, dsl.DepartmentDef{
				ID: "finance", IAMDepartmentID: scopeID.String(),
				Stages: []dsl.StageDef{{Type: "userTask", NodeID: "reviewer", Role: "reviewer"}},
			}))
			row.task.DepartmentID = scopeID
			row.task.NodeKey = tc.nodeKey
			scope := scopeID.String()

			err := h.svc.Reroute(context.Background(), port.DelegationRerouteInput{
				TenantID: h.tenantID, DelegationID: uuid.New(), DelegatorID: h.delegator, DelegateID: h.delegate,
				Scope: "department", ScopeID: &scope,
			})

			require.NoError(t, err)
			assert.Equal(t, []w11bUserListCall{{tenantID: h.tenantID, userID: h.delegator}}, assignments.calls)
			assert.Equal(t, []w11bIDCall{{tenantID: h.tenantID, id: row.task.ID}}, tasks.gets)
			assert.Equal(t, []w11bInstanceRead{{tenantID: h.tenantID, id: row.instance.ID}}, instances.reads)
			if tc.wantMoved {
				assert.False(t, row.assignment.IsActive, "the control row is moved, so the other case stays put only because of its department")
				assert.Equal(t, 1, w11bActiveRowsOf(h, h.delegate))
				assert.Equal(t, []uuid.UUID{h.tenantID}, eligibilityTenants)
				assert.Equal(t, []port.EligibilityCheckRequest{{NewUserID: h.delegate, DepartmentID: scopeID, RequiredLevel: "reviewer"}}, asked)
				require.Len(t, h.outbox.signals, 1)
				assert.Equal(t, port.SignalInstanceReassign, h.outbox.signals[0].SignalName)
				assert.Equal(t, 1, tx.commits)
				return
			}
			h.requireNothingMoved(t, row)
			assert.Empty(t, eligibilityTenants, "nothing is left after the scope filter, so no eligibility call")
			assert.Empty(t, h.locker.keys, "no lock is taken for a move that does not happen")
			assert.Zero(t, tx.commits, "no write")
			assert.Zero(t, tx.rollbacks)
			assert.Len(t, h.assignments.byID, 1, "no assignment was created")
			assert.Empty(t, ended.marked)
		})
	}
}

func TestDelegationReverse_ListingOrEligibilityFails_FailsTheEventAndChangesNothing(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		setup func(h *dlgfixRecon, listing *w11bTaggedListing, asked *[]port.EligibilityCheckRequest, askedTenants *[]uuid.UUID)
		cause error
		asked bool
	}{
		{
			name: "listing the tagged assignments fails",
			setup: func(_ *dlgfixRecon, listing *w11bTaggedListing, _ *[]port.EligibilityCheckRequest, _ *[]uuid.UUID) {
				listing.err = errW11bTaggedList
			},
			cause: errW11bTaggedList,
		},
		{
			name: "the eligibility batch fails",
			setup: func(h *dlgfixRecon, _ *w11bTaggedListing, asked *[]port.EligibilityCheckRequest, askedTenants *[]uuid.UUID) {
				h.eligibility.batchCheck = func(_ context.Context, tenantID uuid.UUID, reqs []port.EligibilityCheckRequest) ([]port.EligibilityResult, error) {
					*askedTenants = append(*askedTenants, tenantID)
					*asked = append(*asked, reqs...)
					return nil, errW11bEligibility
				}
			},
			cause: errW11bEligibility,
			asked: true,
		},
		{
			name: "the eligibility of one pair fails",
			setup: func(h *dlgfixRecon, _ *w11bTaggedListing, asked *[]port.EligibilityCheckRequest, askedTenants *[]uuid.UUID) {
				h.eligibility.batchCheck = func(_ context.Context, tenantID uuid.UUID, reqs []port.EligibilityCheckRequest) ([]port.EligibilityResult, error) {
					*askedTenants = append(*askedTenants, tenantID)
					*asked = append(*asked, reqs...)
					return []port.EligibilityResult{{Err: errW11bEligibleItem}}, nil
				}
			},
			cause: errW11bEligibleItem,
			asked: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := dlgfixNewRecon()
			tx := &dlgfixTx{repo: h.assignments, outbox: h.outbox}
			h.svc.Transactor = tx
			listing := &w11bTaggedListing{fakeAssignmentRepo: h.assignments}
			tasks := &w11bRecordingTasks{fakeTaskRepo: h.tasks}
			instances := &w11bReadFailInstances{fakeInstanceRepo: h.instances, failing: map[uuid.UUID]error{}}
			h.svc.Assignments, h.svc.Tasks, h.svc.Instances = listing, tasks, instances
			delegationID := uuid.New()
			row := h.seed(t, "reviewer")
			row.assignment.UserID = h.delegate
			row.assignment.Reason = "delegation:" + delegationID.String()
			var asked []port.EligibilityCheckRequest
			var askedTenants []uuid.UUID
			tc.setup(h, listing, &asked, &askedTenants)

			err := h.svc.Reverse(context.Background(), port.DelegationReversalInput{
				TenantID: h.tenantID, DelegationID: delegationID, DelegatorID: h.delegator, DelegateID: h.delegate, EndedReason: "expired",
			})

			require.ErrorIs(t, err, tc.cause, "the event fails, so the consumer redelivers it")
			assert.Equal(t, []w11bTaggedListCall{{tenantID: h.tenantID, tag: "delegation:" + delegationID.String()}}, listing.calls)
			if !tc.asked {
				assert.Empty(t, tasks.gets, "the listing failed before any row was read")
				assert.Empty(t, instances.reads)
			}
			if tc.asked {
				assert.Equal(t, []w11bIDCall{{tenantID: h.tenantID, id: row.task.ID}}, tasks.gets)
				assert.Equal(t, []w11bInstanceRead{{tenantID: h.tenantID, id: row.instance.ID}}, instances.reads)
				assert.Equal(t, []uuid.UUID{h.tenantID}, askedTenants)
				assert.Equal(t, []port.EligibilityCheckRequest{{NewUserID: h.delegator, DepartmentID: row.task.DepartmentID, RequiredLevel: "reviewer"}}, asked,
					"the delegator is asked about the task's department and level")
			}
			assert.True(t, row.assignment.IsActive)
			assert.Equal(t, h.delegate, row.assignment.UserID, "the row stays with the delegate")
			assert.Len(t, h.assignments.byID, 1, "the delegator was given nothing")
			assert.Empty(t, h.outbox.enqueued)
			assert.Empty(t, h.outbox.signals)
			assert.Empty(t, h.temporal.signals)
			assert.Zero(t, tx.commits)
			assert.Zero(t, tx.rollbacks, "the failure comes before any transaction opens")
		})
	}
}

func TestDelegationReverse_AvailabilityOrSignalRecordingFails_RollsBackAndTheRedeliveryCommits(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name          string
		outOfOffice   bool
		status        domain.InstanceStatus
		failAvail     bool
		failSignal    string
		wantAttempts  []string
		wantRollbacks int
		wantAfter     []string
	}{
		{
			name: "the availability read fails", status: domain.InstanceStatusRunning, failAvail: true,
			wantRollbacks: 0, wantAfter: []string{port.SignalInstanceReassign},
		},
		{
			name: "the pause of an out-of-office delegator cannot be recorded", outOfOffice: true, status: domain.InstanceStatusRunning,
			failSignal: port.SignalInstancePause, wantAttempts: []string{port.SignalInstancePause}, wantRollbacks: 1,
			wantAfter: []string{port.SignalInstancePause, port.SignalInstanceReassign},
		},
		{
			name: "the resume of an available delegator cannot be recorded", status: domain.InstanceStatusPaused,
			failSignal: port.SignalInstanceResume, wantAttempts: []string{port.SignalInstanceResume}, wantRollbacks: 1,
			wantAfter: []string{port.SignalInstanceResume, port.SignalInstanceReassign},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := dlgfixNewRecon()
			tx := &dlgfixTx{repo: h.assignments, outbox: h.outbox}
			h.svc.Transactor = tx
			avail := &w11bAvailabilityReads{fakeUserAvailability: h.store}
			h.svc.Availability = avail
			tasks := &w11bRecordingTasks{fakeTaskRepo: h.tasks}
			instances := &w11bReadFailInstances{fakeInstanceRepo: h.instances, failing: map[uuid.UUID]error{}}
			h.svc.Tasks, h.svc.Instances = tasks, instances
			signals := &w11bNamedSignalOutbox{fakeOutbox: h.outbox, failName: tc.failSignal, err: errW11bSignalRecord}
			h.svc.Signals = signals
			delegationID := uuid.New()
			row := h.seed(t, "reviewer")
			row.instance.Status = tc.status
			row.assignment.UserID = h.delegate
			row.assignment.Reason = "delegation:" + delegationID.String()
			if tc.outOfOffice {
				require.NoError(t, h.store.byRecord(h.tenantID, h.delegator, port.AvailabilityOOO))
			}
			if tc.failAvail {
				avail.err = errW11bAvailability
			}
			in := port.DelegationReversalInput{
				TenantID: h.tenantID, DelegationID: delegationID, DelegatorID: h.delegator, DelegateID: h.delegate, EndedReason: "expired",
			}

			err := h.svc.Reverse(context.Background(), in)

			if tc.failAvail {
				require.ErrorIs(t, err, errW11bAvailability)
			} else {
				require.ErrorIs(t, err, errW11bSignalRecord)
			}
			assert.Equal(t, []w11bUserListCall{{tenantID: h.tenantID, userID: h.delegator}}, avail.reads, "the delegator's availability is read in the delegation's tenant")
			assert.Equal(t, []w11bIDCall{{tenantID: h.tenantID, id: row.task.ID}}, tasks.gets)
			assert.Equal(t, []w11bInstanceRead{{tenantID: h.tenantID, id: row.instance.ID}}, instances.reads)
			assert.Equal(t, tc.wantAttempts, signals.attempts, "the failing signal is the one under test")
			assert.True(t, row.assignment.IsActive)
			assert.Equal(t, h.delegate, row.assignment.UserID, "the assignment stays with the delegate")
			assert.Len(t, h.assignments.byID, 1, "the delegator holds nothing")
			assert.Empty(t, h.outbox.signals, "no signal is committed")
			assert.Empty(t, h.outbox.enqueued)
			assert.Empty(t, h.temporal.signals, "no signal is sent directly")
			assert.Equal(t, tc.wantRollbacks, tx.rollbacks)
			assert.Zero(t, tx.commits)

			avail.err = nil
			signals.failName = ""
			signals.attempts = nil

			require.NoError(t, h.svc.Reverse(context.Background(), in), "the redelivered event goes through")

			assert.Equal(t, 1, tx.commits)
			assert.Equal(t, []w11bIDCall{{tenantID: h.tenantID, id: row.task.ID}, {tenantID: h.tenantID, id: row.task.ID}}, tasks.gets, "the redelivery reads the row again, in the same tenant")
			assert.Equal(t, []w11bInstanceRead{{tenantID: h.tenantID, id: row.instance.ID}, {tenantID: h.tenantID, id: row.instance.ID}}, instances.reads)
			assert.False(t, row.assignment.IsActive)
			assert.Equal(t, 1, w11bActiveRowsOf(h, h.delegator), "the delegator holds the task again")
			var names []string
			for _, s := range h.outbox.signals {
				names = append(names, s.SignalName)
			}
			assert.Equal(t, tc.wantAfter, names)
			assert.Empty(t, h.temporal.signals)
		})
	}
}

func w11bActiveRowsOf(h *dlgfixRecon, userID uuid.UUID) int {
	n := 0
	for _, a := range h.assignments.byID {
		if a.UserID == userID && a.IsActive {
			n++
		}
	}
	return n
}
