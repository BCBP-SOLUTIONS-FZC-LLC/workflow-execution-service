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
	errW13aListDown  = errors.New("assignment listing unavailable")
	errW13aEndedDown = errors.New("ended-delegation store unavailable")
)

type w13aDefinitionCall struct{ tenantID, versionID uuid.UUID }

// w13aDefinitions answers one compiled plan and records the tenant and version of every read.
type w13aDefinitions struct {
	resp  *port.CompiledWorkflow
	err   error
	calls []w13aDefinitionCall
}

func (d *w13aDefinitions) GetCompiledWorkflow(_ context.Context, tenantID, versionID uuid.UUID) (*port.CompiledWorkflow, error) {
	d.calls = append(d.calls, w13aDefinitionCall{tenantID, versionID})
	if d.err != nil {
		return nil, d.err
	}
	return d.resp, nil
}

type w13aEligibilityCall struct {
	tenantID, userID, departmentID uuid.UUID
	level                          string
}

// w13aEligibility finds every user eligible and records what it was asked.
type w13aEligibility struct {
	calls []w13aEligibilityCall
}

func (e *w13aEligibility) CheckEligibility(_ context.Context, tenantID, userID, departmentID uuid.UUID, level string) (bool, error) {
	e.calls = append(e.calls, w13aEligibilityCall{tenantID, userID, departmentID, level})
	return true, nil
}

func (e *w13aEligibility) CheckEligibilityBatch(context.Context, uuid.UUID, []port.EligibilityCheckRequest) ([]port.EligibilityResult, error) {
	return nil, errors.New("batch eligibility is not used by the delegate moves")
}

type w13aStep struct {
	op string
	id uuid.UUID
}

type w13aStepLog struct{ steps []w13aStep }

// w13aOrderedAssignments records the order of the vacates and creates on top of the recording of the shared wrapper.
type w13aOrderedAssignments struct {
	*csaAssignments
	log *w13aStepLog
}

func (r *w13aOrderedAssignments) VacateActive(ctx context.Context, tenantID, id uuid.UUID) (*domain.TaskAssignment, error) {
	r.log.steps = append(r.log.steps, w13aStep{"vacate", id})
	return r.csaAssignments.VacateActive(ctx, tenantID, id)
}

func (r *w13aOrderedAssignments) Create(ctx context.Context, a *domain.TaskAssignment) error {
	r.log.steps = append(r.log.steps, w13aStep{"create", a.UserID})
	return r.csaAssignments.Create(ctx, a)
}

type w13aClient struct {
	*csaClientHarness
	log         *w13aStepLog
	definitions *w13aDefinitions
	eligibility *w13aEligibility
	ended       *fakeEndedDelegations
	versionID   uuid.UUID
	department  uuid.UUID
}

func w13aNewClient(t *testing.T) *w13aClient {
	t.Helper()
	h := csaNewClientHarness()
	log := &w13aStepLog{}
	h.svc.Assignments = &w13aOrderedAssignments{csaAssignments: h.assignments, log: log}
	versionID, department := uuid.New(), uuid.New()
	c := &w13aClient{
		csaClientHarness: h,
		log:              log,
		definitions: &w13aDefinitions{resp: publishedCompiledWorkflow(uuid.New(), versionID,
			compiledPlanJSONWithDept(t, dsl.DepartmentDef{ID: "finance", IAMDepartmentID: department.String(),
				Stages: []dsl.StageDef{{Type: "userTask", NodeID: "review", Role: "reviewer"}}}))},
		eligibility: &w13aEligibility{},
		ended:       newFakeEndedDelegations(),
		versionID:   versionID,
		department:  department,
	}
	h.svc.Definitions = c.definitions
	h.svc.Eligibility = c.eligibility
	h.svc.Ended = c.ended
	return c
}

// hold gives the user one active assignment with the given reason on a task whose plan names a reviewer stage.
func (c *w13aClient) hold(tenantID, userID uuid.UUID, reason string, lead bool) csaHeld {
	held := c.seed(tenantID, userID, reason, domain.TaskStatusReady, domain.InstanceStatusRunning)
	held.instance.WorkflowVersionID = c.versionID
	held.task.NodeKey = "finance/review"
	held.task.DepartmentID = c.department
	held.assignment.IsLead = lead
	return held
}

// requireNoWrites fails when the call vacated, created, signalled or recorded an event.
func (c *w13aClient) requireNoWrites(t *testing.T) {
	t.Helper()
	assert.Empty(t, c.assignments.vacated, "no assignment is vacated")
	assert.Empty(t, c.assignments.created, "no assignment is created")
	assert.Empty(t, c.outbox.signals, "no workflow signal is recorded")
	assert.Empty(t, c.outbox.enqueued, "no event is recorded")
}

func TestWorkflowClient_ReassignDelegate_AFailedListingChangesAndReadsNothingElse(t *testing.T) {
	cases := []struct {
		name    string
		listErr error
	}{
		{"the listing of the old delegate's rows fails", errW13aListDown},
		{"control: the listing works and the same row is moved", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := w13aNewClient(t)
			tenantID, oldDelegate, newDelegate := uuid.New(), uuid.New(), uuid.New()
			held := h.hold(tenantID, oldDelegate, "delegation:"+uuid.NewString(), false)
			h.assignments.listActiveByUserErr = tc.listErr

			count, err := h.svc.ReassignDelegate(context.Background(), port.ReassignDelegateInput{
				TenantID: tenantID, OldDelegateID: oldDelegate, NewDelegateID: newDelegate,
			})

			assert.Equal(t, []csaUserCall{{tenantID, oldDelegate}}, h.assignments.listed, "the listing is read in the caller's tenant for the old delegate")
			if tc.listErr == nil {
				require.NoError(t, err)
				assert.Equal(t, 1, count)
				assert.NotEmpty(t, h.tasks.reads, "control: a working listing leads to reads")
				assert.Len(t, h.eligibility.calls, 1, "control: a working listing leads to an eligibility read")
				assert.Len(t, h.assignments.vacated, 1)
				assert.Len(t, h.assignments.created, 1)
				assert.Len(t, h.outbox.signals, 1)
				return
			}
			require.Error(t, err)
			assert.Same(t, tc.listErr, err, "the error comes back as it was, not wrapped")
			assert.Zero(t, count)
			assert.Empty(t, h.tasks.reads)
			assert.Empty(t, h.instances.reads)
			assert.Empty(t, h.definitions.calls)
			assert.Empty(t, h.eligibility.calls)
			h.requireNoWrites(t)
			assert.True(t, held.assignment.IsActive, "the old delegate keeps the row")
		})
	}
}

func TestWorkflowClient_ReassignDelegate_TheNewRowCopiesTheOldOneAndFollowsTheVacate(t *testing.T) {
	cases := []struct {
		name   string
		reason string
		lead   bool
	}{
		{"a lead row keeps its lead flag and a hop with the delegator", "delegation:" + uuid.NewString() + ":" + uuid.NewString(), true},
		{"a non-lead row stays a non-lead row", "delegation:" + uuid.NewString(), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := w13aNewClient(t)
			tenantID, oldDelegate, newDelegate := uuid.New(), uuid.New(), uuid.New()
			held := h.hold(tenantID, oldDelegate, tc.reason, tc.lead)

			count, err := h.svc.ReassignDelegate(context.Background(), port.ReassignDelegateInput{
				TenantID: tenantID, OldDelegateID: oldDelegate, NewDelegateID: newDelegate,
			})

			require.NoError(t, err)
			assert.Equal(t, 1, count)
			require.Len(t, h.assignments.creation, 1)
			created := h.assignments.creation[0]
			assert.Equal(t, tc.reason, created.Reason, "the reason is copied as it was")
			assert.Equal(t, tc.lead, created.IsLead, "the lead flag is copied")
			assert.NotEqual(t, uuid.Nil, created.ID)
			assert.NotEqual(t, held.assignment.ID, created.ID, "the new row has a fresh id")
			assert.Equal(t, tenantID, created.TenantID)
			assert.Equal(t, held.task.ID, created.TaskID)
			assert.Equal(t, newDelegate, created.UserID)
			assert.True(t, created.IsActive)
			assert.False(t, held.assignment.IsActive)
			assert.Equal(t, []csaIDCall{{tenantID, held.assignment.ID}}, h.assignments.vacated)
			assert.Equal(t, []w13aStep{{"vacate", held.assignment.ID}, {"create", newDelegate}}, h.log.steps, "the old row is vacated before the new one is created")
			assert.Equal(t, []w13aEligibilityCall{{tenantID, newDelegate, h.department, "reviewer"}}, h.eligibility.calls)
			assert.Equal(t, []w13aDefinitionCall{{tenantID, h.versionID}}, h.definitions.calls)
		})
	}
}

func TestWorkflowClient_CancelByDelegate_ARefusedListingOrAnEmptyMatchVacatesNothing(t *testing.T) {
	const (
		heldNone   = "none"
		heldOwn    = "the named delegation's row"
		heldOther  = "another delegation's row"
		heldManual = "a manual assignment"
	)
	cases := []struct {
		name         string
		named        bool
		listErr      error
		endErr       error
		held         string
		wantErr      error
		wantVacated  int
		wantNamedEnd bool
		wantAllEnd   bool
	}{
		{name: "the listing fails, no delegation named", listErr: errW13aListDown, held: heldOwn, wantErr: errW13aListDown},
		{name: "the listing fails, a delegation named", named: true, listErr: errW13aListDown, held: heldOwn, wantErr: errW13aListDown},
		{name: "nothing held, no delegation named: every delegation of the delegate is recorded as ended", held: heldNone, wantAllEnd: true},
		{name: "nothing held, a delegation named: that delegation is recorded as ended", named: true, held: heldNone, wantNamedEnd: true},
		{name: "only another delegation's row held, a delegation named", named: true, held: heldOther, wantNamedEnd: true},
		{name: "only a manual assignment held, no delegation named", held: heldManual, wantAllEnd: true},
		{name: "the end of a named delegation cannot be recorded", named: true, endErr: errW13aEndedDown, held: heldNone, wantErr: errW13aEndedDown, wantNamedEnd: false},
		{name: "control: the named delegation's row is held, so it is vacated and the end recorded", named: true, held: heldOwn, wantVacated: 1, wantNamedEnd: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := w13aNewClient(t)
			tenantID, delegate, delegationID := uuid.New(), uuid.New(), uuid.New()
			h.ended.markErr = tc.endErr
			h.assignments.listActiveByUserErr = tc.listErr
			reasons := map[string]string{
				heldOwn:    "delegation:" + delegationID.String(),
				heldOther:  "delegation:" + uuid.NewString(),
				heldManual: "manual-assign",
			}
			var held *csaHeld
			if reason, ok := reasons[tc.held]; ok {
				row := h.hold(tenantID, delegate, reason, false)
				held = &row
			}
			in := port.CancelByDelegateInput{TenantID: tenantID, DelegateUserID: delegate}
			if tc.named {
				in.DelegationID = &delegationID
			}

			count, err := h.svc.CancelByDelegate(context.Background(), in)

			assert.Equal(t, []csaUserCall{{tenantID, delegate}}, h.assignments.listed)
			assert.Equal(t, tc.wantVacated, count)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
			if tc.wantNamedEnd {
				assert.Equal(t, []endedDelegationKey{{tenantID, delegationID}}, h.ended.marked)
			} else {
				assert.Empty(t, h.ended.marked)
			}
			endedAt, delegateEnded := h.ended.delegates[endedDelegateKey{tenantID, delegate}]
			assert.Equal(t, tc.wantAllEnd, delegateEnded, "the delegate's own end is recorded only when no delegation is named")
			if tc.wantAllEnd {
				assert.False(t, endedAt.IsZero())
			}
			if tc.wantVacated > 0 {
				assert.Len(t, h.assignments.vacated, tc.wantVacated)
				assert.False(t, held.assignment.IsActive)
				return
			}
			h.requireNoWrites(t)
			assert.Empty(t, h.tasks.reads, "a refused or empty call reads no task")
			assert.Empty(t, h.instances.reads)
			if held != nil {
				assert.True(t, held.assignment.IsActive, "the row stays with the delegate")
			}
		})
	}
}

func TestWorkflowClient_DelegateImpact_AFailedListingOrAnEmptyMatchGivesAnEmptyAnswer(t *testing.T) {
	cases := []struct {
		name      string
		named     bool
		listErr   error
		held      string
		wantCount int
	}{
		{name: "the listing fails", listErr: errW13aListDown, held: "own"},
		{name: "the listing fails for a named delegation", named: true, listErr: errW13aListDown, held: "own"},
		{name: "nothing is held", held: "none"},
		{name: "only another delegation's row is held under a named delegation", named: true, held: "other"},
		{name: "control: the named delegation's row is held", named: true, held: "own", wantCount: 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := w13aNewClient(t)
			tenantID, delegate, delegationID := uuid.New(), uuid.New(), uuid.New()
			h.assignments.listActiveByUserErr = tc.listErr
			var held *csaHeld
			switch tc.held {
			case "own":
				row := h.hold(tenantID, delegate, "delegation:"+delegationID.String(), false)
				held = &row
			case "other":
				row := h.hold(tenantID, delegate, "delegation:"+uuid.NewString(), false)
				held = &row
			}
			in := port.DelegateImpactInput{TenantID: tenantID, DelegateUserID: delegate, Page: port.Page{Limit: 10}}
			if tc.named {
				in.DelegationID = &delegationID
			}

			result, err := h.svc.DelegateImpact(context.Background(), in)

			assert.Equal(t, []csaUserCall{{tenantID, delegate}}, h.assignments.listed)
			if tc.listErr != nil {
				require.Error(t, err)
				assert.Same(t, tc.listErr, err, "the error comes back as it was, not wrapped")
				assert.Equal(t, port.DelegateImpactResult{}, result)
				assert.Empty(t, h.tasks.reads)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantCount, result.ActiveAssignments)
			assert.Equal(t, tc.wantCount, result.ActiveWorkflows)
			assert.Empty(t, result.WorkflowIDs.NextCursor)
			if tc.wantCount > 0 {
				assert.Equal(t, []uuid.UUID{held.instance.ID}, result.WorkflowIDs.Items)
				assert.Equal(t, []csaIDCall{{tenantID, held.task.ID}}, h.tasks.reads)
				return
			}
			assert.NotNil(t, result.WorkflowIDs.Items, "an empty match still answers an empty list, not none")
			assert.Empty(t, result.WorkflowIDs.Items)
			assert.Empty(t, h.tasks.reads, "no task is read for an empty match")
			h.requireNoWrites(t)
		})
	}
}
