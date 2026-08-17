package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/pkg/dsl"
)

// compiledPlanJSON builds a minimal, valid CompiledCollaboration JSON fixture
// with one main plan, one department ("finance"), and the given stages.
func compiledPlanJSON(t *testing.T, stages ...dsl.StageDef) string {
	t.Helper()
	collab := dsl.CompiledCollaboration{
		MainPlan: "main",
		Plans: []*dsl.CompiledPlan{
			{
				Name:      "main",
				TaskQueue: "wf-queue-default",
				Departments: []dsl.DepartmentDef{
					{ID: "finance", Label: "Finance", Stages: stages},
				},
			},
		},
	}
	b, err := json.Marshal(collab)
	require.NoError(t, err)
	return string(b)
}

func newInstanceServiceHarness() (*service.InstanceService, *fakeInstanceRepo, *fakeTaskRepo, *fakeAssignmentRepo, *fakeOutbox, *fakeTemporalClient, *fakeDefinitionClient, *fakeEligibilityChecker) {
	instances := newFakeInstanceRepo()
	tasks := newFakeTaskRepo()
	assignments := newFakeAssignmentRepo()
	outbox := &fakeOutbox{}
	temporal := &fakeTemporalClient{}
	definitions := &fakeDefinitionClient{}
	eligibility := &fakeEligibilityChecker{}

	svc := &service.InstanceService{
		Instances:   instances,
		Tasks:       tasks,
		Assignments: assignments,
		Outbox:      outbox,
		Transactor:  fakeTransactor{},
		Temporal:    temporal,
		Definitions: definitions,
		Eligibility: eligibility,
		Validator:   noopValidator{},
	}
	return svc, instances, tasks, assignments, outbox, temporal, definitions, eligibility
}

func publishedCompiledWorkflow(workflowID, versionID uuid.UUID, planJSON string) *port.CompiledWorkflow {
	return &port.CompiledWorkflow{
		WorkflowID: workflowID, VersionID: versionID, VersionNumber: 1,
		Status: "PUBLISHED", IsValid: true, CompiledPlanJSON: planJSON,
	}
}

func TestInstanceService_Start(t *testing.T) {
	t.Run("success with no default assignees", func(t *testing.T) {
		svc, instances, _, _, outbox, temporal, definitions, _ := newInstanceServiceHarness()
		workflowID, versionID, tenantID := uuid.New(), uuid.New(), uuid.New()
		definitions.resp = publishedCompiledWorkflow(workflowID, versionID, compiledPlanJSON(t, dsl.StageDef{Type: "userTask", NodeID: "review"}))

		got, err := svc.Start(context.Background(), port.StartInstanceInput{
			TenantID: tenantID, WorkflowVersionID: versionID, BusinessKey: "TND-001",
			ContextJSON: json.RawMessage(`{}`), StartedByUserID: uuid.New(),
		})
		require.NoError(t, err)
		assert.Equal(t, "TND-001", got.BusinessKey)
		assert.Equal(t, workflowID, got.WorkflowID)
		assert.Equal(t, port.InstanceStatusRunning, got.Status)
		assert.Equal(t, tenantID.String()+":TND-001", got.TemporalWorkflowID)
		assert.Len(t, instances.byID, 1, "the instance row must be committed")
		assert.Len(t, outbox.enqueued, 1, "WorkflowInstanceStarted must be enqueued")
		assert.Equal(t, domain.EventWorkflowInstanceStarted, outbox.enqueued[0].Type)
		require.Len(t, temporal.signals, 0)
		_ = temporal
	})

	t.Run("eligible default assignee passes", func(t *testing.T) {
		svc, _, _, _, _, _, definitions, eligibility := newInstanceServiceHarness()
		userID := uuid.New()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), uuid.New(),
			compiledPlanJSON(t, dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer", DefaultAssignees: []string{userID.String()}}))
		var gotRequests []port.EligibilityCheckRequest
		eligibility.batchCheck = func(_ context.Context, reqs []port.EligibilityCheckRequest, _ uuid.UUID) ([]bool, error) {
			gotRequests = reqs
			results := make([]bool, len(reqs))
			for i := range results {
				results[i] = true
			}
			return results, nil
		}

		_, err := svc.Start(context.Background(), port.StartInstanceInput{
			TenantID: uuid.New(), WorkflowVersionID: uuid.New(), BusinessKey: "TND-002", StartedByUserID: uuid.New(),
		})
		require.NoError(t, err)
		require.Len(t, gotRequests, 1)
		assert.Equal(t, userID, gotRequests[0].NewUserID)
		assert.Equal(t, "reviewer", gotRequests[0].RequiredLevel)
	})

	t.Run("ineligible default assignee names the offending node", func(t *testing.T) {
		svc, _, _, _, _, _, definitions, eligibility := newInstanceServiceHarness()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), uuid.New(),
			compiledPlanJSON(t, dsl.StageDef{Type: "userTask", NodeID: "review", DefaultAssignees: []string{uuid.New().String()}}))
		eligibility.batchCheck = func(context.Context, []port.EligibilityCheckRequest, uuid.UUID) ([]bool, error) {
			return []bool{false}, nil
		}

		_, err := svc.Start(context.Background(), port.StartInstanceInput{
			TenantID: uuid.New(), WorkflowVersionID: uuid.New(), BusinessKey: "TND-003", StartedByUserID: uuid.New(),
		})
		require.Error(t, err)
		var ineligible *port.AssigneeIneligibleError
		require.ErrorAs(t, err, &ineligible)
		assert.Equal(t, []string{"finance/review"}, ineligible.Nodes)
	})

	t.Run("connector-typed stage is skipped from eligibility checks", func(t *testing.T) {
		svc, _, _, _, _, _, definitions, eligibility := newInstanceServiceHarness()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), uuid.New(),
			compiledPlanJSON(t, dsl.StageDef{Type: "serviceTask", NodeID: "send", ConnectorType: "send-email", DefaultAssignees: []string{uuid.New().String()}}))
		called := false
		eligibility.batchCheck = func(context.Context, []port.EligibilityCheckRequest, uuid.UUID) ([]bool, error) {
			called = true
			return nil, nil
		}

		_, err := svc.Start(context.Background(), port.StartInstanceInput{
			TenantID: uuid.New(), WorkflowVersionID: uuid.New(), BusinessKey: "TND-004", StartedByUserID: uuid.New(),
		})
		require.NoError(t, err)
		assert.False(t, called, "connector-typed stages have no human assignee to check")
	})

	t.Run("override_map replaces the default assignee for eligibility purposes", func(t *testing.T) {
		svc, _, _, _, _, _, definitions, eligibility := newInstanceServiceHarness()
		defaultUser := uuid.New()
		overrideUser := uuid.New()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), uuid.New(),
			compiledPlanJSON(t, dsl.StageDef{Type: "userTask", NodeID: "review", DefaultAssignees: []string{defaultUser.String()}}))
		var gotUser uuid.UUID
		eligibility.batchCheck = func(_ context.Context, reqs []port.EligibilityCheckRequest, _ uuid.UUID) ([]bool, error) {
			require.Len(t, reqs, 1)
			gotUser = reqs[0].NewUserID
			return []bool{true}, nil
		}

		_, err := svc.Start(context.Background(), port.StartInstanceInput{
			TenantID: uuid.New(), WorkflowVersionID: uuid.New(), BusinessKey: "TND-005", StartedByUserID: uuid.New(),
			OverrideMap: map[string]uuid.UUID{"finance/review": overrideUser},
		})
		require.NoError(t, err)
		assert.Equal(t, overrideUser, gotUser)
	})

	t.Run("override_map naming an unknown node is rejected", func(t *testing.T) {
		svc, _, _, _, _, _, definitions, _ := newInstanceServiceHarness()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), uuid.New(), compiledPlanJSON(t, dsl.StageDef{Type: "userTask", NodeID: "review"}))

		_, err := svc.Start(context.Background(), port.StartInstanceInput{
			TenantID: uuid.New(), WorkflowVersionID: uuid.New(), BusinessKey: "TND-006", StartedByUserID: uuid.New(),
			OverrideMap: map[string]uuid.UUID{"finance/does-not-exist": uuid.New()},
		})
		assert.ErrorIs(t, err, port.ErrOverrideMapInvalid)
	})

	t.Run("version not published", func(t *testing.T) {
		svc, _, _, _, _, _, definitions, _ := newInstanceServiceHarness()
		definitions.resp = &port.CompiledWorkflow{Status: "DRAFT", IsValid: true}
		_, err := svc.Start(context.Background(), port.StartInstanceInput{TenantID: uuid.New(), WorkflowVersionID: uuid.New(), BusinessKey: "TND-007", StartedByUserID: uuid.New()})
		assert.ErrorIs(t, err, port.ErrVersionNotPublished)
	})

	t.Run("version invalid", func(t *testing.T) {
		svc, _, _, _, _, _, definitions, _ := newInstanceServiceHarness()
		definitions.resp = &port.CompiledWorkflow{Status: "PUBLISHED", IsValid: false}
		_, err := svc.Start(context.Background(), port.StartInstanceInput{TenantID: uuid.New(), WorkflowVersionID: uuid.New(), BusinessKey: "TND-008", StartedByUserID: uuid.New()})
		assert.ErrorIs(t, err, port.ErrVersionInvalid)
	})

	t.Run("definition service unavailable passes through", func(t *testing.T) {
		svc, _, _, _, _, _, definitions, _ := newInstanceServiceHarness()
		definitions.err = errors.New("upstream unavailable")
		_, err := svc.Start(context.Background(), port.StartInstanceInput{TenantID: uuid.New(), WorkflowVersionID: uuid.New(), BusinessKey: "TND-009", StartedByUserID: uuid.New()})
		require.Error(t, err)
	})

	t.Run("duplicate business key", func(t *testing.T) {
		svc, instances, _, _, _, _, definitions, _ := newInstanceServiceHarness()
		tenantID := uuid.New()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), uuid.New(), compiledPlanJSON(t))
		existing := &domain.Instance{ID: uuid.New(), TenantID: tenantID, BusinessKey: "TND-010", Status: domain.InstanceStatusRunning}
		instances.byID[existing.ID] = existing

		_, err := svc.Start(context.Background(), port.StartInstanceInput{TenantID: tenantID, WorkflowVersionID: uuid.New(), BusinessKey: "TND-010", StartedByUserID: uuid.New()})
		assert.ErrorIs(t, err, port.ErrDuplicateBusinessKey)
	})

	t.Run("StartWorkflow failure after commit is returned, instance stays committed", func(t *testing.T) {
		svc, instances, _, _, _, temporal, definitions, _ := newInstanceServiceHarness()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), uuid.New(), compiledPlanJSON(t))
		temporal.startFunc = func(context.Context, port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
			return port.StartWorkflowOutput{}, errors.New("temporal frontend unreachable")
		}

		_, err := svc.Start(context.Background(), port.StartInstanceInput{TenantID: uuid.New(), WorkflowVersionID: uuid.New(), BusinessKey: "TND-011", StartedByUserID: uuid.New()})
		require.Error(t, err)
		assert.Len(t, instances.byID, 1, "the DB write is not rolled back on a post-commit StartWorkflow failure")
	})

	t.Run("a stage with no NodeID keys on its Type instead", func(t *testing.T) {
		svc, _, _, _, outbox, _, definitions, _ := newInstanceServiceHarness()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), uuid.New(), compiledPlanJSON(t, dsl.StageDef{Type: "gateway"}))

		_, err := svc.Start(context.Background(), port.StartInstanceInput{TenantID: uuid.New(), WorkflowVersionID: uuid.New(), BusinessKey: "TND-012", StartedByUserID: uuid.New()})
		require.NoError(t, err)
		require.Len(t, outbox.enqueued, 1)
	})

	t.Run("a BuildEnvelope failure fails Start", func(t *testing.T) {
		instances, tasks, assignments := newFakeInstanceRepo(), newFakeTaskRepo(), newFakeAssignmentRepo()
		outbox, temporal, definitions, eligibility := &fakeOutbox{}, &fakeTemporalClient{}, &fakeDefinitionClient{}, &fakeEligibilityChecker{}
		svc := &service.InstanceService{
			Instances: instances, Tasks: tasks, Assignments: assignments, Outbox: outbox,
			Transactor: fakeTransactor{}, Temporal: temporal, Definitions: definitions, Eligibility: eligibility,
			Validator: failingValidator{},
		}
		definitions.resp = publishedCompiledWorkflow(uuid.New(), uuid.New(), compiledPlanJSON(t))

		_, err := svc.Start(context.Background(), port.StartInstanceInput{TenantID: uuid.New(), WorkflowVersionID: uuid.New(), BusinessKey: "TND-013", StartedByUserID: uuid.New()})
		assert.Error(t, err)
		assert.Empty(t, outbox.enqueued, "the event never reaches Outbox.Enqueue when BuildEnvelope itself fails")
	})
}

func TestInstanceService_List(t *testing.T) {
	svc, instances, _, _, _, _, _, _ := newInstanceServiceHarness()
	tenantID := uuid.New()
	running := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning}
	paused := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusPaused}
	otherTenant := &domain.Instance{ID: uuid.New(), TenantID: uuid.New(), Status: domain.InstanceStatusRunning}
	instances.byID[running.ID] = running
	instances.byID[paused.ID] = paused
	instances.byID[otherTenant.ID] = otherTenant

	t.Run("tenant scoped", func(t *testing.T) {
		res, err := svc.List(context.Background(), tenantID, port.ReadScope{IsAdmin: true}, port.InstanceFilter{}, port.Page{Limit: 10})
		require.NoError(t, err)
		assert.Len(t, res.Items, 2)
	})

	t.Run("status filter", func(t *testing.T) {
		status := port.InstanceStatusPaused
		res, err := svc.List(context.Background(), tenantID, port.ReadScope{IsAdmin: true}, port.InstanceFilter{Status: &status}, port.Page{Limit: 10})
		require.NoError(t, err)
		require.Len(t, res.Items, 1)
		assert.Equal(t, paused.ID, res.Items[0].ID)
	})

	t.Run("cursor round trip", func(t *testing.T) {
		next := port.Cursor{CreatedAt: running.CreatedAt, ID: running.ID}
		instances.nextCursor = &next
		cursorIn := &port.CursorPosition{CreatedAt: paused.CreatedAt, ID: paused.ID}

		res, err := svc.List(context.Background(), tenantID, port.ReadScope{IsAdmin: true}, port.InstanceFilter{}, port.Page{Cursor: cursorIn, Limit: 10})
		require.NoError(t, err)
		require.NotNil(t, instances.lastPageAfter, "the page cursor must be threaded through to the repo call")
		assert.Equal(t, cursorIn.ID, instances.lastPageAfter.ID)
		assert.NotEmpty(t, res.NextCursor)
	})

	t.Run("repo error propagates", func(t *testing.T) {
		svc, instances, _, _, _, _, _, _ := newInstanceServiceHarness()
		instances.listErr = assert.AnError
		_, err := svc.List(context.Background(), uuid.New(), port.ReadScope{IsAdmin: true}, port.InstanceFilter{}, port.Page{Limit: 10})
		assert.Error(t, err)
	})
}

func TestInstanceService_Get(t *testing.T) {
	svc, instances, tasks, assignments, _, _, _, _ := newInstanceServiceHarness()
	tenantID := uuid.New()
	inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning}
	instances.byID[inst.ID] = inst
	task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: inst.ID, DepartmentID: uuid.New(), Status: domain.TaskStatusReady}
	tasks.byID[task.ID] = task

	t.Run("not found", func(t *testing.T) {
		_, _, err := svc.Get(context.Background(), tenantID, uuid.New(), port.ReadScope{IsAdmin: true})
		assert.ErrorIs(t, err, port.ErrInstanceNotFound)
	})

	t.Run("admin bypasses scope check", func(t *testing.T) {
		got, gotTasks, err := svc.Get(context.Background(), tenantID, inst.ID, port.ReadScope{IsAdmin: true})
		require.NoError(t, err)
		assert.Equal(t, inst.ID, got.ID)
		require.Len(t, gotTasks, 1)
	})

	t.Run("caller with no matching department/assignment is rejected", func(t *testing.T) {
		_, _, err := svc.Get(context.Background(), tenantID, inst.ID, port.ReadScope{CallerUserID: uuid.New(), Departments: []string{"unrelated-dept"}})
		assert.ErrorIs(t, err, port.ErrNotAuthorizedForRead)
	})

	t.Run("caller whose department matches the task is authorized", func(t *testing.T) {
		got, _, err := svc.Get(context.Background(), tenantID, inst.ID, port.ReadScope{Departments: []string{task.DepartmentID.String()}})
		require.NoError(t, err)
		assert.Equal(t, inst.ID, got.ID)
	})

	t.Run("caller who is an active assignee is authorized", func(t *testing.T) {
		userID := uuid.New()
		assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, UserID: userID, IsActive: true}
		got, _, err := svc.Get(context.Background(), tenantID, inst.ID, port.ReadScope{CallerUserID: userID})
		require.NoError(t, err)
		assert.Equal(t, inst.ID, got.ID)
	})

	t.Run("Instances.GetByID error propagates", func(t *testing.T) {
		svc, instances, _, _, _, _, _, _ := newInstanceServiceHarness()
		instances.getErr = assert.AnError
		_, _, err := svc.Get(context.Background(), uuid.New(), uuid.New(), port.ReadScope{IsAdmin: true})
		assert.Error(t, err)
	})

	t.Run("Tasks.ListByInstance error propagates", func(t *testing.T) {
		svc, instances, tasks, _, _, _, _, _ := newInstanceServiceHarness()
		tenantID := uuid.New()
		inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID}
		instances.byID[inst.ID] = inst
		tasks.listErr = assert.AnError

		_, _, err := svc.Get(context.Background(), tenantID, inst.ID, port.ReadScope{IsAdmin: true})
		assert.Error(t, err)
	})

	t.Run("an unreadable assignment is skipped, caller falls through to rejected", func(t *testing.T) {
		svc, instances, tasks, assignments, _, _, _, _ := newInstanceServiceHarness()
		tenantID := uuid.New()
		inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID}
		instances.byID[inst.ID] = inst
		task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: inst.ID, DepartmentID: uuid.New()}
		tasks.byID[task.ID] = task
		assignments.listActiveByTaskErr = assert.AnError

		_, _, err := svc.Get(context.Background(), tenantID, inst.ID, port.ReadScope{CallerUserID: uuid.New()})
		assert.ErrorIs(t, err, port.ErrNotAuthorizedForRead)
	})
}

func TestInstanceService_ListEvents(t *testing.T) {
	svc, instances, _, _, outbox, _, _, _ := newInstanceServiceHarness()
	tenantID := uuid.New()
	inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID}
	instances.byID[inst.ID] = inst

	envelope := map[string]any{"tenant_id": tenantID.String(), "data": map[string]any{}}
	raw, err := json.Marshal(envelope)
	require.NoError(t, err)
	outbox.records = []*domain.OutboxEventRecord{
		{ID: uuid.New(), EventType: domain.EventWorkflowInstanceStarted, Payload: raw},
		{ID: uuid.New(), EventType: "workflow.something.unknown", Payload: raw},
	}

	res, err := svc.ListEvents(context.Background(), tenantID, inst.ID, port.ReadScope{IsAdmin: true}, port.Page{Limit: 10})
	require.NoError(t, err)
	require.Len(t, res.Items, 1, "the unrecognized event type must be skipped, not fail the whole page")
	assert.Equal(t, port.EventInstanceStarted, res.Items[0].EventType)

	t.Run("non-admin caller out of scope is rejected", func(t *testing.T) {
		_, err := svc.ListEvents(context.Background(), tenantID, inst.ID, port.ReadScope{CallerUserID: uuid.New()}, port.Page{Limit: 10})
		assert.ErrorIs(t, err, port.ErrNotAuthorizedForRead)
	})

	t.Run("Tasks.ListByInstance error propagates for a non-admin caller", func(t *testing.T) {
		svc, instances, tasks, _, _, _, _, _ := newInstanceServiceHarness()
		tenantID := uuid.New()
		inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID}
		instances.byID[inst.ID] = inst
		tasks.listErr = assert.AnError

		_, err := svc.ListEvents(context.Background(), tenantID, inst.ID, port.ReadScope{CallerUserID: uuid.New()}, port.Page{Limit: 10})
		assert.Error(t, err)
	})

	t.Run("Outbox.ListByInstance error propagates", func(t *testing.T) {
		svc, instances, _, _, outbox, _, _, _ := newInstanceServiceHarness()
		tenantID := uuid.New()
		inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID}
		instances.byID[inst.ID] = inst
		outbox.listErr = assert.AnError

		_, err := svc.ListEvents(context.Background(), tenantID, inst.ID, port.ReadScope{IsAdmin: true}, port.Page{Limit: 10})
		assert.Error(t, err)
	})
}

func TestInstanceService_LifecycleSignals(t *testing.T) {
	cases := []struct {
		name       string
		status     domain.InstanceStatus
		call       func(svc *service.InstanceService, tenantID, instanceID, actorID uuid.UUID, recordVersion int64) error
		wantSignal string
	}{
		{"pause from running", domain.InstanceStatusRunning, func(s *service.InstanceService, t, i, a uuid.UUID, rv int64) error {
			return s.Pause(context.Background(), t, i, a, "reason", rv)
		}, port.SignalInstancePause},
		{"resume from paused", domain.InstanceStatusPaused, func(s *service.InstanceService, t, i, a uuid.UUID, rv int64) error {
			return s.Resume(context.Background(), t, i, a, rv)
		}, port.SignalInstanceResume},
		{"cancel from running", domain.InstanceStatusRunning, func(s *service.InstanceService, t, i, a uuid.UUID, rv int64) error {
			return s.Cancel(context.Background(), t, i, a, "reason", rv)
		}, port.SignalInstanceCancel},
		{"force-back from running", domain.InstanceStatusRunning, func(s *service.InstanceService, t, i, a uuid.UUID, rv int64) error {
			return s.ForceBack(context.Background(), t, i, a, rv)
		}, port.SignalInstanceForceBack},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, instances, _, _, _, temporal, _, _ := newInstanceServiceHarness()
			tenantID, actorID := uuid.New(), uuid.New()
			inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: tc.status, RecordVersion: 3, TemporalWorkflowID: "tenant:biz"}
			instances.byID[inst.ID] = inst

			err := tc.call(svc, tenantID, inst.ID, actorID, 3)
			require.NoError(t, err)
			require.Len(t, temporal.signals, 1)
			assert.Equal(t, tc.wantSignal, temporal.signals[0].SignalName)
			assert.Equal(t, "tenant:biz", temporal.signals[0].TemporalWorkflowID)
			assert.Equal(t, inst.ID, temporal.signals[0].InstanceID)
		})
	}

	t.Run("record version conflict", func(t *testing.T) {
		svc, instances, _, _, _, _, _, _ := newInstanceServiceHarness()
		tenantID := uuid.New()
		inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning, RecordVersion: 5}
		instances.byID[inst.ID] = inst

		err := svc.Pause(context.Background(), tenantID, inst.ID, uuid.New(), "", 1)
		assert.ErrorIs(t, err, port.ErrRecordVersionConflict)
	})

	t.Run("invalid state for signal", func(t *testing.T) {
		svc, instances, _, _, _, _, _, _ := newInstanceServiceHarness()
		tenantID := uuid.New()
		inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusPaused, RecordVersion: 1}
		instances.byID[inst.ID] = inst

		err := svc.Pause(context.Background(), tenantID, inst.ID, uuid.New(), "", 1)
		assert.ErrorIs(t, err, port.ErrInvalidInstanceState)
	})

	t.Run("already terminal", func(t *testing.T) {
		svc, instances, _, _, _, _, _, _ := newInstanceServiceHarness()
		tenantID := uuid.New()
		inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusCompleted, RecordVersion: 1}
		instances.byID[inst.ID] = inst

		err := svc.Pause(context.Background(), tenantID, inst.ID, uuid.New(), "", 1)
		assert.ErrorIs(t, err, port.ErrInstanceAlreadyTerminal)
	})

	t.Run("force-forward not found", func(t *testing.T) {
		svc, _, _, _, _, _, _, _ := newInstanceServiceHarness()
		err := svc.ForceForward(context.Background(), uuid.New(), uuid.New(), uuid.New(), "finance/settlement", 1)
		assert.ErrorIs(t, err, port.ErrInstanceNotFound)
	})

	t.Run("force-forward record version conflict", func(t *testing.T) {
		svc, instances, _, _, _, _, _, _ := newInstanceServiceHarness()
		tenantID := uuid.New()
		inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning, RecordVersion: 5}
		instances.byID[inst.ID] = inst
		err := svc.ForceForward(context.Background(), tenantID, inst.ID, uuid.New(), "finance/settlement", 1)
		assert.ErrorIs(t, err, port.ErrRecordVersionConflict)
	})

	t.Run("force-forward invalid state", func(t *testing.T) {
		svc, instances, _, _, _, _, _, _ := newInstanceServiceHarness()
		tenantID := uuid.New()
		inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusCompleted, RecordVersion: 1}
		instances.byID[inst.ID] = inst
		err := svc.ForceForward(context.Background(), tenantID, inst.ID, uuid.New(), "finance/settlement", 1)
		assert.ErrorIs(t, err, port.ErrInstanceAlreadyTerminal)
	})

	t.Run("SignalWorkflow failure is returned", func(t *testing.T) {
		svc, instances, _, _, _, temporal, _, _ := newInstanceServiceHarness()
		tenantID := uuid.New()
		inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning, RecordVersion: 1, TemporalWorkflowID: "tenant:biz"}
		instances.byID[inst.ID] = inst
		temporal.signalFunc = func(context.Context, string, uuid.UUID, string, any) error { return assert.AnError }

		err := svc.Pause(context.Background(), tenantID, inst.ID, uuid.New(), "reason", 1)
		assert.Error(t, err)
	})

	t.Run("force-forward carries the target node key", func(t *testing.T) {
		svc, instances, _, _, _, temporal, _, _ := newInstanceServiceHarness()
		tenantID := uuid.New()
		inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning, RecordVersion: 1, TemporalWorkflowID: "tenant:biz"}
		instances.byID[inst.ID] = inst

		err := svc.ForceForward(context.Background(), tenantID, inst.ID, uuid.New(), "finance/settlement", 1)
		require.NoError(t, err)
		require.Len(t, temporal.signals, 1)
		assert.Equal(t, port.SignalInstanceForceFwd, temporal.signals[0].SignalName)
		b, err := json.Marshal(temporal.signals[0].Payload)
		require.NoError(t, err)
		assert.Contains(t, string(b), `"finance/settlement"`)
	})
}

func TestInstanceService_Terminate(t *testing.T) {
	t.Run("cascades tasks to FAILED and vacates assignments", func(t *testing.T) {
		svc, instances, tasks, assignments, outbox, temporal, _, _ := newInstanceServiceHarness()
		tenantID := uuid.New()
		inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning, RecordVersion: 1, TemporalWorkflowID: "tenant:biz"}
		instances.byID[inst.ID] = inst
		task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: inst.ID, Status: domain.TaskStatusInProgress, RecordVersion: 1}
		tasks.byID[task.ID] = task
		assignment := &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, IsActive: true}
		assignments.byID[assignment.ID] = assignment

		err := svc.Terminate(context.Background(), tenantID, inst.ID, uuid.New(), "admin requested")
		require.NoError(t, err)

		assert.Equal(t, domain.InstanceStatusTerminated, instances.byID[inst.ID].Status)
		assert.Equal(t, domain.TaskStatusFailed, tasks.byID[task.ID].Status)
		assert.False(t, assignments.byID[assignment.ID].IsActive)
		require.Len(t, outbox.enqueued, 1)
		assert.Equal(t, domain.EventWorkflowInstanceTerminated, outbox.enqueued[0].Type)
		require.Len(t, temporal.terminateCalls(), 1)
	})

	t.Run("not found", func(t *testing.T) {
		svc, _, _, _, _, _, _, _ := newInstanceServiceHarness()
		err := svc.Terminate(context.Background(), uuid.New(), uuid.New(), uuid.New(), "reason")
		assert.ErrorIs(t, err, port.ErrInstanceNotFound)
	})

	t.Run("nil Log falls back to a no-op logger without panicking", func(t *testing.T) {
		svc, instances, _, _, _, temporal, _, _ := newInstanceServiceHarness()
		tenantID := uuid.New()
		inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning, RecordVersion: 1, TemporalWorkflowID: "tenant:biz"}
		instances.byID[inst.ID] = inst
		temporal.terminateFunc = func(context.Context, string, string) error { return errors.New("boom") }

		assert.NotPanics(t, func() {
			_ = svc.Terminate(context.Background(), tenantID, inst.ID, uuid.New(), "reason")
		})
	})

	t.Run("already terminal is rejected before any write", func(t *testing.T) {
		svc, instances, _, _, _, _, _, _ := newInstanceServiceHarness()
		tenantID := uuid.New()
		inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusFailed}
		instances.byID[inst.ID] = inst

		err := svc.Terminate(context.Background(), tenantID, inst.ID, uuid.New(), "reason")
		assert.ErrorIs(t, err, port.ErrInstanceAlreadyTerminal)
	})

	t.Run("TerminateWorkflow failure is returned even though DB already committed TERMINATED", func(t *testing.T) {
		svc, instances, _, _, _, temporal, _, _ := newInstanceServiceHarness()
		tenantID := uuid.New()
		inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning, RecordVersion: 1, TemporalWorkflowID: "tenant:biz"}
		instances.byID[inst.ID] = inst
		temporal.terminateFunc = func(context.Context, string, string) error { return errors.New("temporal unavailable") }

		err := svc.Terminate(context.Background(), tenantID, inst.ID, uuid.New(), "reason")
		require.Error(t, err)
		assert.Equal(t, domain.InstanceStatusTerminated, instances.byID[inst.ID].Status, "DB state is already correct regardless of the RPC outcome")
	})

	t.Run("Tasks.ListByInstance failure during cascade propagates", func(t *testing.T) {
		svc, instances, tasks, _, _, _, _, _ := newInstanceServiceHarness()
		tenantID := uuid.New()
		inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning, RecordVersion: 1, TemporalWorkflowID: "tenant:biz"}
		instances.byID[inst.ID] = inst
		tasks.listErr = assert.AnError

		err := svc.Terminate(context.Background(), tenantID, inst.ID, uuid.New(), "reason")
		assert.Error(t, err)
	})

	t.Run("Tasks.UpdateStatus failure during cascade propagates", func(t *testing.T) {
		svc, instances, tasks, _, _, _, _, _ := newInstanceServiceHarness()
		tenantID := uuid.New()
		inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning, RecordVersion: 1, TemporalWorkflowID: "tenant:biz"}
		instances.byID[inst.ID] = inst
		task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: inst.ID, Status: domain.TaskStatusReady, RecordVersion: 1}
		tasks.byID[task.ID] = task
		tasks.updateStatusErr = assert.AnError

		err := svc.Terminate(context.Background(), tenantID, inst.ID, uuid.New(), "reason")
		assert.Error(t, err)
	})

	t.Run("Assignments.ListActiveByTask failure during cascade propagates", func(t *testing.T) {
		svc, instances, tasks, assignments, _, _, _, _ := newInstanceServiceHarness()
		tenantID := uuid.New()
		inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning, RecordVersion: 1, TemporalWorkflowID: "tenant:biz"}
		instances.byID[inst.ID] = inst
		task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: inst.ID, Status: domain.TaskStatusReady, RecordVersion: 1}
		tasks.byID[task.ID] = task
		assignments.listActiveByTaskErr = assert.AnError

		err := svc.Terminate(context.Background(), tenantID, inst.ID, uuid.New(), "reason")
		assert.Error(t, err)
	})

	t.Run("Assignments.Vacate failure during cascade propagates", func(t *testing.T) {
		svc, instances, tasks, assignments, _, _, _, _ := newInstanceServiceHarness()
		tenantID := uuid.New()
		inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning, RecordVersion: 1, TemporalWorkflowID: "tenant:biz"}
		instances.byID[inst.ID] = inst
		task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: inst.ID, Status: domain.TaskStatusReady, RecordVersion: 1}
		tasks.byID[task.ID] = task
		assignments.byID[uuid.New()] = &domain.TaskAssignment{ID: uuid.New(), TenantID: tenantID, TaskID: task.ID, IsActive: true}
		assignments.vacateErr = assert.AnError

		err := svc.Terminate(context.Background(), tenantID, inst.ID, uuid.New(), "reason")
		assert.Error(t, err)
	})
}

func TestInstanceService_Start_CompiledPlanCache(t *testing.T) {
	t.Run("a cache hit skips the Definitions call entirely", func(t *testing.T) {
		svc, _, _, _, _, _, definitions, _ := newInstanceServiceHarness()
		cache := newFakeCacheStore()
		svc.Cache = cache
		tenantID, versionID := uuid.New(), uuid.New()

		cached := publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, dsl.StageDef{Type: "userTask", NodeID: "review"}))
		raw, err := json.Marshal(cached)
		require.NoError(t, err)
		cache.values["compiled_plan:"+tenantID.String()+":"+versionID.String()] = string(raw)
		definitions.err = errors.New("Definitions must not be called on a cache hit")

		_, err = svc.Start(context.Background(), port.StartInstanceInput{
			TenantID: tenantID, WorkflowVersionID: versionID, BusinessKey: "TND-CACHE-1", StartedByUserID: uuid.New(),
		})
		require.NoError(t, err)
	})

	t.Run("a cache miss falls through to Definitions", func(t *testing.T) {
		svc, _, _, _, _, _, definitions, _ := newInstanceServiceHarness()
		cache := newFakeCacheStore()
		svc.Cache = cache
		tenantID, versionID := uuid.New(), uuid.New()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, dsl.StageDef{Type: "userTask", NodeID: "review"}))

		_, err := svc.Start(context.Background(), port.StartInstanceInput{
			TenantID: tenantID, WorkflowVersionID: versionID, BusinessKey: "TND-CACHE-2", StartedByUserID: uuid.New(),
		})
		require.NoError(t, err)
	})

	t.Run("a cache read error falls through to Definitions instead of failing Start", func(t *testing.T) {
		svc, _, _, _, _, _, definitions, _ := newInstanceServiceHarness()
		cache := newFakeCacheStore()
		cache.getErr = errors.New("valkey unavailable")
		svc.Cache = cache
		log := &fakeLogger{}
		svc.Log = log
		tenantID, versionID := uuid.New(), uuid.New()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, dsl.StageDef{Type: "userTask", NodeID: "review"}))

		_, err := svc.Start(context.Background(), port.StartInstanceInput{
			TenantID: tenantID, WorkflowVersionID: versionID, BusinessKey: "TND-CACHE-3", StartedByUserID: uuid.New(),
		})
		require.NoError(t, err)
		assert.NotEmpty(t, log.warnCalls)
	})
}
