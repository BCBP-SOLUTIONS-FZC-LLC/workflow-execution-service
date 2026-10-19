package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/enums"
)

// compiledPlanJSON builds a minimal, valid CompiledCollaboration JSON fixture
// with one main plan, one department ("finance"), and the given stages.
// assignedStage is a human stage that names a default assignee — the shape a
// published plan is required to have. A stage naming nobody is rejected at
// instantiation, so it is a fixture only for the tests that assert that.
func assignedStage(nodeID string) dsl.StageDef {
	return dsl.StageDef{Type: "userTask", NodeID: nodeID, DefaultAssignees: []string{uuid.NewString()}}
}

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
	instances.linkScope(tasks, assignments)
	queues := newFakeActiveTaskQueueRepo()

	svc := &service.InstanceService{
		Instances:   instances,
		Tasks:       tasks,
		Assignments: assignments,
		Outbox:      outbox,
		Transactor:  fakeTransactor{},
		Temporal:    temporal,
		Definitions: definitions,
		Eligibility: eligibility,
		Queues:      queues,
		Validator:   realValidator(),
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
	t.Run("success with a default assignee", func(t *testing.T) {
		svc, instances, _, _, outbox, temporal, definitions, _ := newInstanceServiceHarness()
		workflowID, versionID, tenantID := uuid.New(), uuid.New(), uuid.New()
		definitions.resp = publishedCompiledWorkflow(workflowID, versionID, compiledPlanJSON(t, assignedStage("review")))

		got, err := svc.Start(context.Background(), port.StartInstanceInput{
			TenantID: tenantID, WorkflowVersionID: versionID, SubjectID: "SUBJ-1", BusinessKey: "TND-001",
			ContextJSON: json.RawMessage(`{}`), StartedByUserID: uuid.New(),
		})
		require.NoError(t, err)
		assert.Equal(t, "TND-001", got.BusinessKey)
		assert.Equal(t, workflowID, got.WorkflowID)
		assert.Equal(t, port.InstanceStatusRunning, got.Status)
		assert.Equal(t, tenantID.String()+":SUBJ-1:TND-001", got.TemporalWorkflowID)
		assert.Len(t, instances.byID, 1, "the instance row must be committed")
		assert.Len(t, outbox.enqueued, 1, "WorkflowInstanceStarted must be enqueued")
		assert.Equal(t, domain.EventWorkflowInstanceStarted, outbox.enqueued[0].Type)
		require.Len(t, temporal.signals, 0)
		_ = temporal
	})

	t.Run("a terminate that lands before the workflow start succeeds leaves no live execution and the start reports it terminated", func(t *testing.T) {
		svc, instances, _, _, _, temporal, definitions, _ := newInstanceServiceHarness()
		tenantID, versionID := uuid.New(), uuid.New()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))
		live := map[string]bool{}
		temporal.terminateFunc = func(_ context.Context, id, _, _ string) error {
			delete(live, id)
			return nil
		}
		temporal.startFunc = func(_ context.Context, in port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
			require.NoError(t, svc.Terminate(context.Background(), tenantID, in.InstanceID, uuid.New(), "admin requested"))
			live[in.TemporalWorkflowID] = true
			return port.StartWorkflowOutput{TemporalWorkflowID: in.TemporalWorkflowID, TemporalRunID: "run-1"}, nil
		}

		got, err := svc.Start(context.Background(), port.StartInstanceInput{
			TenantID: tenantID, WorkflowVersionID: versionID, SubjectID: "SUBJ-1", BusinessKey: "TND-001",
			ContextJSON: json.RawMessage(`{}`), StartedByUserID: uuid.New(),
		})
		require.NoError(t, err)
		assert.Equal(t, domain.InstanceStatusTerminated, instances.byID[got.ID].Status)
		assert.Equal(t, port.InstanceStatusTerminated, got.Status)
		assert.Empty(t, live, "the execution the start created must be closed")
	})

	t.Run("a failure to close the execution of an instance terminated while starting is logged and the start still reports it", func(t *testing.T) {
		svc, _, _, _, _, temporal, definitions, _ := newInstanceServiceHarness()
		log := &fakeLogger{}
		svc.Log = log
		tenantID, versionID := uuid.New(), uuid.New()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))
		temporal.startFunc = func(_ context.Context, in port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
			require.NoError(t, svc.Terminate(context.Background(), tenantID, in.InstanceID, uuid.New(), "admin requested"))
			temporal.terminateFunc = func(context.Context, string, string, string) error { return errors.New("temporal unavailable") }
			return port.StartWorkflowOutput{TemporalWorkflowID: in.TemporalWorkflowID, TemporalRunID: "run-1"}, nil
		}

		got, err := svc.Start(context.Background(), port.StartInstanceInput{
			TenantID: tenantID, WorkflowVersionID: versionID, SubjectID: "SUBJ-1", BusinessKey: "TND-001",
			ContextJSON: json.RawMessage(`{}`), StartedByUserID: uuid.New(),
		})
		require.NoError(t, err)
		assert.Equal(t, port.InstanceStatusTerminated, got.Status)
		assert.NotEmpty(t, log.errorCalls)
	})

	t.Run("a failure to re-read the instance after the workflow started does not fail the start", func(t *testing.T) {
		svc, instances, _, _, _, temporal, definitions, _ := newInstanceServiceHarness()
		log := &fakeLogger{}
		svc.Log = log
		versionID := uuid.New()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))
		temporal.startFunc = func(_ context.Context, in port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
			instances.getErr = assert.AnError
			return port.StartWorkflowOutput{TemporalWorkflowID: in.TemporalWorkflowID, TemporalRunID: "run-1"}, nil
		}

		got, err := svc.Start(context.Background(), port.StartInstanceInput{
			TenantID: uuid.New(), WorkflowVersionID: versionID, SubjectID: "SUBJ-1", BusinessKey: "TND-001",
			ContextJSON: json.RawMessage(`{}`), StartedByUserID: uuid.New(),
		})
		require.NoError(t, err)
		assert.Equal(t, port.InstanceStatusRunning, got.Status)
		assert.NotEmpty(t, log.warnCalls)
	})

	t.Run("an instance starts on the tenant's registered queue", func(t *testing.T) {
		svc, instances, _, _, _, temporal, definitions, _ := newInstanceServiceHarness()
		queues := newFakeActiveTaskQueueRepo()
		svc.Queues = queues
		tenantID, versionID := uuid.New(), uuid.New()
		queueName := "wf-queue-" + tenantID.String()
		_, err := queues.Register(context.Background(), tenantID, queueName)
		require.NoError(t, err)
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))
		var startedOn string
		temporal.startFunc = func(_ context.Context, in port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
			startedOn = in.TaskQueue
			return port.StartWorkflowOutput{TemporalWorkflowID: in.TemporalWorkflowID, TemporalRunID: "run-1"}, nil
		}

		got, err := svc.Start(context.Background(), port.StartInstanceInput{
			TenantID: tenantID, WorkflowVersionID: versionID, SubjectID: "SUBJ-1", BusinessKey: "TND-001",
			ContextJSON: json.RawMessage(`{}`), StartedByUserID: uuid.New(),
		})
		require.NoError(t, err)
		assert.Equal(t, queueName, startedOn)
		assert.Equal(t, queueName, instances.byID[got.ID].TaskQueue, "the queue is snapshotted on the row")
	})

	t.Run("a tenant with no registered queue starts on the plan's queue", func(t *testing.T) {
		svc, instances, _, _, _, temporal, definitions, _ := newInstanceServiceHarness()
		svc.Queues = newFakeActiveTaskQueueRepo()
		tenantID, versionID := uuid.New(), uuid.New()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))
		var startedOn string
		temporal.startFunc = func(_ context.Context, in port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
			startedOn = in.TaskQueue
			return port.StartWorkflowOutput{TemporalWorkflowID: in.TemporalWorkflowID, TemporalRunID: "run-1"}, nil
		}

		got, err := svc.Start(context.Background(), port.StartInstanceInput{
			TenantID: tenantID, WorkflowVersionID: versionID, SubjectID: "SUBJ-1", BusinessKey: "TND-001",
			ContextJSON: json.RawMessage(`{}`), StartedByUserID: uuid.New(),
		})
		require.NoError(t, err)
		assert.NotEmpty(t, startedOn)
		assert.NotContains(t, startedOn, tenantID.String())
		assert.Equal(t, startedOn, instances.byID[got.ID].TaskQueue)
	})

	t.Run("a queue that stopped routing is not used for a new instance", func(t *testing.T) {
		svc, instances, _, _, _, _, definitions, _ := newInstanceServiceHarness()
		queues := newFakeActiveTaskQueueRepo()
		svc.Queues = queues
		tenantID, versionID := uuid.New(), uuid.New()
		queueName := "wf-queue-" + tenantID.String()
		queues.byName[queueName] = &domain.ActiveTaskQueue{TenantID: tenantID, QueueName: queueName, Routable: false}
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))

		got, err := svc.Start(context.Background(), port.StartInstanceInput{
			TenantID: tenantID, WorkflowVersionID: versionID, SubjectID: "SUBJ-1", BusinessKey: "TND-001",
			ContextJSON: json.RawMessage(`{}`), StartedByUserID: uuid.New(),
		})
		require.NoError(t, err)
		assert.NotEqual(t, queueName, instances.byID[got.ID].TaskQueue)
	})

	t.Run("a missing queue registry fails the start instead of silently using the shared queue", func(t *testing.T) {
		svc, instances, _, _, _, _, definitions, _ := newInstanceServiceHarness()
		svc.Queues = nil
		versionID := uuid.New()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))

		_, err := svc.Start(context.Background(), port.StartInstanceInput{
			TenantID: uuid.New(), WorkflowVersionID: versionID, SubjectID: "SUBJ-1", BusinessKey: "TND-001",
			ContextJSON: json.RawMessage(`{}`), StartedByUserID: uuid.New(),
		})
		require.Error(t, err)
		assert.Empty(t, instances.byID)
	})

	t.Run("a failed registry read fails the start and persists nothing", func(t *testing.T) {
		svc, instances, _, _, outbox, temporal, definitions, _ := newInstanceServiceHarness()
		svc.Queues = failingQueueLookup{err: errors.New("registry unavailable")}
		versionID := uuid.New()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))

		_, err := svc.Start(context.Background(), port.StartInstanceInput{
			TenantID: uuid.New(), WorkflowVersionID: versionID, SubjectID: "SUBJ-1", BusinessKey: "TND-001",
			ContextJSON: json.RawMessage(`{}`), StartedByUserID: uuid.New(),
		})
		require.ErrorContains(t, err, "registry unavailable")
		assert.Empty(t, instances.byID)
		assert.Empty(t, outbox.enqueued)
		assert.Empty(t, temporal.terminateCalls())
	})

	t.Run("eligible default assignee passes", func(t *testing.T) {
		svc, _, _, _, _, _, definitions, eligibility := newInstanceServiceHarness()
		userID := uuid.New()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), uuid.New(),
			compiledPlanJSON(t, dsl.StageDef{Type: "userTask", NodeID: "review", Role: "reviewer", DefaultAssignees: []string{userID.String()}}))
		var gotRequests []port.EligibilityCheckRequest
		eligibility.batchCheck = func(_ context.Context, reqs []port.EligibilityCheckRequest, _ uuid.UUID) ([]port.EligibilityResult, error) {
			gotRequests = reqs
			results := make([]port.EligibilityResult, len(reqs))
			for i := range results {
				results[i] = port.EligibilityResult{Eligible: true}
			}
			return results, nil
		}

		_, err := svc.Start(context.Background(), port.StartInstanceInput{
			TenantID: uuid.New(), WorkflowVersionID: uuid.New(), SubjectID: "SUBJ-1", BusinessKey: "TND-002", StartedByUserID: uuid.New(),
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
		eligibility.batchCheck = func(context.Context, []port.EligibilityCheckRequest, uuid.UUID) ([]port.EligibilityResult, error) {
			return []port.EligibilityResult{{Eligible: false}}, nil
		}

		_, err := svc.Start(context.Background(), port.StartInstanceInput{
			TenantID: uuid.New(), WorkflowVersionID: uuid.New(), SubjectID: "SUBJ-1", BusinessKey: "TND-003", StartedByUserID: uuid.New(),
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
		eligibility.batchCheck = func(context.Context, []port.EligibilityCheckRequest, uuid.UUID) ([]port.EligibilityResult, error) {
			called = true
			return nil, nil
		}

		_, err := svc.Start(context.Background(), port.StartInstanceInput{
			TenantID: uuid.New(), WorkflowVersionID: uuid.New(), SubjectID: "SUBJ-1", BusinessKey: "TND-004", StartedByUserID: uuid.New(),
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
		eligibility.batchCheck = func(_ context.Context, reqs []port.EligibilityCheckRequest, _ uuid.UUID) ([]port.EligibilityResult, error) {
			require.Len(t, reqs, 1)
			gotUser = reqs[0].NewUserID
			return []port.EligibilityResult{{Eligible: true}}, nil
		}

		_, err := svc.Start(context.Background(), port.StartInstanceInput{
			TenantID: uuid.New(), WorkflowVersionID: uuid.New(), SubjectID: "SUBJ-1", BusinessKey: "TND-005", StartedByUserID: uuid.New(),
			OverrideMap: map[string]uuid.UUID{"finance/review": overrideUser},
		})
		require.NoError(t, err)
		assert.Equal(t, overrideUser, gotUser)
	})

	t.Run("override_map naming an unknown node is rejected", func(t *testing.T) {
		svc, _, _, _, _, _, definitions, _ := newInstanceServiceHarness()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), uuid.New(), compiledPlanJSON(t, assignedStage("review")))

		_, err := svc.Start(context.Background(), port.StartInstanceInput{
			TenantID: uuid.New(), WorkflowVersionID: uuid.New(), SubjectID: "SUBJ-1", BusinessKey: "TND-006", StartedByUserID: uuid.New(),
			OverrideMap: map[string]uuid.UUID{"finance/does-not-exist": uuid.New()},
		})
		assert.ErrorIs(t, err, port.ErrOverrideMapInvalid)
	})

	t.Run("version not published", func(t *testing.T) {
		svc, _, _, _, _, _, definitions, _ := newInstanceServiceHarness()
		definitions.resp = &port.CompiledWorkflow{Status: "DRAFT", IsValid: true}
		_, err := svc.Start(context.Background(), port.StartInstanceInput{TenantID: uuid.New(), WorkflowVersionID: uuid.New(), SubjectID: "SUBJ-1", BusinessKey: "TND-007", StartedByUserID: uuid.New()})
		assert.ErrorIs(t, err, port.ErrVersionNotPublished)
	})

	t.Run("version invalid", func(t *testing.T) {
		svc, _, _, _, _, _, definitions, _ := newInstanceServiceHarness()
		definitions.resp = &port.CompiledWorkflow{Status: "PUBLISHED", IsValid: false}
		_, err := svc.Start(context.Background(), port.StartInstanceInput{TenantID: uuid.New(), WorkflowVersionID: uuid.New(), SubjectID: "SUBJ-1", BusinessKey: "TND-008", StartedByUserID: uuid.New()})
		assert.ErrorIs(t, err, port.ErrVersionInvalid)
	})

	t.Run("definition service unavailable passes through", func(t *testing.T) {
		svc, _, _, _, _, _, definitions, _ := newInstanceServiceHarness()
		definitions.err = errors.New("upstream unavailable")
		_, err := svc.Start(context.Background(), port.StartInstanceInput{TenantID: uuid.New(), WorkflowVersionID: uuid.New(), SubjectID: "SUBJ-1", BusinessKey: "TND-009", StartedByUserID: uuid.New()})
		require.Error(t, err)
	})

	t.Run("duplicate business key", func(t *testing.T) {
		svc, instances, _, _, _, _, definitions, _ := newInstanceServiceHarness()
		tenantID := uuid.New()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), uuid.New(), compiledPlanJSON(t))
		existing := &domain.Instance{ID: uuid.New(), TenantID: tenantID, SubjectID: "SUBJ-1", BusinessKey: "TND-010", Status: domain.InstanceStatusRunning}
		instances.byID[existing.ID] = existing

		_, err := svc.Start(context.Background(), port.StartInstanceInput{TenantID: tenantID, WorkflowVersionID: uuid.New(), SubjectID: "SUBJ-1", BusinessKey: "TND-010", StartedByUserID: uuid.New()})
		assert.ErrorIs(t, err, port.ErrDuplicateBusinessKey)
	})

	t.Run("one subject holds several concurrent instances", func(t *testing.T) {
		svc, instances, _, _, _, _, definitions, _ := newInstanceServiceHarness()
		tenantID := uuid.New()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), uuid.New(), compiledPlanJSON(t))

		for _, key := range []string{"stage-prequalification", "stage-bid-no-bid"} {
			_, err := svc.Start(context.Background(), port.StartInstanceInput{
				TenantID: tenantID, WorkflowVersionID: uuid.New(),
				SubjectID: "TND-001", BusinessKey: key, StartedByUserID: uuid.New(),
			})
			require.NoError(t, err, "a second workflow for the same subject must start")
		}
		assert.Len(t, instances.byID, 2)
	})

	t.Run("the same business key under a different subject does not collide", func(t *testing.T) {
		svc, instances, _, _, _, _, definitions, _ := newInstanceServiceHarness()
		tenantID := uuid.New()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), uuid.New(), compiledPlanJSON(t))

		for _, subject := range []string{"TND-001", "TND-002"} {
			_, err := svc.Start(context.Background(), port.StartInstanceInput{
				TenantID: tenantID, WorkflowVersionID: uuid.New(),
				SubjectID: subject, BusinessKey: "stage-prequalification", StartedByUserID: uuid.New(),
			})
			require.NoError(t, err)
		}
		assert.Len(t, instances.byID, 2)
	})

	t.Run("the Temporal id carries subject and key", func(t *testing.T) {
		svc, _, _, _, _, _, definitions, _ := newInstanceServiceHarness()
		tenantID := uuid.New()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), uuid.New(), compiledPlanJSON(t))

		got, err := svc.Start(context.Background(), port.StartInstanceInput{
			TenantID: tenantID, WorkflowVersionID: uuid.New(),
			SubjectID: "TND-001", BusinessKey: "stage-bid-no-bid", StartedByUserID: uuid.New(),
		})
		require.NoError(t, err)
		assert.Equal(t, tenantID.String()+":TND-001:stage-bid-no-bid", got.TemporalWorkflowID)
	})

	t.Run("StartWorkflow failure after commit is returned, instance stays committed", func(t *testing.T) {
		svc, instances, _, _, _, temporal, definitions, _ := newInstanceServiceHarness()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), uuid.New(), compiledPlanJSON(t))
		temporal.startFunc = func(context.Context, port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
			return port.StartWorkflowOutput{}, errors.New("temporal frontend unreachable")
		}

		_, err := svc.Start(context.Background(), port.StartInstanceInput{TenantID: uuid.New(), WorkflowVersionID: uuid.New(), SubjectID: "SUBJ-1", BusinessKey: "TND-011", StartedByUserID: uuid.New()})
		require.Error(t, err)
		assert.Len(t, instances.byID, 1, "the DB write is not rolled back on a post-commit StartWorkflow failure")
	})

	t.Run("a stage with no NodeID keys on its Type instead", func(t *testing.T) {
		svc, _, _, _, outbox, _, definitions, _ := newInstanceServiceHarness()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), uuid.New(), compiledPlanJSON(t, dsl.StageDef{Type: "gateway", DefaultAssignees: []string{uuid.NewString()}}))

		_, err := svc.Start(context.Background(), port.StartInstanceInput{TenantID: uuid.New(), WorkflowVersionID: uuid.New(), SubjectID: "SUBJ-1", BusinessKey: "TND-012", StartedByUserID: uuid.New()})
		require.NoError(t, err)
		require.Len(t, outbox.enqueued, 1)
	})

	t.Run("a stage naming no default assignee is rejected and named", func(t *testing.T) {
		svc, instances, _, _, _, _, definitions, _ := newInstanceServiceHarness()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), uuid.New(), compiledPlanJSON(t,
			assignedStage("review"),
			dsl.StageDef{Type: "userTask", NodeID: "approve"},
		))

		_, err := svc.Start(context.Background(), port.StartInstanceInput{
			TenantID: uuid.New(), WorkflowVersionID: uuid.New(), SubjectID: "SUBJ-1", BusinessKey: "TND-013", StartedByUserID: uuid.New(),
		})

		var ineligible *port.AssigneeIneligibleError
		require.ErrorAs(t, err, &ineligible)
		assert.Equal(t, []string{"finance/approve"}, ineligible.Nodes,
			"the offending node is named, and the assigned one is not")
		assert.Empty(t, instances.byID, "no instance row is written")
	})

	t.Run("a stage whose only default assignee is unparseable is rejected", func(t *testing.T) {
		svc, _, _, _, _, _, definitions, _ := newInstanceServiceHarness()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), uuid.New(), compiledPlanJSON(t,
			dsl.StageDef{Type: "userTask", NodeID: "review", DefaultAssignees: []string{"not-a-uuid"}},
		))

		_, err := svc.Start(context.Background(), port.StartInstanceInput{
			TenantID: uuid.New(), WorkflowVersionID: uuid.New(), SubjectID: "SUBJ-1", BusinessKey: "TND-014", StartedByUserID: uuid.New(),
		})

		var ineligible *port.AssigneeIneligibleError
		require.ErrorAs(t, err, &ineligible)
		assert.Equal(t, []string{"finance/review"}, ineligible.Nodes)
	})

	t.Run("an override supplies the assignee a stage lacks", func(t *testing.T) {
		svc, _, _, _, _, _, definitions, _ := newInstanceServiceHarness()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), uuid.New(), compiledPlanJSON(t,
			dsl.StageDef{Type: "userTask", NodeID: "review"},
		))

		_, err := svc.Start(context.Background(), port.StartInstanceInput{
			TenantID: uuid.New(), WorkflowVersionID: uuid.New(), SubjectID: "SUBJ-1", BusinessKey: "TND-015", StartedByUserID: uuid.New(),
			OverrideMap: map[string]uuid.UUID{"finance/review": uuid.New()},
		})
		require.NoError(t, err)
	})

	t.Run("send and receive stages need no assignee", func(t *testing.T) {
		svc, _, _, _, _, _, definitions, _ := newInstanceServiceHarness()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), uuid.New(), compiledPlanJSON(t,
			assignedStage("review"),
			dsl.StageDef{Type: string(enums.StageTypeSendTask), NodeID: "notify"},
			dsl.StageDef{Type: string(enums.StageTypeReceiveTask), NodeID: "await-reply"},
		))

		_, err := svc.Start(context.Background(), port.StartInstanceInput{
			TenantID: uuid.New(), WorkflowVersionID: uuid.New(), SubjectID: "SUBJ-1", BusinessKey: "TND-016", StartedByUserID: uuid.New(),
		})
		require.NoError(t, err, "message stages resolve through the message buffer and create no task")
	})

	t.Run("a connector stage needs no assignee", func(t *testing.T) {
		svc, _, _, _, _, _, definitions, _ := newInstanceServiceHarness()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), uuid.New(), compiledPlanJSON(t,
			dsl.StageDef{Type: "serviceTask", NodeID: "fetch", ConnectorType: "rest-call"},
		))

		_, err := svc.Start(context.Background(), port.StartInstanceInput{
			TenantID: uuid.New(), WorkflowVersionID: uuid.New(), SubjectID: "SUBJ-1", BusinessKey: "TND-017", StartedByUserID: uuid.New(),
		})
		require.NoError(t, err)
	})

	t.Run("a BuildEnvelope failure fails Start", func(t *testing.T) {
		instances, tasks, assignments := newFakeInstanceRepo(), newFakeTaskRepo(), newFakeAssignmentRepo()
		outbox, temporal, definitions, eligibility := &fakeOutbox{}, &fakeTemporalClient{}, &fakeDefinitionClient{}, &fakeEligibilityChecker{}
		svc := &service.InstanceService{
			Instances: instances, Tasks: tasks, Assignments: assignments, Outbox: outbox,
			Transactor: fakeTransactor{}, Temporal: temporal, Definitions: definitions, Eligibility: eligibility,
			Queues: newFakeActiveTaskQueueRepo(), Validator: failingValidator{},
		}
		definitions.resp = publishedCompiledWorkflow(uuid.New(), uuid.New(), compiledPlanJSON(t))

		_, err := svc.Start(context.Background(), port.StartInstanceInput{TenantID: uuid.New(), WorkflowVersionID: uuid.New(), SubjectID: "SUBJ-1", BusinessKey: "TND-013", StartedByUserID: uuid.New()})
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

	t.Run("admin caller leaves the repo filter unscoped", func(t *testing.T) {
		svc, instances, _, _, _, _, _, _ := newInstanceServiceHarness()
		_, err := svc.List(context.Background(), uuid.New(), port.ReadScope{IsAdmin: true}, port.InstanceFilter{}, port.Page{Limit: 10})
		require.NoError(t, err)
		assert.Nil(t, instances.lastFilter.Scope)
	})

	t.Run("non-admin caller's departments and identity reach the repo filter", func(t *testing.T) {
		svc, instances, _, _, _, _, _, _ := newInstanceServiceHarness()
		callerUserID := uuid.New()
		deptA, deptB := uuid.New(), uuid.New()
		scope := port.ReadScope{
			CallerUserID: callerUserID,
			Departments:  []port.DepartmentRole{{DepartmentID: deptA, Role: "member"}, {DepartmentID: deptB, Role: "lead"}},
		}
		_, err := svc.List(context.Background(), uuid.New(), scope, port.InstanceFilter{}, port.Page{Limit: 10})
		require.NoError(t, err)
		require.NotNil(t, instances.lastFilter.Scope)
		assert.Equal(t, callerUserID, instances.lastFilter.Scope.CallerUserID)
		assert.ElementsMatch(t, []uuid.UUID{deptA, deptB}, instances.lastFilter.Scope.DepartmentIDs)
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
		_, _, err := svc.Get(context.Background(), tenantID, inst.ID, port.ReadScope{
			CallerUserID: uuid.New(),
			Departments:  []port.DepartmentRole{{DepartmentID: uuid.New(), Role: "approver"}},
		})
		assert.ErrorIs(t, err, port.ErrNotAuthorizedForRead)
	})

	t.Run("caller whose department matches the task is authorized", func(t *testing.T) {
		got, _, err := svc.Get(context.Background(), tenantID, inst.ID, port.ReadScope{
			Departments: []port.DepartmentRole{{DepartmentID: task.DepartmentID, Role: "reviewer"}},
		})
		require.NoError(t, err)
		assert.Equal(t, inst.ID, got.ID)
	})

	t.Run("caller with roles in multiple departments, one matching, is authorized", func(t *testing.T) {
		got, _, err := svc.Get(context.Background(), tenantID, inst.ID, port.ReadScope{
			Departments: []port.DepartmentRole{
				{DepartmentID: uuid.New(), Role: "member"},
				{DepartmentID: task.DepartmentID, Role: "approver"},
			},
		})
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
		{"pause from degraded", domain.InstanceStatusDegraded, func(s *service.InstanceService, t, i, a uuid.UUID, rv int64) error {
			return s.Pause(context.Background(), t, i, a, "reason", rv)
		}, port.SignalInstancePause},
		{"resume from degraded", domain.InstanceStatusDegraded, func(s *service.InstanceService, t, i, a uuid.UUID, rv int64) error {
			return s.Resume(context.Background(), t, i, a, rv)
		}, port.SignalInstanceResume},
		{"cancel from running", domain.InstanceStatusRunning, func(s *service.InstanceService, t, i, a uuid.UUID, rv int64) error {
			return s.Cancel(context.Background(), t, i, a, "reason", rv)
		}, port.SignalInstanceCancel},
		{"force-back from running", domain.InstanceStatusRunning, func(s *service.InstanceService, t, i, a uuid.UUID, rv int64) error {
			return s.ForceBack(context.Background(), t, i, a, "", rv)
		}, port.SignalInstanceForceBack},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			svc, instances, tasks, _, _, temporal, _, _ := newInstanceServiceHarness()
			tenantID, actorID := uuid.New(), uuid.New()
			inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: tc.status, RecordVersion: 3, TemporalWorkflowID: "tenant:biz"}
			instances.byID[inst.ID] = inst
			done := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: inst.ID, Status: domain.TaskStatusCompleted}
			tasks.byID[done.ID] = done

			err := tc.call(svc, tenantID, inst.ID, actorID, 3)
			require.NoError(t, err)
			require.Len(t, temporal.signals, 1)
			assert.Equal(t, tc.wantSignal, temporal.signals[0].SignalName)
			assert.Equal(t, "tenant:biz", temporal.signals[0].TemporalWorkflowID)
			assert.Equal(t, inst.ID, temporal.signals[0].InstanceID)
		})
	}

	t.Run("force-back with no completed step is refused", func(t *testing.T) {
		svc, instances, _, _, _, temporal, _, _ := newInstanceServiceHarness()
		tenantID := uuid.New()
		inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning, RecordVersion: 3, TemporalWorkflowID: "tenant:biz"}
		instances.byID[inst.ID] = inst

		err := svc.ForceBack(context.Background(), tenantID, inst.ID, uuid.New(), "", 3)
		assert.ErrorIs(t, err, port.ErrInvalidInstanceState)
		assert.Empty(t, temporal.signals)
	})

	t.Run("force-back of a degraded instance needs no completed step and passes the node key", func(t *testing.T) {
		svc, instances, _, _, _, temporal, _, _ := newInstanceServiceHarness()
		tenantID := uuid.New()
		inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusDegraded, RecordVersion: 3, TemporalWorkflowID: "tenant:biz"}
		instances.byID[inst.ID] = inst

		require.NoError(t, svc.ForceBack(context.Background(), tenantID, inst.ID, uuid.New(), "billing/prep", 3))
		require.Len(t, temporal.signals, 1)
		assert.Contains(t, fmt.Sprint(temporal.signals[0].Payload), "billing/prep")
	})

	t.Run("force-back surfaces a task listing failure", func(t *testing.T) {
		svc, instances, tasks, _, _, temporal, _, _ := newInstanceServiceHarness()
		tenantID := uuid.New()
		inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning, RecordVersion: 3, TemporalWorkflowID: "tenant:biz"}
		instances.byID[inst.ID] = inst
		tasks.listErr = assert.AnError

		assert.ErrorIs(t, svc.ForceBack(context.Background(), tenantID, inst.ID, uuid.New(), "", 3), assert.AnError)
		assert.Empty(t, temporal.signals)
	})

	t.Run("force-back of an unknown instance", func(t *testing.T) {
		svc, _, _, _, _, _, _, _ := newInstanceServiceHarness()
		assert.Error(t, svc.ForceBack(context.Background(), uuid.New(), uuid.New(), uuid.New(), "", 1))
	})

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

	t.Run("resume of a running instance is refused", func(t *testing.T) {
		svc, instances, _, _, _, temporal, _, _ := newInstanceServiceHarness()
		tenantID := uuid.New()
		inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning, RecordVersion: 1}
		instances.byID[inst.ID] = inst

		err := svc.Resume(context.Background(), tenantID, inst.ID, uuid.New(), 1)
		assert.ErrorIs(t, err, port.ErrInvalidInstanceState)
		assert.Empty(t, temporal.signals)
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

	// TestInstanceService_LifecycleSignals/pause-resume-carry-admin-initiator
	// is the regression test for the initiator finding: a direct admin-API
	// Pause/Resume call must report initiator=admin on the signal wire, not
	// leave it empty for handleInstancePause/handleInstanceResume to
	// silently drop — Pause/Resume are otherwise this package's own callers
	// of signalLifecycle, distinct from the reconcilers' non-admin triggers
	// covered in tenant_lifecycle_reconciler_test.go/ooo_availability_reconciler_test.go/user_task_pauser_test.go.
	t.Run("pause and resume carry admin initiator", func(t *testing.T) {
		svc, instances, _, _, _, temporal, _, _ := newInstanceServiceHarness()
		tenantID := uuid.New()

		pausedInst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning, RecordVersion: 1, TemporalWorkflowID: "tenant:biz"}
		instances.byID[pausedInst.ID] = pausedInst
		require.NoError(t, svc.Pause(context.Background(), tenantID, pausedInst.ID, uuid.New(), "reason", 1))
		require.Len(t, temporal.signals, 1)
		b, err := json.Marshal(temporal.signals[0].Payload)
		require.NoError(t, err)
		assert.Contains(t, string(b), `"Initiator":"admin"`)

		resumedInst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusPaused, RecordVersion: 1, TemporalWorkflowID: "tenant:biz"}
		instances.byID[resumedInst.ID] = resumedInst
		require.NoError(t, svc.Resume(context.Background(), tenantID, resumedInst.ID, uuid.New(), 1))
		require.Len(t, temporal.signals, 2)
		b, err = json.Marshal(temporal.signals[1].Payload)
		require.NoError(t, err)
		assert.Contains(t, string(b), `"Initiator":"admin"`)
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
		require.NotEmpty(t, temporal.terminateCalls())
	})

	t.Run("an execution created after the first close and before the write is closed by the second", func(t *testing.T) {
		svc, instances, _, _, _, temporal, _, _ := newInstanceServiceHarness()
		tenantID := uuid.New()
		inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning, RecordVersion: 1, TemporalWorkflowID: "tenant:biz"}
		instances.byID[inst.ID] = inst
		live := map[string]bool{}
		calls := 0
		temporal.terminateFunc = func(_ context.Context, id, _, _ string) error {
			calls++
			delete(live, id)
			if calls == 1 {
				live[id] = true
			}
			return nil
		}

		require.NoError(t, svc.Terminate(context.Background(), tenantID, inst.ID, uuid.New(), "reason"))
		assert.Empty(t, live)
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
		temporal.terminateFunc = func(context.Context, string, string, string) error { return errors.New("boom") }

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

	t.Run("a TerminateWorkflow failure is returned and nothing is written", func(t *testing.T) {
		svc, instances, tasks, _, outbox, temporal, _, _ := newInstanceServiceHarness()
		tenantID := uuid.New()
		inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning, RecordVersion: 1, TemporalWorkflowID: "tenant:biz"}
		instances.byID[inst.ID] = inst
		task := &domain.Task{ID: uuid.New(), TenantID: tenantID, WorkflowInstanceID: inst.ID, Status: domain.TaskStatusReady, RecordVersion: 1}
		tasks.byID[task.ID] = task
		temporal.terminateFunc = func(context.Context, string, string, string) error { return errors.New("temporal unavailable") }

		err := svc.Terminate(context.Background(), tenantID, inst.ID, uuid.New(), "reason")
		require.Error(t, err)
		assert.Equal(t, domain.InstanceStatusRunning, instances.byID[inst.ID].Status, "a live execution must never sit under a TERMINATED row")
		assert.Equal(t, domain.TaskStatusReady, tasks.byID[task.ID].Status)
		assert.Empty(t, outbox.enqueued)
	})

	t.Run("a database failure after the execution was closed is returned and a retry completes it", func(t *testing.T) {
		svc, instances, _, _, _, _, _, _ := newInstanceServiceHarness()
		tenantID := uuid.New()
		inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning, RecordVersion: 1, TemporalWorkflowID: "tenant:biz"}
		instances.byID[inst.ID] = inst
		instances.updateErrFor = map[uuid.UUID]error{inst.ID: assert.AnError}

		err := svc.Terminate(context.Background(), tenantID, inst.ID, uuid.New(), "reason")
		require.ErrorIs(t, err, assert.AnError)
		assert.Equal(t, domain.InstanceStatusRunning, instances.byID[inst.ID].Status)

		instances.updateErrFor = nil
		require.NoError(t, svc.Terminate(context.Background(), tenantID, inst.ID, uuid.New(), "reason"))
		assert.Equal(t, domain.InstanceStatusTerminated, instances.byID[inst.ID].Status)
	})

	t.Run("the version is re-read after the execution is closed, so a concurrent write does not fail the termination", func(t *testing.T) {
		svc, instances, _, _, _, temporal, _, _ := newInstanceServiceHarness()
		tenantID := uuid.New()
		inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning, RecordVersion: 1, TemporalWorkflowID: "tenant:biz"}
		instances.byID[inst.ID] = inst
		temporal.terminateFunc = func(context.Context, string, string, string) error {
			written := *instances.byID[inst.ID]
			written.RecordVersion++
			instances.byID[inst.ID] = &written
			return nil
		}

		require.NoError(t, svc.Terminate(context.Background(), tenantID, inst.ID, uuid.New(), "reason"))
		assert.Equal(t, domain.InstanceStatusTerminated, instances.byID[inst.ID].Status)
	})

	t.Run("a failure to close the execution again after the write is logged and the termination stands", func(t *testing.T) {
		svc, instances, _, _, _, temporal, _, _ := newInstanceServiceHarness()
		tenantID := uuid.New()
		inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning, RecordVersion: 1, TemporalWorkflowID: "tenant:biz"}
		instances.byID[inst.ID] = inst
		calls := 0
		temporal.terminateFunc = func(context.Context, string, string, string) error {
			calls++
			if calls == 2 {
				return errors.New("temporal unavailable")
			}
			return nil
		}

		require.NoError(t, svc.Terminate(context.Background(), tenantID, inst.ID, uuid.New(), "reason"))
		assert.Equal(t, domain.InstanceStatusTerminated, instances.byID[inst.ID].Status)
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
	// Start reads the Definition Service every time: Definition evicts its
	// own copy when a version is invalidated or archived, while this cache
	// would keep answering valid for up to an hour (execution LLD §1).
	t.Run("start reads Definitions even when the version is cached", func(t *testing.T) {
		for name, tc := range map[string]struct {
			mutate func(*port.CompiledWorkflow)
			want   error
		}{
			"invalidated": {func(cw *port.CompiledWorkflow) { cw.IsValid = false }, port.ErrVersionInvalid},
			"archived":    {func(cw *port.CompiledWorkflow) { cw.Status = "ARCHIVED" }, port.ErrVersionNotPublished},
		} {
			t.Run(name, func(t *testing.T) {
				svc, _, _, _, _, _, definitions, _ := newInstanceServiceHarness()
				cache := newFakeCacheStore()
				svc.Cache = cache
				tenantID, versionID := uuid.New(), uuid.New()

				cached := publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))
				raw, err := json.Marshal(cached)
				require.NoError(t, err)
				cache.values["compiled_plan:"+tenantID.String()+":"+versionID.String()] = string(raw)

				current := publishedCompiledWorkflow(cached.WorkflowID, versionID, cached.CompiledPlanJSON)
				tc.mutate(current)
				definitions.resp = current

				_, err = svc.Start(context.Background(), port.StartInstanceInput{
					TenantID: tenantID, WorkflowVersionID: versionID, BusinessKey: "TND-CACHE-1", StartedByUserID: uuid.New(),
				})
				require.ErrorIs(t, err, tc.want)
			})
		}
	})

	t.Run("a start writes the fetched plan through for running instances", func(t *testing.T) {
		svc, _, _, _, _, _, definitions, _ := newInstanceServiceHarness()
		cache := newFakeCacheStore()
		svc.Cache = cache
		tenantID, versionID := uuid.New(), uuid.New()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))

		_, err := svc.Start(context.Background(), port.StartInstanceInput{
			TenantID: tenantID, WorkflowVersionID: versionID, BusinessKey: "TND-CACHE-2", StartedByUserID: uuid.New(),
		})
		require.NoError(t, err)
		require.Contains(t, cache.values, "compiled_plan:"+tenantID.String()+":"+versionID.String())
	})

	t.Run("a cache write-through failure after a miss still returns the fetched plan", func(t *testing.T) {
		svc, _, _, _, _, _, definitions, _ := newInstanceServiceHarness()
		cache := newFakeCacheStore()
		cache.setErr = errors.New("valkey unavailable")
		svc.Cache = cache
		log := &fakeLogger{}
		svc.Log = log
		tenantID, versionID := uuid.New(), uuid.New()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))

		_, err := svc.Start(context.Background(), port.StartInstanceInput{
			TenantID: tenantID, WorkflowVersionID: versionID, BusinessKey: "TND-CACHE-6", StartedByUserID: uuid.New(),
		})
		require.NoError(t, err, "a write-through failure must be fail-open, never block Start")
		assert.NotEmpty(t, log.warnCalls)
	})

	t.Run("an unreadable cache does not affect Start", func(t *testing.T) {
		svc, _, _, _, _, _, definitions, _ := newInstanceServiceHarness()
		cache := newFakeCacheStore()
		cache.getErr = errors.New("valkey unavailable")
		svc.Cache = cache
		tenantID, versionID := uuid.New(), uuid.New()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))

		_, err := svc.Start(context.Background(), port.StartInstanceInput{
			TenantID: tenantID, WorkflowVersionID: versionID, BusinessKey: "TND-CACHE-3", StartedByUserID: uuid.New(),
		})
		require.NoError(t, err)
	})

	// The 503 the handler answers with carries a fixed detail, so the cause
	// must reach the log here or nowhere.
	t.Run("a Definitions failure is logged with its cause", func(t *testing.T) {
		svc, _, _, _, _, _, definitions, _ := newInstanceServiceHarness()
		log := &fakeLogger{}
		svc.Log = log
		definitions.err = fmt.Errorf("%w: GET http://definition:8080/x: connection refused", port.ErrDefinitionUnavailable)

		_, err := svc.Start(context.Background(), port.StartInstanceInput{
			TenantID: uuid.New(), WorkflowVersionID: uuid.New(), BusinessKey: "TND-CACHE-7", StartedByUserID: uuid.New(),
		})
		require.ErrorIs(t, err, port.ErrDefinitionUnavailable)
		require.Len(t, log.warnFields, 1)
		assert.Equal(t, definitions.err.Error(), log.warnFields[0]["error"])
	})
}

type failingQueueLookup struct {
	port.ActiveTaskQueueRepository
	err error
}

func (f failingQueueLookup) GetByQueueName(context.Context, string) (*domain.ActiveTaskQueue, error) {
	return nil, f.err
}
