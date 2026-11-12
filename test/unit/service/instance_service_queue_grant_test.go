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
)

type fakeFeatureGrants struct {
	has         bool
	err         error
	asked       []string
	askedTenant []uuid.UUID
}

func (g *fakeFeatureGrants) HasFeature(_ context.Context, tenantID uuid.UUID, feature string) (bool, error) {
	g.asked = append(g.asked, feature)
	g.askedTenant = append(g.askedTenant, tenantID)
	return g.has, g.err
}

const planQueue = "wf-queue-default"

// startOnQueue starts one instance for tenantID and returns the queue it was started on.
func startOnQueue(t *testing.T, grants port.FeatureGrants, queues *fakeActiveTaskQueueRepo, tenantID uuid.UUID) (queue string, log *fakeLogger) {
	t.Helper()
	svc, instances, _, _, _, temporal, definitions, _ := newInstanceServiceHarness()
	svc.Queues = queues
	svc.Grants = grants
	log = &fakeLogger{}
	svc.Log = log
	versionID := uuid.New()
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
	assert.Equal(t, startedOn, instances.byID[got.ID].TaskQueue)
	return startedOn, log
}

func TestStart_QueueFollowsTheDedicatedQueueGrant(t *testing.T) {
	t.Run("a grant with no registered queue starts on the plan queue and registers the tenant queue for the next start", func(t *testing.T) {
		tenantID, queues := uuid.New(), newFakeActiveTaskQueueRepo()
		grants := &fakeFeatureGrants{has: true}
		name := domain.TenantTaskQueueName(tenantID)

		queue, _ := startOnQueue(t, grants, queues, tenantID)

		assert.Equal(t, planQueue, queue, "workers poll a new queue only on their topology interval, so this start must not wait on it")
		assert.Equal(t, []string{domain.FeatureDedicatedWorkflowQueue}, grants.asked)
		assert.Equal(t, []uuid.UUID{tenantID}, grants.askedTenant)
		assert.Equal(t, []registerCall{{TenantID: tenantID, QueueName: name}}, queues.registered)
		assert.True(t, queues.byName[name].Routable)

		next, _ := startOnQueue(t, grants, queues, tenantID)
		assert.Equal(t, name, next, "the next start uses the registered queue")
	})

	t.Run("a grant with a routable registered queue starts on it and writes nothing", func(t *testing.T) {
		tenantID, queues := uuid.New(), newFakeActiveTaskQueueRepo()
		name := domain.TenantTaskQueueName(tenantID)
		queues.byName[name] = &domain.ActiveTaskQueue{TenantID: tenantID, QueueName: name, Routable: true}

		queue, _ := startOnQueue(t, &fakeFeatureGrants{has: true}, queues, tenantID)

		assert.Equal(t, name, queue)
		assert.Empty(t, queues.registered, "no upsert on every start of a granted tenant")
	})

	t.Run("a grant makes a queue that stopped routing routable again", func(t *testing.T) {
		tenantID, queues := uuid.New(), newFakeActiveTaskQueueRepo()
		name := domain.TenantTaskQueueName(tenantID)
		queues.byName[name] = &domain.ActiveTaskQueue{TenantID: tenantID, QueueName: name, Routable: false}

		queue, _ := startOnQueue(t, &fakeFeatureGrants{has: true}, queues, tenantID)

		assert.Equal(t, name, queue)
		assert.True(t, queues.byName[name].Routable)
		assert.Equal(t, []registerCall{{TenantID: tenantID, QueueName: name}}, queues.registered)
	})

	t.Run("no grant starts on the plan's queue and stops the registry row routing, so the fallback agrees", func(t *testing.T) {
		tenantID, queues := uuid.New(), newFakeActiveTaskQueueRepo()
		name := domain.TenantTaskQueueName(tenantID)
		queues.byName[name] = &domain.ActiveTaskQueue{TenantID: tenantID, QueueName: name, Routable: true}
		grants := &fakeFeatureGrants{has: false}

		queue, _ := startOnQueue(t, grants, queues, tenantID)

		assert.Equal(t, planQueue, queue)
		assert.Equal(t, []uuid.UUID{tenantID}, grants.askedTenant)
		assert.Equal(t, []string{name}, queues.stopped)
		assert.False(t, queues.byName[name].Routable)

		// The commercial service is now down: the registry alone must give the same answer.
		queue, _ = startOnQueue(t, &fakeFeatureGrants{err: errors.New("commercial is down")}, queues, tenantID)
		assert.Equal(t, planQueue, queue)
	})

	t.Run("no grant and no registered queue writes nothing", func(t *testing.T) {
		tenantID, queues := uuid.New(), newFakeActiveTaskQueueRepo()

		queue, _ := startOnQueue(t, &fakeFeatureGrants{has: false}, queues, tenantID)

		assert.Equal(t, planQueue, queue)
		assert.Empty(t, queues.byName)
		assert.Empty(t, queues.registered)
		assert.Empty(t, queues.stopped, "an ungranted tenant's every start must not write")
	})

	t.Run("no grant and a queue already stopped writes nothing", func(t *testing.T) {
		tenantID, queues := uuid.New(), newFakeActiveTaskQueueRepo()
		name := domain.TenantTaskQueueName(tenantID)
		queues.byName[name] = &domain.ActiveTaskQueue{TenantID: tenantID, QueueName: name, Routable: false}

		startOnQueue(t, &fakeFeatureGrants{has: false}, queues, tenantID)

		assert.Empty(t, queues.stopped)
	})

	t.Run("a failed stop is logged and never fails the start", func(t *testing.T) {
		tenantID, queues := uuid.New(), newFakeActiveTaskQueueRepo()
		name := domain.TenantTaskQueueName(tenantID)
		queues.byName[name] = &domain.ActiveTaskQueue{TenantID: tenantID, QueueName: name, Routable: true}
		queues.stopRoutingErr = errors.New("registry write failed")

		queue, log := startOnQueue(t, &fakeFeatureGrants{has: false}, queues, tenantID)

		assert.Equal(t, planQueue, queue)
		assert.Equal(t, 1, warnsWithCause(log, "registry write failed"))
	})

	t.Run("an unreadable grant falls back to the registry: a routable row gives the tenant queue", func(t *testing.T) {
		tenantID, queues := uuid.New(), newFakeActiveTaskQueueRepo()
		name := domain.TenantTaskQueueName(tenantID)
		queues.byName[name] = &domain.ActiveTaskQueue{TenantID: tenantID, QueueName: name, Routable: true}

		queue, log := startOnQueue(t, &fakeFeatureGrants{err: errors.New("commercial is down")}, queues, tenantID)

		assert.Equal(t, name, queue)
		assert.Equal(t, 1, warnsWithCause(log, "commercial is down"))
	})

	t.Run("an unreadable grant falls back to the registry: no row gives the plan queue", func(t *testing.T) {
		tenantID, queues := uuid.New(), newFakeActiveTaskQueueRepo()

		queue, log := startOnQueue(t, &fakeFeatureGrants{err: errors.New("commercial is down")}, queues, tenantID)

		assert.Equal(t, planQueue, queue)
		assert.Equal(t, 1, warnsWithCause(log, "commercial is down"))
		assert.Empty(t, queues.byName, "the fallback only reads the registry")
	})

	t.Run("an unreadable grant writes nothing to the registry", func(t *testing.T) {
		tenantID, queues := uuid.New(), newFakeActiveTaskQueueRepo()
		name := domain.TenantTaskQueueName(tenantID)
		queues.byName[name] = &domain.ActiveTaskQueue{TenantID: tenantID, QueueName: name, Routable: true}

		startOnQueue(t, &fakeFeatureGrants{err: errors.New("down")}, queues, tenantID)

		assert.Empty(t, queues.registered)
		assert.Empty(t, queues.stopped)
	})

	t.Run("an unreadable grant with a non-routable row gives the plan queue", func(t *testing.T) {
		tenantID, queues := uuid.New(), newFakeActiveTaskQueueRepo()
		name := domain.TenantTaskQueueName(tenantID)
		queues.byName[name] = &domain.ActiveTaskQueue{TenantID: tenantID, QueueName: name, Routable: false}

		queue, _ := startOnQueue(t, &fakeFeatureGrants{err: errors.New("down")}, queues, tenantID)

		assert.Equal(t, planQueue, queue)
	})

	t.Run("without a grants reader the registry decides, as before", func(t *testing.T) {
		tenantID, queues := uuid.New(), newFakeActiveTaskQueueRepo()
		name := domain.TenantTaskQueueName(tenantID)
		queues.byName[name] = &domain.ActiveTaskQueue{TenantID: tenantID, QueueName: name, Routable: true}

		queue, _ := startOnQueue(t, nil, queues, tenantID)

		assert.Equal(t, name, queue)
	})

	t.Run("a failed registration fails the start", func(t *testing.T) {
		svc, instances, _, _, _, _, definitions, _ := newInstanceServiceHarness()
		queues := newFakeActiveTaskQueueRepo()
		queues.registerErr = errors.New("registry write failed")
		svc.Queues, svc.Grants = queues, &fakeFeatureGrants{has: true}
		versionID := uuid.New()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))

		_, err := svc.Start(context.Background(), port.StartInstanceInput{
			TenantID: uuid.New(), WorkflowVersionID: versionID, SubjectID: "SUBJ-1", BusinessKey: "TND-001",
			ContextJSON: json.RawMessage(`{}`), StartedByUserID: uuid.New(),
		})

		require.ErrorContains(t, err, "registry write failed")
		assert.Empty(t, instances.byID)
	})

	t.Run("an unreadable registry fails the start when the grant is readable", func(t *testing.T) {
		svc, instances, _, _, _, _, definitions, _ := newInstanceServiceHarness()
		queues := newFakeActiveTaskQueueRepo()
		queues.getErr = errors.New("registry read failed")
		svc.Queues, svc.Grants = queues, &fakeFeatureGrants{has: true}
		versionID := uuid.New()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))

		_, err := svc.Start(context.Background(), port.StartInstanceInput{
			TenantID: uuid.New(), WorkflowVersionID: versionID, SubjectID: "SUBJ-1", BusinessKey: "TND-001",
			ContextJSON: json.RawMessage(`{}`), StartedByUserID: uuid.New(),
		})

		require.ErrorContains(t, err, "registry read failed")
		assert.Empty(t, instances.byID)
	})
}
