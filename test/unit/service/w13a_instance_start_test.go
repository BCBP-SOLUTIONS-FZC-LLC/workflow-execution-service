package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

var (
	errW13aDefinitionsDown = errors.New("definition service unreachable")
	errW13aQueueDown       = errors.New("task queue registry unreachable")
)

type w13aLiveLookup struct {
	tenantID          uuid.UUID
	subjectID, bizKey string
	attemptsSoFar     int
}

// w13aStartInstances records the repeat-start lookup and every attempted create.
type w13aStartInstances struct {
	*fakeInstanceRepo
	lookups  []w13aLiveLookup
	attempts []*domain.Instance
}

func (r *w13aStartInstances) GetLiveByBusinessKey(ctx context.Context, tenantID uuid.UUID, subjectID, businessKey string) (*domain.Instance, error) {
	r.lookups = append(r.lookups, w13aLiveLookup{tenantID, subjectID, businessKey, len(r.attempts)})
	return r.fakeInstanceRepo.GetLiveByBusinessKey(ctx, tenantID, subjectID, businessKey)
}

func (r *w13aStartInstances) Create(ctx context.Context, inst *domain.Instance) error {
	r.attempts = append(r.attempts, inst)
	return r.fakeInstanceRepo.Create(ctx, inst)
}

type w13aGate struct {
	err   error
	calls []uuid.UUID
}

func (g *w13aGate) Allow(_ context.Context, tenantID uuid.UUID) error {
	g.calls = append(g.calls, tenantID)
	return g.err
}

// w13aBatchEligibility answers every request with one verdict and records the tenant of each batch.
type w13aBatchEligibility struct {
	ineligible bool
	tenants    []uuid.UUID
}

func (e *w13aBatchEligibility) CheckEligibility(context.Context, uuid.UUID, uuid.UUID, uuid.UUID, string) (bool, error) {
	return false, errors.New("single eligibility reads are not used by a start")
}

func (e *w13aBatchEligibility) CheckEligibilityBatch(_ context.Context, tenantID uuid.UUID, requests []port.EligibilityCheckRequest) ([]port.EligibilityResult, error) {
	e.tenants = append(e.tenants, tenantID)
	results := make([]port.EligibilityResult, len(requests))
	for i := range results {
		results[i] = port.EligibilityResult{Eligible: !e.ineligible}
	}
	return results, nil
}

// w13aQueues has no queue registered, so a start falls back to the plan's queue, unless err is set.
type w13aQueues struct {
	port.ActiveTaskQueueRepository
	err   error
	names []string
}

func (q *w13aQueues) GetByQueueName(_ context.Context, name string) (*domain.ActiveTaskQueue, error) {
	q.names = append(q.names, name)
	if q.err != nil {
		return nil, q.err
	}
	return nil, domain.ErrNotFound
}

type w13aStartWorld struct {
	svc         *service.InstanceService
	instances   *w13aStartInstances
	definitions *w13aDefinitions
	gate        *w13aGate
	eligibility *w13aBatchEligibility
	queues      *w13aQueues
	starts      int
	tenantID    uuid.UUID
	versionID   uuid.UUID
	caller      uuid.UUID
	input       port.StartInstanceInput
	seeded      domain.Instance
}

func w13aNewStartWorld(t *testing.T) *w13aStartWorld {
	t.Helper()
	svc, instances, _, _, _, temporal, _, _ := newInstanceServiceHarness()
	w := &w13aStartWorld{
		svc:         svc,
		instances:   &w13aStartInstances{fakeInstanceRepo: instances},
		gate:        &w13aGate{},
		eligibility: &w13aBatchEligibility{},
		queues:      &w13aQueues{},
		tenantID:    uuid.New(),
		versionID:   uuid.New(),
		caller:      uuid.New(),
	}
	w.definitions = &w13aDefinitions{resp: publishedCompiledWorkflow(uuid.New(), w.versionID,
		compiledPlanJSON(t, assignedStage("review"), assignedStage("approve")))}
	svc.Instances = w.instances
	svc.Definitions = w.definitions
	svc.Gate = w.gate
	svc.Eligibility = w.eligibility
	svc.Queues = w.queues
	temporal.startFunc = func(_ context.Context, in port.StartWorkflowInput) (port.StartWorkflowOutput, error) {
		w.starts++
		return port.StartWorkflowOutput{TemporalWorkflowID: in.TemporalWorkflowID, TemporalRunID: "run-new"}, nil
	}
	w.input = port.StartInstanceInput{
		TenantID: w.tenantID, WorkflowVersionID: w.versionID, SubjectID: "SUBJ-1", BusinessKey: "TND-001", StartedByUserID: w.caller,
	}
	return w
}

// seedLive stores a live, started instance of the world's start, with the given stored context and override map.
func (w *w13aStartWorld) seedLive(contextJSON, overrideJSON []byte) *domain.Instance {
	inst := &domain.Instance{
		ID: uuid.New(), TenantID: w.tenantID, WorkflowVersionID: w.versionID, SubjectID: w.input.SubjectID,
		BusinessKey: w.input.BusinessKey, StartedByUserID: w.caller, Status: domain.InstanceStatusRunning,
		TemporalRunID: "run-seeded", TemporalWorkflowID: "tenant:seeded", ContextJSON: contextJSON, OverrideMap: overrideJSON, RecordVersion: 1,
	}
	w.instances.byID[inst.ID] = inst
	w.seeded = *inst
	return inst
}

// requireSameStart checks that the start was answered with the stored instance and wrote nothing.
func (w *w13aStartWorld) requireSameStart(t *testing.T, stored *domain.Instance, got *port.Instance, err error) {
	t.Helper()
	require.NoError(t, err)
	assert.Equal(t, stored.ID, got.ID)
	assert.Len(t, w.instances.byID, 1)
	assert.Empty(t, w.instances.attempts, "no second row is attempted")
	assert.Empty(t, w.definitions.calls, "a repeat is matched before the Definitions read")
	assert.Zero(t, w.starts, "the workflow is not started again")
	assert.Equal(t, []w13aLiveLookup{{w.tenantID, w.input.SubjectID, w.input.BusinessKey, 0}}, w.instances.lookups)
}

// requireOtherStart checks that the start was refused as a duplicate of the stored instance, after the plan read.
func (w *w13aStartWorld) requireOtherStart(t *testing.T, stored *domain.Instance, err error) {
	t.Helper()
	require.ErrorIs(t, err, port.ErrDuplicateBusinessKey)
	assert.Len(t, w.instances.byID, 1)
	assert.Equal(t, w.seeded, *w.instances.byID[stored.ID], "the stored row is untouched")
	assert.Equal(t, []w13aDefinitionCall{{w.tenantID, w.versionID}}, w.definitions.calls, "control: the start went past the repeat match")
	assert.Len(t, w.instances.attempts, 1, "control: the start tried to write its own row")
	assert.Zero(t, w.starts)
}

func TestInstanceService_StartRetry_AnEmptyOrNullContextIsTheEmptyObjectAndOtherJSONIsComparedByValue(t *testing.T) {
	cases := []struct {
		name      string
		stored    string
		requested string
		same      bool
	}{
		{"a stored empty context equals a requested empty one", "", "", true},
		{"a stored empty context equals a requested empty object", "", "{}", true},
		{"a stored whitespace-only context equals a requested empty object", " \n\t ", "{}", true},
		{"a stored null equals a requested empty object", "null", "{}", true},
		{"a stored empty object equals a requested null", "{}", "null", true},
		{"a stored empty object equals a requested whitespace-only context", "{}", "  ", true},
		{"a stored empty object equals a requested empty context", "{}", "", true},
		{"an array equals the same array with other spacing", "[1,2]", "[1, 2]", true},
		{"an array is not the same array in another order", "[1,2]", "[2,1]", false},
		{"an empty object is not a context with a key", "{}", `{"a":1}`, false},
		{"a context with a key is not an empty object", `{"a":1}`, "{}", false},
		{"an empty object is not an empty array", "{}", "[]", false},
		{"a requested context that is not valid JSON is not the same start", "{}", `{"a":`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := w13aNewStartWorld(t)
			stored := w.seedLive([]byte(tc.stored), nil)
			w.input.ContextJSON = json.RawMessage(tc.requested)

			got, err := w.svc.Start(context.Background(), w.input)

			if tc.same {
				w.requireSameStart(t, stored, got, err)
				return
			}
			w.requireOtherStart(t, stored, err)
		})
	}
}

func TestInstanceService_StartRetry_ALiveInstanceInAnyOpenStatusIsTheSameStart(t *testing.T) {
	statuses := []domain.InstanceStatus{
		domain.InstanceStatusRunning, domain.InstanceStatusPaused, domain.InstanceStatusDegraded,
	}
	for _, status := range statuses {
		t.Run(string(status), func(t *testing.T) {
			w := w13aNewStartWorld(t)
			stored := w.seedLive(nil, nil)
			stored.Status = status
			w.seeded = *stored

			got, err := w.svc.Start(context.Background(), w.input)

			w.requireSameStart(t, stored, got, err)
		})
	}
}

func TestInstanceService_StartRetry_AnOverrideMapIsComparedByNodeKeyAndUserId(t *testing.T) {
	first, second := uuid.New(), uuid.New()
	review, approve := "finance/review", "finance/approve"
	cases := []struct {
		name      string
		stored    map[string]string
		storedRaw string
		requested map[string]uuid.UUID
		same      bool
	}{
		{name: "no stored map equals no requested map", requested: nil, same: true},
		{name: "no stored map equals an empty requested map", requested: map[string]uuid.UUID{}, same: true},
		{name: "a stored empty object equals no requested map", storedRaw: `{}`, same: true},
		{name: "a stored null equals no requested map", storedRaw: `null`, same: true},
		{name: "the same one-entry map is the same start", stored: map[string]string{review: first.String()}, requested: map[string]uuid.UUID{review: first}, same: true},
		{name: "the same two-entry map is the same start", stored: map[string]string{review: first.String(), approve: second.String()}, requested: map[string]uuid.UUID{approve: second, review: first}, same: true},
		{name: "a map of the same size under another node key is another start", stored: map[string]string{review: first.String()}, requested: map[string]uuid.UUID{approve: first}},
		{name: "a map of the same size with another user is another start", stored: map[string]string{review: first.String()}, requested: map[string]uuid.UUID{review: second}},
		{name: "a stored entry with no requested map is another start", stored: map[string]string{review: first.String()}},
		{name: "a requested entry with no stored map is another start", storedRaw: `{}`, requested: map[string]uuid.UUID{review: first}},
		{name: "a requested subset of the stored map is another start", stored: map[string]string{review: first.String(), approve: second.String()}, requested: map[string]uuid.UUID{review: first}},
		{name: "a requested superset of the stored map is another start", stored: map[string]string{review: first.String()}, requested: map[string]uuid.UUID{review: first, approve: second}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := w13aNewStartWorld(t)
			raw := []byte(tc.storedRaw)
			if tc.stored != nil {
				var err error
				raw, err = json.Marshal(tc.stored)
				require.NoError(t, err)
			}
			stored := w.seedLive([]byte(`{}`), raw)
			w.input.ContextJSON = json.RawMessage(`{}`)
			w.input.OverrideMap = tc.requested

			got, err := w.svc.Start(context.Background(), w.input)

			if tc.same {
				w.requireSameStart(t, stored, got, err)
				return
			}
			w.requireOtherStart(t, stored, err)
		})
	}
}

// w13aRefusal is one way a start is refused, with the reads it needs to get there.
type w13aRefusal struct {
	name  string
	apply func(w *w13aStartWorld)
	is    func(err error) bool
}

func w13aRefusals() []w13aRefusal {
	return []w13aRefusal{
		{"the tenant gate refuses", func(w *w13aStartWorld) { w.gate.err = port.ErrTenantNotActive },
			func(err error) bool { return errors.Is(err, port.ErrTenantNotActive) }},
		{"the Definitions read fails", func(w *w13aStartWorld) { w.definitions.err = errW13aDefinitionsDown },
			func(err error) bool { return errors.Is(err, errW13aDefinitionsDown) }},
		{"the version is not published", func(w *w13aStartWorld) { w.definitions.resp.Status = "DRAFT" },
			func(err error) bool { return errors.Is(err, port.ErrVersionNotPublished) }},
		{"the version is not valid", func(w *w13aStartWorld) { w.definitions.resp.IsValid = false },
			func(err error) bool { return errors.Is(err, port.ErrVersionInvalid) }},
		{"the plan cannot be decoded", func(w *w13aStartWorld) { w.definitions.resp.CompiledPlanJSON = "{" },
			func(err error) bool {
				return errors.Is(err, port.ErrVersionInvalid) && strings.Contains(err.Error(), "not valid JSON")
			}},
		{"the override map names an unknown node", func(w *w13aStartWorld) {
			w.input.OverrideMap = map[string]uuid.UUID{"finance/nowhere": uuid.New()}
		}, func(err error) bool { return errors.Is(err, port.ErrOverrideMapInvalid) }},
		{"an assignee is not eligible", func(w *w13aStartWorld) { w.eligibility.ineligible = true },
			func(err error) bool {
				var ineligible *port.AssigneeIneligibleError
				return errors.As(err, &ineligible)
			}},
		{"the task queue lookup fails", func(w *w13aStartWorld) { w.queues.err = errW13aQueueDown },
			func(err error) bool { return errors.Is(err, errW13aQueueDown) }},
	}
}

// w13aFirstUse is the index of the first refusal that needs each read: the gate, the Definitions read, the
// eligibility read and the queue lookup.
const (
	w13aGateStep         = 0
	w13aDefinitionsStep  = 1
	w13aNotPublishedStep = 2
	w13aNotValidStep     = 3
	w13aEligibilityStep  = 6
	w13aQueueStep        = 7
)

// w13aAccidentalPair is a pair whose order is not a decision: an unpublished version is never valid-checked
// by anyone, and the eligibility and queue refusals do not interact.
func w13aAccidentalPair(i, j int) bool {
	return (i == w13aNotPublishedStep && j == w13aNotValidStep) || (i == w13aEligibilityStep && j == w13aQueueStep)
}

func TestInstanceService_Start_TwoRefusalsAtOnceAnswerTheEarlierOneAndSkipTheLaterReads(t *testing.T) {
	refusals := w13aRefusals()
	type startCase struct {
		name    string
		failing []int
	}
	cases := []startCase{{"control: nothing is refused", nil}}
	for i := range refusals {
		cases = append(cases, startCase{"only " + refusals[i].name, []int{i}})
		for j := i + 1; j < len(refusals); j++ {
			if w13aAccidentalPair(i, j) {
				continue
			}
			cases = append(cases, startCase{refusals[i].name + " and later " + refusals[j].name, []int{i, j}})
		}
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := w13aNewStartWorld(t)
			for _, idx := range tc.failing {
				refusals[idx].apply(w)
			}

			got, err := w.svc.Start(context.Background(), w.input)

			reached := w13aQueueStep
			if len(tc.failing) > 0 {
				reached = tc.failing[0]
			}
			for _, tenant := range w.gate.calls {
				assert.Equal(t, w.tenantID, tenant)
			}
			for _, call := range w.definitions.calls {
				assert.Equal(t, w13aDefinitionCall{w.tenantID, w.versionID}, call)
			}
			for _, tenant := range w.eligibility.tenants {
				assert.Equal(t, w.tenantID, tenant)
			}
			assert.Equal(t, reached >= w13aGateStep, len(w.gate.calls) > 0, "gate reads")
			assert.Equal(t, reached >= w13aDefinitionsStep, len(w.definitions.calls) > 0, "Definitions reads")
			assert.Equal(t, reached >= w13aEligibilityStep, len(w.eligibility.tenants) > 0, "eligibility reads")
			assert.Equal(t, reached >= w13aQueueStep, len(w.queues.names) > 0, "queue lookups")
			if len(tc.failing) == 0 {
				require.NoError(t, err)
				assert.Len(t, w.instances.byID, 1)
				assert.Equal(t, 1, w.starts)
				assert.Equal(t, w.tenantID, got.TenantID)
				return
			}
			require.Error(t, err)
			assert.True(t, refusals[tc.failing[0]].is(err), "the earlier refusal is the answer: %v", err)
			for _, later := range tc.failing[1:] {
				assert.False(t, refusals[later].is(err), "the later refusal must not be the answer: %v", err)
			}
			assert.Empty(t, w.instances.byID, "nothing is written")
			assert.Empty(t, w.instances.attempts)
			assert.Zero(t, w.starts)
		})
	}
}

func TestInstanceService_Start_APlanWithUnparseableJSONIsAVersionInvalidRefusal(t *testing.T) {
	cases := []struct {
		name        string
		planJSON    string
		wantRefusal bool
	}{
		{name: "the plan JSON does not parse", planJSON: "{", wantRefusal: true},
		{name: "control: a plan that is complete starts", planJSON: compiledPlanJSON(t, assignedStage("review"))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := w13aNewStartWorld(t)
			w.definitions.resp.CompiledPlanJSON = tc.planJSON

			_, err := w.svc.Start(context.Background(), w.input)

			assert.Equal(t, []w13aDefinitionCall{{w.tenantID, w.versionID}}, w.definitions.calls)
			if !tc.wantRefusal {
				require.NoError(t, err)
				assert.Len(t, w.instances.byID, 1)
				return
			}
			require.Error(t, err)
			assert.ErrorIs(t, err, port.ErrVersionInvalid)
			for _, sentinel := range []error{
				port.ErrVersionNotPublished, port.ErrVersionNotFound, port.ErrOverrideMapInvalid,
				port.ErrDuplicateBusinessKey, port.ErrTenantNotActive, port.ErrAssigneeIneligible,
			} {
				assert.NotErrorIs(t, err, sentinel, "a broken plan is only a broken version")
			}
			assert.Empty(t, w.instances.byID, "nothing is written")
			assert.Empty(t, w.instances.attempts)
			assert.Empty(t, w.queues.names, "the queue is not looked up")
		})
	}
}
