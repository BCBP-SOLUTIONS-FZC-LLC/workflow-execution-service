package service

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
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

var errW14aPlanSource = errors.New("definitions answered 502")

type w14aPlanFetch struct{ tenantID, versionID uuid.UUID }

// w14aPlanSource records the tenant and version of every fetch and answers the plan JSON it was given, or err.
type w14aPlanSource struct {
	planJSON string
	err      error
	fetches  []w14aPlanFetch
}

func (s *w14aPlanSource) GetCompiledWorkflow(_ context.Context, tenantID, versionID uuid.UUID) (*port.CompiledWorkflow, error) {
	s.fetches = append(s.fetches, w14aPlanFetch{tenantID, versionID})
	if s.err != nil {
		return nil, s.err
	}
	return &port.CompiledWorkflow{Status: "PUBLISHED", IsValid: true, VersionID: versionID, CompiledPlanJSON: s.planJSON}, nil
}

type w14aLogged struct {
	msg    string
	fields map[string]any
}

type w14aLogRecorder struct{ warns []w14aLogged }

func (*w14aLogRecorder) Debug(string, map[string]any) {}
func (*w14aLogRecorder) Info(string, map[string]any)  {}
func (*w14aLogRecorder) Error(string, map[string]any) {}
func (l *w14aLogRecorder) Warn(msg string, fields map[string]any) {
	l.warns = append(l.warns, w14aLogged{msg, fields})
}

func w14aCollaborationJSON(t *testing.T, mainPlan string, depts ...dsl.DepartmentDef) string {
	t.Helper()
	collab := dsl.CompiledCollaboration{
		SchemaVersion: dsl.CurrentSchemaVersion, MainPlan: mainPlan,
		Plans: []*dsl.CompiledPlan{{Name: "main", TaskQueue: "wf-queue-default", Departments: depts}},
	}
	b, err := json.Marshal(collab)
	require.NoError(t, err)
	return string(b)
}

func w14aStage(nodeID, role string) dsl.StageDef {
	return dsl.StageDef{Type: "userTask", NodeID: nodeID, Role: role}
}

func TestRequiredLevelForTask_NeedsTheDepartmentUUIDAndTheNodeKeyToMatchTogether(t *testing.T) {
	salesID, opsID := uuid.New(), uuid.New()
	plan := &dsl.CompiledPlan{Departments: []dsl.DepartmentDef{
		{ID: "sales", IAMDepartmentID: salesID.String(), Stages: []dsl.StageDef{w14aStage("quote", "rep"), w14aStage("close", "manager")}},
		{ID: "ops", IAMDepartmentID: opsID.String(), Stages: []dsl.StageDef{w14aStage("ship", "packer")}},
	}}
	cases := []struct {
		name      string
		task      domain.Task
		wantLevel string
		wantFound bool
	}{
		{"the first stage of a department", domain.Task{DepartmentID: salesID, NodeKey: "sales/quote"}, "rep", true},
		{"a later stage of the same department", domain.Task{DepartmentID: salesID, NodeKey: "sales/close"}, "manager", true},
		{"a stage of another department", domain.Task{DepartmentID: opsID, NodeKey: "ops/ship"}, "packer", true},
		{"the department matches but the node key names no stage of it", domain.Task{DepartmentID: salesID, NodeKey: "sales/ship"}, "", false},
		{"the department matches and the node key is another department's stage", domain.Task{DepartmentID: salesID, NodeKey: "ops/ship"}, "", false},
		{"the node key matches but the department uuid is another's", domain.Task{DepartmentID: opsID, NodeKey: "sales/quote"}, "", false},
		{"the node key matches but the department uuid is unknown", domain.Task{DepartmentID: uuid.New(), NodeKey: "sales/quote"}, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			level, found := requiredLevelForTask(plan, &tc.task)

			assert.Equal(t, tc.wantFound, found)
			assert.Equal(t, tc.wantLevel, level)
		})
	}

	t.Run("a plan with no departments finds no level", func(t *testing.T) {
		level, found := requiredLevelForTask(&dsl.CompiledPlan{}, &domain.Task{DepartmentID: salesID, NodeKey: "sales/quote"})

		assert.False(t, found)
		assert.Empty(t, level)
	})

	t.Run("a department with no stages finds no level", func(t *testing.T) {
		bare := &dsl.CompiledPlan{Departments: []dsl.DepartmentDef{{ID: "sales", IAMDepartmentID: salesID.String()}}}

		level, found := requiredLevelForTask(bare, &domain.Task{DepartmentID: salesID, NodeKey: "sales/quote"})

		assert.False(t, found)
		assert.Empty(t, level)
	})
}

func TestCompiledPlanCache_MainPlan_APlanThatCannotBeReadIsAnErrorAndAWarning(t *testing.T) {
	cases := []struct {
		name  string
		plan  w14aPlanSource
		match func(t *testing.T, err error)
	}{
		{"the fetch fails", w14aPlanSource{err: errW14aPlanSource}, func(t *testing.T, err error) {
			require.ErrorIs(t, err, errW14aPlanSource)
		}},
		{"the plan is not JSON", w14aPlanSource{planJSON: "not json"}, func(t *testing.T, err error) {
			var syntax *json.SyntaxError
			require.ErrorAs(t, err, &syntax)
		}},
		{"the main plan is not among the plans", w14aPlanSource{planJSON: `{"main_plan":"ghost","schema_version":` + w14aSchemaVersionJSON() + `,"plans":[]}`}, func(t *testing.T, err error) {
			require.ErrorContains(t, err, "ghost")
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			source := tc.plan
			log := &w14aLogRecorder{}
			cache := newCompiledPlanCache(&source, log)
			tenantID, versionID := uuid.New(), uuid.New()

			plan, err := cache.mainPlan(context.Background(), tenantID, versionID)

			require.Error(t, err)
			tc.match(t, err)
			assert.Nil(t, plan)
			require.Len(t, log.warns, 1, "the failure is logged once")
			assert.Equal(t, versionID, log.warns[0].fields["version_id"])
			assert.Equal(t, err.Error(), log.warns[0].fields["error"], "the logged cause is the returned error")
			assert.Equal(t, []w14aPlanFetch{{tenantID, versionID}}, source.fetches)

			_, err = cache.mainPlan(context.Background(), tenantID, versionID)
			require.Error(t, err)
			assert.Len(t, source.fetches, 2, "a plan that could not be read is not remembered")
		})
	}

	t.Run("control: a readable plan is returned with no warning", func(t *testing.T) {
		source := &w14aPlanSource{planJSON: w14aCollaborationJSON(t, "main", dsl.DepartmentDef{ID: "sales", Stages: []dsl.StageDef{w14aStage("quote", "rep")}})}
		log := &w14aLogRecorder{}
		tenantID, versionID := uuid.New(), uuid.New()

		plan, err := newCompiledPlanCache(source, log).mainPlan(context.Background(), tenantID, versionID)

		require.NoError(t, err)
		require.NotNil(t, plan)
		assert.Equal(t, "main", plan.Name)
		assert.Empty(t, log.warns)
		assert.Equal(t, []w14aPlanFetch{{tenantID, versionID}}, source.fetches)
	})
}

func w14aSchemaVersionJSON() string {
	b, _ := json.Marshal(dsl.CurrentSchemaVersion)
	return string(b)
}

// w14aScopeWorld is one candidate task of an instance whose plan has the departments finance and legal.
type w14aScopeWorld struct {
	source    *w14aPlanSource
	log       *w14aLogRecorder
	cache     *compiledPlanCache
	tenantID  uuid.UUID
	versionID uuid.UUID
	financeID uuid.UUID
	legalID   uuid.UUID
}

func newW14aScopeWorld(t *testing.T) *w14aScopeWorld {
	t.Helper()
	w := &w14aScopeWorld{log: &w14aLogRecorder{}, tenantID: uuid.New(), versionID: uuid.New(), financeID: uuid.New(), legalID: uuid.New()}
	w.source = &w14aPlanSource{planJSON: w14aCollaborationJSON(t, "main",
		dsl.DepartmentDef{ID: "finance", IAMDepartmentID: w.financeID.String(), Stages: []dsl.StageDef{w14aStage("a", "rep")}},
		dsl.DepartmentDef{ID: "legal", IAMDepartmentID: w.legalID.String(), Stages: []dsl.StageDef{w14aStage("b", "rep")}},
	)}
	w.cache = newCompiledPlanCache(w.source, w.log)
	return w
}

func (w *w14aScopeWorld) matches(nodeKey string, taskDept uuid.UUID, subjectID, scope string, scopeID *string) bool {
	candidate := delegationCandidate{
		task:     &domain.Task{ID: uuid.New(), NodeKey: nodeKey, DepartmentID: taskDept},
		instance: &domain.Instance{ID: uuid.New(), WorkflowVersionID: w.versionID, SubjectID: subjectID},
	}
	return scopeMatches(context.Background(), w.log, w.cache, w.tenantID, candidate, scope, scopeID)
}

func TestScopeMatches_DepartmentScopeNeedsTheScopeIDAndTheTaskRowToNameTheSameDepartment(t *testing.T) {
	w := newW14aScopeWorld(t)
	finance, legal := w.financeID.String(), w.legalID.String()
	cases := []struct {
		name     string
		nodeKey  string
		taskDept uuid.UUID
		scopeID  string
		want     bool
	}{
		{"the plan's IAM id, the scope id and the task row all agree", "finance/a", w.financeID, finance, true},
		{"the scope id is the plan department's IAM id but the task row names another department", "finance/a", w.legalID, finance, false},
		{"the scope id is the plan department's IAM id but the task row has no department", "finance/a", uuid.Nil, finance, false},
		{"the task row agrees with the plan but the scope id is another department's", "finance/a", w.financeID, legal, false},
		{"the scope id is an id the plan does not know", "finance/a", w.financeID, uuid.NewString(), false},
		{"the task's department is not in the plan", "ghost/a", w.financeID, finance, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			scopeID := tc.scopeID

			assert.Equal(t, tc.want, w.matches(tc.nodeKey, tc.taskDept, "subject", "department", &scopeID))
		})
	}
	for _, read := range w.source.fetches {
		assert.Equal(t, w14aPlanFetch{w.tenantID, w.versionID}, read)
	}
	assert.Len(t, w.source.fetches, 1, "the plan is read once for every candidate of the version")
}

func TestScopeMatches_DepartmentScopeSplitsTheTaskNodeKeyAtTheFirstSlash(t *testing.T) {
	w := newW14aScopeWorld(t)
	finance := w.financeID.String()
	cases := []struct {
		name    string
		nodeKey string
		want    bool
	}{
		{"department and node", "finance/a", true},
		{"a key with no slash is all department", "finance", true},
		{"a node that holds more slashes", "finance/a/b", true},
		{"a node that names another department", "finance/legal", true},
		{"a node that is the department's name behind another department", "legal/finance", false},
		{"a trailing slash leaves an empty node", "finance/", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			scopeID := finance

			assert.Equal(t, tc.want, w.matches(tc.nodeKey, w.financeID, "subject", "department", &scopeID))
		})
	}
}

func TestScopeMatches_TenderScopeNeedsUUIDsThatAgreeWithTheSubjectID(t *testing.T) {
	upper := "3F2504E0-4F89-41D3-9A0C-0305E82C3301"
	cases := []struct {
		name      string
		subjectID string
		scopeID   *string
		want      bool
	}{
		{"the same text that is not a uuid matches nothing", "TND-001", w14aPtr("TND-001"), false},
		{"another letter case", "TND-001", w14aPtr("tnd-001"), false},
		{"a trailing space", "TND-001", w14aPtr("TND-001 "), false},
		{"a leading space", "TND-001", w14aPtr(" TND-001"), false},
		{"another subject", "TND-001", w14aPtr("TND-002"), false},
		{"the same uuid text in upper case on both sides", upper, w14aPtr(upper), true},
		{"no scope id", "TND-001", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := newW14aScopeWorld(t)

			assert.Equal(t, tc.want, w.matches("finance/a", w.financeID, tc.subjectID, scopeTender, tc.scopeID))
			assert.Empty(t, w.source.fetches, "a tender scope never reads the plan")
		})
	}
}

func w14aPtr(s string) *string { return &s }

func TestScopeMatches_DepartmentScopeWithAnUnreadablePlanAnswersFalseAndWarnsOfTheTask(t *testing.T) {
	w := newW14aScopeWorld(t)
	w.source.err = errW14aPlanSource
	scopeID := w.financeID.String()
	task := &domain.Task{ID: uuid.New(), NodeKey: "finance/a", DepartmentID: w.financeID}
	candidate := delegationCandidate{task: task, instance: &domain.Instance{ID: uuid.New(), WorkflowVersionID: w.versionID}}

	got := scopeMatches(context.Background(), w.log, w.cache, w.tenantID, candidate, "department", &scopeID)

	assert.False(t, got, "a candidate whose plan cannot be read is not in the scope")
	assert.Equal(t, []w14aPlanFetch{{w.tenantID, w.versionID}}, w.source.fetches)
	var forTask int
	for _, warn := range w.log.warns {
		if warn.fields["task_id"] == task.ID {
			forTask++
		}
	}
	assert.Equal(t, 1, forTask, "one warning names the skipped task")

	t.Run("control: the same candidate matches once the plan can be read", func(t *testing.T) {
		w.source.err = nil
		w.cache = newCompiledPlanCache(w.source, w.log)

		assert.True(t, scopeMatches(context.Background(), w.log, w.cache, w.tenantID, candidate, "department", &scopeID))
	})
}
