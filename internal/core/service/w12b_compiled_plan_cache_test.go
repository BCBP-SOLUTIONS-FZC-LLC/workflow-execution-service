package service

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-models/v2/pkg/dsl"
)

var (
	errW12bDefinitionsDown = errors.New("definitions answered 503")
	errW12bCacheDown       = errors.New("cache refused the call")
)

type w12bPlanCacheOp struct {
	op    string
	key   string
	value string
	ttl   time.Duration
}

// w12bPlanStore is a cache that keeps what it was given and remembers each call with its key and TTL.
type w12bPlanStore struct {
	values map[string]string
	ops    []w12bPlanCacheOp
	getErr error
	setErr error
}

func newW12bPlanStore() *w12bPlanStore { return &w12bPlanStore{values: map[string]string{}} }

func (s *w12bPlanStore) Get(_ context.Context, key string) (string, error) {
	s.ops = append(s.ops, w12bPlanCacheOp{op: "get", key: key})
	if s.getErr != nil {
		return "", s.getErr
	}
	return s.values[key], nil
}

func (s *w12bPlanStore) Set(_ context.Context, key, value string, ttl time.Duration) error {
	s.ops = append(s.ops, w12bPlanCacheOp{op: "set", key: key, value: value, ttl: ttl})
	if s.setErr != nil {
		return s.setErr
	}
	s.values[key] = value
	return nil
}

func (s *w12bPlanStore) Del(_ context.Context, keys ...string) error {
	for _, k := range keys {
		s.ops = append(s.ops, w12bPlanCacheOp{op: "del", key: k})
	}
	return nil
}

func (s *w12bPlanStore) SetNX(context.Context, string, string, time.Duration) (bool, error) {
	s.ops = append(s.ops, w12bPlanCacheOp{op: "setnx"})
	return false, nil
}

func (s *w12bPlanStore) Ping(context.Context) error { return nil }

func (s *w12bPlanStore) opsNamed(op string) []w12bPlanCacheOp {
	var out []w12bPlanCacheOp
	for _, o := range s.ops {
		if o.op == op {
			out = append(out, o)
		}
	}
	return out
}

type w12bDefinitionsCall struct{ tenantID, versionID uuid.UUID }

type w12bDefinitions struct {
	resp  *port.CompiledWorkflow
	err   error
	calls []w12bDefinitionsCall
}

func (d *w12bDefinitions) GetCompiledWorkflow(_ context.Context, tenantID, versionID uuid.UUID) (*port.CompiledWorkflow, error) {
	d.calls = append(d.calls, w12bDefinitionsCall{tenantID, versionID})
	if d.err != nil {
		return nil, d.err
	}
	return d.resp, nil
}

type w12bLogLine struct {
	msg    string
	fields map[string]any
}

type w12bPlanLog struct{ warns, errors []w12bLogLine }

func (l *w12bPlanLog) Debug(string, map[string]any) {}
func (l *w12bPlanLog) Info(string, map[string]any)  {}
func (l *w12bPlanLog) Warn(msg string, f map[string]any) {
	l.warns = append(l.warns, w12bLogLine{msg, f})
}
func (l *w12bPlanLog) Error(msg string, f map[string]any) {
	l.errors = append(l.errors, w12bLogLine{msg, f})
}

func w12bPlanJSON(t *testing.T) string {
	t.Helper()
	b, err := json.Marshal(dsl.CompiledCollaboration{
		SchemaVersion: dsl.CurrentSchemaVersion,
		MainPlan:      "main",
		Plans:         []*dsl.CompiledPlan{{Name: "main", TaskQueue: "wf-queue-default"}},
	})
	require.NoError(t, err)
	return string(b)
}

func w12bCompiled(t *testing.T, versionID uuid.UUID, number int32, status string, valid bool) *port.CompiledWorkflow {
	t.Helper()
	return &port.CompiledWorkflow{
		WorkflowID: uuid.New(), VersionID: versionID, VersionNumber: number,
		Status: status, IsValid: valid, CompiledPlanJSON: w12bPlanJSON(t),
	}
}

func w12bMarshal(t *testing.T, c *port.CompiledWorkflow) string {
	t.Helper()
	b, err := json.Marshal(c)
	require.NoError(t, err)
	return string(b)
}

func TestLiveCompiledWorkflow_WriteThrough(t *testing.T) {
	cases := []struct {
		name         string
		definitions  func(t *testing.T, versionID uuid.UUID) *w12bDefinitions
		store        func() *w12bPlanStore
		noStore      bool
		wantErr      error
		wantWrites   int
		wantWarnText string
	}{
		{
			name: "a plan from Definitions is written under the tenant and version key for one hour",
			definitions: func(t *testing.T, v uuid.UUID) *w12bDefinitions {
				return &w12bDefinitions{resp: w12bCompiled(t, v, 4, "PUBLISHED", true)}
			},
			store:      newW12bPlanStore,
			wantWrites: 1,
		},
		{
			name: "a Definitions error is returned unchanged and nothing is written",
			definitions: func(*testing.T, uuid.UUID) *w12bDefinitions {
				return &w12bDefinitions{err: errW12bDefinitionsDown}
			},
			store:   newW12bPlanStore,
			wantErr: errW12bDefinitionsDown,
		},
		{
			name: "a failed cache write only logs and the plan is still returned",
			definitions: func(t *testing.T, v uuid.UUID) *w12bDefinitions {
				return &w12bDefinitions{resp: w12bCompiled(t, v, 4, "PUBLISHED", true)}
			},
			store: func() *w12bPlanStore {
				s := newW12bPlanStore()
				s.setErr = errW12bCacheDown
				return s
			},
			wantWrites:   1,
			wantWarnText: errW12bCacheDown.Error(),
		},
		{
			name: "no cache configured returns the plan and writes nothing",
			definitions: func(t *testing.T, v uuid.UUID) *w12bDefinitions {
				return &w12bDefinitions{resp: w12bCompiled(t, v, 4, "PUBLISHED", true)}
			},
			store:   newW12bPlanStore,
			noStore: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tenantID, versionID := uuid.New(), uuid.New()
			definitions := tc.definitions(t, versionID)
			store := tc.store()
			var cache port.CacheStore = store
			if tc.noStore {
				cache = nil
			}
			log := &w12bPlanLog{}

			got, err := liveCompiledWorkflow(context.Background(), cache, definitions, log, tenantID, versionID)

			assert.Equal(t, []w12bDefinitionsCall{{tenantID, versionID}}, definitions.calls, "Definitions is asked for this tenant and version")
			assert.Empty(t, store.opsNamed("get"), "the live read never consults the cache")
			if tc.wantErr != nil {
				require.Equal(t, tc.wantErr, err, "the Definitions error comes back as it is")
				assert.Nil(t, got)
				assert.Empty(t, store.ops, "nothing is written when Definitions fails")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, definitions.resp, got)
			writes := store.opsNamed("set")
			require.Len(t, writes, tc.wantWrites)
			if tc.wantWrites == 0 {
				return
			}
			assert.Equal(t, "compiled_plan:"+tenantID.String()+":"+versionID.String(), writes[0].key)
			assert.Equal(t, time.Hour, writes[0].ttl)
			var written port.CompiledWorkflow
			require.NoError(t, json.Unmarshal([]byte(writes[0].value), &written))
			assert.Equal(t, *definitions.resp, written, "the cached value is the plan Definitions returned")
			if tc.wantWarnText == "" {
				assert.Empty(t, log.warns)
				return
			}
			require.Len(t, log.warns, 1)
			assert.Equal(t, tc.wantWarnText, log.warns[0].fields["error"])
		})
	}
}

func TestCachedCompiledWorkflow_ServesWhatTheCacheHoldsAndFallsBackOnlyForAnUnusableEntry(t *testing.T) {
	const cachedNumber, liveNumber = 1, 2
	cases := []struct {
		name string
		// entry is the raw cached value; "" means no entry. Built after the ids exist.
		entry        func(t *testing.T, versionID uuid.UUID) string
		otherTenant  bool
		getErr       error
		wantLive     bool
		wantStatus   string
		wantValid    bool
		wantWarnText string
	}{
		{
			name: "a cached DRAFT plan is served without asking Definitions",
			entry: func(t *testing.T, v uuid.UUID) string {
				return w12bMarshal(t, w12bCompiled(t, v, cachedNumber, "DRAFT", true))
			},
			wantStatus: "DRAFT", wantValid: true,
		},
		{
			name: "a cached archived and invalidated plan is served without asking Definitions",
			entry: func(t *testing.T, v uuid.UUID) string {
				return w12bMarshal(t, w12bCompiled(t, v, cachedNumber, "ARCHIVED", false))
			},
			wantStatus: "ARCHIVED", wantValid: false,
		},
		{
			name: "a cached published plan is served without asking Definitions",
			entry: func(t *testing.T, v uuid.UUID) string {
				return w12bMarshal(t, w12bCompiled(t, v, cachedNumber, "PUBLISHED", true))
			},
			wantStatus: "PUBLISHED", wantValid: true,
		},
		{
			name: "a plan cached for another tenant is not served",
			entry: func(t *testing.T, v uuid.UUID) string {
				return w12bMarshal(t, w12bCompiled(t, v, cachedNumber, "DRAFT", true))
			},
			otherTenant: true,
			wantLive:    true, wantStatus: "PUBLISHED", wantValid: true,
		},
		{
			name:     "no entry reads Definitions",
			entry:    func(*testing.T, uuid.UUID) string { return "" },
			wantLive: true, wantStatus: "PUBLISHED", wantValid: true,
		},
		{
			name:     "an entry of JSON null reads Definitions and is replaced",
			entry:    func(*testing.T, uuid.UUID) string { return "null" },
			wantLive: true, wantStatus: "PUBLISHED", wantValid: true,
			wantWarnText: "compiled-plan cache entry is not a compiled plan, falling back to Definitions",
		},
		{
			name:     "an entry of an empty object reads Definitions and is replaced",
			entry:    func(*testing.T, uuid.UUID) string { return "{}" },
			wantLive: true, wantStatus: "PUBLISHED", wantValid: true,
			wantWarnText: "compiled-plan cache entry is not a compiled plan, falling back to Definitions",
		},
		{
			name: "an entry whose collaboration has no main plan reads Definitions and is replaced",
			entry: func(t *testing.T, v uuid.UUID) string {
				c := w12bCompiled(t, v, cachedNumber, "PUBLISHED", true)
				c.CompiledPlanJSON = `{"schema_version":1,"main_plan":"main","plans":[]}`
				return w12bMarshal(t, c)
			},
			wantLive: true, wantStatus: "PUBLISHED", wantValid: true,
			wantWarnText: "compiled-plan cache entry is not a compiled plan, falling back to Definitions",
		},
		{
			name:     "an entry that is not JSON reads Definitions and is replaced",
			entry:    func(*testing.T, uuid.UUID) string { return "not json" },
			wantLive: true, wantStatus: "PUBLISHED", wantValid: true,
			wantWarnText: "compiled-plan cache entry is not a compiled plan, falling back to Definitions",
		},
		{
			name:     "a cache read error reads Definitions",
			entry:    func(*testing.T, uuid.UUID) string { return "" },
			getErr:   errW12bCacheDown,
			wantLive: true, wantStatus: "PUBLISHED", wantValid: true,
			wantWarnText: "compiled-plan cache read failed, falling back to Definitions",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tenantID, versionID := uuid.New(), uuid.New()
			store := newW12bPlanStore()
			store.getErr = tc.getErr
			keyTenant := tenantID
			if tc.otherTenant {
				keyTenant = uuid.New()
			}
			key := "compiled_plan:" + keyTenant.String() + ":" + versionID.String()
			if raw := tc.entry(t, versionID); raw != "" {
				store.values[key] = raw
			}
			definitions := &w12bDefinitions{resp: w12bCompiled(t, versionID, liveNumber, "PUBLISHED", true)}
			log := &w12bPlanLog{}

			got, err := cachedCompiledWorkflow(context.Background(), store, definitions, log, tenantID, versionID)

			require.NoError(t, err)
			assert.Equal(t, tc.wantStatus, got.Status)
			assert.Equal(t, tc.wantValid, got.IsValid)
			reads := store.opsNamed("get")
			require.Len(t, reads, 1)
			assert.Equal(t, "compiled_plan:"+tenantID.String()+":"+versionID.String(), reads[0].key, "the entry is looked up under this tenant's key")
			if !tc.wantLive {
				assert.EqualValues(t, cachedNumber, got.VersionNumber, "the answer is the cached copy")
				assert.Empty(t, definitions.calls, "Definitions is not asked")
				assert.Empty(t, store.opsNamed("set"), "and nothing is written back")
				assert.Empty(t, log.warns)
				return
			}
			assert.EqualValues(t, liveNumber, got.VersionNumber, "the answer is Definitions' copy")
			assert.Equal(t, []w12bDefinitionsCall{{tenantID, versionID}}, definitions.calls)
			if tc.getErr == nil {
				writes := store.opsNamed("set")
				require.Len(t, writes, 1, "the fresh plan replaces the unusable entry")
				assert.Equal(t, reads[0].key, writes[0].key)
				var replaced port.CompiledWorkflow
				require.NoError(t, json.Unmarshal([]byte(writes[0].value), &replaced))
				assert.EqualValues(t, liveNumber, replaced.VersionNumber)
			}
			if tc.wantWarnText == "" {
				assert.Empty(t, log.warns)
				return
			}
			require.NotEmpty(t, log.warns)
			assert.Equal(t, tc.wantWarnText, log.warns[0].msg)
		})
	}
}
