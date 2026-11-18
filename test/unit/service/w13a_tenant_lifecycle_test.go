package service_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
)

var (
	errW13aStatusDown = errors.New("tenant status store unavailable")
	errW13aSweepDown  = errors.New("instance listing unavailable")
)

type w13aStatusRecord struct {
	tenantID  uuid.UUID
	status    string
	changedAt time.Time
}

// w13aTenantStates keeps statuses per tenant and records the tenant of every read and write.
type w13aTenantStates struct {
	status    map[uuid.UUID]string
	getErr    error
	recordErr error
	reads     []uuid.UUID
	records   []w13aStatusRecord
}

func (s *w13aTenantStates) Get(_ context.Context, tenantID uuid.UUID) (string, bool, error) {
	s.reads = append(s.reads, tenantID)
	if s.getErr != nil {
		return "", false, s.getErr
	}
	status, ok := s.status[tenantID]
	return status, ok, nil
}

func (s *w13aTenantStates) Record(_ context.Context, tenantID uuid.UUID, status string, changedAt time.Time) error {
	s.records = append(s.records, w13aStatusRecord{tenantID, status, changedAt})
	return s.recordErr
}

func TestTenantStartGate_AFailedStatusReadFailsClosedAndIsNotATenantRefusal(t *testing.T) {
	tenantID := uuid.New()
	cases := []struct {
		name    string
		refused []string
		getErr  error
		status  string
	}{
		{name: "the read fails with nothing configured", getErr: errW13aStatusDown},
		{name: "the read fails with a refused status configured", refused: []string{"suspended"}, getErr: errW13aStatusDown},
		{name: "control: the read works and a refused status is a tenant refusal", refused: []string{"suspended"}, status: "suspended"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			states := &w13aTenantStates{status: map[uuid.UUID]string{tenantID: tc.status}, getErr: tc.getErr}
			gate := service.NewTenantStartGate(states, tc.refused)

			err := gate.Allow(context.Background(), tenantID)

			require.Error(t, err)
			assert.Equal(t, []uuid.UUID{tenantID}, states.reads)
			if tc.getErr == nil {
				assert.ErrorIs(t, err, port.ErrTenantNotActive)
				return
			}
			assert.NotErrorIs(t, err, port.ErrTenantNotActive, "a failed read must not look like a refusal of the tenant")
			assert.ErrorIs(t, err, tc.getErr, "the cause can be found")
			assert.ErrorContains(t, err, "read the tenant status")
		})
	}
}

func TestTenantStartGate_TheConfiguredListIsTrimmedAndLowerCased(t *testing.T) {
	tenantID := uuid.New()
	cases := []struct {
		name    string
		refused []string
		stored  string
		want    bool
	}{
		{"a configured status with spaces is trimmed", []string{"  suspended  "}, "suspended", true},
		{"a configured upper-case status is lower-cased", []string{"SUSPENDED"}, "suspended", true},
		{"a configured mixed-case status with spaces is both", []string{" Trial_Expired\t"}, "trial_expired", true},
		{"every configured entry is normalised", []string{"past_due", " Suspended "}, "suspended", true},
		{"control: the stored lower-case offboarded is refused with nothing configured", nil, "offboarded", true},
		{"control: a stored status that is not configured is allowed", []string{"suspended"}, "active", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			states := &w13aTenantStates{status: map[uuid.UUID]string{tenantID: tc.stored}}
			gate := service.NewTenantStartGate(states, tc.refused)

			err := gate.Allow(context.Background(), tenantID)

			assert.Equal(t, []uuid.UUID{tenantID}, states.reads)
			if tc.want {
				require.ErrorIs(t, err, port.ErrTenantNotActive)
				return
			}
			assert.NoError(t, err)
		})
	}
}

type w13aReconciler struct {
	svc       *service.TenantLifecycleReconciler
	instances *fakeInstanceRepo
	queues    *fakeActiveTaskQueueRepo
	outbox    *fakeOutbox
	temporal  *fakeTemporalClient
	states    *w13aTenantStates
	retention *fakeRetention
	tenantID  uuid.UUID
	live      *domain.Instance
}

func w13aNewReconciler() *w13aReconciler {
	svc, instances, _, _, queues, outbox, temporal := newTenantLifecycleHarness()
	r := &w13aReconciler{
		svc: svc, instances: instances, queues: queues, outbox: outbox, temporal: temporal,
		states: &w13aTenantStates{}, retention: &fakeRetention{}, tenantID: uuid.New(),
	}
	svc.States = r.states
	svc.Retention = r.retention
	r.live = &domain.Instance{ID: uuid.New(), TenantID: r.tenantID, Status: domain.InstanceStatusRunning, TemporalWorkflowID: "tenant:live", TemporalRunID: "run-live", RecordVersion: 1}
	instances.byID[r.live.ID] = r.live
	return r
}

// requireNoTransition fails when the call listed, signalled, terminated or recorded retention for the tenant's instances.
func (r *w13aReconciler) requireNoTransition(t *testing.T) {
	t.Helper()
	assert.Zero(t, r.instances.listByTenantCalls, "the tenant's instances are not listed")
	assert.Empty(t, r.temporal.signals)
	assert.Empty(t, r.temporal.terminate)
	assert.Empty(t, r.retention.returned)
	assert.Empty(t, r.retention.recorded)
	assert.Empty(t, r.outbox.enqueued)
	assert.Equal(t, domain.InstanceStatusRunning, r.live.Status, "the instance is left as it was")
}

func TestTenantLifecycleReconciler_Apply_AnUnchangedStatusIsRecordedButNothingTransitions(t *testing.T) {
	changedAt := time.Date(2026, 10, 18, 9, 30, 0, 0, time.UTC)
	cases := []struct {
		name         string
		status       string
		previous     string
		wantSignal   string
		wantTerminal bool
	}{
		{name: "active stays active", status: "active", previous: "active"},
		{name: "suspended stays suspended", status: "suspended", previous: "suspended"},
		{name: "offboarded stays offboarded", status: "offboarded", previous: "offboarded"},
		{name: "control: active to suspended pauses", status: "suspended", previous: "active", wantSignal: port.SignalInstancePause},
		{name: "control: suspended to active resumes", status: "active", previous: "suspended", wantSignal: port.SignalInstanceResume},
		{name: "control: active to offboarded terminates", status: "offboarded", previous: "active", wantTerminal: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := w13aNewReconciler()

			err := r.svc.Apply(context.Background(), port.TenantLifecycleInput{
				TenantID: r.tenantID, Status: tc.status, PreviousStatus: tc.previous, ChangedAt: changedAt,
			})

			require.NoError(t, err)
			assert.Equal(t, []w13aStatusRecord{{r.tenantID, tc.status, changedAt}}, r.states.records, "the status is recorded either way")
			switch {
			case tc.wantSignal != "":
				require.Len(t, r.temporal.signals, 1)
				assert.Equal(t, tc.wantSignal, r.temporal.signals[0].SignalName)
				assert.Equal(t, r.live.ID, r.temporal.signals[0].InstanceID)
				assert.Equal(t, []recordedReturn{{r.tenantID, changedAt}}, r.retention.returned)
			case tc.wantTerminal:
				assert.Equal(t, domain.InstanceStatusTerminated, r.live.Status)
				assert.NotEmpty(t, r.temporal.terminate)
				require.Len(t, r.retention.recorded, 1)
				assert.Equal(t, r.tenantID, r.retention.recorded[0].tenantID)
			default:
				r.requireNoTransition(t)
			}
		})
	}
}

func TestTenantLifecycleReconciler_Apply_AFailedStatusTransitionStopsBeforeThePlanChange(t *testing.T) {
	cases := []struct {
		name      string
		plan      string
		previous  string
		failSweep bool
	}{
		{name: "an upgrade to enterprise is not registered", plan: "enterprise", previous: "standard", failSweep: true},
		{name: "a downgrade from enterprise does not stop routing", plan: "standard", previous: "enterprise", failSweep: true},
		{name: "control: an upgrade to enterprise registers the queue when the transition works", plan: "enterprise", previous: "standard"},
		{name: "control: a downgrade stops routing when the transition works", plan: "standard", previous: "enterprise"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := w13aNewReconciler()
			if tc.failSweep {
				r.instances.listErr = errW13aSweepDown
			}
			queueName := domain.TenantTaskQueueName(r.tenantID)
			r.queues.byName[queueName] = &domain.ActiveTaskQueue{TenantID: r.tenantID, QueueName: queueName, Routable: true}
			changedAt := time.Date(2026, 10, 18, 9, 30, 0, 0, time.UTC)

			err := r.svc.Apply(context.Background(), port.TenantLifecycleInput{
				TenantID: r.tenantID, Status: "suspended", PreviousStatus: "active", Plan: tc.plan, PreviousPlan: tc.previous, ChangedAt: changedAt,
			})

			assert.Equal(t, []w13aStatusRecord{{r.tenantID, "suspended", changedAt}}, r.states.records, "the new status is stored before the transition runs")
			if tc.failSweep {
				require.ErrorIs(t, err, errW13aSweepDown)
				assert.Empty(t, r.queues.registered, "the plan change of the same event is not applied")
				assert.Empty(t, r.queues.stopped)
				return
			}
			require.NoError(t, err)
			if tc.plan == "enterprise" {
				assert.Equal(t, []registerCall{{r.tenantID, queueName}}, r.queues.registered)
				assert.Empty(t, r.queues.stopped)
			} else {
				assert.Equal(t, []string{queueName}, r.queues.stopped)
				assert.Empty(t, r.queues.registered)
			}
		})
	}
}

func TestTenantLifecycleReconciler_Apply_AFailedStatusRecordStopsBeforeAnyTransition(t *testing.T) {
	changedAt := time.Date(2026, 10, 18, 9, 30, 0, 0, time.UTC)
	cases := []struct {
		name       string
		status     string
		previous   string
		recordErr  error
		nilStates  bool
		wantSignal string
	}{
		{name: "a suspension that would pause", status: "suspended", previous: "active", recordErr: errW13aStatusDown},
		{name: "a reactivation that would resume", status: "active", previous: "suspended", recordErr: errW13aStatusDown},
		{name: "an offboarding that would terminate", status: "offboarded", previous: "active", recordErr: errW13aStatusDown},
		{name: "control: no status store configured, the transition still runs", status: "suspended", previous: "active", nilStates: true, wantSignal: port.SignalInstancePause},
		{name: "control: the record works, the transition runs", status: "suspended", previous: "active", wantSignal: port.SignalInstancePause},
		{name: "an empty status records nothing and moves nothing", status: "", previous: "active"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := w13aNewReconciler()
			r.states.recordErr = tc.recordErr
			if tc.nilStates {
				r.svc.States = nil
			}

			err := r.svc.Apply(context.Background(), port.TenantLifecycleInput{
				TenantID: r.tenantID, Status: tc.status, PreviousStatus: tc.previous, ChangedAt: changedAt,
				Plan: "enterprise", PreviousPlan: "standard",
			})

			if tc.recordErr != nil {
				require.ErrorIs(t, err, tc.recordErr)
				assert.Equal(t, []w13aStatusRecord{{r.tenantID, tc.status, changedAt}}, r.states.records)
				r.requireNoTransition(t)
				assert.Empty(t, r.queues.registered, "the plan change of the same event is not applied")
				return
			}
			require.NoError(t, err)
			if tc.nilStates {
				assert.Empty(t, r.states.records)
			}
			if tc.wantSignal == "" {
				assert.Empty(t, r.states.records, "an empty status is not recorded")
				r.requireNoTransition(t)
				return
			}
			require.Len(t, r.temporal.signals, 1)
			assert.Equal(t, tc.wantSignal, r.temporal.signals[0].SignalName)
		})
	}
}

type w13aSweepCall struct {
	tenantID uuid.UUID
	filter   port.InstanceListFilter
	page     port.PageRequest
}

// w13aPagedInstances serves the tenant's live instances a page at a time, in order, after the cursor of the
// previous page; it honours the status filter and fails the listing number failOn (counting from 1).
type w13aPagedInstances struct {
	*fakeInstanceRepo
	live     []*domain.Instance
	failOn   int
	calls    []w13aSweepCall
	returned []*port.Cursor
}

func (r *w13aPagedInstances) ListByTenant(_ context.Context, tenantID uuid.UUID, filter port.InstanceListFilter, page port.PageRequest) ([]*domain.Instance, *port.Cursor, error) {
	r.calls = append(r.calls, w13aSweepCall{tenantID, filter, page})
	if r.failOn == len(r.calls) {
		return nil, nil, errW13aSweepDown
	}
	var matching []*domain.Instance
	for _, inst := range r.live {
		if inst.TenantID == tenantID && containsInstanceStatus(filter.Statuses, inst.Status) {
			matching = append(matching, inst)
		}
	}
	start := 0
	if page.After != nil {
		for i, inst := range matching {
			if inst.ID == page.After.ID {
				start = i + 1
			}
		}
	}
	end := min(start+page.Limit, len(matching))
	var next *port.Cursor
	if end < len(matching) {
		next = &port.Cursor{CreatedAt: matching[end-1].CreatedAt, ID: matching[end-1].ID}
	}
	r.returned = append(r.returned, next)
	return matching[start:end], next, nil
}

func TestTenantLifecycleReconciler_Apply_ASweepReadsEveryPageOfOneHundredAfterThePreviousCursor(t *testing.T) {
	statuses := []domain.InstanceStatus{domain.InstanceStatusRunning, domain.InstanceStatusPaused, domain.InstanceStatusDegraded}
	cases := []struct {
		name       string
		status     string
		previous   string
		failOn     int
		wantSignal string
	}{
		{name: "a pausing sweep over 250 instances", status: "suspended", previous: "active", wantSignal: port.SignalInstancePause},
		{name: "a terminating sweep over 250 instances", status: "offboarded", previous: "active"},
		{name: "a pausing sweep whose second page cannot be read", status: "suspended", previous: "active", failOn: 2},
		{name: "a terminating sweep whose second page cannot be read", status: "offboarded", previous: "active", failOn: 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := w13aNewReconciler()
			delete(r.instances.byID, r.live.ID)
			var live, finished []*domain.Instance
			for i := 0; i < 250; i++ {
				inst := &domain.Instance{
					ID: uuid.New(), TenantID: r.tenantID, Status: statuses[i%len(statuses)],
					TemporalWorkflowID: "tenant:" + uuid.NewString(), TemporalRunID: "run", RecordVersion: 1,
				}
				live = append(live, inst)
				r.instances.byID[inst.ID] = inst
				if i%50 == 0 {
					done := &domain.Instance{ID: uuid.New(), TenantID: r.tenantID, Status: domain.InstanceStatusCompleted, TemporalWorkflowID: "tenant:done", RecordVersion: 1}
					finished = append(finished, done)
					r.instances.byID[done.ID] = done
				}
			}
			otherTenant := &domain.Instance{ID: uuid.New(), TenantID: uuid.New(), Status: domain.InstanceStatusRunning, TemporalWorkflowID: "tenant:other", RecordVersion: 1}
			r.instances.byID[otherTenant.ID] = otherTenant
			paged := &w13aPagedInstances{fakeInstanceRepo: r.instances, live: append(append([]*domain.Instance{}, live...), append(finished, otherTenant)...), failOn: tc.failOn}
			r.svc.Instances = paged

			err := r.svc.Apply(context.Background(), port.TenantLifecycleInput{TenantID: r.tenantID, Status: tc.status, PreviousStatus: tc.previous})

			for _, call := range paged.calls {
				assert.Equal(t, r.tenantID, call.tenantID)
				assert.Equal(t, 100, call.page.Limit, "each page asks for 100")
				assert.ElementsMatch(t, statuses, call.filter.Statuses, "only RUNNING, PAUSED and DEGRADED instances are listed")
				assert.Nil(t, call.filter.Status)
			}
			if tc.failOn != 0 {
				require.Error(t, err)
				assert.Same(t, errW13aSweepDown, err)
				require.Len(t, paged.calls, 2, "the sweep stops at the failed page")
				assert.Nil(t, paged.calls[0].page.After)
				assert.Equal(t, paged.returned[0], paged.calls[1].page.After)
				assert.Empty(t, r.temporal.signals, "nothing is signalled when a page cannot be read")
				assert.Empty(t, r.temporal.terminate)
				return
			}
			require.NoError(t, err)
			require.Len(t, paged.calls, 3, "250 instances are three pages")
			require.Len(t, paged.returned, 3)
			require.NotNil(t, paged.returned[0])
			require.NotNil(t, paged.returned[1])
			assert.Nil(t, paged.returned[2])
			assert.Nil(t, paged.calls[0].page.After)
			assert.Equal(t, paged.returned[0], paged.calls[1].page.After, "the second page continues after the first page's cursor")
			assert.Equal(t, paged.returned[1], paged.calls[2].page.After, "the third page continues after the second page's cursor")
			var wantIDs []uuid.UUID
			for _, inst := range live {
				wantIDs = append(wantIDs, inst.ID)
			}
			if tc.wantSignal != "" {
				var gotIDs []uuid.UUID
				for _, sig := range r.temporal.signals {
					assert.Equal(t, tc.wantSignal, sig.SignalName)
					gotIDs = append(gotIDs, sig.InstanceID)
				}
				assert.ElementsMatch(t, wantIDs, gotIDs, "every live instance of the tenant is signalled once, and no other")
				return
			}
			for _, inst := range live {
				assert.Equal(t, domain.InstanceStatusTerminated, r.instances.byID[inst.ID].Status)
			}
			for _, done := range finished {
				assert.Equal(t, domain.InstanceStatusCompleted, r.instances.byID[done.ID].Status)
			}
			assert.Equal(t, domain.InstanceStatusRunning, r.instances.byID[otherTenant.ID].Status)
		})
	}
}
