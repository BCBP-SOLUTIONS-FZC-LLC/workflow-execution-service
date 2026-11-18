package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/events"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var errW14bOutbox = errors.New("w14b outbox down")

// w14bEnqueueRecorder records every envelope handed to Enqueue, including the
// ones the shared fake then refuses.
type w14bEnqueueRecorder struct {
	*fakeOutbox
	attempts []events.Envelope[json.RawMessage]
}

func (o *w14bEnqueueRecorder) Enqueue(ctx context.Context, env events.Envelope[json.RawMessage]) error {
	o.attempts = append(o.attempts, env)
	return o.fakeOutbox.Enqueue(ctx, env)
}

type w14bEventRig struct {
	outbox    *w14bEnqueueRecorder
	run       func() (uuid.UUID, error)
	tenantID  uuid.UUID
	userID    uuid.UUID
	eventType string
	failWith  func(validator port.EventValidator)
}

// w14bNewEventRig builds an InstanceService whose one event goes through the
// instance event sink: "start" records the instance-started event, "terminate"
// records the instance-terminated event of an instance with no tasks.
func w14bNewEventRig(t *testing.T, entry string) *w14bEventRig {
	t.Helper()
	svc, instances, _, _, fake, _, definitions, _ := newInstanceServiceHarness()
	recorder := &w14bEnqueueRecorder{fakeOutbox: fake}
	svc.Outbox = recorder
	rig := &w14bEventRig{outbox: recorder, tenantID: uuid.New(), userID: uuid.New()}
	rig.failWith = func(v port.EventValidator) { svc.Validator = v }

	switch entry {
	case "start":
		rig.eventType = domain.EventWorkflowInstanceStarted
		versionID := uuid.New()
		definitions.resp = publishedCompiledWorkflow(uuid.New(), versionID, compiledPlanJSON(t, assignedStage("review")))
		rig.run = func() (uuid.UUID, error) {
			got, err := svc.Start(context.Background(), port.StartInstanceInput{
				TenantID: rig.tenantID, WorkflowVersionID: versionID, SubjectID: "SUBJ-1", BusinessKey: "TND-w14b",
				ContextJSON: json.RawMessage(`{}`), StartedByUserID: rig.userID,
			})
			if got == nil {
				return uuid.Nil, err
			}
			return got.ID, err
		}
	case "terminate":
		rig.eventType = domain.EventWorkflowInstanceTerminated
		inst := &domain.Instance{
			ID: uuid.New(), TenantID: rig.tenantID, Status: domain.InstanceStatusRunning, RecordVersion: 1,
			TemporalWorkflowID: "tenant:biz", StartedByUserID: uuid.New(),
		}
		instances.byID[inst.ID] = inst
		rig.run = func() (uuid.UUID, error) {
			return inst.ID, svc.Terminate(context.Background(), rig.tenantID, inst.ID, rig.userID, "admin requested")
		}
	default:
		t.Fatalf("unknown entry %q", entry)
	}
	return rig
}

func TestInstanceEventSink_StartAndTerminate_EventSubjectNamesTheInstance(t *testing.T) {
	t.Parallel()

	for _, entry := range []string{"start", "terminate"} {
		t.Run(entry, func(t *testing.T) {
			t.Parallel()
			rig := w14bNewEventRig(t, entry)

			instanceID, err := rig.run()

			require.NoError(t, err)
			require.Len(t, rig.outbox.attempts, 1)
			env := rig.outbox.attempts[0]
			assert.Equal(t, "instances/"+instanceID.String(), env.Subject)
			assert.Equal(t, rig.eventType, env.Type)
			assert.Equal(t, rig.tenantID.String(), env.TenantID)
			assert.Equal(t, rig.userID.String(), env.Actor)
		})
	}
}

func TestInstanceEventSink_StartAndTerminate_RefusedEnvelopeNeverReachesTheOutbox(t *testing.T) {
	t.Parallel()

	for _, entry := range []string{"start", "terminate"} {
		t.Run(entry, func(t *testing.T) {
			t.Parallel()
			rig := w14bNewEventRig(t, entry)
			rig.failWith(failingValidator{})

			_, err := rig.run()

			require.ErrorIs(t, err, errValidationFailed)
			assert.Empty(t, rig.outbox.attempts, "the outbox is not touched when the envelope cannot be built")
		})
	}
}

func TestInstanceEventSink_StartAndTerminate_OutboxErrorComesBackUnchanged(t *testing.T) {
	t.Parallel()

	for _, entry := range []string{"start", "terminate"} {
		t.Run(entry, func(t *testing.T) {
			t.Parallel()
			rig := w14bNewEventRig(t, entry)
			rig.outbox.enqueueErr = errW14bOutbox

			_, err := rig.run()

			require.ErrorIs(t, err, errW14bOutbox)
			assert.Equal(t, errW14bOutbox.Error(), err.Error(), "the outbox error is not wrapped on the way out")
			require.Len(t, rig.outbox.attempts, 1)
			assert.Equal(t, rig.eventType, rig.outbox.attempts[0].Type)
		})
	}
}

type w14bEventListCall struct {
	tenantID   uuid.UUID
	instanceID uuid.UUID
	after      *port.Cursor
	limit      int
}

// w14bEventPager records each outbox listing request and answers with a
// scripted next cursor.
type w14bEventPager struct {
	*fakeOutbox
	next  *port.Cursor
	calls []w14bEventListCall
}

func (o *w14bEventPager) ListByInstance(_ context.Context, tenantID, instanceID uuid.UUID, page port.PageRequest) ([]*domain.OutboxEventRecord, *port.Cursor, error) {
	call := w14bEventListCall{tenantID: tenantID, instanceID: instanceID, limit: page.Limit}
	if page.After != nil {
		after := *page.After
		call.after = &after
	}
	o.calls = append(o.calls, call)
	return nil, o.next, nil
}

func TestInstanceServiceListEvents_PageCursorGoesToTheRepositoryAndTheNextOneIsEncoded(t *testing.T) {
	t.Parallel()

	from := &port.CursorPosition{CreatedAt: time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC), ID: uuid.New()}
	next := &port.Cursor{CreatedAt: time.Date(2026, 3, 4, 5, 6, 9, 0, time.UTC), ID: uuid.New()}
	cases := []struct {
		name     string
		cursor   *port.CursorPosition
		next     *port.Cursor
		wantNext bool
	}{
		{name: "no cursor in, no next cursor out", cursor: nil, next: nil},
		{name: "a cursor in reaches the repository, and the next cursor comes back encoded", cursor: from, next: next, wantNext: true},
		{name: "a cursor in on the last page gives no next cursor", cursor: from, next: nil},
		{name: "no cursor in on a first page with more behind it still encodes the next one", cursor: nil, next: next, wantNext: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			svc, instances, _, _, fake, _, _, _ := newInstanceServiceHarness()
			pager := &w14bEventPager{fakeOutbox: fake, next: tc.next}
			svc.Outbox = pager
			tenantID := uuid.New()
			inst := &domain.Instance{ID: uuid.New(), TenantID: tenantID, Status: domain.InstanceStatusRunning}
			instances.byID[inst.ID] = inst

			res, err := svc.ListEvents(context.Background(), tenantID, inst.ID, port.ReadScope{IsAdmin: true}, port.Page{Cursor: tc.cursor, Limit: 7})

			require.NoError(t, err)
			require.Len(t, pager.calls, 1)
			call := pager.calls[0]
			assert.Equal(t, tenantID, call.tenantID)
			assert.Equal(t, inst.ID, call.instanceID)
			assert.Equal(t, 7, call.limit)
			if tc.cursor == nil {
				assert.Nil(t, call.after)
			} else {
				require.NotNil(t, call.after)
				assert.True(t, tc.cursor.CreatedAt.Equal(call.after.CreatedAt))
				assert.Equal(t, tc.cursor.ID, call.after.ID)
			}
			if !tc.wantNext {
				assert.Empty(t, res.NextCursor)
				return
			}
			require.NotEmpty(t, res.NextCursor)
			decoded, err := port.DecodeCursor(res.NextCursor)
			require.NoError(t, err)
			assert.True(t, tc.next.CreatedAt.Equal(decoded.CreatedAt))
			assert.Equal(t, tc.next.ID, decoded.ID)
		})
	}
}
