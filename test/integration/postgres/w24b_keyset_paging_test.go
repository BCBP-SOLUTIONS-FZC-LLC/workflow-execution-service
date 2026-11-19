//go:build integration

package postgres_test

import (
	"bytes"
	"context"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

// w24bSortedNewestFirst orders ids the way the keyset pages must return rows
// that share one created_at: id descending.
func w24bSortedNewestFirst(ids []uuid.UUID) []uuid.UUID {
	sorted := append([]uuid.UUID(nil), ids...)
	sort.Slice(sorted, func(i, j int) bool { return bytes.Compare(sorted[i][:], sorted[j][:]) > 0 })
	return sorted
}

// w24bPage is one page of a list call, reduced to the ids and the cursor.
type w24bPage struct {
	ids  []uuid.UUID
	next *port.Cursor
}

// w24bPager reads one page of a seeded list after the given cursor.
type w24bPager func(t *testing.T, after *port.Cursor, limit int) w24bPage

// w24bStoredCreatedAt reads a row's created_at straight from the table.
func w24bStoredCreatedAt(t *testing.T, superPool *pgcommon.Pool, table string, id uuid.UUID) time.Time {
	t.Helper()
	var createdAt time.Time
	require.NoError(t, superPool.WithConn(context.Background(), func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx, "SELECT created_at FROM "+table+" WHERE id = $1", id).Scan(&createdAt)
	}))
	return createdAt
}

func TestKeysetPaging_RowsSharingOneCreatedAt_SplitByIDWithoutSkippingOrRepeating(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instanceRepo := postgres.NewInstanceRepo(appPool)
	taskRepo := postgres.NewTaskRepo(appPool)
	outboxRepo := postgres.NewOutboxRepo(appPool)
	transactor := postgres.NewTransactor(appPool)
	ctx := context.Background()

	// A timestamp with a sub-microsecond part: the table keeps microseconds, so
	// the cursor the repository hands back must still select exactly the rows
	// after the last one returned.
	shared := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond).Add(500 * time.Nanosecond)

	type seeded struct {
		table string
		ids   []uuid.UUID
		pager w24bPager
	}
	cases := []struct {
		name string
		seed func(t *testing.T, count int) seeded
	}{
		{
			name: "instances of a tenant",
			seed: func(t *testing.T, count int) seeded {
				tenant := uuid.New()
				var ids []uuid.UUID
				for i := 0; i < count; i++ {
					inst := newInstance(tenant, shared)
					require.NoError(t, instanceRepo.Create(ctx, inst))
					seedInstanceAt(t, superPool, ctx, inst.ID, shared)
					ids = append(ids, inst.ID)
				}
				return seeded{table: "workflow_instance", ids: ids, pager: func(t *testing.T, after *port.Cursor, limit int) w24bPage {
					rows, next, err := instanceRepo.ListByTenant(ctx, tenant, port.InstanceListFilter{}, port.PageRequest{After: after, Limit: limit})
					require.NoError(t, err)
					page := w24bPage{next: next}
					for _, row := range rows {
						page.ids = append(page.ids, row.ID)
					}
					return page
				}}
			},
		},
		{
			name: "tasks of an instance",
			seed: func(t *testing.T, count int) seeded {
				tenant := uuid.New()
				inst := newInstance(tenant, shared)
				require.NoError(t, instanceRepo.Create(ctx, inst))
				var ids []uuid.UUID
				for i := 0; i < count; i++ {
					task := newTask(tenant, inst.ID)
					require.NoError(t, taskRepo.Create(ctx, task))
					seedTaskAt(t, superPool, ctx, task.ID, shared)
					ids = append(ids, task.ID)
				}
				return seeded{table: "workflow_task", ids: ids, pager: func(t *testing.T, after *port.Cursor, limit int) w24bPage {
					rows, next, err := taskRepo.ListByInstance(ctx, tenant, inst.ID, port.PageRequest{After: after, Limit: limit})
					require.NoError(t, err)
					page := w24bPage{next: next}
					for _, row := range rows {
						page.ids = append(page.ids, row.ID)
					}
					return page
				}}
			},
		},
		{
			name: "tasks of a tenant",
			seed: func(t *testing.T, count int) seeded {
				tenant := uuid.New()
				inst := newInstance(tenant, shared)
				require.NoError(t, instanceRepo.Create(ctx, inst))
				var ids []uuid.UUID
				for i := 0; i < count; i++ {
					task := newTask(tenant, inst.ID)
					require.NoError(t, taskRepo.Create(ctx, task))
					seedTaskAt(t, superPool, ctx, task.ID, shared)
					ids = append(ids, task.ID)
				}
				return seeded{table: "workflow_task", ids: ids, pager: func(t *testing.T, after *port.Cursor, limit int) w24bPage {
					rows, next, err := taskRepo.ListByTenant(ctx, tenant, port.TaskListFilter{}, port.PageRequest{After: after, Limit: limit})
					require.NoError(t, err)
					page := w24bPage{next: next}
					for _, row := range rows {
						page.ids = append(page.ids, row.ID)
					}
					return page
				}}
			},
		},
		{
			name: "outbox events of an instance",
			seed: func(t *testing.T, count int) seeded {
				tenant := uuid.New()
				instanceID := uuid.New()
				var ids []uuid.UUID
				for i := 0; i < count; i++ {
					env := instanceEventEnvelope(tenant, instanceID)
					require.NoError(t, transactor.RunInTx(withGUC(ctx, tenant), func(ctx context.Context) error {
						return outboxRepo.Enqueue(ctx, env)
					}))
					id := uuid.MustParse(env.ID)
					w8bSuperExec(t, superPool, "UPDATE outbox_events SET created_at = $2 WHERE id = $1", id, shared)
					ids = append(ids, id)
				}
				return seeded{table: "outbox_events", ids: ids, pager: func(t *testing.T, after *port.Cursor, limit int) w24bPage {
					rows, next, err := outboxRepo.ListByInstance(ctx, tenant, instanceID, port.PageRequest{After: after, Limit: limit})
					require.NoError(t, err)
					page := w24bPage{next: next}
					for _, row := range rows {
						page.ids = append(page.ids, row.ID)
					}
					return page
				}}
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name+" walk five rows two at a time", func(t *testing.T) {
			s := tc.seed(t, 5)
			want := w24bSortedNewestFirst(s.ids)
			storedAt := w24bStoredCreatedAt(t, superPool, s.table, s.ids[0])

			first := s.pager(t, nil, 2)
			assert.Equal(t, want[0:2], first.ids)
			require.NotNil(t, first.next, "a full page with rows left carries a cursor")
			assert.Equal(t, want[1], first.next.ID, "the cursor is the last returned row")
			assert.True(t, first.next.CreatedAt.Equal(storedAt), "the cursor carries the stored created_at, got %s want %s", first.next.CreatedAt, storedAt)

			second := s.pager(t, first.next, 2)
			assert.Equal(t, want[2:4], second.ids)
			require.NotNil(t, second.next)
			assert.Equal(t, want[3], second.next.ID)

			third := s.pager(t, second.next, 2)
			assert.Equal(t, want[4:5], third.ids, "the short last page holds the one row left")
			assert.Nil(t, third.next, "a short last page has no cursor")
		})

		t.Run(tc.name+" exactly limit rows give no cursor", func(t *testing.T) {
			s := tc.seed(t, 4)
			want := w24bSortedNewestFirst(s.ids)

			exact := s.pager(t, nil, 4)
			assert.Equal(t, want, exact.ids)
			assert.Nil(t, exact.next, "exactly limit rows leave nothing for a next page")

			oneShort := s.pager(t, nil, 3)
			assert.Equal(t, want[:3], oneShort.ids)
			require.NotNil(t, oneShort.next, "limit smaller than the rows left gives a cursor")
			assert.Equal(t, want[2], oneShort.next.ID)

			rest := s.pager(t, oneShort.next, 3)
			assert.Equal(t, want[3:], rest.ids)
			assert.Nil(t, rest.next)
		})
	}
}

func TestOutboxRepo_ListByInstance_OnlyTheCallersEventsForThatInstance(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	outboxRepo := postgres.NewOutboxRepo(appPool)
	transactor := postgres.NewTransactor(appPool)
	ctx := context.Background()

	tenantA, tenantB := uuid.New(), uuid.New()
	instanceOfA, otherInstanceOfA := uuid.New(), uuid.New()

	base := time.Now().UTC().Add(-time.Hour).Truncate(time.Microsecond)
	enqueueAt := func(t *testing.T, tenant uuid.UUID, instanceID uuid.UUID, offset time.Duration) uuid.UUID {
		t.Helper()
		env := instanceEventEnvelope(tenant, instanceID)
		require.NoError(t, transactor.RunInTx(withGUC(ctx, tenant), func(ctx context.Context) error {
			return outboxRepo.Enqueue(ctx, env)
		}))
		id := uuid.MustParse(env.ID)
		w8bSuperExec(t, superPool, "UPDATE outbox_events SET created_at = $2 WHERE id = $1", id, base.Add(offset))
		return id
	}

	oldest := enqueueAt(t, tenantA, instanceOfA, 0)
	newest := enqueueAt(t, tenantA, instanceOfA, time.Minute)
	otherInstanceEvent := enqueueAt(t, tenantA, otherInstanceOfA, 2*time.Minute)
	tenantBEvent := enqueueAt(t, tenantB, instanceOfA, 3*time.Minute)

	noInstance := taskEventEnvelope(tenantA, uuid.New(), "WorkflowTaskCreated")
	require.NoError(t, transactor.RunInTx(withGUC(ctx, tenantA), func(ctx context.Context) error {
		return outboxRepo.Enqueue(ctx, noInstance)
	}))
	noInstanceID := uuid.MustParse(noInstance.ID)
	w8bSuperExec(t, superPool, "UPDATE outbox_events SET created_at = $2 WHERE id = $1", noInstanceID, base.Add(4*time.Minute))

	idsOf := func(records []*domain.OutboxEventRecord) []uuid.UUID {
		var ids []uuid.UUID
		for _, record := range records {
			ids = append(ids, record.ID)
		}
		return ids
	}

	cases := []struct {
		name       string
		tenant     uuid.UUID
		instance   uuid.UUID
		want       []uuid.UUID
		wantAbsent []uuid.UUID
	}{
		{
			name: "the owning tenant gets the instance's events newest first", tenant: tenantA, instance: instanceOfA,
			want:       []uuid.UUID{newest, oldest},
			wantAbsent: []uuid.UUID{otherInstanceEvent, tenantBEvent, noInstanceID},
		},
		{
			name: "another tenant asking for the same instance id gets only its own events for it", tenant: tenantB, instance: instanceOfA,
			want:       []uuid.UUID{tenantBEvent},
			wantAbsent: []uuid.UUID{oldest, newest, otherInstanceEvent, noInstanceID},
		},
		{
			name: "another tenant asking for an instance that is not its own gets an empty page", tenant: tenantB, instance: otherInstanceOfA,
			want:       nil,
			wantAbsent: []uuid.UUID{otherInstanceEvent},
		},
		{
			name: "an instance with no events gets an empty page", tenant: tenantA, instance: uuid.New(),
			want: nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			records, next, err := outboxRepo.ListByInstance(ctx, tc.tenant, tc.instance, port.PageRequest{Limit: 10})
			require.NoError(t, err)
			assert.Nil(t, next)
			got := idsOf(records)
			assert.Equal(t, tc.want, got)
			for _, absent := range tc.wantAbsent {
				assert.NotContains(t, got, absent)
			}
		})
	}
}
