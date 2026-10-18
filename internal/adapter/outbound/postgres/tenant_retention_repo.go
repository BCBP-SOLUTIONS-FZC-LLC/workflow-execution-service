package postgres

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres/db"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var (
	_ port.TenantRetentionRepository = (*TenantRetentionRepo)(nil)
	_ port.TenantDataPurger          = (*TenantRetentionRepo)(nil)
)

type TenantRetentionRepo struct {
	pool *pgcommon.Pool
}

func NewTenantRetentionRepo(pool *pgcommon.Pool) *TenantRetentionRepo {
	return &TenantRetentionRepo{pool: pool}
}

func (r *TenantRetentionRepo) RecordOffboarded(ctx context.Context, tenantID uuid.UUID, offboardedAt, purgeAfter time.Time) error {
	return exec(ctx, r.pool, func(dbtx db.DBTX) error {
		return mapErr(db.New(dbtx).RecordTenantOffboarded(ctx, db.RecordTenantOffboardedParams{
			TenantID: tenantID, OffboardedAt: offboardedAt, HistoryPurgeAfter: purgeAfter,
		}))
	})
}

func (r *TenantRetentionRepo) ListDue(ctx context.Context, now time.Time, limit int) ([]uuid.UUID, error) {
	var ids []uuid.UUID
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		rows, err := db.New(dbtx).ListTenantsDueForPurge(ctx, db.ListTenantsDueForPurgeParams{HistoryPurgeAfter: now, Limit: int32(limit)})
		if err != nil {
			return mapErr(err)
		}
		ids = rows
		return nil
	})
	return ids, err
}

func (r *TenantRetentionRepo) MarkPurged(ctx context.Context, tenantID uuid.UUID, at time.Time) error {
	return exec(ctx, r.pool, func(dbtx db.DBTX) error {
		return mapErr(db.New(dbtx).MarkTenantPurged(ctx, db.MarkTenantPurgedParams{TenantID: tenantID, PurgedAt: pgtype.Timestamptz{Time: at, Valid: true}}))
	})
}

type purgeStep struct {
	table string
	run   func(ctx context.Context, q *db.Queries, tenantID uuid.UUID, limit int32) (int64, error)
}

var tenantPurgeSteps = []purgeStep{
	{"assignee_overrides", func(ctx context.Context, q *db.Queries, t uuid.UUID, n int32) (int64, error) {
		return q.PurgeTenantAssigneeOverrides(ctx, db.PurgeTenantAssigneeOverridesParams{TenantID: t, Limit: n})
	}},
	{"workflow_task_assignment", func(ctx context.Context, q *db.Queries, t uuid.UUID, n int32) (int64, error) {
		return q.PurgeTenantTaskAssignments(ctx, db.PurgeTenantTaskAssignmentsParams{TenantID: t, Limit: n})
	}},
	{"workflow_task_link", func(ctx context.Context, q *db.Queries, t uuid.UUID, n int32) (int64, error) {
		return q.UnlinkTenantDeferredTasks(ctx, db.UnlinkTenantDeferredTasksParams{TenantID: t, Limit: n})
	}},
	{"workflow_task", func(ctx context.Context, q *db.Queries, t uuid.UUID, n int32) (int64, error) {
		return q.PurgeTenantTasks(ctx, db.PurgeTenantTasksParams{TenantID: t, Limit: n})
	}},
	{"workflow_instance", func(ctx context.Context, q *db.Queries, t uuid.UUID, n int32) (int64, error) {
		return q.PurgeTenantInstances(ctx, db.PurgeTenantInstancesParams{TenantID: t, Limit: n})
	}},
	{"connector_documents", func(ctx context.Context, q *db.Queries, t uuid.UUID, n int32) (int64, error) {
		return q.PurgeTenantConnectorDocuments(ctx, db.PurgeTenantConnectorDocumentsParams{TenantID: t.String(), Limit: n})
	}},
	{"connector_document_attempts", func(ctx context.Context, q *db.Queries, t uuid.UUID, n int32) (int64, error) {
		return q.PurgeTenantConnectorDocumentAttempts(ctx, db.PurgeTenantConnectorDocumentAttemptsParams{TenantID: t.String(), Limit: n})
	}},
	{"connector_send_intents", func(ctx context.Context, q *db.Queries, t uuid.UUID, n int32) (int64, error) {
		return q.PurgeTenantConnectorSendIntents(ctx, db.PurgeTenantConnectorSendIntentsParams{TenantID: t.String(), Limit: n})
	}},
	{"active_task_queues", func(ctx context.Context, q *db.Queries, t uuid.UUID, _ int32) (int64, error) {
		return q.PurgeTenantQueues(ctx, t)
	}},
	{"event_recency_guard", func(ctx context.Context, q *db.Queries, t uuid.UUID, n int32) (int64, error) {
		return q.PurgeTenantRecencyGuards(ctx, db.PurgeTenantRecencyGuardsParams{
			ScopeKey: "tenant:" + t.String(), ScopeKey_2: "user_availability:" + t.String() + ":%", Limit: n,
		})
	}},
}

func (r *TenantRetentionRepo) PurgeBatch(ctx context.Context, tenantID uuid.UUID, limit int) (map[string]int64, error) {
	deleted := map[string]int64{}
	ctx = withTenantGUC(ctx, tenantID)
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		q := db.New(dbtx)
		for _, step := range tenantPurgeSteps {
			n, err := step.run(ctx, q, tenantID, int32(limit))
			if err != nil {
				return fmt.Errorf("purge %s: %w", step.table, mapErr(err))
			}
			if n > 0 {
				deleted[step.table] = n
				return nil
			}
		}
		return nil
	})
	return deleted, err
}
