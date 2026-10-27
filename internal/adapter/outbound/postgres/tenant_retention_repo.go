package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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
			TenantID: tenantID, OffboardedAt: timestamptz(offboardedAt), HistoryPurgeAfter: timestamptz(purgeAfter),
		}))
	})
}

func (r *TenantRetentionRepo) RecordReturned(ctx context.Context, tenantID uuid.UUID, at time.Time) error {
	return exec(ctx, r.pool, func(dbtx db.DBTX) error {
		return mapErr(db.New(dbtx).RecordTenantReturned(ctx, db.RecordTenantReturnedParams{TenantID: tenantID, ReturnedAt: timestamptz(at)}))
	})
}

func (r *TenantRetentionRepo) ListDue(ctx context.Context, now time.Time, limit int) ([]port.TenantPurgeDue, error) {
	var due []port.TenantPurgeDue
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		rows, err := db.New(dbtx).ListTenantsDueForPurge(ctx, db.ListTenantsDueForPurgeParams{HistoryPurgeAfter: timestamptz(now), Limit: clampInt32(limit)})
		if err != nil {
			return mapErr(err)
		}
		for _, row := range rows {
			due = append(due, port.TenantPurgeDue{TenantID: row.TenantID, Attempts: int(row.Attempts), RecordVersion: row.RecordVersion})
		}
		return nil
	})
	return due, err
}

func (r *TenantRetentionRepo) MarkPurged(ctx context.Context, tenantID uuid.UUID, version int64, at time.Time) (int64, error) {
	var next int64
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		var err error
		next, err = db.New(dbtx).MarkTenantPurged(ctx, db.MarkTenantPurgedParams{TenantID: tenantID, RecordVersion: version, PurgedAt: timestamptz(at)})
		return retentionChanged(err)
	})
	return next, err
}

func (r *TenantRetentionRepo) RecordFailure(ctx context.Context, tenantID uuid.UUID, version int64, attempts int, next time.Time) error {
	return exec(ctx, r.pool, func(dbtx db.DBTX) error {
		_, err := db.New(dbtx).RecordTenantPurgeFailure(ctx, db.RecordTenantPurgeFailureParams{
			TenantID: tenantID, RecordVersion: version, Attempts: clampInt32(attempts), NextAttemptAt: next,
		})
		return retentionChanged(err)
	})
}

func (r *TenantRetentionRepo) Current(ctx context.Context, tenantID uuid.UUID, version int64) error {
	return exec(ctx, r.pool, func(dbtx db.DBTX) error {
		return lockCurrentRetention(ctx, db.New(dbtx), tenantID, version)
	})
}

func lockCurrentRetention(ctx context.Context, q *db.Queries, tenantID uuid.UUID, version int64) error {
	_, err := q.LockCurrentTenantRetention(ctx, db.LockCurrentTenantRetentionParams{TenantID: tenantID, RecordVersion: version})
	return retentionChanged(err)
}

func retentionChanged(err error) error {
	if errors.Is(err, pgx.ErrNoRows) {
		return port.ErrTenantRetentionChanged
	}
	return mapErr(err)
}

func timestamptz(t time.Time) pgtype.Timestamptz {
	return pgtype.Timestamptz{Time: t, Valid: true}
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

func (r *TenantRetentionRepo) PurgeBatch(ctx context.Context, tenantID uuid.UUID, version int64, limit int) (map[string]int64, error) {
	deleted := map[string]int64{}
	ctx = withTenantGUC(ctx, tenantID)
	err := pgcommon.RunInTx(ctx, r.pool, pgx.TxOptions{}, func(ctx context.Context, tx pgx.Tx) error {
		q := db.New(tx)
		if err := lockCurrentRetention(ctx, q, tenantID, version); err != nil {
			return err
		}
		for _, step := range tenantPurgeSteps {
			n, err := step.run(ctx, q, tenantID, clampInt32(limit))
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
	if err != nil {
		return nil, fmt.Errorf("purge batch: %w", err)
	}
	return deleted, nil
}

func (r *TenantRetentionRepo) HasRows(ctx context.Context, tenantID uuid.UUID) (bool, error) {
	ctx = withTenantGUC(ctx, tenantID)
	var has bool
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		v, err := db.New(dbtx).TenantHasRows(ctx, db.TenantHasRowsParams{
			TenantID: tenantID, TenantText: tenantID.String(),
			TenantScope: "tenant:" + tenantID.String(), UserScope: "user_availability:" + tenantID.String() + ":%",
		})
		if err != nil {
			return mapErr(err)
		}
		has = v
		return nil
	})
	return has, err
}
