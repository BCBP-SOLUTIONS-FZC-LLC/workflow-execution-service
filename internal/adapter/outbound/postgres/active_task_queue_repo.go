package postgres

import (
	"context"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/pkg/pgcommon"
	"github.com/google/uuid"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres/db"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
)

var _ port.ActiveTaskQueueRepository = (*ActiveTaskQueueRepo)(nil)

// ActiveTaskQueueRepo has no RLS tenant scoping — active_task_queues is
// deliberately excluded from row-level security (db/migrations' own note): a
// Worker process legitimately reads every tenant's registered queue in one
// query, so none of these methods sets the tenant GUC.
type ActiveTaskQueueRepo struct {
	pool *pgcommon.Pool
}

func NewActiveTaskQueueRepo(pool *pgcommon.Pool) *ActiveTaskQueueRepo {
	return &ActiveTaskQueueRepo{pool: pool}
}

func (r *ActiveTaskQueueRepo) ListActive(ctx context.Context) ([]*domain.ActiveTaskQueue, error) {
	var queues []*domain.ActiveTaskQueue
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		rows, err := db.New(dbtx).ListActiveTaskQueues(ctx)
		if err != nil {
			return mapErr(err)
		}
		queues = make([]*domain.ActiveTaskQueue, len(rows))
		for i, row := range rows {
			queues[i] = activeTaskQueueFromDB(row)
		}
		return nil
	})
	return queues, err
}

func (r *ActiveTaskQueueRepo) GetByQueueName(ctx context.Context, queueName string) (*domain.ActiveTaskQueue, error) {
	var queue *domain.ActiveTaskQueue
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		row, err := db.New(dbtx).GetActiveTaskQueueByName(ctx, queueName)
		if err != nil {
			return mapErr(err)
		}
		queue = activeTaskQueueFromDB(row)
		return nil
	})
	return queue, err
}

func (r *ActiveTaskQueueRepo) Register(ctx context.Context, tenantID uuid.UUID, queueName string) (*domain.ActiveTaskQueue, error) {
	var queue *domain.ActiveTaskQueue
	err := exec(ctx, r.pool, func(dbtx db.DBTX) error {
		row, err := db.New(dbtx).RegisterActiveTaskQueue(ctx, db.RegisterActiveTaskQueueParams{
			ID: uuid.New(), TenantID: tenantID, QueueName: queueName,
		})
		if err != nil {
			return mapErr(err)
		}
		queue = activeTaskQueueFromDB(row)
		return nil
	})
	return queue, err
}

func (r *ActiveTaskQueueRepo) Deregister(ctx context.Context, queueName string) error {
	return exec(ctx, r.pool, func(dbtx db.DBTX) error {
		if err := db.New(dbtx).DeregisterActiveTaskQueue(ctx, queueName); err != nil {
			return mapErr(err)
		}
		return nil
	})
}
