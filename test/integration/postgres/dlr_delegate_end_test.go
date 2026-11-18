//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/service"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

func TestEndedDelegateRepo_RemembersTheLatestCancelPerTenantAndDelegate(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	repo := postgres.NewEndedDelegationRepo(appPool)
	ctx := context.Background()
	tenantA, tenantB, delegate := uuid.New(), uuid.New(), uuid.New()
	first := time.Date(2026, 11, 17, 9, 0, 0, 0, time.UTC)

	_, found, err := repo.DelegateEndedAt(ctx, tenantA, delegate)
	require.NoError(t, err)
	assert.False(t, found, "a delegate nobody cancelled has no time")

	require.NoError(t, repo.MarkDelegateEnded(ctx, tenantA, delegate, first))
	require.NoError(t, repo.MarkDelegateEnded(ctx, tenantA, delegate, first.Add(-time.Hour)), "an older cancel delivered late")
	at, found, err := repo.DelegateEndedAt(ctx, tenantA, delegate)
	require.NoError(t, err)
	require.True(t, found)
	assert.True(t, first.Equal(at), "an older cancel never moves the time back")

	require.NoError(t, repo.MarkDelegateEnded(ctx, tenantA, delegate, first.Add(time.Hour)))
	at, _, err = repo.DelegateEndedAt(ctx, tenantA, delegate)
	require.NoError(t, err)
	assert.True(t, first.Add(time.Hour).Equal(at), "a later cancel moves it forward")

	_, found, err = repo.DelegateEndedAt(ctx, tenantB, delegate)
	require.NoError(t, err)
	assert.False(t, found, "another tenant's delegate with the same id is not cancelled")
}

func TestEndedDelegateTable_ForcesRowLevelSecurityAndIsolatesTenants(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	ctx := context.Background()

	enabled, forced := rlsFlags(t, superPool, ctx, "ended_delegate")
	assert.True(t, enabled)
	assert.True(t, forced)

	tenantA, tenantB, delegate := uuid.New(), uuid.New(), uuid.New()
	require.NoError(t, postgres.NewEndedDelegationRepo(appPool).MarkDelegateEnded(ctx, tenantA, delegate, time.Now().UTC()))

	var ownTenant, visibleToB, withoutTenant int
	require.NoError(t, appPool.WithConn(withGUC(ctx, tenantA), func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx, "SELECT count(*) FROM ended_delegate WHERE delegate_id = $1", delegate).Scan(&ownTenant)
	}))
	require.NoError(t, appPool.WithConn(withGUC(ctx, tenantB), func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx, "SELECT count(*) FROM ended_delegate WHERE delegate_id = $1", delegate).Scan(&visibleToB)
	}))
	require.NoError(t, appPool.WithConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx, "SELECT count(*) FROM ended_delegate").Scan(&withoutTenant)
	}))
	assert.Equal(t, 1, ownTenant, "the tenant that wrote the row sees it")
	assert.Zero(t, visibleToB, "a connection scoped to another tenant sees nothing")
	assert.Zero(t, withoutTenant, "a connection with no tenant sees nothing")

	require.Error(t, appPool.WithConn(withGUC(ctx, tenantB), func(ctx context.Context, conn *pgxpool.Conn) error {
		_, err := conn.Exec(ctx, "INSERT INTO ended_delegate (tenant_id, delegate_id, ended_at) VALUES ($1, $2, now())", tenantA, uuid.New())
		return err
	}), "a connection scoped to another tenant cannot write the tenant's rows")
}

func TestTenantPurge_DeletesTheTenantsEndedDelegatesAndLeavesAnotherTenantsAlone(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	repo := postgres.NewTenantRetentionRepo(appPool)
	ended := postgres.NewEndedDelegationRepo(superPool)
	ctx := context.Background()
	due, other := uuid.New(), uuid.New()
	now := time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		require.NoError(t, ended.MarkDelegateEnded(ctx, due, uuid.New(), now))
	}
	require.NoError(t, ended.MarkDelegateEnded(ctx, other, uuid.New(), now))
	require.NoError(t, repo.RecordOffboarded(ctx, due, now.Add(-48*time.Hour), now.Add(-time.Hour)))

	has, err := repo.HasRows(ctx, due)
	require.NoError(t, err)
	assert.True(t, has, "a delegate cancel alone counts as a row of the tenant")

	recorded := map[string]int64{}
	purger := service.NewTenantPurger(service.TenantPurgerDeps{
		Retention: repo, Data: repo, Batch: 2, Interval: time.Hour,
		Now:    func() time.Time { return now },
		Record: func(table string, n int64) { recorded[table] += n },
		Failed: func() {},
	})
	require.NoError(t, purger.Tick(ctx))

	assert.Zero(t, countRows(t, superPool, "ended_delegate", due))
	assert.Equal(t, 1, countRows(t, superPool, "ended_delegate", other), "another tenant's rows stay")
	assert.EqualValues(t, 5, recorded["ended_delegate"])
	has, err = repo.HasRows(ctx, due)
	require.NoError(t, err)
	assert.False(t, has)
}

func TestCancelByDelegate_WithNoDelegationNamedRecordsTheTimeForTheDelegateUnderRowLevelSecurity(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	ended := postgres.NewEndedDelegationRepo(appPool)
	client := &service.WorkflowClient{
		Instances: postgres.NewInstanceRepo(appPool), Tasks: postgres.NewTaskRepo(appPool), Assignments: postgres.NewTaskAssignmentRepo(appPool),
		Transactor: postgres.NewTransactor(appPool), Signals: postgres.NewOutboxRepo(appPool), Ended: ended,
		Locks: postgres.NewRecencyGuardRepo(appPool, appPool),
	}
	ctx := context.Background()
	tenantID, delegate := uuid.New(), uuid.New()

	_, err := client.CancelByDelegate(ctx, port.CancelByDelegateInput{TenantID: tenantID, DelegateUserID: delegate})

	require.NoError(t, err)
	_, found, err := ended.DelegateEndedAt(ctx, tenantID, delegate)
	require.NoError(t, err)
	assert.True(t, found, "the cancel recorded when every delegation to the delegate ended")
	_, found, err = ended.DelegateEndedAt(ctx, uuid.New(), delegate)
	require.NoError(t, err)
	assert.False(t, found, "another tenant reads nothing")
}

func TestRecencyGuardRepo_WithLocks_TakesEveryScopeOnOneConnection(t *testing.T) {
	_, dsn := fixtures.NewTestPoolAndDSN(t)
	ctx := context.Background()
	single, err := pgcommon.NewPool(ctx, pgcommon.Config{DSN: dsn, MaxConns: 1})
	require.NoError(t, err)
	t.Cleanup(single.Close)
	repo := postgres.NewRecencyGuardRepo(single, single)
	keys := []string{"dlr:" + uuid.NewString(), "dlr:" + uuid.NewString(), "dlr:" + uuid.NewString(), "dlr:" + uuid.NewString()}

	ran := false
	err = repo.WithLocks(ctx, append(keys, keys[0]), func(ctx context.Context) error {
		ran = true
		return nil
	})

	require.NoError(t, err, "a pool of one connection is enough, and a repeated scope is taken once")
	assert.True(t, ran)
	assert.Zero(t, advisoryLocksHeld(t, single), "every scope was released")
}

func TestRecencyGuardRepo_WithLocks_WaitsForAnyScopeAndLeavesTheOthersFree(t *testing.T) {
	pool := fixtures.NewTestPool(t)
	repo := postgres.NewRecencyGuardRepo(pool, pool)
	shared, onlyFirst, onlySecond := "dlr:"+uuid.NewString(), "dlr:"+uuid.NewString(), "dlr:"+uuid.NewString()
	ctx := context.Background()

	inside := make(chan struct{})
	release := make(chan struct{})
	holderDone := make(chan error, 1)
	go func() {
		holderDone <- repo.WithLocks(ctx, []string{shared, onlyFirst}, func(context.Context) error {
			close(inside)
			<-release
			return nil
		})
	}()
	<-inside

	waitCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
	defer cancel()
	ran := false
	err := repo.WithLocks(waitCtx, []string{onlySecond, shared}, func(context.Context) error {
		ran = true
		return nil
	})
	require.ErrorIs(t, err, context.DeadlineExceeded, "a scope held by someone else keeps the caller waiting")
	assert.False(t, ran)

	freed := false
	require.NoError(t, repo.WithLocks(ctx, []string{onlySecond}, func(context.Context) error {
		freed = true
		return nil
	}), "the scope the waiting caller had taken first was given back")
	assert.True(t, freed)

	close(release)
	require.NoError(t, <-holderDone)
}
