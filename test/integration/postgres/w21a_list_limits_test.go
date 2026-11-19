//go:build integration

package postgres_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

const w21aSeededRows = 101

// w21aLimitCases are page limits a caller can ask for; a page never holds more than 100 rows.
var w21aLimitCases = []struct {
	name  string
	limit int
	want  int
}{
	{"the maximum", 100, 100},
	{"one over the maximum", 101, 100},
	{"far over the maximum", 100000, 100},
}

func TestW21aInstanceRepo_ListByTenant_SubjectFilterMatchesTheWholeSubjectOnly(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	repo := postgres.NewInstanceRepo(appPool)
	ctx := context.Background()
	tenant, otherTenant := uuid.New(), uuid.New()
	now := time.Now().UTC()

	seed := func(tenantID uuid.UUID, subject string) *uuid.UUID {
		inst := newInstance(tenantID, now)
		inst.SubjectID = subject
		require.NoError(t, repo.Create(ctx, inst))
		return &inst.ID
	}
	exact := seed(tenant, "subj-1")
	seed(tenant, "subj-10")
	seed(tenant, "SUBJ-1")
	seed(tenant, "subj-1 ")
	seed(tenant, "subj-")
	otherExact := seed(otherTenant, "subj-1")

	subject := "subj-1"
	got, next, err := repo.ListByTenant(ctx, tenant, port.InstanceListFilter{SubjectID: &subject}, port.PageRequest{Limit: 10})

	require.NoError(t, err)
	require.Len(t, got, 1, "a longer, shorter, differently cased or padded subject is not a match")
	assert.Equal(t, *exact, got[0].ID)
	assert.Equal(t, "subj-1", got[0].SubjectID)
	assert.Equal(t, tenant, got[0].TenantID)
	assert.Nil(t, next)

	theirs, _, err := repo.ListByTenant(ctx, otherTenant, port.InstanceListFilter{SubjectID: &subject}, port.PageRequest{Limit: 10})
	require.NoError(t, err)
	require.Len(t, theirs, 1, "the other tenant sees only its own subj-1")
	assert.Equal(t, *otherExact, theirs[0].ID)
	assert.Equal(t, otherTenant, theirs[0].TenantID)

	all, _, err := repo.ListByTenant(ctx, tenant, port.InstanceListFilter{}, port.PageRequest{Limit: 10})
	require.NoError(t, err)
	assert.Len(t, all, 5, "without the filter every subject of the tenant is listed, and none of the other tenant's")
}

func TestW21aInstanceRepo_ListByTenant_PageLimitIsClampedToOneHundred(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	repo := postgres.NewInstanceRepo(appPool)
	ctx := context.Background()
	tenant := uuid.New()
	for i := 0; i < w21aSeededRows; i++ {
		require.NoError(t, repo.Create(ctx, newInstance(tenant, time.Now().UTC())))
	}

	for _, tc := range w21aLimitCases {
		t.Run(tc.name, func(t *testing.T) {
			got, next, err := repo.ListByTenant(ctx, tenant, port.InstanceListFilter{}, port.PageRequest{Limit: tc.limit})

			require.NoError(t, err)
			assert.Len(t, got, tc.want)
			assert.NotNil(t, next, "the 101st row is still to come")
		})
	}
}

func TestW21aTaskRepo_ListByTenant_PageLimitIsClampedToOneHundred(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	instances := postgres.NewInstanceRepo(appPool)
	tasks := postgres.NewTaskRepo(appPool)
	ctx := context.Background()
	tenant := uuid.New()
	inst := newInstance(tenant, time.Now().UTC())
	require.NoError(t, instances.Create(ctx, inst))
	for i := 0; i < w21aSeededRows; i++ {
		task := newTask(tenant, inst.ID)
		task.NodeKey = fmt.Sprintf("finance/review-%d", i)
		require.NoError(t, tasks.Create(ctx, task))
	}

	for _, tc := range w21aLimitCases {
		t.Run(tc.name, func(t *testing.T) {
			got, next, err := tasks.ListByTenant(ctx, tenant, port.TaskListFilter{}, port.PageRequest{Limit: tc.limit})

			require.NoError(t, err)
			assert.Len(t, got, tc.want)
			assert.NotNil(t, next, "the 101st row is still to come")
		})
	}
}

func TestW21aOutboxRepo_ListByInstance_PageLimitIsClampedToOneHundred(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	outbox := postgres.NewOutboxRepo(appPool)
	transactor := postgres.NewTransactor(appPool)
	ctx := context.Background()
	tenant, instanceID := uuid.New(), uuid.New()
	for i := 0; i < w21aSeededRows; i++ {
		env := instanceEventEnvelope(tenant, instanceID)
		require.NoError(t, transactor.RunInTx(withGUC(ctx, tenant), func(ctx context.Context) error {
			return outbox.Enqueue(ctx, env)
		}))
	}

	for _, tc := range w21aLimitCases {
		t.Run(tc.name, func(t *testing.T) {
			got, next, err := outbox.ListByInstance(ctx, tenant, instanceID, port.PageRequest{Limit: tc.limit})

			require.NoError(t, err)
			assert.Len(t, got, tc.want)
			assert.NotNil(t, next, "the 101st event is still to come")
		})
	}
}
