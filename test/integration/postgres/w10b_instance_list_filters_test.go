//go:build integration

package postgres_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/adapter/outbound/postgres"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/domain"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/core/port"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

const w10bSubjectOne = "w10b-subject-one"

type w10bSeededInstances struct {
	versionOne uuid.UUID
	byName     map[string]uuid.UUID
}

type w10bVersions struct{ one, two uuid.UUID }

// w10bSeedFilterInstances inserts five instances of one tenant. The target matches every filter; each decoy
// misses exactly one of status, subject and version; "unrelated" misses all three.
func w10bSeedFilterInstances(ctx context.Context, t *testing.T, repo *postgres.InstanceRepo, tenant uuid.UUID, versions w10bVersions) w10bSeededInstances {
	t.Helper()
	seed := w10bSeededInstances{versionOne: versions.one, byName: map[string]uuid.UUID{}}
	versionTwo := versions.two
	startedAt := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	rows := []struct {
		name    string
		subject string
		version uuid.UUID
		status  domain.InstanceStatus
	}{
		{name: "target", subject: w10bSubjectOne, version: seed.versionOne, status: domain.InstanceStatusPaused},
		{name: "wrong version", subject: w10bSubjectOne, version: versionTwo, status: domain.InstanceStatusPaused},
		{name: "wrong subject", subject: "w10b-subject-two", version: seed.versionOne, status: domain.InstanceStatusPaused},
		{name: "wrong status", subject: w10bSubjectOne, version: seed.versionOne, status: domain.InstanceStatusRunning},
		{name: "unrelated", subject: "w10b-subject-three", version: uuid.New(), status: domain.InstanceStatusRunning},
	}
	for _, row := range rows {
		inst := newInstance(tenant, startedAt)
		inst.SubjectID = row.subject
		inst.WorkflowVersionID = row.version
		require.NoError(t, repo.Create(ctx, inst))
		if row.status != domain.InstanceStatusRunning {
			_, err := repo.UpdateStatus(ctx, tenant, inst.ID, row.status, inst.RecordVersion)
			require.NoError(t, err)
		}
		seed.byName[row.name] = inst.ID
	}
	return seed
}

func TestInstanceRepo_ListByTenant_StatusSubjectAndVersionFiltersCombineWithAnd(t *testing.T) {
	superPool, superDSN := fixtures.NewTestPoolAndDSN(t)
	appPool := newAppRolePool(t, superPool, superDSN)
	repo := postgres.NewInstanceRepo(appPool)
	ctx := context.Background()

	paused := domain.InstanceStatusPaused
	subject := w10bSubjectOne

	// Both tenants hold instances with the same subject, versions and statuses, so the same filter matches in each.
	versions := w10bVersions{one: uuid.New(), two: uuid.New()}
	otherTenant := uuid.New()
	otherSeed := w10bSeedFilterInstances(ctx, t, repo, otherTenant, versions)

	cases := []struct {
		name   string
		filter func(seed w10bSeededInstances) port.InstanceListFilter
		want   []string
	}{
		{
			name: "status, subject and version together leave the one instance matching all three",
			filter: func(seed w10bSeededInstances) port.InstanceListFilter {
				return port.InstanceListFilter{Status: &paused, SubjectID: &subject, WorkflowVersionID: &seed.versionOne}
			},
			want: []string{"target"},
		},
		{
			name: "status and subject alone keep the instance of the other version",
			filter: func(w10bSeededInstances) port.InstanceListFilter {
				return port.InstanceListFilter{Status: &paused, SubjectID: &subject}
			},
			want: []string{"target", "wrong version"},
		},
		{
			name: "subject and version alone keep the instance of the other status",
			filter: func(seed w10bSeededInstances) port.InstanceListFilter {
				return port.InstanceListFilter{SubjectID: &subject, WorkflowVersionID: &seed.versionOne}
			},
			want: []string{"target", "wrong status"},
		},
		{
			name: "status and version alone keep the instance of the other subject",
			filter: func(seed w10bSeededInstances) port.InstanceListFilter {
				return port.InstanceListFilter{Status: &paused, WorkflowVersionID: &seed.versionOne}
			},
			want: []string{"target", "wrong subject"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tenant := uuid.New()
			seed := w10bSeedFilterInstances(ctx, t, repo, tenant, versions)
			filter := tc.filter(seed)
			wantIDs := func(s w10bSeededInstances) []uuid.UUID {
				out := make([]uuid.UUID, len(tc.want))
				for i, name := range tc.want {
					out[i] = s.byName[name]
				}
				return out
			}
			listIDs := func(forTenant uuid.UUID) []uuid.UUID {
				items, _, err := repo.ListByTenant(ctx, forTenant, filter, port.PageRequest{Limit: 10})
				require.NoError(t, err)
				out := make([]uuid.UUID, len(items))
				for i, item := range items {
					out[i] = item.ID
					assert.Equal(t, forTenant, item.TenantID)
				}
				return out
			}

			assert.ElementsMatch(t, wantIDs(seed), listIDs(tenant))

			otherGot := listIDs(otherTenant)
			assert.ElementsMatch(t, wantIDs(otherSeed), otherGot, "the other tenant sees only its own matches")
			for _, id := range seed.byName {
				assert.NotContains(t, otherGot, id, "no instance of this tenant leaks into the other tenant's list")
			}
		})
	}
}
