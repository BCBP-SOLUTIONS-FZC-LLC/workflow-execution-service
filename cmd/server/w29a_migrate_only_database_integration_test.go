//go:build integration

package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// The migrate argument needs nothing but the database address: the
// definition and eligibility service addresses (and, outside dev, the relay
// address) belong to serving and are not asked for.
func TestServerBinary_MigrateWithOnlyTheDatabaseAddress_AppliesEveryMigration(t *testing.T) {
	adminDSN := w9aEmptyPostgres(t)

	cases := []struct {
		name string
		env  func(dsn string) []string
	}{
		{name: "in dev", env: func(dsn string) []string { return []string{"DATABASE_URL=" + dsn} }},
		{name: "in prod", env: func(dsn string) []string { return []string{"APP_ENV=prod", "DATABASE_URL=" + dsn} }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dsn := w9aFreshDatabase(t, adminDSN)

			res := w9aRunServer(t, tc.env(dsn), "migrate")

			assert.Equal(t, 0, res.exitCode, "stderr = %q", res.stderr)
			assert.NotContains(t, res.stderr, "config error")
			for _, step := range w9aMigrationSteps {
				version, dirty := w9aVersionRow(t, dsn, step.table)
				assert.Positive(t, version, step.name)
				assert.False(t, dirty, step.name)
			}
			version, _ := w9aVersionRow(t, dsn, "wf_execution_migrations")
			assert.Equal(t, w9aDomainHeadVersion(t), version, "the domain migrations ran to the newest one")
			assert.True(t, w9aTableExists(t, dsn, "workflow_instance"))
		})
	}
}
