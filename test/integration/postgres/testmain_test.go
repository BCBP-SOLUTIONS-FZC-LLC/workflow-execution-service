//go:build integration

package postgres_test

import (
	"os"
	"testing"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/test/fixtures"
)

// TestMain runs this package against one shared Postgres server: each test still gets its own
// freshly migrated database, without starting a container of its own.
func TestMain(m *testing.M) {
	os.Exit(fixtures.RunWithSharedPostgres(m))
}
