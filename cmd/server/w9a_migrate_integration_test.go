//go:build integration

package main

import (
	"context"
	"fmt"
	"io/fs"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/inbox"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-events/v2/pkg/outbox"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/migrate"
	documentsql "github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/connectors/documents/sqlstore"
	intentsql "github.com/BCBP-SOLUTIONS-FZC-LLC/workflow-connectors/v2/pkg/connectors/sendintent/sqlstore"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/db/migrations"
	"github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/internal/config"
)

// w9aMigrationSteps are the five version tables runMigrations fills, in the order the sequence applies them.
var w9aMigrationSteps = []struct {
	name   string
	prefix string
	table  string
}{
	{name: "outbox", prefix: "outbox schema: ", table: outbox.MigrationsTable},
	{name: "inbox", prefix: "inbox schema: ", table: inbox.MigrationsTable},
	{name: "connector documents", prefix: "connector documents schema: ", table: documentsql.MigrationsTable},
	{name: "connector send intents", prefix: "connector send intents schema: ", table: intentsql.MigrationsTable},
	{name: "domain", prefix: "domain migrations: ", table: "wf_execution_migrations"},
}

// w9aEmptyPostgres starts one Postgres container with no schema and returns its admin DSN.
func w9aEmptyPostgres(t *testing.T) string {
	t.Helper()
	if testing.Short() {
		t.Skip("skipping integration test: -short flag set (Docker required)")
	}
	ctx := context.Background()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:18-alpine",
			ExposedPorts: []string{"5432/tcp"},
			Env:          map[string]string{"POSTGRES_USER": "postgres", "POSTGRES_PASSWORD": "postgres", "POSTGRES_DB": "postgres"},
			WaitingFor: wait.ForAll(
				wait.ForLog("database system is ready to accept connections").WithOccurrence(2),
				wait.ForListeningPort("5432/tcp"),
			).WithDeadline(120 * time.Second),
		},
		Started: true,
	})
	if container != nil {
		t.Cleanup(func() { _ = container.Terminate(context.Background()) })
	}
	require.NoError(t, err)
	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, "5432/tcp")
	require.NoError(t, err)
	return fmt.Sprintf("postgres://postgres:postgres@%s:%s/postgres?sslmode=disable", host, port.Port())
}

var w9aDatabaseSeq atomic.Int64

// w9aFreshDatabase creates an empty database inside the container and returns its DSN.
func w9aFreshDatabase(t *testing.T, adminDSN string) string {
	t.Helper()
	name := fmt.Sprintf("w9a_db_%d", w9aDatabaseSeq.Add(1))
	w9aExec(t, adminDSN, "CREATE DATABASE "+name)
	u, err := url.Parse(adminDSN)
	require.NoError(t, err)
	u.Path = "/" + name
	return u.String()
}

func w9aExec(t *testing.T, dsn string, statements ...string) {
	t.Helper()
	w9aWithConn(t, dsn, func(ctx context.Context, conn *pgxpool.Conn) error {
		for _, stmt := range statements {
			if _, err := conn.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("%s: %w", stmt, err)
			}
		}
		return nil
	})
}

func w9aTableExists(t *testing.T, dsn, table string) bool {
	t.Helper()
	var exists bool
	w9aWithConn(t, dsn, func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx, "SELECT to_regclass($1) IS NOT NULL", table).Scan(&exists)
	})
	return exists
}

func w9aVersionRow(t *testing.T, dsn, table string) (version int64, dirty bool) {
	t.Helper()
	w9aWithConn(t, dsn, func(ctx context.Context, conn *pgxpool.Conn) error {
		return conn.QueryRow(ctx, fmt.Sprintf("SELECT version, dirty FROM %s", table)).Scan(&version, &dirty)
	})
	return version, dirty
}

// w9aCreationOrder returns the given tables sorted by the order Postgres created them (its object ids only grow).
func w9aCreationOrder(t *testing.T, dsn string, tables []string) []string {
	t.Helper()
	var ordered []string
	w9aWithConn(t, dsn, func(ctx context.Context, conn *pgxpool.Conn) error {
		rows, err := conn.Query(ctx, "SELECT relname FROM pg_class WHERE relname = ANY($1) AND relkind = 'r' ORDER BY oid", tables)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var name string
			if err := rows.Scan(&name); err != nil {
				return err
			}
			ordered = append(ordered, name)
		}
		return rows.Err()
	})
	return ordered
}

func w9aDomainHeadVersion(t *testing.T) int64 {
	t.Helper()
	var head int64
	entries, err := fs.ReadDir(migrations.FS, ".")
	require.NoError(t, err)
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".up.sql") {
			continue
		}
		n, err := strconv.ParseInt(strings.SplitN(e.Name(), "_", 2)[0], 10, 64)
		require.NoError(t, err)
		head = max(head, n)
	}
	require.Positive(t, head)
	return head
}

func w9aStepTables() []string {
	tables := make([]string, 0, len(w9aMigrationSteps))
	for _, s := range w9aMigrationSteps {
		tables = append(tables, s.table)
	}
	return tables
}

func TestRunMigrations_AnEmptyDatabaseGetsFiveCleanVersionTablesInOrderAndARerunChangesNothing(t *testing.T) {
	dsn := w9aFreshDatabase(t, w9aEmptyPostgres(t))
	log := &w9aLogger{}

	require.NoError(t, runMigrations(context.Background(), dsn, log))

	assert.Equal(t, w9aStepTables(), w9aCreationOrder(t, dsn, w9aStepTables()), "outbox, inbox, documents, send intents, then the domain table")
	first := map[string]int64{}
	for _, step := range w9aMigrationSteps {
		version, dirty := w9aVersionRow(t, dsn, step.table)
		assert.Positive(t, version, step.name)
		assert.False(t, dirty, step.name)
		first[step.table] = version
	}
	assert.Equal(t, w9aDomainHeadVersion(t), first["wf_execution_migrations"], "the domain table sits at the newest migration the binary carries")
	messages := []string{}
	for _, r := range log.all() {
		messages = append(messages, r.msg)
	}
	assert.Contains(t, messages, "running migrations")
	assert.Contains(t, messages, "migrations applied")

	require.NoError(t, runMigrations(context.Background(), dsn, &w9aLogger{}), "a rerun on an up-to-date database is not an error")
	for _, step := range w9aMigrationSteps {
		version, dirty := w9aVersionRow(t, dsn, step.table)
		assert.Equal(t, first[step.table], version, step.name)
		assert.False(t, dirty, step.name)
	}
}

func TestRunMigrations_AFailingLibrarySchemaStepSkipsTheLaterStepsAndNamesItself(t *testing.T) {
	adminDSN := w9aEmptyPostgres(t)
	for i, failing := range w9aMigrationSteps[:4] {
		t.Run(failing.name, func(t *testing.T) {
			dsn := w9aFreshDatabase(t, adminDSN)
			w9aExec(t, dsn,
				fmt.Sprintf("CREATE TABLE %s (version bigint NOT NULL PRIMARY KEY, dirty boolean NOT NULL)", failing.table),
				fmt.Sprintf("INSERT INTO %s (version, dirty) VALUES (1, true)", failing.table))

			err := runMigrations(context.Background(), dsn, &w9aLogger{})

			require.Error(t, err)
			require.ErrorIs(t, err, migrate.ErrMigrationDirty)
			assert.True(t, strings.HasPrefix(err.Error(), failing.prefix), "error = %v", err)
			for j, step := range w9aMigrationSteps {
				switch {
				case j < i:
					version, dirty := w9aVersionRow(t, dsn, step.table)
					assert.Positive(t, version, "%s ran before the failure", step.name)
					assert.False(t, dirty, step.name)
				case j > i:
					assert.False(t, w9aTableExists(t, dsn, step.table), "%s must be skipped after %s failed", step.name, failing.name)
				}
			}
			assert.False(t, w9aTableExists(t, dsn, "workflow_instance"), "the domain tables are never reached")
		})
	}

	t.Run("the built binary prints migrate error and exits one", func(t *testing.T) {
		dsn := w9aFreshDatabase(t, adminDSN)
		w9aExec(t, dsn,
			"CREATE TABLE inbox_migrations (version bigint NOT NULL PRIMARY KEY, dirty boolean NOT NULL)",
			"INSERT INTO inbox_migrations (version, dirty) VALUES (1, true)")

		res := w9aRunServer(t, []string{"APP_ENV=dev", "DATABASE_URL=" + dsn}, "migrate")

		assert.Equal(t, 1, res.exitCode)
		assert.Contains(t, res.stderr, "migrate error: inbox schema: ")
		assert.False(t, w9aTableExists(t, dsn, documentsql.MigrationsTable), "the later steps did not run")
	})
}

func TestRunMigrations_ADirtyDomainTableReturnsDomainMigrationsErrorAfterTheLibrarySchemas(t *testing.T) {
	adminDSN := w9aEmptyPostgres(t)
	cases := []struct {
		name string
		seed func(t *testing.T, dsn string)
	}{
		{name: "a version table left dirty", seed: func(t *testing.T, dsn string) {
			w9aExec(t, dsn,
				"CREATE TABLE wf_execution_migrations (version bigint NOT NULL PRIMARY KEY, dirty boolean NOT NULL)",
				"INSERT INTO wf_execution_migrations (version, dirty) VALUES (2, true)")
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dsn := w9aFreshDatabase(t, adminDSN)
			tc.seed(t, dsn)

			err := runMigrations(context.Background(), dsn, &w9aLogger{})

			require.Error(t, err)
			require.ErrorIs(t, err, migrate.ErrMigrationDirty)
			assert.True(t, strings.HasPrefix(err.Error(), "domain migrations: "), "error = %v", err)
			for _, step := range w9aMigrationSteps[:4] {
				_, dirty := w9aVersionRow(t, dsn, step.table)
				assert.False(t, dirty, "%s completed before the domain step", step.name)
			}
			version, dirty := w9aVersionRow(t, dsn, "wf_execution_migrations")
			assert.Equal(t, int64(2), version, "the dirty record is left as found")
			assert.True(t, dirty)
			assert.False(t, w9aTableExists(t, dsn, "workflow_instance"), "no domain table is created over a dirty record")
		})
	}
}

func TestRunMigrations_SixRunsReleasedTogetherOnOneDatabaseAllSucceedAndLeaveASingleCleanSchema(t *testing.T) {
	adminDSN := w9aEmptyPostgres(t)
	const runners = 6
	for round := range 3 {
		t.Run(fmt.Sprintf("round %d", round+1), func(t *testing.T) {
			dsn := w9aFreshDatabase(t, adminDSN)
			cfg := &config.Config{DatabaseURL: dsn}
			require.NoError(t, cfg.RequireMigrate())

			start := make(chan struct{})
			errs := make([]error, runners)
			var wg sync.WaitGroup
			for i := range runners {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					errs[i] = runMigrations(context.Background(), cfg.MigrationDSN(), &w9aLogger{})
				}()
			}
			close(start)
			wg.Wait()

			for i, err := range errs {
				assert.NoError(t, err, "run %d", i)
			}
			for _, step := range w9aMigrationSteps {
				version, dirty := w9aVersionRow(t, dsn, step.table)
				assert.Positive(t, version, step.name)
				assert.False(t, dirty, step.name)
			}
			domainVersion, _ := w9aVersionRow(t, dsn, "wf_execution_migrations")
			assert.Equal(t, w9aDomainHeadVersion(t), domainVersion)
		})
	}
}
