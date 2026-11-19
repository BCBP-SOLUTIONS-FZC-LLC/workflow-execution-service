package fixtures

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sync/atomic"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/BCBP-SOLUTIONS-FZC-LLC/platform-pgcommon/v2/pkg/pgcommon"
)

// sharedPostgres is one Postgres server for a whole test binary. The schema is migrated once
// into a template database, and every NewTestPoolAndDSN call gets its own copy of it, which
// takes a fraction of a second instead of a new container and a full migration run.
type sharedPostgres struct {
	host, port string
	baseRoles  map[string]bool
	next       atomic.Int64
}

const sharedTemplateDB = "testdb_template"

var shared *sharedPostgres

// RunWithSharedPostgres runs a package's tests against one Postgres server. Call it from the
// package's TestMain: os.Exit(fixtures.RunWithSharedPostgres(m)). Only packages whose tests do
// not run in parallel may use it: tests take turns on the server, and each test's roles are
// dropped when it ends.
func RunWithSharedPostgres(m *testing.M) int {
	if !flag.Parsed() {
		flag.Parse()
	}
	if testing.Short() {
		return m.Run()
	}
	terminate, err := startShared()
	if err != nil {
		fmt.Fprintf(os.Stderr, "fixtures: shared Postgres: %v\n", err)
		return 1
	}
	defer terminate()
	return m.Run()
}

func startShared() (func(), error) {
	ctx := context.Background()
	container, host, port, err := startWithMappedPort(ctx, postgresRequest(), func(ctx context.Context, c testcontainersContainer) (string, error) {
		p, err := c.MappedPort(ctx, "5432")
		return p.Port(), err
	})
	if err != nil {
		return nil, fmt.Errorf("start container: %w", err)
	}
	terminate := func() { _ = container.Terminate(context.Background()) }
	s := &sharedPostgres{host: host, port: port}
	if err := s.prepareTemplate(ctx); err != nil {
		terminate()
		return nil, err
	}
	shared = s
	return terminate, nil
}

// prepareTemplate migrates the template database and records the roles that exist before any
// test runs. The database named testdb stays as the container created it, because some tests
// grant CONNECT on it by name.
func (s *sharedPostgres) prepareTemplate(ctx context.Context) error {
	if err := s.admin(ctx, "CREATE DATABASE "+sharedTemplateDB); err != nil {
		return err
	}
	if err := applyMigrationsTo(ctx, s.dsn(sharedTemplateDB)); err != nil {
		return fmt.Errorf("migrate template: %w", err)
	}
	roles, err := s.roles(ctx)
	if err != nil {
		return err
	}
	s.baseRoles = roles
	return nil
}

// newDatabase gives one test its own migrated database and drops it, and any role the test
// created, when the test ends.
func (s *sharedPostgres) newDatabase(t *testing.T) (*pgcommon.Pool, string) {
	t.Helper()
	ctx := context.Background()
	name := fmt.Sprintf("test_%d", s.next.Add(1))
	if err := s.admin(ctx, fmt.Sprintf("CREATE DATABASE %s TEMPLATE %s", name, sharedTemplateDB)); err != nil {
		t.Fatalf("fixtures: create test database: %v", err)
	}
	t.Cleanup(func() { s.dropDatabase(t, name) })

	dsn := s.dsn(name)
	pool, err := pgcommon.NewPool(ctx, pgcommon.Config{DSN: dsn})
	if err != nil {
		t.Fatalf("fixtures: create pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool, dsn
}

func (s *sharedPostgres) dropDatabase(t *testing.T, name string) {
	ctx := context.Background()
	if err := s.admin(ctx, fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE)", name)); err != nil {
		t.Errorf("fixtures: drop test database: %v", err)
		return
	}
	roles, err := s.roles(ctx)
	if err != nil {
		t.Errorf("fixtures: list roles: %v", err)
		return
	}
	for role := range roles {
		if s.baseRoles[role] {
			continue
		}
		quoted := pgx.Identifier{role}.Sanitize()
		if err := s.admin(ctx, "REVOKE ALL ON DATABASE testdb FROM "+quoted, "DROP ROLE "+quoted); err != nil {
			t.Errorf("fixtures: drop role %s: %v", role, err)
		}
	}
}

func (s *sharedPostgres) roles(ctx context.Context) (map[string]bool, error) {
	out := map[string]bool{}
	err := s.withAdminConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		rows, err := conn.Query(ctx, "SELECT rolname FROM pg_roles")
		if err != nil {
			return fmt.Errorf("list roles: %w", err)
		}
		names, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return fmt.Errorf("list roles: %w", err)
		}
		for _, n := range names {
			out[n] = true
		}
		return nil
	})
	return out, err
}

// admin runs statements one by one on the maintenance database, outside any transaction, as
// CREATE DATABASE and DROP DATABASE require.
func (s *sharedPostgres) admin(ctx context.Context, statements ...string) error {
	return s.withAdminConn(ctx, func(ctx context.Context, conn *pgxpool.Conn) error {
		for _, stmt := range statements {
			if _, err := conn.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("%s: %w", stmt, err)
			}
		}
		return nil
	})
}

func (s *sharedPostgres) withAdminConn(ctx context.Context, fn func(context.Context, *pgxpool.Conn) error) error {
	pool, err := pgcommon.NewPool(ctx, pgcommon.Config{DSN: s.dsn("postgres")})
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer pool.Close()
	if err := pool.WithConn(ctx, fn); err != nil {
		return fmt.Errorf("maintenance database: %w", err)
	}
	return nil
}

func (s *sharedPostgres) dsn(database string) string {
	return fmt.Sprintf("postgres://postgres:postgres@%s:%s/%s?sslmode=disable", s.host, s.port, database)
}
