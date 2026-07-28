## Description
Provide a clear description of the changes.

---

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Refactor
- [ ] Documentation
- [ ] Test
- [ ] Breaking change
- [ ] New migration
- [ ] Proto contract change (`api/proto/`)

---

## Testing
- [ ] Unit tests added/updated (`make test`)
- [ ] Postgres / RLS integration tests added/updated (`make test-integration`)
- [ ] All tests passing with race detector
- [ ] `make cover-check` passes the coverage floor
- [ ] Manual testing performed (if required)

---

## Checklist

### Code Quality
- [ ] Code is properly formatted (`make fmt-check`)
- [ ] Linting passed (`make lint`)
- [ ] Vet passed (`make check` runs `go vet`)
- [ ] Architecture rules pass (`make arch-lint`) — no `adapter/` import of `internal/workflow`, no `core/` import of `adapter/`
- [ ] No debug logs / commented-out code
- [ ] No secrets or DSNs hardcoded

### Code Generation
- [ ] No changes to proto, sqlc queries, or port interfaces
- OR:
- [ ] `make generate` re-run after changing `.proto` or `db/queries/*.sql` — `buf lint` clean, sqlc output committed nowhere (gitignored)
- [ ] `make mock` re-run after changing `internal/core/port/*.go` interfaces

### Database / Migrations
- [ ] No new migrations in this PR
- OR:
- [ ] New migrations have matching `.up.sql` and `.down.sql` pairs (golang-migrate `NNNNNN_name` numbering)
- [ ] Down migration correctly reverses the up migration
- [ ] New tenant-scoped tables use the centralized `app_tenant_id()` function in RLS policies, not inlined `current_setting(...)`
- [ ] RLS `FORCE`-enabled and covered by a negative-path test (`test/integration/postgres`) if a new tenant-scoped table was added
- [ ] Verified the migration applies cleanly (`make migrate` against `make docker-up` Postgres)

### Temporal / Workflow (once `internal/workflow` has real code)
- [ ] Workflow code contains no non-deterministic calls (`time.Now()`, `rand`, unguarded goroutines, direct I/O) outside Activities
- [ ] New Activities are idempotent and safe to retry
- [ ] `internal/workflow` still imports only `domain` + `port` — no `adapter/` import

### Security
- [ ] No secrets or DSNs hardcoded
- [ ] Tenant context (`app.tenant_id`) reaches every new tenant-scoped query via RLS GUC injection, not an application-level `WHERE tenant_id = ...` filter alone

### Documentation
- [ ] README updated (if setup steps or environment variables changed)
- [ ] `.claude/CLAUDE.md` updated only if a genuinely new critical guardrail is needed — keep it minimal, not a code reference
- [ ] CHANGELOG.md entry added under `## [Unreleased]` (once one exists)

---

## Related Issue
Closes #<issue-id>

---

## Deployment Notes
Mention anything important for operators upgrading (migration steps, new required env vars, config changes). Remember `cmd/server` and `cmd/worker` are separately deployed processes — call out if a change only affects one of them.
