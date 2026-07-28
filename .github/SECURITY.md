# Security Policy

## Supported Versions

| Version | Supported |
|---------|-----------|
| `main` (latest) | Active |
| Older tags | Not supported |

Only the latest commit on `main` receives security fixes at this stage (pre-release).

## Reporting a Vulnerability

**Do not open a public GitHub issue for security vulnerabilities.**

Report vulnerabilities privately via [GitHub Security Advisories](https://github.com/BCBP-SOLUTIONS-FZC-LLC/execution-service/security/advisories/new).

Include:

- A clear description of the vulnerability and the affected component (gRPC handler, RLS policy, Temporal workflow/Activity, outbox payload, cache, etc.)
- Steps to reproduce
- Impact assessment (tenant data leakage, RLS bypass, privilege escalation, workflow-state corruption, DoS)
- Any suggested fix or workaround

### Response timeline

| Step | Target |
|------|--------|
| Initial acknowledgement | 48 hours |
| Severity assessment | 5 business days |
| Patch release (critical/high) | 14 days |
| Public disclosure | After patch ships |

## Scope

Areas of particular sensitivity in this service:

- **Row-Level Security (RLS)** — tenant isolation enforced at the Postgres layer via the centralized `app_tenant_id()` function; a bypass would expose cross-tenant workflow instances, tasks, and assignments.
- **GUC injection** — `app.tenant_id` is set per-request/per-transaction; incorrect propagation would allow privilege escalation across tenants.
- **Temporal workflow determinism** — non-deterministic workflow code (time, randomness, unguarded goroutines) can corrupt replay and workflow state; treat as a correctness *and* security concern once the workflow function lands.
- **gRPC surface** — `CheckActiveInstances` / `PauseUserTasks` (inbound) and `GetCompiledWorkflow` (outbound) are the only cross-service trust boundary at this Tier.
- **Outbox payloads** — event payloads must not leak tenant data across the outbox's shared infrastructure.

## Dependency Vulnerabilities

Dependencies are scanned by `govulncheck` on every CI run and by Dependabot weekly. If you find a vulnerable transitive dependency, please report it via the advisory process above rather than opening a public issue.
