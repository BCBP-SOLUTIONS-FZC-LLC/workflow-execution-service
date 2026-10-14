# Observability

Logs, metrics and traces go through `platform-gincommon`. Database metrics come from `platform-pgcommon`, and `platform-events` identifies its library version in every scrape. [metric-registry.md](metric-registry.md) lists every series this service exposes.

## Tiers

| Tier | Names | Owner |
| --- | --- | --- |
| 1 platform-shared | `platform_http_*`, `platform_db_*`, `platform_outbox_*`, `platform_messages_*`, `platform_build_info`, `platform_library_info` | The platform libraries; identical in every service |
| 2 workflow domain-shared | `workflow_*` | The platform registry; none defined yet |
| 3 service-specific | `workflow_execution_service_*` | This service |

Every series carries the constant labels `domain` (`workflow`), `service` (`workflow-execution-service`) and `environment` (`APP_ENV`). Only `platform_build_info` carries a `version` label.

## Construction rules

- A collector is built in `internal/observability` with a `gincommon` constructor, which validates the name, the suffix (`_total` on counters, `_seconds` on histograms) and the labels, and injects the identity.
- Identifiers such as `*_id`, `email` and `*_ip` are never labels.
- The identity is fixed before any collector or pool is created; `InitTelemetry` in `internal/observability` holds that order and runs in all three binaries (API server, Temporal worker, connector worker).

## Checks

| Command | What it fails on |
| --- | --- |
| `make metrics-lint` | a collector built outside the package, a name or label the standard rejects, a scrape that does not match the registry, an alert or doc naming a retired metric, a stale inventory |
| `make metrics-inventory` | regenerates [metric-registry.md](metric-registry.md) |

`TestStandard_LibrariesShareRegistryCollectors` (in `cmd/server`) produces the scrape the linter reads and asserts the platform series are present; `test/integration/telemetry` asserts the `platform_db_*` series appear with a real pool.
