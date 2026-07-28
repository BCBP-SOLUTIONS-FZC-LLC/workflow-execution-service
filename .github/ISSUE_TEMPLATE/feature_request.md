---
name: Feature request
about: Propose a new gRPC method, workflow/Activity behaviour, database schema change, or service behaviour
title: '[FEAT] '
labels: enhancement
assignees: ''
---

## Problem / motivation
What problem does this solve? Which consumers or use cases are affected (Definition Service, dashboard/UI, downstream event consumers)?

## Proposed solution
Describe the API or behaviour change you'd like.

```protobuf
// Example: new rpc method or message field
```

## Affected areas
- [ ] gRPC contract (`api/proto/`) — new or changed method/message
- [ ] Temporal workflow function / Activities (`internal/workflow`)
- [ ] Database schema (new migration required)
- [ ] Outbox / event contract
- [ ] cmd/server
- [ ] cmd/worker

## Design constraints
- Does this require a database migration? Is the new table/column tenant-scoped (needs RLS via `app_tenant_id()`)?
- Does this change the `api/proto` contract consumed by Definition Service?
- Does this introduce new non-determinism risk in the workflow function?

## Alternatives considered
Other approaches you evaluated and why you ruled them out.

## Acceptance criteria
- [ ]
- [ ]
- [ ]

## Additional context
Links to related issues, the LLD (`design/LLD/execution_service.md`), or prior art.
