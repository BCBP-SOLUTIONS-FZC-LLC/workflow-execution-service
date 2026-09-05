# Questions for IAM: event wire-format casing and tenant lifecycle relay

Workflow Service (execution_service) consumes several IAM-originated events. Two things came up while reviewing this integration that need IAM's direct confirmation before we change anything on our side.

## 1. Delegation event casing

Workflow Service currently expects the wire `type` for delegation events as dotted-lowercase: `delegation.started`, `delegation.ended`.

IAM's Delegation Service AsyncAPI spec and LLD (both current, most recently revised today) document these events as PascalCase: `DelegationStarted`, `DelegationEnded`, with explicit wire-format examples and a note that this PascalCase value is what consumer SNS filter policies match against.

Separately, User Profile's own events use dotted-lowercase on the wire (`user.updated`, `user.availability.changed`, `user.deleted`) — confirmed explicitly in that service's own spec, which distinguishes this from the Glue schema registry's PascalCase naming (a separate, non-wire concern).

So across IAM's own services there doesn't appear to be one consistent casing convention: User Profile is dotted-lowercase, Delegation is documented as PascalCase. Workflow Service's own code was changed to dotted-lowercase for delegation events in the past, on the assumption that this matched IAM's convention — that assumption was never independently confirmed, and it now conflicts with Delegation's current documented spec.

Nothing is deployed yet on either side, so there's no live traffic either of us can point to as a tiebreaker.

**Ask**: Please confirm, per event type, what the actual wire `type` value is (or will be) for `DelegationStarted`/`DelegationEnded` — PascalCase or dotted-lowercase — and whether there's an intended platform-wide casing convention across all IAM event types, or whether each service sets its own.

## 2. `tenant.state.changed` — does this event exist, and who owns it?

Workflow Service has an inbound handler for an event called `tenant.state.changed` (payload: tenant_id, status, previous_status, plan, previous_plan, changed_at, cause), expected to arrive on the same channel as the Delegation events, and documented on our side as a resolved tenant-lifecycle projection owned by "IAM Org & Membership."

We can't find this event defined in any current IAM spec we have access to. We only have a spec for IAM's Delegation service — we understand Org & Membership is split into several services beyond Delegation, and we don't have visibility into the rest, so we can't confirm this ourselves.

**Ask**: Does a tenant-lifecycle relay event like this actually exist (or is it planned) on IAM's side? If so:
- Which service owns/publishes it?
- What's its real name and wire `type`?
- What topic/queue does it arrive on?
- What's the actual payload shape?

If nothing like this exists or is planned, we'd rather know that now so we can remove the handler instead of carrying dead code.
