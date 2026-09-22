# An "insert if absent" idempotency guard silently loses to a racing writer that inserts a weaker row first

Found fixing issue #1028 (`animusd`'s `bootstrap`): a 4-node RF-3 cluster's
first table occasionally landed on an arbitrary 3 of its 4 nodes (1/45 runs
under load), and the same mechanism on a 3-node RF-3 cluster can mint a
genuinely under-replicated 2-replica first tablet — the exact regression
`bootstrap`'s own doc said it had closed (ADR 0030 §3, option (a)).

## The trap: "present" is not the property the writer actually guarantees

`bootstrap` runs on the control leader every 200ms and proposes
`UpsertMember { status: Active }` for each config-declared data node, so
that `provision_tablet` (which seeds a new tablet's replica set from
whichever members are `Active` *right now*) never sees a transiently
under-replicated membership. Its idempotency guard was the obvious one:

```rust
if !meta.members.contains_key(node) { propose(UpsertMember { Active }) }
```

That guard encodes "if I already wrote this row, don't write it again" as
"if the row exists, don't write it." Those are the same statement only when
this loop is the *sole* writer of the row — and it wasn't. Every node also
self-registers via `MetaCommand::RegisterNode`, a wholly decoupled path
whose apply arm inserts the member as `{ status: Down, has_activated:
false }` when nothing is there yet. Whenever self-registration landed
first, `bootstrap` saw the id present and skipped it *forever*; the node
became `Active` only when the failure detector promoted it after its first
heartbeat, ~100-200ms later (longer under load). `provision_tablet` running
inside that window is the whole defect.

Nothing errored. Nothing logged. Both writers were individually correct and
idempotent; the row always ended up `Active` eventually; a re-run went
green. The guard just quietly held only when `bootstrap` won a race it
didn't know it was in — and the doc comment above it kept asserting the
invariant it no longer provided.

## The rule: key an idempotency guard on the property, not on presence

When two writers may claim the same row, and one of them inserts a
*weaker* row (a placeholder, a default, a "not yet" state) than the other
guarantees, the stronger writer's "have I already done my job?" check must
test **the property it exists to guarantee**, never mere presence. Here
that property is "has this member ever been recorded `Active`" — and the
schema already carried exactly that as a sticky flag, `Member::
has_activated` (ADR 0040 PR6, set by the `UpsertMember` apply arm the
moment any `Active` status is applied, never cleared). The fix is one
extra promotable shape:

```rust
// promote when absent, OR present-but-never-activated (a self-registered
// / operator-added row nobody has ever recorded Active), labels preserved
None => BTreeMap::new(),
Some(m) if m.status == NodeStatus::Down && !m.has_activated => m.labels.clone(),
Some(_) => return None,
```

Choosing the *sticky* flag rather than `status == Down` alone is what
keeps the guard from over-correcting: a founding member that was `Active`
and then crashed has `has_activated == true`, so `bootstrap` leaves it to
the detector and never resurrects a dead node into placement; a
declared-but-never-booted phantom is upserted `Active` exactly once, flips
the flag, gets demoted by the detector's synthetic first observation, and
is never touched again. A guard keyed on the property is idempotent
against *every* writer of the row, including ones added later.

## How to spot the next one

- Grep the apply/insert sites for the row an "insert if absent" loop
  guards. If any other command can insert it — especially with a weaker
  default state — the guard is racing that command, whether or not anyone
  has seen it lose yet.
- Ask what the loop's doc claims it guarantees, then ask whether
  `contains_key`/`exists`/`is_some` actually tests *that*. If the answer is
  "only if nobody else inserted first," the guard is wrong, not merely
  narrow.
- Prefer a sticky, monotone property (`has_ever_been_X`) to a current-state
  check (`status == X`) as the key: the current state can be legitimately
  weakened later by a third writer (here, the detector's `Active`→`Down`),
  and a current-state key would then re-fire and undo that writer's work.
- Extract the decision into a pure `fn(&state, &inputs) -> Vec<Command>`
  and unit-test the racing shape by applying the *real* competing command
  to an empty state (here `RegisterNode` → `bootstrap_active_upserts`),
  not a hand-built row — that test is red on the presence-keyed guard and
  needs no timing at all, whereas the end-to-end symptom reproduced 1 in 45.
