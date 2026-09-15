# Host-capability traits let a leaf loop move before its "brain" does (ADR 0061 rung C2)

Phase C's plan called the six `animusd` background loops (`ttl_reaper`,
`backup_janitor`, `pitr_janitor`, `segment_janitor`, `backup_completion`,
`index_backfill`) easy first movers into `animus-node`. Scoping the rung
found that wrong: every one of them takes `ClientCtx` by value or reference,
and `ClientCtx` is the crate's 5,569-line brain, not scheduled to move until
rung C5. On the plan's own ordering, nothing in C2 could move at all.

The fix generalizes beyond this one rung: when code B (a loop, a handler, a
consumer) can only move because it depends on code A (a big, not-yet-movable
"god object"), don't wait for A to move and don't reimplement A's logic
inside B's new home. Instead, scope exactly which **operations** of A, B
actually calls — usually a small, named slice, even when A itself is huge —
define a narrow trait for just that slice, implement it for A as a **thin,
logic-free delegation** (translate the shape, call the existing method,
nothing more), and move B generic over the trait. A stayed exactly where it
was; B stopped depending on its *type*, only on a few of its *operations*.
Three traits came out of this rung (`ControlLeaderHost<E>`,
`BackupObjectStore`, `TtlScanHost` — see `animus-node/CLAUDE.md`'s own rung
C2 entry), sized by cohesive capability rather than one trait per loop or one
fat trait for everything; a trait every implementor exercises in full is a
good sign, a trait that exists only to make one specific move compile is not.

This is *better* for testability than moving the loops unchanged would have
been, not merely a workaround: a loop generic over a capability trait can be
driven under `SimEnv` against a synthetic fake implementing just that trait
— no cluster, no sockets, no `ClientCtx` — which is deterministic coverage
those loops had never had. A loop moved but left coupled to a concrete
`ClientCtx` would still have been untestable until C5 landed; the capability
trait is what makes the move worth doing now rather than later.

Corollary, worth stating because it looks like a shortcut and isn't one: not
every loop in the batch has to move. `segment_janitor.rs` stayed in
`animusd` this rung — its replica-repair phase makes real placement/
membership decisions (which replicas are still live, where to push a
repaired copy), not a value nameable as one narrow I/O delegation the way
"durably store these bytes" is. Forcing it to move would have meant either
smuggling real decision logic into the leaf crate (exactly what this phase
is supposed to prevent) or building a capability surface wide enough to
expose that logic anyway, which is a contorted trait wearing a narrow one's
clothes. A partial rung with a precise, per-loop account of what moved and
why the rest didn't is a better outcome than forcing every item on the list
to move.
