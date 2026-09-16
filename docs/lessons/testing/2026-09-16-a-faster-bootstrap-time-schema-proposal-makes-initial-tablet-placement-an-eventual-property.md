# A faster bootstrap-time schema proposal makes a freshly-provisioned tablet's *initial* replica set an eventual property, not a one-shot fact (2026-09-16, issue #610).

**A faster bootstrap-time schema proposal makes a freshly-provisioned
tablet's *initial* replica set an eventual property, not a one-shot fact
(2026-09-16, issue #610).** Issue #610's fix raced `propose_schema`'s
"no locally-known leader" broadcast fallback concurrently instead of
serially, which made the very first `CreateTable`/auto-create after a
fresh cluster's bootstrap resolve close to instantly instead of paying
up to `(N-1) * FORWARD_HOP_TIMEOUT`. That removed an incidental delay
several tests depended on without knowing it: `provision_tablet`'s own
replica-selection read (`meta.members.iter().filter(Active)`,
`animusd/src/schema.rs`) used to reliably land *after* every founding
member's own `RegisterNode` had committed and applied, simply because
the old broadcast was slow. Once the broadcast got fast, that read can
now legitimately observe only a subset of the founding members as
`Active` — whichever member's own registration commits and applies
first, not fixed to any particular node id — even on a fixture that
brings up exactly N nodes and awaits "some leader + non-empty
membership everywhere" before writing.

This is not a new bug: `provision_tablet` always could mint an
under-sized initial set (that is precisely why the RF policy it attaches
records the *target* `MAX_REPLICATION_FACTOR`, never the observed
initial size — see `tests/tablet_rf_self_heals.rs`'s own module doc, and
`reconcile_placement`'s violation-repair path that grows any
under-provisioned tablet to the recorded target on its next tick). Issue
#610's fix just widened the window in which a test can *observe* it,
turning a previously-vanishingly-rare race into one CI hit repeatedly
across three independent tests in one afternoon
(`tablet_rf_self_heals.rs`, `split_placing_completion.rs`,
`split_placing_two_replica_diff_e2e.rs` — the last already fixed once
for the same shape under issue #622/#670, PR #891, before #610 ever
existed).

**General form**: any test that (1) brings up a small, fresh cluster,
(2) does the first write/`CreateTable` against it, and (3) then reads a
tablet's replica set, placement, or leader is reading a value the
control plane is still converging — never assert on it once. Poll
converged-or-timeout instead (`support::poll_until_or_stalled` in
`crates/animusd/tests/support/mod.rs`, or the equivalent inline pattern
in `crates/animusd/src/lib.rs`'s own lib tests), and when a later part
of the same test genuinely needs the *full* member set as a precondition
for what it goes on to prove (e.g. "growth forces a placement move"),
poll for that full set before proceeding rather than asserting the first
read. A `target` value that is written once and never rewritten (e.g.
`split_placing[tablet].target`, ADR 0062 §2) is the one kind of
placement-adjacent state that *is* safe to assert on synchronously —
the eventual-property caution applies to anything the reconciler can
still revise, not to write-once state.
