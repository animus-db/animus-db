# A real-socket test's converged-or-timeout loop can hide a genuine real-time race, not just an eventual property — check which before collapsing it in a `SimCluster` conversion (ADR 0061 rung J, C-10 PR 5)

Converting `tests/stream_backfill_seed_filter.rs`'s sealed-path scenario to
`sim_cluster_stream_backfill_seed_filter.rs` required deciding what to do
with its own 60s converged-or-timeout poll (this log already has many
entries insisting an eventual property gets one, never a fixed one-shot
assert). Reading *why* that poll existed mattered before deciding: the
original raced a genuinely concurrent real-time writer against a live
periodic sealer — two independent real threads whose relative order across
polls was not otherwise controlled — so the loop's job was to wait out an
actual race, not just to observe a value settle. Under `SimCluster`, that
race does not exist: every op call (`dynamo`/`drive_backfill_seed`/
`drive_stream_seal`/…) fully drains the shared virtual clock before
returning (an existing, already-recorded property of this fixture), so by
the time the scenario issues its one `drive_stream_seal` call every write
that precedes it in program order is already committed, and the call's own
documented contract (`seal_now` looped to `Ok(None)`) already seals
everything currently pending in one shot. Collapsing the poll to a single
direct call-then-assert is therefore correct here, not a violation of the
converged-or-timeout rule — the rule is about eventual properties this
fixture's own deterministic, turn-based execution has *already* made
non-eventual, not about eventual properties in general. The general
takeaway: when converting a real-socket regression's own polling loop to a
`SimCluster` sibling, read what the loop was waiting *on* — a value that
settles because of a real concurrent process (safe to collapse once
`SimCluster`'s single-threaded, fully-draining-per-call model removes that
concurrency) versus a value that settles because of a still-eventual
in-process mechanism (keep the loop). Getting this backwards either
reintroduces a flake `SimCluster` was supposed to make deterministic
(keeping an unnecessary loop is harmless, just misleading) or, worse,
silently assumes away a race the conversion never actually eliminated
(collapsing a loop that was doing real work). This conversion's own
open-path scenario kept a bounded stable-poll shape anyway, defensively and
at zero cost, rather than asserting off the very first `GetRecords` call —
worth doing whenever the original's own discipline costs nothing to
preserve, even once its original reason no longer strictly applies.
