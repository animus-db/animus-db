# Widening a `pub` function with many test callers: mint a `_with_settings` sibling instead of adding parameters directly (issue #676)

The layered-wrapper convention this file already documents (mint a thin,
original-arity wrapper above the layer you're widening, so existing
callers keep compiling) is usually applied to *private* or *narrow*
wrappers a few callers use. `animusd::run_node_join`/`run_node_data_join`
were a harder case: they are the crate's own outermost `pub` entry points
for the join/seed paths, and — unlike an internal layer — had 8+ direct
callers spread across this crate's own `tests/*.rs` (`seed_join.rs`,
`seed_join_allocated.rs`, `data_join.rs`, `decommission.rs`,
`split_placing_completion.rs`, `split_placing_two_replica_diff_e2e.rs`,
`cluster_gt_rf_split_bench.rs`, `tests/support/mod.rs`'s own shared
helpers used by still more files transitively). The task's own briefing
anticipated the standard move here — widen the function directly, then
fix the resulting `error[E0063]` fan-out at every enumerated call site —
and that would have worked, but at real cost: 8+ files touched for a
change whose actual goal (`join`/`data --seed` resolving the same
on-by-default knobs `--config`/`--node` already does) has nothing to do
with any of those tests' own scope.

**The cheaper move, when the widen target itself is the many-caller
function**: keep it at its own original signature, but change what it
does internally — resolve the new knobs to their production ON-by-default
values (`DEFAULT_QUIESCE_AFTER_SECS`/`DEFAULT_HEARTBEAT_BATCH`/
`DEFAULT_SHARED_WAL`) instead of the old hardcoded off-values — and mint a
new, separate `pub` sibling (`run_node_join_with_settings`/
`run_node_data_join_with_settings`) that takes the new parameters
explicitly, for the one caller (`main.rs`'s own CLI dispatch) that
actually needs to set them from a flag. This is the same "widen the
innermost layer, default in the narrower one" shape as the standard
layered-wrapper convention — the twist is which function plays which
role: the widely-called function becomes the *outer*, defaulted layer
(even though it used to be the widest-arity one), and the new function
becomes the *inner*, fully-parameterized one CLI dispatch calls directly.
Net result: zero test-fixture changes (confirmed by `cargo check -p
animusd --tests` before and after — identical, no `E0063` anywhere), and
every existing caller's behavior *improved* for free (the on-by-default
posture bug the issue was about) without anyone having to touch those
callers to get it.

**When to reach for this over the "widen and fix every site" approach**:
prefer minting a sibling specifically when (a) the function being widened
is `pub` with call sites *outside* the module/crate doing the widening —
not just narrower in-crate wrappers, where the direct-widen-and-fix
approach is usually less code overall — and (b) there is a reasonable,
already-established default value the existing callers should keep
getting (here, the very `DEFAULT_*` constants the CLI itself already
resolves an omitted flag to) rather than every caller needing to make a
fresh decision. Reach for the standard "widen it, let the compiler
enumerate every site" approach instead when there's no sane shared
default (a genuinely new axis of behavior every caller must decide for
itself) or when the type being widened is a data type (a struct literal),
where a builder/sibling-constructor split is usually more awkward than
just fixing the enumerated sites directly.
