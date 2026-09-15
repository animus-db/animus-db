# A function's own generic `<E: Env, R: RelayClient>` signature proves nothing about whether its body still calls the real clock/timer — only running it under `SimEnv` does (ADR 0061 rung G, C-07 PR 2, 2026-09-08)

`docs/adr/0061-*.md`'s rung G opener stated, as a fact from a read-only
tree pass: "`seal_now<E, R> (proposes `SealStreamShard`, writes
`ctx.segment_store`) is already generic — a fact from the tree." True, and
still misleading: `seal_now`'s own commit-wait poll read the wall clock
via bare `tokio::time::Instant::now()`/`tokio::time::sleep` internally,
despite the function's signature being `<E: Env, R: RelayClient>` for
years. Nothing under `SimEnv` had ever actually called `seal_now` before
this PR (`change_consumer_loop`'s real periodic seal arm is the only
production caller, and `SimCluster` never spawns that loop), so a
read-only investigation had no way to see the gap — it only manifests at
runtime, and only under an environment with no real Tokio reactor
(`SimEnv`), where `tokio::time::sleep` panics outright ("there is no
reactor running") the instant the loop's first poll iteration is reached.
The new `SimCluster::drive_stream_seal` smoke test hit this immediately
and unambiguously — a hard panic with a full backtrace naming the exact
line, not a subtle behavioral divergence.

**General lesson**: a "this function is already generic over `E`" claim in
a blocker/investigation writeup is a claim about the *signature*, checked
by `cargo build`/reading the `fn` line — it says nothing about whether the
*body* still reaches through to the real clock/timer rather than the `Env`
seam, which can only be confirmed by actually driving the function under
`SimEnv` (or by grepping its own body for `tokio::time`/`std::time`/
`Instant::now`/`SystemTime::now`, which a read-only investigation pass
should do for any function it plans to lean on generically, not just check
the signature). This is the same root distinction ADR 0061 rung C5 step
3b's own mechanical-conversion pass already discovered once (see that
rung's "A subtler bug this rung's own mechanical pass introduced" entry in
`crates/animusd/CLAUDE.md`) — a function can be `E`-generic in name while
still being `ProdEnv`-only in practice, and the only thing that actually
proves otherwise is a real `SimEnv`-driven caller reaching every line.

The fix followed the established rung C5 step 3b conversion exactly:
`tokio::time::Instant::now() + X` → `ctx.env.now().saturating_add(X)`
(`Nanos` has no `Add<Duration>`), `tokio::time::sleep(D)` → `ctx.env.
sleep(D)`. `ProdEnv` behavior is unchanged (its own `env.now()`/`env.sleep`
are the real clock/timer). `index_drain::pitr_seal_now` — `seal_now`'s
structural twin (`SealPitrSegment` in place of `SealStreamShard`) — was
confirmed (by direct inspection, not just pattern-matching the function
name) to carry the identical bug, and was deliberately left unfixed: it is
not reachable from anything this PR wires up, so fixing it would be a
drive-by change outside this PR's own stated scope; a future rung driving
PITR sealing under `SimCluster` will need to make the identical
`ctx.env.now()`/`ctx.env.sleep(..)` conversion before its own first
`SimEnv`-driven caller can reach it. `index_drain.rs` is one of the
crate's `#[allow(clippy::disallowed_methods)]`-covered modules (not one of
the ten narrower `#[deny(...)]` modules ADR 0061's own closing rung named
— see root `CLAUDE.md`'s determinism section), which is exactly why
`cargo clippy -p animusd --all-targets --all-features -- -D warnings`
never caught this gap on its own: the lint that would have flagged a raw
`tokio::time` call is switched off for this file by design (the
`animusd`-wide process-boundary carve-out), so a function that happens to
be `E`-generic but still reaches for the real clock only reveals itself by
actually being driven under `SimEnv` once.

**2026-09-08, third recurrence (ADR 0061 rung H, C-08 PR 6)**: `ClientCtx::
admin_transfer_control_leadership` (`lib.rs`) — already `<E: Env, R:
RelayClient>`-generic since rung C5 — had the identical gap: its own
commit-wait loop read `tokio::time::Instant::now()`/called `tokio::time::
sleep(..)` directly. Found the same way, by the same signal: `SimCluster`'s
own `control_transfer_moves_leadership_to_the_named_node` scenario, this
method's first-ever `SimEnv`-driven caller, panicked immediately with "no
reactor running." Fixed with the identical `self.env.now()`/`self.env.
sleep(..)` conversion. Three occurrences of the same root cause in one
crate is a pattern, not a coincidence — when widening any `ClientCtx`
method's signature to `E`-generic without also driving it under `SimEnv`
in the same change, grep the body for `tokio::time`/`Instant::now`/
`SystemTime::now` before declaring it done, per the general lesson above.

**2026-09-09, fourth recurrence (ADR 0061 rung J, C-10 PR 2)**:
`index_drain::clear_backfill_cursor<E: Env>` — already generic (its two
sibling functions, `advance_backfill_cursor`/`seed_change_log_record`,
needed the identical widening in the very same change, so this one's own
still-bare `tokio::time::Instant::now()`/`tokio::time::sleep` body was
easy to miss by pattern-matching "this file's own backfill functions all
look alike") had the identical gap. Found the same way, by the same
signal: the very first `sim_cluster_index_ddl.rs` smoke run (scenario
(b), `UpdateTable` dropping a GSI, whose `drop_index` cascade calls
`ClientCtx::clear_backfill_cursor_for_table` → this function) panicked
immediately with "no reactor running." Fixed with the identical
`group.env().now()`/`group.env().sleep(..)` conversion (this function
takes `group: &CpGroup<E>` with no `&self`, so it reads the clock off
`group.env()` rather than `ctx.env`/`self.env` — the same accessor
`advance_backfill_cursor`/`seed_change_log_record` right beside it
already use for the identical reason). Four occurrences now — when
widening a *group* of sibling functions together, grep every one of
them individually for `tokio::time`/`Instant::now`/`SystemTime::now`,
not just the ones whose diff you're already looking at; a function that
"already looks generic" next to freshly-converted siblings is exactly
the one most likely to get skipped.

**2026-09-09, fifth recurrence (ADR 0061 rung L, C-12 PR 4e)**:
`ClientCtx::admin_remove_control_member` (`lib.rs`) — already `<E: Env, R:
RelayClient>`-generic, and sitting in the very same file, right next to
`admin_transfer_control_leadership` (the third recurrence's own site) —
had the identical gap: its own leader-self-removal transfer-wait loop
read `tokio::time::Instant::now()`/called `tokio::time::sleep(SCHEMA_
POLL_INTERVAL)` directly. Found the same way, by the same signal: `sim_
cluster_control_membership_admin.rs`'s own `remove_control_voter_
refusals_transfer_and_quorum_warnings` scenario — this loop's first-ever
`SimEnv`-driven caller, since `SimCluster` had no route to the leader-
self-removal arm before this PR gave `/admin/control/member/remove` a sim
sibling at all — panicked immediately with "no reactor running." Fixed
with the identical `self.env.now().saturating_add(..)`/`self.env.now() >=
deadline`/`self.env.sleep(..)` conversion; re-verified byte-for-byte
behavior-preserving under `ProdEnv` by re-running every other real-socket
caller of this method (`admin_endpoint.rs`, `decommission.rs`,
`control_membership_split.rs`, `heartbeat_live_destinations.rs`), all
green. Five occurrences now, in one crate, and the third and fifth sit in
the SAME function's own file, a few hundred lines apart — proximity to an
already-fixed sibling is no protection at all: `admin_add_control_member`,
right beside both, was separately checked by hand for this same PR and
found already seam-clean (it already used `self.env`/no bare
`tokio::time` anywhere). **The generalizable takeaway sharpens with each
recurrence**: don't grep only the function this PR's own diff touches —
before treating any `ClientCtx`/`CpGroup` method as usable from a new
`SimCluster` scenario, grep *that specific function's own body* (not just
its neighbors, not just its signature) for `tokio::time`/`Instant::now`/
`SystemTime::now`, every time, even when a sibling two functions up was
already fixed in an earlier rung.
