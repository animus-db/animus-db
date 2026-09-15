# The dominant cost the entry above left unfixed: an unquiesced CP-data Raft group and a fixed-budget probe helper, both closed without touching production code or dropping coverage (2026-09-09, follow-up to #772, PR 2/2)

The entry above correctly named the real cost driver but explicitly left it
unfixed: "real per-tablet/control-plane Raft consensus traffic... every
synchronous op call drives a full, unconditional `Simulator::
run_for(OP_BUDGET)` (12s) regardless of how quickly the call itself
resolves... a fuller fix... would need to address the corpus's own
`OP_BUDGET`-per-call probe design... out of this fix's scope." A follow-up
bisect (`ANIMUS_DYNAMO_WIRE_SEEDS=25` depth-1 wall time at each first-parent
merge commit between the stale `12e46df5` baseline and the red-nightly tip)
localized the ~4.6x per-scenario regression to one commit, `eef8a102` (C-06
PR 4, "Transact ops in the corpus"): 22.77s → 71.52s at depth-1 (8
scenarios), a jump bigger than every other candidate's own contribution
combined (D4 PR 1's real-`Reconciler` hosting cutover, `5d80de25`, moved
depth-1 wall time by under 4% on its own — ruled out by direct
measurement, not by reasoning). The mechanism: `run_transact_probe`
(`sim_cluster_dynamo_corpus.rs`) issues ~11 sequential `SimCluster::dynamo`
calls per node, and a tokened `TransactWriteItems` among them
auto-provisions the internal `__animus_txn_idempotency` table — a brand
new, permanently-unquiesced CP-data tablet group that then keeps ticking
its own Raft heartbeat/election-timeout machinery for the rest of the
scenario's virtual-time span, on top of every *existing* group doing the
same for the full 12s of every one of the corpus's own probe calls, whether
or not that call ever touches it.

**Two independent, additive root-cause fixes, both entirely inside the
`#[cfg(test)]` `SimCluster` fixture — zero production code touched:**

1. **CP-data quiescence (ADR 0048), never previously wired into
   `SimCluster` at all.** `SimCluster::new_with_cp_quiescence` (a new
   constructor sibling of `new`/`new_with_segment_janitor_retention`,
   `sim_cluster.rs`) opts every node's `Reconciler` into
   `enable_quiescence(after)` right after construction, before any tablet
   is ever hosted — the one place this fixture can call it at all, since by
   the time `SimCluster::new` returns, every reconciler has already been
   moved into its own driving task. `sim_cluster_dynamo_corpus.rs::
   run_scenario` opts in at production's own default threshold
   (`DEFAULT_QUIESCE_AFTER_SECS` = 5s). Measured contribution in isolation:
   ~24% of this corpus's own executor cost (`Simulator::stats()`,
   `dynamowire_baseline`) — real, but a minority of the total; narrowing
   `quiesce_after` toward the 200ms floor barely moved it further (~5%
   more), which was itself informative: it proved the *quiesce-after
   window* was never the dominant lever, ruling out a hypothesis before it
   could be mistaken for the fix.
2. **`spawn_and_capture_fast`, a second synchronous-op driver that returns
   the instant its future resolves instead of always burning the full
   `OP_BUDGET` (12s) window — used ONLY by this corpus's own probe/
   verification helpers** (`run_delete_probe`/`run_batch_write_probe`/
   `run_transact_probe`/`force_resolve_all_keys`), never by the shared
   `SimCluster::dynamo`/`put`/`get`/`scan` every other `sim_cluster_*`
   module calls. This is the dominant lever: combined with quiescence,
   `dynamowire_baseline`'s own executor cost fell from ~786K task_polls
   (pre-fix) to ~35K (post-fix) — a ~22x reduction — because the SAME
   control-plane `RaftCore` heartbeat traffic (`heartbeat_interval` = 50ms,
   never quiesced by design, ADR 0044 phase-1 fork G) that quiescence
   cannot touch was previously being paid in full for the ~11+ virtual
   seconds after a probe op had *already resolved*, on every single one of
   the corpus's dozens of probe calls per scenario.

**Why this couldn't be a change to `spawn_and_capture` itself.** The
shared method's "always burn the full budget" behavior is load-bearing
elsewhere in the SAME test tier: `sim_cluster_dynamo_update_table.rs`'s
`update_table_raising_units_admits_more` relies on every `SimCluster::
dynamo` call unconditionally advancing virtual time by `OP_BUDGET` so a
`ThrottleBucket` has genuinely refilled by the next retry, with no explicit
sleep between attempts. Changing the shared method would have silently
broken that (and possibly other, unaudited) reliance across the ~30 sibling
`sim_cluster_*` modules built on it — exactly the failure mode this task's
own hard rule against widening a fix "to make a change compile" exists to
prevent. The fix is additive instead: a new method, opt-in per call site,
touching only the four functions that actually have no dependency on the
full-budget virtual-time advance.

**Result**: `SCENARIO_TIMER_FIRES_BUDGET` tightened from 3,300,000 to
110,000 (~2.1x the new measured `two_tables` maximum, 52,273 timer
fires/scenario — mirroring the original constant's own sizing discipline);
`ANIMUS_DYNAMO_WIRE_SEEDS=25` wall time fell from 26:56 (post-#772-PR-1,
still over 4x the stale ~601s baseline the entry above declined to chase)
to 77.6s — well under that stale baseline, not just under the CI timeout.
A second, independent measurement race this fix's own gate run surfaced
(a corpus non-vacuity check racing an in-flight tablet-replica migration,
not a system defect) is recorded in its own separate entry below. See
`docs/adr/0061-testability-node-crate-simulator.md`'s matching amendment
for the exact before/after table. The general lesson, on top of the one
above: **a documented "out of scope, would need touching a shared
mechanism" gap is sometimes closeable by adding a second, narrower
mechanism instead of widening the shared one** — the width of the fix
should match the width of the actual dependency (four call sites had no
dependency on the shared method's own timing contract; ~26 others did),
not the width of the file the slow code happens to live in.
