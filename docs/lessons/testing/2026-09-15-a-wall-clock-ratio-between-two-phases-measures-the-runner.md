# A wall-clock ratio between two test phases measures the runner's own scheduling, not the mechanism under test — assert the mechanistic observable instead, and expect it to need an explained tolerance of its own (issue #601)

`crates/animusd/tests/batch_write.rs::batched_write_beats_per_key` asserted
`batched < per_key` on two `Instant::now()` spans in a real-socket `ProdEnv`
test — N individual `PutItem`s vs. the same N items chunked into
`BatchWriteItem` calls. On a loaded/shared CI runner a scheduling stall, a
compaction, or the group's first-write warm-up landing in either phase can
invert or flatten that ratio with no regression in the mechanism at all
(measured 0.9x and, separately, 0.7x in CI while the same tree measured
2.3x-3.7x locally). **A wall-clock ratio between two phases of one process is
a statement about the runner's scheduler at that moment, never a repeatable
property of the code**, even when the true underlying effect is large (here,
routinely 20x+ locally) — noise on a shared/loaded box does not respect the
size of the true effect, it just needs the effect to be smaller than whatever
stall the runner happened to inject during either phase.

**The fix is to name the actual mechanism and assert that, not a proxy for
it.** The property under test was "one Raft entry per
`BATCH_WRITE_MAX_ITEMS`-sized chunk instead of one per item" — exactly what
`animus-env`'s `cp_proposals_accepted` counter (ADR 0015) already tracks,
incremented once per accepted `put`/`put_batch`/`put_kind_batch` propose and
never per item inside one. Scraping the real `GET /metrics` endpoint
before/after each phase and asserting the counter *delta* replaced the
wall-clock comparison with the real mechanism, deterministic in principle and
immune to scheduler noise. Keep timing as an `eprintln!` diagnostic only —
useful for a human reading test output, never a pass/fail input.

**But "deterministic in principle" still needed an explained tolerance in
practice, and finding out why was the more valuable half of this fix.**
Fifty real local runs on a loaded, shared box showed the batched phase's
proposal count is *usually* exactly `ceil(N/BATCH_WRITE_MAX_ITEMS)`, but
occasionally one higher — never on the per-key side, in any of 50 runs
(6000+ individual writes). Root cause, not just observed: `cp_kind_raw_
local`'s confirm loop (`crates/animusd/src/write_path.rs`) can classify an
accepted-but-not-yet-visible entry as superseded via `decide::confirm_wait_
is_futile`, whose own doc already names the mechanism — a Raft group's tick
loop stalled by real CPU contention can miss its own heartbeat deadline and
trigger a term bump, invalidating an already-accepted-but-uncommitted entry
(the same issue #268 lineage `ClientCtx::provision_tablet`'s own doc
describes on the control-plane schema-proposal path). The caller's
`"; retry"` convention then re-proposes the identical, idempotent write,
which lands fine but counts as a second accepted propose — real, if rare,
production behavior, filed separately as issue #911, not something to fix as
a side effect of a test-assertion PR.

**The generalizable rule**: replacing a wall-clock assertion with a counter
or other mechanistic observable is necessary but not sufficient — a counter
can still have legitimate real-world variance (a retry, a background sweep,
a warm-up event) that an *exact* equality or a tight ceiling will
occasionally trip on a loaded runner, reproducing the exact same class of
flake one level down. Before asserting an exact count, run the real test
enough times under real load to find its actual steady-state variance, then
either (a) assert the exact invariant that genuinely never varies (here, the
per-key side's `== N`, true in all 50 runs), or (b) name the specific,
understood source of variance and size a small, explained margin around it —
never a silent "add slack until CI passes" pad. A margin with a cited
mechanism and a measured maximum is a documented tolerance; a margin with
neither is next quarter's unexplained flake.
