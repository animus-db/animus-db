//! Regression for GitHub issue #804: a real multi-node `ProdEnv` cluster
//! panicked in `assert_ts_monotonic` (`lib.rs` — "raftkv apply: HLC ts ...
//! did not strictly exceed the last applied ... the witnessing chain is
//! broken") under ordinary client load, with no fault injection at all.
//!
//! Every `ProdEnv` node captures its own `Instant::now()` base independently
//! at bind, so distinct nodes' `env.now()` readings are offset from each
//! other by their startup skew (observed ≥1.2s on the reporting cluster).
//! The panic's own numbers are the tell: the rejected entry's `logical: 0`
//! means the *minting* leader's `Hlc` had never witnessed anything at or
//! above its own `ts` — its local clock read ahead of everything it had
//! ever witnessed, even though a *different*, higher-`ts` entry was already
//! committed and applied elsewhere.
//!
//! # The reproducing mechanism
//!
//! Every witnessing point in `lib.rs` fine except one:
//!
//! - `witness_append_entries` (ordinary `AppendEntries` receipt) witnesses
//!   **every** entry's `ts`, whether or not that entry's apply actually
//!   wrote a row (`command_ts` returns `Some` for `Cas`/`Delete`/etc.
//!   regardless of outcome).
//! - WAL recovery replay witnesses every logged entry's `ts` the same way.
//! - **`InstallSnapshot` install and group start both witness only
//!   `storage.latest_version()`** — the engine's own highest *written*
//!   MVCC version. A committed-and-applied entry that carries a `ts` but
//!   writes no row (a failed `Cas` whose `expected` never matched, a
//!   condition-failed put, an aborted txn, a no-op `KindBatch`) never
//!   advances `latest_version()`, so a replica that catches up **purely
//!   via a snapshot that already covers such an entry** never witnesses
//!   its `ts` at all.
//!
//! If that replica later becomes leader with a local clock reading behind
//! the missed `ts`'s wall time (but ahead of whatever it *did* witness —
//! the last real row write), `Hlc::mint` returns `max(now_ms, state.wall_ms)`
//! off its own, now too-low, floor: strictly less than the `ts` every other
//! replica already applied. The next entry it proposes then hard-panics
//! `assert_ts_monotonic` on every replica that actually saw the missed
//! entry.
//!
//! # Why this needs *differential*, not uniform, skew
//!
//! `crates/animus-test/tests/txn_serializable.rs` already exercises
//! `Simulator::set_clock_skew_for` — but applies the **same** skew to every
//! replica of a group, which can never expose this: the bug is about one
//! replica's clock reading *low relative to what the group has actually
//! committed*, not about any replica's clock reading unrealistically vs a
//! wall clock. Each replica here gets its own, distinct, fixed skew.
//!
//! # Scenario
//!
//! 1. Elect a leader among 3 replicas over an unskewed clock, then assign
//!    each replica its own distinct skew (deliberately *after* electing —
//!    skew is a pure read-side offset with no effect on which replica wins
//!    the initial race, so fixing it up-front would need to guess the
//!    winner; reading it off afterward removes the guesswork entirely).
//!    The eventual-leader-to-be gets a large negative skew (its clock reads
//!    far behind); the other two get small, merely "different from each
//!    other" skews, matching production's sub-2s startup-skew scale.
//! 2. Crash (partition) the to-be-lagging replica so it witnesses nothing
//!    from here on.
//! 3. The surviving two commit a burst of ordinary `put`s (real row
//!    writes, well past `COMPACT_THRESHOLD`), then a burst of **failed**
//!    `Cas`s (committed and applied, `ts`-bearing, but writing no row —
//!    `expected` never matches) large enough to itself cross
//!    `COMPACT_THRESHOLD` again. This is what forces the *next* compaction
//!    pass's snapshot base to include the failed CASs, with no log tail
//!    left after them — a later `InstallSnapshot` can carry nothing else.
//! 4. Heal/restart the lagging replica: it must catch up via a pure
//!    `InstallSnapshot` (its own log start is long gone), witnessing only
//!    `storage.latest_version()` — the last real `put`, not the higher-`ts`
//!    failed `Cas`s that came after it.
//! 5. Transfer leadership to it (`transfer_leadership`, retried until
//!    armed and effective — its own log is fully caught up post-snapshot,
//!    so the peer-match gate admits it) and have it propose one more
//!    `put`. Its own `Hlc` floor is still pinned at the last real write, so
//!    with its skewed clock reading behind the failed CASs' `ts` (but at
//!    or above that floor), the new mint lands below what every other
//!    replica already applied.
//!
//! The oracle is the pre-existing hard `assert!` in `assert_ts_monotonic`
//! itself — no new assertion is added. It fires inside the per-group async
//! apply task (`apply_loop`, spawned via `env.spawn_task` at
//! `RaftKvNode::start`), which `Simulator`'s single-threaded cooperative
//! executor polls synchronously from inside `sim.run_for`/`run_until` — a
//! panic there unwinds straight out of that call on the test's own thread
//! (`animus-sim` has no `catch_unwind` anywhere in its polling loop), so it
//! fails this test with no completion flag or extra plumbing needed.
//!
//! Seeded and replayable: `ANIMUS_SEED=<decimal seed> cargo test -p
//! animus-cp-data --test hlc_differential_skew`.

use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::nid;
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;

const NODES: [u64; 3] = [0, 1, 2];

type KvNode = RaftKvNode<SimEnv, MemoryEngine>;

fn group(seed: u64) -> (Simulator, Vec<KvNode>) {
    let sim = Simulator::new(seed);
    let nodes = NODES
        .iter()
        .map(|&id| {
            RaftKvNode::start(
                sim.env(nid(id)),
                NODES.iter().copied().map(nid).collect(),
                MemoryEngine::new(),
            )
        })
        .collect();
    (sim, nodes)
}

/// The current leader among `nodes`, if exactly one reports it.
fn leader_among(nodes: &[KvNode]) -> Option<usize> {
    let ls: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    if ls.len() == 1 { Some(ls[0]) } else { None }
}

/// Drive `sim` in small ticks, repeatedly (re-)arming a transfer to `target`
/// on whoever currently leads, until `target` itself reports leadership or
/// the budget runs out. Mirrors the retry shape already used in
/// `inplace_split_reconciler.rs`'s "force leadership onto p0" helper — a
/// single `transfer_leadership` call only *arms* the handoff; it still needs
/// ticks of virtual time (and, the first few calls, a leader whose peer-match
/// on `target` has caught all the way up) for `TimeoutNow` to actually land.
fn force_leadership(sim: &mut Simulator, nodes: &[KvNode], target: usize, seed: u64) {
    let target_id = nid(target as u64);
    for _ in 0..200 {
        if nodes[target].is_leader() {
            return;
        }
        if let Some(l) = leader_among(nodes)
            && l != target
        {
            nodes[l].transfer_leadership(target_id.clone());
        }
        sim.run_for(Duration::from_millis(50));
    }
    panic!("could not force leadership onto node {target} (seed={seed})");
}

/// Run the full scenario once for `seed`, with `lag_skew_ms` (always
/// negative) applied to the eventual-leader-to-be after the initial
/// election. Panics (via the pre-existing `assert_ts_monotonic`) if the
/// witnessing gap reproduces.
fn run_scenario(seed: u64, lag_skew_ms: i64) {
    assert!(
        lag_skew_ms < 0,
        "the lagging replica's skew must be negative"
    );
    let (mut sim, nodes) = group(seed);
    sim.run_for(Duration::from_secs(2)); // elect, unskewed
    let l0 = leader_among(&nodes).unwrap_or_else(|| panic!("no initial leader (seed={seed})"));
    let lagging = (0..3)
        .find(|&i| i != l0)
        .expect("a non-leader replica exists");
    let third = (0..3)
        .find(|&i| i != l0 && i != lagging)
        .expect("a third replica exists");

    // Differential, per-replica skew — deliberately distinct on all three,
    // assigned only now that the initial winner is known (skew is read-side
    // only, so it cannot have influenced who won). The two live replicas get
    // small, merely-different offsets (production's sub-2s startup-skew
    // scale); the one about to be partitioned gets a large negative skew —
    // large enough that it still reads behind the failed-CAS burst's `ts`
    // even after however long the catch-up + leadership-transfer machinery
    // below takes in virtual time.
    sim.set_clock_skew_for(nid(l0 as u64), 300_000_000); // leader: +300ms
    sim.set_clock_skew_for(nid(third as u64), -200_000_000); // survivor: -200ms
    sim.set_clock_skew_for(nid(lagging as u64), lag_skew_ms * 1_000_000);

    // Partition the lagging replica: from here on it witnesses nothing via
    // AppendEntries at all.
    sim.crash(nid(lagging as u64));

    // A real write burst: ordinary row-writing `put`s, comfortably past
    // `COMPACT_THRESHOLD` (64), establishing the "last real write" the
    // lagging replica's eventual snapshot install SHOULD (and does) witness.
    const REAL_WRITES: u64 = 100;
    for i in 0..REAL_WRITES {
        match nodes[l0].put(
            format!("k{i:04}").into_bytes(),
            format!("v{i}").into_bytes(),
        ) {
            ProposeResult::Accepted { .. } => {}
            other => panic!("leader rejected real put {i}: {other:?} (seed={seed})"),
        }
    }
    sim.run_for(Duration::from_secs(2)); // replicate + apply + compact on {l0, third}

    // A failed-CAS burst: committed, applied, `ts`-bearing entries that
    // write NO row (the `expected` below can never match — the key was
    // never written). Large enough to itself cross `COMPACT_THRESHOLD`
    // again, which is what forces the *next* compaction pass to advance the
    // snapshot base all the way through these entries, leaving no log tail
    // after them for a later `InstallSnapshot` peer to replay separately.
    const FAILED_CAS: u64 = 100;
    for i in 0..FAILED_CAS {
        match nodes[l0].cas(
            b"never-written".to_vec(),
            Some(format!("bogus-expected-{i}").into_bytes()),
            format!("would-be-v{i}").into_bytes(),
        ) {
            ProposeResult::Accepted { .. } => {}
            other => panic!("leader rejected failed-cas {i}: {other:?} (seed={seed})"),
        }
    }
    sim.run_for(Duration::from_secs(3)); // apply + compact the failed-cas tail too

    // Heal the lagging replica: its own log start is long gone, so it must
    // catch up via a pure `InstallSnapshot` — witnessing only
    // `storage.latest_version()` (the last real `put`), never the
    // higher-`ts` failed CASs that wrote nothing.
    sim.restart(nid(lagging as u64));
    sim.run_for(Duration::from_secs(4));

    // Move leadership onto the under-witnessed replica and have it propose
    // one more write. If the gap reproduced, its `Hlc` floor is still
    // pinned at the last real write's `ts`; with its skewed clock reading
    // behind the failed CASs' `ts` but at/above that floor, this mint lands
    // below what {l0, third} already applied — and their next apply of it
    // hard-panics the pre-existing `assert_ts_monotonic`.
    force_leadership(&mut sim, &nodes, lagging, seed);
    match nodes[lagging].put(b"post-catchup-key".to_vec(), b"post-catchup-value".to_vec()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("new leader {lagging} rejected its own put: {other:?} (seed={seed})"),
    }
    // Drive long enough for the low-ts entry to reach and apply on every
    // replica that already holds the higher watermark — this is where
    // `assert_ts_monotonic` fires if the witnessing gap reproduced.
    sim.run_for(Duration::from_secs(3));
}

/// Iterates a handful of seeds (cheap: each run is a fraction of a second of
/// wall-clock time) so a seed-specific election/timing accident doesn't mask
/// the bug. Replay a single one with a **decimal** `ANIMUS_SEED` (`std::env::
/// var`/`str::parse::<u64>` don't accept a `0x` prefix, matching every other
/// seeded test in this crate — e.g. `ANIMUS_SEED=55300`).
#[test]
fn lagging_replica_mints_below_a_committed_non_row_writing_entry() {
    let seeds: Vec<u64> = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .map(|s| vec![s])
        .unwrap_or_else(|| vec![55_300, 55_301, 55_302, 55_303, 55_304]);
    // A large, fixed negative skew (60s) — comfortably longer than this
    // scenario's own end-to-end virtual-time budget (well under 20s across
    // every `run_for` call above), so the lagging replica's clock reads
    // behind the failed-CAS burst's `ts` throughout, regardless of exactly
    // how long catch-up + leadership transfer take.
    const LAG_SKEW_MS: i64 = -60_000;
    for seed in seeds {
        run_scenario(seed, LAG_SKEW_MS);
    }
}
