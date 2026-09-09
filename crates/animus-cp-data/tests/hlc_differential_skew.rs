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

/// Also returns each replica's own `MemoryEngine` handle (issue #804
/// follow-up), so a scenario that needs a genuine process restart
/// (`sim.stop` + a fresh `RaftKvNode::start`, never `sim.crash`/
/// `sim.restart` alone — see `witnessing.rs`'s own restart test for why:
/// `crash`/`restart` only toggle network reachability and never drop a
/// node's in-memory `Hlc`/apply-task state) can re-`start` the SAME node id
/// on the SAME durable engine, exactly like a real process restart would.
fn group(seed: u64) -> (Simulator, Vec<KvNode>, Vec<MemoryEngine>) {
    let sim = Simulator::new(seed);
    let engines: Vec<MemoryEngine> = NODES.iter().map(|_| MemoryEngine::new()).collect();
    let nodes = NODES
        .iter()
        .zip(engines.iter())
        .map(|(&id, engine)| {
            RaftKvNode::start(
                sim.env(nid(id)),
                NODES.iter().copied().map(nid).collect(),
                engine.clone(),
            )
        })
        .collect();
    (sim, nodes, engines)
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

/// Which extra restart step (if any) `run_scenario` injects into the
/// otherwise-identical crash/skew/heal/leadership-transfer choreography —
/// each variant targets one specific half of the issue #804 fix.
#[derive(Clone, Copy)]
enum Variant {
    /// The original reproduction: no true process restart anywhere, just
    /// `sim.crash`/`sim.restart` (network-reachability toggling only) on
    /// the lagging replica. Exercises the in-memory
    /// `hlc.witness(install_max_ts, ..)` fold at the `InstallSnapshot`
    /// call site.
    Baseline,
    /// Fix (2) — the sender's own `storage.latest_version()` fold: restart
    /// the ORIGINAL leader (the eventual `InstallSnapshot` sender) right
    /// after its own compaction, with nothing applied since — resetting
    /// its apply task's `max_applied_ts` to `None` — then force leadership
    /// straight back onto it, so it is provably the one whose apply task
    /// builds the lagging replica's `InstallSnapshot` image. Without fix
    /// (2), `engine_image`'s header would carry `None` (the raw, just-
    /// reset `max_applied_ts`) even though this replica's own durable
    /// `hwm.rs` marker (written at its earlier compaction) already makes
    /// `storage.latest_version()` correct — reopening the gap on the
    /// RECEIVING side purely because of what the SENDER shipped.
    SenderRestartNothingAppliedSince,
}

/// Run the full scenario once for `seed`, with `lag_skew_ms` (always
/// negative) applied to the eventual-leader-to-be after the initial
/// election, and `variant` selecting which extra restart step (if any) to
/// inject — see [`Variant`]. Panics (via the pre-existing
/// `assert_ts_monotonic`) if the witnessing gap reproduces.
fn run_scenario(seed: u64, lag_skew_ms: i64, variant: Variant) {
    assert!(
        lag_skew_ms < 0,
        "the lagging replica's skew must be negative"
    );
    let (mut sim, mut nodes, engines) = group(seed);
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

    if matches!(variant, Variant::SenderRestartNothingAppliedSince) {
        // Fix (2): a genuine process restart of l0 — its own apply task's
        // `max_applied_ts` resets to `None` — with nothing applied since.
        // `lagging` is still crashed/partitioned throughout this step, so
        // it plays no part in it yet.
        sim.stop(nid(l0 as u64));
        nodes[l0] = RaftKvNode::start(
            sim.env(nid(l0 as u64)),
            NODES.iter().copied().map(nid).collect(),
            engines[l0].clone(),
        );
        sim.run_for(Duration::from_secs(2)); // WAL recovery + re-election settle
        // Force leadership straight back onto l0 so IT — not `third`, whose
        // own `max_applied_ts` was never reset — is provably the one whose
        // apply task builds `lagging`'s upcoming `InstallSnapshot` image.
        force_leadership(&mut sim, &nodes, l0, seed);
    }

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
        run_scenario(seed, LAG_SKEW_MS, Variant::Baseline);
    }
}

/// Fix (1)'s own regression: `install_engine_image` must durably `merge` the
/// receiver's own `hwm.rs` marker in the SAME batch as the installed rows —
/// not just `hlc.witness` the header value in memory at the call site —
/// because a receiver whose own engine never durably reflects it has nothing
/// for a future restart's group-start witness to lean on (see `hwm.rs`'s
/// module doc, "Why the `InstallSnapshot`... half needs its own path").
///
/// **Deliberately restart-free.** The literal end-to-end proof — actually
/// `sim.stop` + a fresh `RaftKvNode::start` of `lagging` right after this
/// install, before its own next compaction, then transfer leadership onto it
/// and write — was the first design tried here. It was abandoned: that exact
/// shape (a genuine process restart of a live, fully-caught-up voter inside
/// an otherwise-active 3-node group) triggers a separate, **pre-existing**
/// hang/livelock, confirmed independent of today's fix by reproducing it
/// identically with `lib.rs`/`codec.rs` reverted to this branch's own base
/// commit — see `docs/engineering-lessons.md`'s matching entry for the full
/// investigation. Filed as a follow-up, not fixed here (an incidental
/// pre-existing bug gets its own PR, never a drive-by fix riding along).
///
/// This still directly proves fix (1)'s own mechanism, one layer down:
/// `RaftKvNode::engine_latest_version()` reads `storage.latest_version()` —
/// the exact same read a future restart's group-start witness would use —
/// completely independent of whatever the in-memory `Hlc` has witnessed. Before
/// fix (1), only the rows an `InstallSnapshot` actually carries ever raised
/// this value on the receiver; the failed-CAS burst's own mark, carried
/// solely in the image *header*, never did. This test would fail exactly
/// the way `compaction_durably_advances_the_engine_watermark_past_a_non_row_
/// writing_entry` below proves the sender-side half, if fix (1) were
/// reverted (confirmed by reading `install_engine_image`'s own `if let
/// Some(ts) = max_ts { ops.push(..) }` block out during development).
#[test]
fn receiver_installs_the_durable_high_water_mark_not_just_the_rows() {
    let seeds: Vec<u64> = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .map(|s| vec![s])
        .unwrap_or_else(|| vec![55_500, 55_501, 55_502, 55_503, 55_504]);
    for seed in seeds {
        let (mut sim, nodes, _engines) = group(seed);
        sim.run_for(Duration::from_secs(2)); // elect
        let l0 = leader_among(&nodes).unwrap_or_else(|| panic!("no initial leader (seed={seed})"));
        let lagging = (0..3)
            .find(|&i| i != l0)
            .expect("a non-leader replica exists");

        // Partition `lagging` so it must catch up via a pure `InstallSnapshot`
        // later — its own log start will be long gone by then.
        sim.crash(nid(lagging as u64));

        // A real write burst past `COMPACT_THRESHOLD` (64).
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
        let after_real_writes = nodes[l0].engine_latest_version();

        // A failed-CAS burst — committed, applied, `ts`-bearing, but writing
        // NO row — past `COMPACT_THRESHOLD` again, forcing the sender's next
        // compaction to advance its snapshot base through it, leaving no log
        // tail for `lagging` to replay separately later.
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
        let after_failed_cas = nodes[l0].engine_latest_version();
        assert!(
            after_failed_cas > after_real_writes,
            "sanity: the sender's own compaction must durably raise its \
             watermark past the failed-CAS burst even though it wrote no row \
             (seed={seed}, after_real_writes={after_real_writes}, \
             after_failed_cas={after_failed_cas})"
        );

        // Heal `lagging`: it must catch up via a pure `InstallSnapshot` —
        // its own log start is long gone.
        sim.restart(nid(lagging as u64));
        sim.run_for(Duration::from_secs(4));

        let lagging_after_install = nodes[lagging].engine_latest_version();
        assert!(
            lagging_after_install >= after_failed_cas,
            "the receiver's own engine must durably reflect the sender's \
             high-water mark immediately after installing a snapshot that \
             covers it, even though the failed-CAS burst wrote no row — an \
             in-memory `hlc.witness` alone would leave THIS read (what a \
             future restart's group-start witness would see) undercounting \
             (seed={seed}, after_failed_cas={after_failed_cas}, \
             lagging_after_install={lagging_after_install})"
        );
    }
}

/// Fix (2)'s own regression: the `InstallSnapshot` image header must carry
/// the true high-water mark even when the SENDER's own apply-task
/// `max_applied_ts` is `None` (freshly restarted, nothing applied since) —
/// folding in `storage.latest_version()` at image-build time, not the raw
/// running max alone. See [`Variant::SenderRestartNothingAppliedSince`].
/// Before fix (2) this panics via `assert_ts_monotonic` exactly like the
/// baseline scenario; after it, it must not.
#[test]
fn sender_restart_with_nothing_applied_since_still_ships_the_true_high_water_mark() {
    let seeds: Vec<u64> = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .map(|s| vec![s])
        .unwrap_or_else(|| vec![55_600, 55_601, 55_602, 55_603, 55_604]);
    const LAG_SKEW_MS: i64 = -60_000;
    for seed in seeds {
        run_scenario(seed, LAG_SKEW_MS, Variant::SenderRestartNothingAppliedSince);
    }
}

/// The restart/group-start half of the same fix (`hwm.rs`'s durable marker),
/// proven directly against the compaction side effect that backs it —
/// **without** a process restart, and without any clock skew at all.
///
/// `RaftKvNode::start_inner`'s group-start witness reads
/// `storage.latest_version()` — the exact same read `engine_latest_version()`
/// exposes here. Before the fix, a committed-and-applied entry that writes
/// no row (a failed `Cas`) never advances that read, no matter how many
/// compaction passes run afterward; after the fix, `apply_and_compact`'s
/// compaction path durably `merge`s `hwm.rs`'s marker at the highest `ts`
/// seen so far, which unconditionally raises the engine's own global MVCC
/// high-water mark (`LsmEngine`'s `manifest.max_version`/`MemoryEngine`'s
/// analogous tracking — both implement the same per-key-LWW-but-global-max
/// contract) even though the marker's own key is a different key from every
/// row the burst below ever touched.
///
/// This is why a real multi-node dance (crash/skew/heal/leadership-transfer,
/// as `lagging_replica_mints_below_a_committed_non_row_writing_entry` above
/// needs to reach the *InstallSnapshot* half of the fix) is unnecessary
/// here: the compaction path runs identically on every replica whether or
/// not anyone ever restarts or catches up via a snapshot, so a single
/// live leader proves it.
#[test]
fn compaction_durably_advances_the_engine_watermark_past_a_non_row_writing_entry() {
    let seeds: Vec<u64> = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .map(|s| vec![s])
        .unwrap_or_else(|| vec![55_400, 55_401, 55_402, 55_403, 55_404]);
    for seed in seeds {
        let (mut sim, nodes, _engines) = group(seed);
        sim.run_for(Duration::from_secs(2)); // elect
        let leader =
            leader_among(&nodes).unwrap_or_else(|| panic!("no initial leader (seed={seed})"));

        // A real write burst past `COMPACT_THRESHOLD` (64), so compaction
        // truncates the WAL prefix these writes occupy — establishing the
        // engine watermark a fix-less build would be stuck at forever after.
        const REAL_WRITES: u64 = 100;
        for i in 0..REAL_WRITES {
            match nodes[leader].put(
                format!("k{i:04}").into_bytes(),
                format!("v{i}").into_bytes(),
            ) {
                ProposeResult::Accepted { .. } => {}
                other => panic!("leader rejected real put {i}: {other:?} (seed={seed})"),
            }
        }
        sim.run_for(Duration::from_secs(2)); // apply + compact
        let after_real_writes = nodes[leader].engine_latest_version();

        // A failed-CAS burst — committed, applied, `ts`-bearing, but writing
        // NO row (`expected` can never match a never-written key) — past
        // `COMPACT_THRESHOLD` again, so it too gets compacted.
        const FAILED_CAS: u64 = 100;
        for i in 0..FAILED_CAS {
            match nodes[leader].cas(
                b"never-written".to_vec(),
                Some(format!("bogus-expected-{i}").into_bytes()),
                format!("would-be-v{i}").into_bytes(),
            ) {
                ProposeResult::Accepted { .. } => {}
                other => panic!("leader rejected failed-cas {i}: {other:?} (seed={seed})"),
            }
        }
        sim.run_for(Duration::from_secs(3)); // apply + compact the failed-cas tail too
        let after_failed_cas = nodes[leader].engine_latest_version();

        assert!(
            after_failed_cas > after_real_writes,
            "compaction must durably raise the engine watermark past the \
             failed-CAS burst's own ts even though it wrote no row \
             (seed={seed}, after_real_writes={after_real_writes}, \
             after_failed_cas={after_failed_cas}) — the group-start witness \
             reads exactly this value on restart"
        );
    }
}
