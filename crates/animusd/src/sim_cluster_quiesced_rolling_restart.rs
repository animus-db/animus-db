//! Issue #920: a `ConsistentRead: true` read routed through a node that
//! hosts no replica of the target tablet must not hang for tens of
//! seconds after every replica of an idle (quiesced, ADR 0048) tablet
//! group has been crashed and restarted in turn with durable storage.
//!
//! **Two scenarios, because the investigation found the plain
//! quiescence-only hypothesis insufficient to reproduce the hang.**
//! `quiesced_group_survives_rolling_restart_every_order` (below) drives
//! the ORIGINAL issue text's own scenario — a clean rolling restart of an
//! otherwise-untouched quiesced group — and **passes on unmodified
//! `main`**: a restarted replica always comes back with `leader_id: None`
//! (volatile, never persisted, ADR 0048), so `handle_pre_vote`'s own
//! leader-lease check (`animus-control/src/raft.rs`) never blocks it, and
//! its own ordinary election timeout (never quiesced immediately after a
//! restart) campaigns and converges in well under a second every time.
//! Kept as a permanent assurance test that this simpler mechanism is, and
//! stays, sound. `quiesced_group_survives_rolling_restart_racing_failure_
//! driven_repair_every_order` (below that) is the ACTUAL regression: it
//! reproduces the production incident by racing the SAME rolling restart
//! against the control plane's own failure-driven placement repair (ADR
//! 0012), which a real pod recreation trips just as readily as a genuine
//! failure — see that test's own doc for the full mechanism and the fix
//! (`RaftKvNode::reconfigure_step`, `animus-cp-data/src/lib.rs`).
//!
//! **Setup** (shared by both). A 4-node combined `SimCluster` (`SimCluster::
//! new_with_cp_quiescence`), one table whose sole tablet is hosted on
//! nodes `0..3` (`SimCluster::create_table`'s own deterministic
//! `0..replication` placement — see that method's doc; with only one
//! tablet on 4 nodes the control-plane's `rebalance_step` has no
//! balance-improving move available, since any 3-of-4 placement already
//! satisfies its own max-min ≤ 1 convergence target, so node 3 stays a
//! genuine non-replica for the tablet's whole lifetime — confirmed by
//! `sim_cluster_growth.rs`'s own "three tables, deliberately not one"
//! rebalance-trigger note). One write, one linearizable read, then the
//! group idles well past `QUIESCE_AFTER` and every replica is confirmed
//! quiesced via `/admin/raftkv` (ADR 0048 PR7's diagnostic, a pure frozen
//! read that never itself wakes anything, fork F).
//!
//! **The fault.** Every one of the tablet's three replicas is crashed and
//! restarted in turn (`SimCluster::crash`/`restart` — a true process
//! restart reusing the SAME durable per-tablet engine, this fixture's own
//! "durable storage" contract, see `SimCluster::restart`'s own doc), each
//! waited to rejoin the control plane and rehost the tablet before the
//! next is touched, idling well past `QUIESCE_AFTER` again after each
//! restart (mirroring the e2e-kind rollout's own "each pod Ready before
//! the next" pacing, which gives the group every chance to re-quiesce
//! between restarts exactly as it would in production). This runs for
//! **every restart order** of the three replicas (all `3! = 6`
//! permutations of node ids `{0,1,2}` — a superset of "leader-first",
//! "leader-middle" and "leader-last", since which physical node ends up
//! leader is itself seed-dependent).
//!
//! **The assertion.** A `ConsistentRead: true` read of the primed key,
//! issued from node 3 (which hosts no replica and must forward — ADR
//! 0017 #3b/ADR 0047), must succeed within a small, bounded number of
//! election timeouts — never the 50+ seconds the production incident
//! measured. [`SimCluster::get_timed`] reports the actual virtual time
//! elapsed so a regression shows up as a number, not just a timeout.
//!
//! Depth knob: `ANIMUS_QUIESCE_SEEDS` (default 1) — deliberately the SAME
//! knob `animus-cp-data/tests/quiescence.rs` already uses, rather than a
//! new `ANIMUS_*_SEEDS` variable, per root `CLAUDE.md`'s corpus-knob
//! doctrine: this is a new *cell* of the quiescence corpus (it needs
//! `animusd`'s own `ClientCtx`/forwarding machinery a pure
//! `RaftKvNode`-level test cannot reach), not a reason to mint a new knob.
//! `ANIMUS_SEED=<seed> cargo test -p animusd --lib
//! quiesced_group_survives_rolling_restart_every_order` replays one run.

use std::time::Duration;

use animus_test::corpus;

use super::sim_cluster::SimCluster;

/// Short relative to this test's own settle windows, long relative to one
/// heartbeat interval — mirrors `animus-cp-data/tests/quiescence.rs`'s own
/// `QUIESCE_AFTER` choice exactly (that file's own comment explains the
/// sizing).
const QUIESCE_AFTER: Duration = Duration::from_millis(200);

/// How long each step of the scenario idles to let the group settle past
/// `QUIESCE_AFTER` — several multiples of it.
const SETTLE: Duration = Duration::from_secs(2);

/// The bound the final read must resolve within. Generous relative to a
/// handful of election timeouts (default control/data election timeouts in
/// this crate's `SimCluster` are on the order of hundreds of
/// milliseconds), nowhere near the 50+ real seconds the production
/// incident measured.
const READ_BOUND: Duration = Duration::from_secs(8);

fn seed_depth() -> u64 {
    corpus::seeds_from_env("ANIMUS_QUIESCE_SEEDS") as u64
}

fn seeds(base: u64) -> impl Iterator<Item = u64> {
    (0..seed_depth()).map(move |k| base.wrapping_add(k))
}

fn create_table(cluster: &mut SimCluster, table: &str) {
    cluster.create_table(table);
}

/// Whether every replica of `tablet` on `node` reports `quiesced: true` in
/// `/admin/raftkv` — a pure diagnostic read (ADR 0048 fork F), never a
/// wake. Panics with the raw body if the tablet isn't listed at all (a
/// caller only asks this once it knows `node` hosts the tablet).
fn is_quiesced(cluster: &mut SimCluster, node: u64, tablet_id: u64) -> bool {
    let (status, body) = cluster.admin(node, "GET", "/admin/raftkv", "", &[]);
    assert_eq!(status, 200, "node {node} /admin/raftkv failed: {body}");
    let parsed: serde_json::Value = serde_json::from_str(&body)
        .unwrap_or_else(|e| panic!("bad /admin/raftkv JSON: {e}: {body}"));
    let groups = parsed["groups"]
        .as_array()
        .unwrap_or_else(|| panic!("no groups array in /admin/raftkv: {body}"));
    let group = groups
        .iter()
        .find(|g| g["tablet"].as_u64() == Some(tablet_id))
        .unwrap_or_else(|| panic!("node {node} does not host tablet {tablet_id}: {body}"));
    group["quiesced"]
        .as_bool()
        .unwrap_or_else(|| panic!("group has no quiesced field: {body}"))
}

/// Poll (never a fixed-deadline one-shot assert, root `CLAUDE.md`'s Testing
/// rule) until `node` hosts `tablet` again after a restart — this
/// fixture's own stand-in for "waited to Ready before the next" from the
/// production rollout.
fn wait_until_rehosted(
    cluster: &mut SimCluster,
    node: u64,
    tablet: animus_tablet::TabletId,
    seed: u64,
) {
    const BUDGET: Duration = Duration::from_secs(10);
    const STEP: Duration = Duration::from_millis(50);
    let mut elapsed = Duration::ZERO;
    loop {
        if cluster.hosted_tablets(node).contains(&tablet) {
            return;
        }
        assert!(
            elapsed < BUDGET,
            "seed={seed}: node {node} never rehosted tablet {tablet:?} within {BUDGET:?} of \
             a restart"
        );
        cluster.run_for(STEP);
        elapsed += STEP;
    }
}

/// One full run of the scenario at `seed`, restarting the tablet's three
/// replicas in `order` — returns the virtual time the final read through
/// node 3 took to resolve, and its own result, so the caller can both
/// assert a bound and print the measured number per root `CLAUDE.md`'s
/// "print the seed in assertion messages" rule.
fn run_scenario(seed: u64, order: [u64; 3]) -> (Duration, Result<Option<Vec<u8>>, String>) {
    let mut cluster = SimCluster::new_with_cp_quiescence(seed, 4, 3, Some(QUIESCE_AFTER));
    let table = "t";
    create_table(&mut cluster, table);
    let tablet = *cluster
        .metadata(0)
        .tablets_for_table(table)
        .next()
        .unwrap_or_else(|| panic!("seed={seed}: table {table} has no tablet"))
        .0;

    // Sanity: the tablet's replica set is exactly {0,1,2} and node 3 hosts
    // nothing for it — the precondition every restart order below relies
    // on (see this module's own doc for why a single-tablet 4-node cluster
    // never rebalances this away).
    let mut replicas: Vec<animus_env::NodeId> = {
        let meta = cluster.metadata(0);
        meta.tablets
            .get(&tablet)
            .unwrap_or_else(|| panic!("seed={seed}: tablet {tablet:?} missing from Metadata"))
            .replicas
            .to_vec()
    };
    replicas.sort_unstable();
    let mut expected: Vec<animus_env::NodeId> =
        [0u64, 1, 2].into_iter().map(animus_env::nid).collect();
    expected.sort_unstable();
    assert_eq!(
        replicas, expected,
        "seed={seed}: expected the sole tablet on nodes 0,1,2 with node 3 idle"
    );
    assert!(
        !cluster.hosted_tablets(3).contains(&tablet),
        "seed={seed}: node 3 must host no replica of the tablet"
    );

    // One write, one linearizable read to prime real data before quiescing.
    cluster
        .put(0, table, "k", "v", b"hello")
        .unwrap_or_else(|e| panic!("seed={seed}: priming put failed: {e}"));
    let got = cluster
        .get(0, table, "k", "v", true)
        .unwrap_or_else(|e| panic!("seed={seed}: priming read failed: {e}"));
    assert_eq!(got, Some(b"hello".to_vec()), "seed={seed}");

    // Idle past QUIESCE_AFTER and confirm every replica genuinely quiesced.
    cluster.run_for(SETTLE);
    for &n in &[0u64, 1, 2] {
        assert!(
            is_quiesced(&mut cluster, n, tablet.0),
            "seed={seed}: node {n} should have quiesced while idle"
        );
    }

    // Crash and restart every replica in `order`, waiting for each to
    // rejoin (rehost the tablet) before touching the next, then idling
    // again so the group gets every chance to re-quiesce between restarts
    // exactly as the production rollout's own pod-by-pod pacing does.
    for &node in &order {
        cluster.crash(node);
        cluster.run_for(Duration::from_millis(300));
        cluster.restart(node);
        wait_until_rehosted(&mut cluster, node, tablet, seed);
        cluster.run_for(SETTLE);
    }

    // The linearizable read through node 3 — the whole point of this
    // scenario: node 3 hosts no replica, so this must forward (ADR 0017
    // #3b) to whichever replica can serve/route it.
    cluster.get_timed(3, table, "k", "v", true, Duration::from_millis(50), 400)
}

fn assert_recovers(seed: u64, order: [u64; 3]) {
    let (elapsed, result) = run_scenario(seed, order);
    let value = result.unwrap_or_else(|e| {
        panic!(
            "seed={seed} order={order:?}: ConsistentRead via node 3 (no local replica) failed \
             after {elapsed:?}: {e} -- this is issue #920: a quiesced group's leader was lost \
             across a rolling restart and nothing campaigned to replace it"
        )
    });
    assert_eq!(
        value,
        Some(b"hello".to_vec()),
        "seed={seed} order={order:?}: read succeeded but returned the wrong value after {elapsed:?}"
    );
    assert!(
        elapsed <= READ_BOUND,
        "seed={seed} order={order:?}: ConsistentRead via node 3 took {elapsed:?} to succeed, \
         past the {READ_BOUND:?} bound -- issue #920 measured 50+s in production for exactly \
         this shape"
    );
    eprintln!(
        "issue #920 repro: seed={seed} order={order:?} ConsistentRead via node 3 resolved in \
         {elapsed:?}"
    );
}

/// All 3! restart orderings of the tablet's three replicas — a superset of
/// leader-first/leader-middle/leader-last, since which node ends up leader
/// is itself seed-dependent (see this module's own doc).
fn every_restart_order() -> [[u64; 3]; 6] {
    [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ]
}

#[test]
fn quiesced_group_survives_rolling_restart_every_order() {
    for seed in seeds(0x9200_0001) {
        for order in every_restart_order() {
            assert_recovers(seed, order);
        }
    }
}

// ---------------------------------------------------------------------------
// The actual issue #920 root cause: a rolling restart racing the control
// plane's OWN failure-driven placement repair.
//
// A crash long enough to trip `RaftCore`'s liveness detection (ADR 0012's
// `CONTROL_PEER_LIVENESS_TIMEOUT`/`DETECT_TIMEOUT`/`LEADER_GRACE`, all
// 500ms in production, no CLI override) is indistinguishable, to the
// control plane, from a genuine node failure — including the pod
// recreation a rolling restart with durable storage performs, which
// routinely takes far longer than 500ms in a real cluster. The production
// incident's own evidence (per-pod `/admin/raftkv` dumps from the
// e2e-kind run) showed exactly this: one recreated pod came back
// `hosts_cp: false` (its own tablet-host reconciler had already released
// the group, correctly, since Metadata's desired replicas no longer named
// it) while the other two replicas' `voter_history` showed a NEW node
// being admitted mid-restart — a live `CasTabletReplicas`-driven repair
// racing the rollout, not a plain idle-group quiescence stall.
//
// **Root cause, confirmed by this reproduction**: `RaftKvNode::
// reconfigure_step`'s old step 1 removed an extra `Down` voter
// IMMEDIATELY, ahead of adding its replacement — shrinking the group's
// live voter count for the whole in-flight window (e.g. 3 → 2, instead of
// 3 → 4 → 3 via the learner phase every OTHER reconfigure path already
// uses). A rolling restart that goes on to touch a SECOND voter while the
// group is transiently down to that shrunk count can permanently strand
// it below majority, with no recovery path: the evicted voter's own
// reconciler has already released it and Metadata no longer names it a
// replica, so it can never simply rejoin. See `reconfigure_step`'s own
// doc (`animus-cp-data/src/lib.rs`) for the full before/after reasoning
// and the fix (reordering the down-extra removal to only fire once any
// pending replacement is already a voter — the same ADD-before-REMOVE
// discipline the learner phase already guarantees for every other path).
//
// This scenario forces the race deterministically (a direct `crash` long
// enough to trip the REAL 500ms detector, not a hand-proposed
// `CasTabletReplicas`) rather than relying on an organic multi-tablet
// rebalance, which a single-tablet 4-node cluster never triggers on its
// own (see this module's own top doc) — the SimEnv equivalent of "the
// rollout's own pod recreation takes long enough to look, to the control
// plane, exactly like the node dying."
fn run_scenario_racing_repair(
    seed: u64,
    order: [u64; 3],
) -> (Duration, Result<Option<Vec<u8>>, String>) {
    let mut cluster = SimCluster::new_with_cp_quiescence(seed, 4, 3, Some(QUIESCE_AFTER));
    let table = "t";
    create_table(&mut cluster, table);
    let tablet = *cluster
        .metadata(0)
        .tablets_for_table(table)
        .next()
        .unwrap_or_else(|| panic!("seed={seed}: table {table} has no tablet"))
        .0;
    cluster
        .put(0, table, "k", "v", b"hello")
        .unwrap_or_else(|e| panic!("seed={seed}: priming put failed: {e}"));
    cluster.run_for(SETTLE);
    for &n in &[0u64, 1, 2] {
        assert!(
            is_quiesced(&mut cluster, n, tablet.0),
            "seed={seed} order={order:?}: node {n} should have quiesced while idle"
        );
    }

    for &node in &order {
        cluster.crash(node);
        // Past CONTROL_PEER_LIVENESS_TIMEOUT + LEADER_GRACE (500ms each,
        // ADR 0012) so the control leader marks `node` Down and
        // `reconcile_placement` gets a real chance to propose moving its
        // replica onto the spare (node 3) — then restart BEFORE that
        // repair (if any) has necessarily converged, exactly mirroring a
        // rolling restart's own pacing racing the repair it triggers.
        cluster.run_for(Duration::from_millis(1500));
        cluster.restart(node);
        cluster.run_for(Duration::from_millis(500));
    }

    cluster.get_timed(3, table, "k", "v", true, Duration::from_millis(50), 400)
}

fn assert_recovers_racing_repair(seed: u64, order: [u64; 3]) {
    let (elapsed, result) = run_scenario_racing_repair(seed, order);
    let value = result.unwrap_or_else(|e| {
        panic!(
            "seed={seed} order={order:?}: ConsistentRead via node 3 failed after {elapsed:?}: \
             {e} -- this is issue #920's actual root cause: a rolling restart raced the control \
             plane's own failure-driven repair (reconfigure_step's down-extra-removal ordering)"
        )
    });
    assert_eq!(
        value,
        Some(b"hello".to_vec()),
        "seed={seed} order={order:?}: read succeeded but returned the wrong value after {elapsed:?}"
    );
    assert!(
        elapsed <= READ_BOUND,
        "seed={seed} order={order:?}: ConsistentRead via node 3 took {elapsed:?} to succeed, \
         past the {READ_BOUND:?} bound -- issue #920 measured 50+s in production for exactly \
         this shape"
    );
    eprintln!(
        "issue #920 repro (racing repair): seed={seed} order={order:?} ConsistentRead via node \
         3 resolved in {elapsed:?}"
    );
}

#[test]
fn quiesced_group_survives_rolling_restart_racing_failure_driven_repair_every_order() {
    for seed in seeds(0x9200_0002) {
        for order in every_restart_order() {
            assert_recovers_racing_repair(seed, order);
        }
    }
}
