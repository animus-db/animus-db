//! `SimCluster`-driven deterministic siblings for two of `tests/
//! cp_plane.rs`'s real-socket tests (ADR 0061 rung O, issue #997 —
//! closing the "node assembly / raw `ClientRequest`" real-socket test
//! group C-08's own close-out flagged for an assess-and-close decision
//! rather than a dedicated rung).
//!
//! ## Scenarios (seed-parameterized, `_over_seeds` at 5 seeds each)
//!
//! (1) [`run_cp_member_addresses_register_and_replicate`] — converts
//!     `cp_plane.rs::cp_member_addresses_register_and_replicate` (ADR 0032
//!     PR1): every node's own replicated `Metadata::node_addrs` carries an
//!     entry for every one of the 3 member ids, and that entry is
//!     byte-identical across every node (the replication property).
//!     **Deliberately does NOT assert `SocketAddr` parseability** — the
//!     real test's own "parses as a `SocketAddr`" check has no
//!     `SimCluster` analog: `SimCluster::seed_members` mints every
//!     `NodeAddrs.internal` as a bare `NodeId`-string routing key (`"0"`,
//!     `"1"`, `"2"`), never a real socket address — a documented fixture
//!     fact (`sim_cluster.rs`'s own `seed_members` doc; also noted in
//!     `crates/animusd/CLAUDE.md`'s own SimCluster module-map entry). This
//!     scenario instead asserts the actual ADR 0032 PR1 property under
//!     test: the replicated presence and cross-node identity of every
//!     member's own address-book entry.
//! (2) [`run_cp_tablet_splits_and_both_halves_serve`] — converts
//!     `cp_plane.rs::cp_tablet_splits_and_both_halves_serve`: a raw-KV
//!     table (`SimCluster::create_table`/`put_raw`/`raw_get` — literal,
//!     un-encoded byte keys, needed because `ClientCtx::trigger_split`'s
//!     `split_key` is compared against raw stored-key bytes with no
//!     decoding step, exactly the reason `sim_cluster_admin.rs`'s own
//!     `run_raftkv_key_count_is_scoped_per_tablet_after_split` uses the
//!     same primitives), a manual split of the bootstrap tablet at
//!     `"k5"` via `POST /admin/tablet/split` (the identical
//!     `ClientCtx::trigger_split` choke point the plain-protocol
//!     `ClientRequest::SplitTablet` itself funnels into), driven to
//!     convergence via [`SimCluster::drive_inplace_split_cutover`] on
//!     every node (this fixture never spawns the cutover driver as a
//!     background loop — see that method's own doc) until the parent
//!     tablet is gone and every node's own `Metadata` shows exactly two
//!     `Active`, routable children. Asserts at least every property the
//!     `ProdEnv` original did — `k9`→`upper` read through a different
//!     node than the writer, `k1`→`lower` still served, a new
//!     upper-range write `k7`→`upper2` round-tripping through yet a third
//!     node — plus that the two routable tablets' ranges genuinely
//!     partition the keyspace (`k1` and `k9` land in different tablets).
//!     Every verifying read asks for a **consistent** (linearizable) read
//!     (ADR 0055) — `raw_get(.., consistent: true)`.
//! (3) [`run_tablet_auto_splits_on_bytes_with_skewed_value_sizes`] — a
//!     BOUNDED SPIKE (ADR 0061 rung O's own instruction), and it worked:
//!     converts `cp_plane.rs::tablet_auto_splits_on_bytes_with_skewed_
//!     value_sizes`'s subject (ADR 0034's byte-weighted-median trigger) to
//!     a deterministic sibling via the same raw-KV primitives scenario (2)
//!     uses — `SimCluster::put_raw`'s literal, un-encoded key makes the
//!     ProdEnv original's own key-to-token-range correlation trivial here,
//!     unlike `sim_cluster_auto_split.rs`'s own DynamoDB-wire scenarios,
//!     whose `item_key` encoding hash-token-prefixes every key and so
//!     cannot be correlated to a written key at all (that file's own doc
//!     names this gap explicitly). 6 tiny + 6 large raw values, no manual
//!     trigger, `SimCluster::set_auto_split_thresholds` alone: both halves
//!     serve, every written key falls in exactly one routable child's
//!     range, and the smaller child holds at least 15% of the total bytes
//!     — the same quantitative balance claim the `ProdEnv` original makes.
//!     **The `ProdEnv` original stays regardless** (per this rung's own
//!     instruction) — see `cp_plane.rs`'s own doc for why.
//!
//! `cp_plane.rs::reads_and_writes_route_through_the_raft_group` has no sim
//! sibling here — see that file's own doc for why it was deleted outright
//! as redundant rather than converted (`tests/cluster.rs::
//! cluster_serves_put_get_and_status_over_tcp` already proves a strictly
//! stronger version of the identical property, over the same
//! `bind_cluster`/`start_cluster` production entry point).
//! `cp_plane.rs::single_write_latency_is_low` stays `ProdEnv` permanently
//! and unconverted — real-thread wall-clock latency, not reproducible
//! under `SimEnv`; see that file's own doc.
//!
//! `ANIMUS_SEED=<seed> cargo test -p animusd --lib <test name>` replays
//! any of the three scenarios (repo convention).

use std::collections::BTreeSet;
use std::time::Duration;

use animus_env::nid;
use animus_tablet::{TabletId, TabletState};

use super::AutoSplitThresholds;
use super::sim_cluster::SimCluster;

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

// ---------------------------------------------------------------------------
// (1) cp_member_addresses_register_and_replicate
// ---------------------------------------------------------------------------

fn run_cp_member_addresses_register_and_replicate(seed: u64) {
    let cluster = SimCluster::new(seed, 3, 3);
    let want: BTreeSet<_> = (0..3u64).map(nid).collect();

    let mut reference = None;
    for node in 0..3u64 {
        let meta = cluster.metadata(node);
        for id in &want {
            assert!(
                meta.node_addrs.contains_key(id),
                "seed={seed}: node {node}'s own view is missing a node_addrs entry for {id:?}"
            );
        }
        match &reference {
            None => reference = Some(meta.node_addrs.clone()),
            Some(r) => assert_eq!(
                &meta.node_addrs, r,
                "seed={seed}: node {node}'s own node_addrs view must be byte-identical to \
                 node 0's (the ADR 0032 PR1 replication property)"
            ),
        }
    }
}

#[test]
fn cp_member_addresses_register_and_replicate() {
    run_cp_member_addresses_register_and_replicate(env_seed(0x9970_0001));
}

#[test]
fn cp_member_addresses_register_and_replicate_over_seeds() {
    for i in 0..5 {
        run_cp_member_addresses_register_and_replicate(0x9970_1000 + i);
    }
}

// ---------------------------------------------------------------------------
// (2) cp_tablet_splits_and_both_halves_serve
// ---------------------------------------------------------------------------

/// Poll [`SimCluster::drive_inplace_split_cutover`] on every node,
/// interleaved with `run_for`, until `parent` has retired and `table`
/// shows exactly two `Active`, routable tablets on EVERY node in
/// `nodes` — mirroring `sim_cluster_admin.rs`'s own hand-rolled poll for
/// the identical primitive and `sim_cluster_auto_split.rs`'s
/// `poll_split_converged` doc for why this can never be a one-shot
/// assert (the fork and its cutover each need their own tick to land,
/// and this fixture never spawns the cutover driver as a background
/// loop).
fn poll_split_converged(
    cluster: &mut SimCluster,
    table: &str,
    parent: TabletId,
    nodes: &[u64],
    budget: Duration,
) {
    const STEP: Duration = Duration::from_millis(100);
    let seed = cluster.seed();
    let mut elapsed = Duration::ZERO;
    loop {
        for &n in nodes {
            cluster.drive_inplace_split_cutover(n);
        }
        let mut converged = true;
        for &n in nodes {
            let meta = cluster.metadata(n);
            if meta.tablets.contains_key(&parent) {
                converged = false;
                break;
            }
            let active = meta
                .tablets_for_table(table)
                .filter(|(_, t)| t.state == TabletState::Active)
                .count();
            if active != 2 {
                converged = false;
                break;
            }
        }
        if converged {
            return;
        }
        assert!(
            elapsed < budget,
            "seed={seed}: split of {table} did not converge to two Active children within \
             {budget:?}"
        );
        cluster.run_for(STEP);
        elapsed += STEP;
    }
}

fn run_cp_tablet_splits_and_both_halves_serve(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let parent = cluster.create_table("kv");

    // Write a lower key and an upper key (split point will be "k5") before
    // the split, from node 0 — mirroring the ProdEnv original's own
    // `put_until_ok`.
    cluster
        .put_raw(0, "kv", b"k1".to_vec(), b"lower".to_vec())
        .unwrap_or_else(|e| panic!("seed={seed}: put_raw(k1) failed: {e}"));
    cluster
        .put_raw(0, "kv", b"k9".to_vec(), b"upper".to_vec())
        .unwrap_or_else(|e| panic!("seed={seed}: put_raw(k9) failed: {e}"));

    // Trigger the split of the bootstrap tablet at "k5" — the same
    // production admin route (`ClientCtx::trigger_split`) the real
    // `ClientRequest::SplitTablet` handler funnels into.
    let split_body = format!(r#"{{"tablet":{},"split_key":"k5"}}"#, parent.0);
    let (status, split) =
        cluster.admin(0, "POST", "/admin/tablet/split", "", split_body.as_bytes());
    assert_eq!(status, 200, "seed={seed}: split kickoff failed: {split}");

    let nodes: [u64; 3] = [0, 1, 2];
    poll_split_converged(&mut cluster, "kv", parent, &nodes, Duration::from_secs(20));

    // The two routable tablets partition the keyspace: k1 and k9 land in
    // different tablets, and every written key falls in exactly one of
    // them.
    let meta = cluster.metadata(0);
    let children: Vec<_> = meta
        .tablets_for_table("kv")
        .filter(|(_, t)| t.is_routable())
        .collect();
    assert_eq!(
        children.len(),
        2,
        "seed={seed}: expected exactly two routable children, got {children:?}"
    );
    let k1_owner = children
        .iter()
        .find(|(_, t)| t.range.contains(b"k1"))
        .unwrap_or_else(|| {
            panic!("seed={seed}: no child tablet's range contains k1: {children:?}")
        });
    let k9_owner = children
        .iter()
        .find(|(_, t)| t.range.contains(b"k9"))
        .unwrap_or_else(|| {
            panic!("seed={seed}: no child tablet's range contains k9: {children:?}")
        });
    assert_ne!(
        k1_owner.0, k9_owner.0,
        "seed={seed}: k1 and k9 must land in different tablets after the split at k5: \
         {children:?}"
    );

    // The upper key is now served by the new group: read it back via a
    // different node than the writer, consistent (linearizable) read.
    let got = cluster
        .raw_get(2, "kv", b"k9".to_vec(), true)
        .unwrap_or_else(|e| panic!("seed={seed}: raw_get(k9) on node 2 failed: {e}"));
    assert_eq!(
        got,
        Some(b"upper".to_vec()),
        "seed={seed}: k9 must be served by the new upper-range tablet"
    );

    // The lower key still round-trips on the original group.
    let got = cluster
        .raw_get(0, "kv", b"k1".to_vec(), true)
        .unwrap_or_else(|e| panic!("seed={seed}: raw_get(k1) on node 0 failed: {e}"));
    assert_eq!(
        got,
        Some(b"lower".to_vec()),
        "seed={seed}: k1 must still be served by the original tablet"
    );

    // A new upper-range write routes to the new group and round-trips
    // through yet a third node.
    cluster
        .put_raw(0, "kv", b"k7".to_vec(), b"upper2".to_vec())
        .unwrap_or_else(|e| panic!("seed={seed}: put_raw(k7) failed: {e}"));
    let got = cluster
        .raw_get(1, "kv", b"k7".to_vec(), true)
        .unwrap_or_else(|e| panic!("seed={seed}: raw_get(k7) on node 1 failed: {e}"));
    assert_eq!(
        got,
        Some(b"upper2".to_vec()),
        "seed={seed}: a new upper-range write must round-trip through the new tablet"
    );
}

#[test]
fn cp_tablet_splits_and_both_halves_serve() {
    run_cp_tablet_splits_and_both_halves_serve(env_seed(0x9970_0002));
}

#[test]
fn cp_tablet_splits_and_both_halves_serve_over_seeds() {
    for i in 0..5 {
        run_cp_tablet_splits_and_both_halves_serve(0x9970_2000 + i);
    }
}

// ---------------------------------------------------------------------------
// (3) tablet_auto_splits_on_bytes_with_skewed_value_sizes (bounded spike,
// ADR 0061 rung O, issue #997 — see this module's own doc for why the
// ProdEnv original stays regardless of this outcome)
// ---------------------------------------------------------------------------

/// [`poll_split_converged`]'s sibling for the auto-split trigger: no manual
/// kickoff and no known parent id ahead of time (the byte trigger picks its
/// own tablet), so this polls for "exactly two `Active`, zero `Splitting`"
/// on `table` directly, mirroring `sim_cluster_auto_split.rs::
/// poll_split_converged` exactly (same reasons: the fork and its cutover
/// each need their own tick, and this fixture never spawns the cutover
/// driver as a background loop).
fn poll_auto_split_converged(
    cluster: &mut SimCluster,
    table: &str,
    nodes: &[u64],
    budget: Duration,
) {
    const STEP: Duration = Duration::from_millis(100);
    let seed = cluster.seed();
    let mut elapsed = Duration::ZERO;
    loop {
        for &n in nodes {
            cluster.drive_inplace_split_cutover(n);
        }
        let mut converged = true;
        for &n in nodes {
            let meta = cluster.metadata(n);
            let mut active = 0;
            let mut splitting = 0;
            for (_, t) in meta.tablets_for_table(table) {
                match t.state {
                    TabletState::Active => active += 1,
                    TabletState::Splitting => splitting += 1,
                    _ => {}
                }
            }
            if active != 2 || splitting != 0 {
                converged = false;
                break;
            }
        }
        if converged {
            return;
        }
        assert!(
            elapsed < budget,
            "seed={seed}: byte auto-split of {table} did not converge to two Active \
             tablets within {budget:?}"
        );
        cluster.run_for(STEP);
        elapsed += STEP;
    }
}

const SPIKE_BYTES_THRESHOLD: u64 = 8_000;

/// The raw-KV path (`put_raw`, literal un-encoded keys) makes the
/// byte-weighted-median correlation the `ProdEnv` original proves
/// (`tests/cp_plane.rs`) trivial to reproduce here too: `SimCluster::
/// put_raw`'s key is stored verbatim, so a child tablet's own `KeyRange`
/// can be checked directly against the literal keys this scenario wrote —
/// unlike the DynamoDB-wire path (`sim_cluster_auto_split.rs`), whose
/// `item_key` encoding hash-token-prefixes every key and so cannot be
/// correlated to a token range from a test.
fn run_tablet_auto_splits_on_bytes_with_skewed_value_sizes(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    cluster.create_table("kv");

    // 6 tiny rows (~10 bytes each) + 6 large rows (~2000 bytes each) —
    // tiny keys sort before large ones ("a" < "b"), so a plain positional
    // median would land right at the first large key, putting ~99% of the
    // bytes on one side. The byte-weighted median instead balances by
    // actual bytes.
    let mut written: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    for i in 0..6u32 {
        written.push((format!("a{i:02}").into_bytes(), b"tiny".to_vec()));
    }
    for i in 0..6u32 {
        written.push((format!("b{i:02}").into_bytes(), vec![b'x'; 2000]));
    }
    let total_bytes: u64 = written
        .iter()
        .map(|(k, v)| (k.len() + v.len()) as u64)
        .sum();

    for (key, value) in &written {
        cluster
            .put_raw(0, "kv", key.clone(), value.clone())
            .unwrap_or_else(|e| panic!("seed={seed}: put_raw({key:?}) failed: {e}"));
    }

    // No manual trigger — the byte-threshold auto-split loop alone must
    // fire, since the key count (12) never approaches a meaningful
    // key-count threshold (there is none any more — ADR 0034).
    cluster.set_auto_split_thresholds(AutoSplitThresholds {
        bytes: Some(SPIKE_BYTES_THRESHOLD),
        change_rate: None,
        ops_rate: None,
        tablet_capacity_ceilings: Default::default(),
    });

    let nodes: [u64; 3] = [0, 1, 2];
    poll_auto_split_converged(&mut cluster, "kv", &nodes, Duration::from_secs(30));

    // Both halves serve: a tiny-side key and a large-side key both read
    // back, through a node other than the writer.
    let got = cluster
        .raw_get(2, "kv", b"a00".to_vec(), true)
        .unwrap_or_else(|e| panic!("seed={seed}: raw_get(a00) failed: {e}"));
    assert_eq!(
        got,
        Some(b"tiny".to_vec()),
        "seed={seed}: tiny-side key must still be served after the byte auto-split"
    );
    let got = cluster
        .raw_get(2, "kv", b"b05".to_vec(), true)
        .unwrap_or_else(|e| panic!("seed={seed}: raw_get(b05) failed: {e}"));
    assert_eq!(
        got,
        Some(vec![b'x'; 2000]),
        "seed={seed}: large-side key must still be served after the byte auto-split"
    );

    // The point of the byte-weighted median: every written key falls in
    // exactly one of the two routable children's own ranges, and the
    // smaller child holds at least 15% of the total bytes (a loose bound,
    // since this is an estimate-driven split, but tight enough to
    // distinguish it from a plain positional median — which would put
    // ~99% of the bytes on the "b" side here).
    let meta = cluster.metadata(0);
    let children: Vec<_> = meta
        .tablets_for_table("kv")
        .filter(|(_, t)| t.is_routable())
        .collect();
    assert_eq!(
        children.len(),
        2,
        "seed={seed}: expected exactly two routable children, got {children:?}"
    );

    let mut child_bytes = [0u64; 2];
    for (key, value) in &written {
        let mut owners = children
            .iter()
            .enumerate()
            .filter(|(_, (_, t))| t.range.contains(key));
        let (idx, _) = owners.next().unwrap_or_else(|| {
            panic!("seed={seed}: key {key:?} falls in no child tablet's range: {children:?}")
        });
        assert!(
            owners.next().is_none(),
            "seed={seed}: key {key:?} falls in more than one child tablet's range: {children:?}"
        );
        child_bytes[idx] += (key.len() + value.len()) as u64;
    }
    let smallest = child_bytes[0].min(child_bytes[1]);
    let floor = total_bytes * 15 / 100;
    assert!(
        smallest >= floor,
        "seed={seed}: smallest child holds {smallest} of {total_bytes} total bytes, below \
         the 15% floor ({floor}); child_bytes={child_bytes:?}"
    );
}

#[test]
fn tablet_auto_splits_on_bytes_with_skewed_value_sizes() {
    run_tablet_auto_splits_on_bytes_with_skewed_value_sizes(env_seed(0x9970_0003));
}

#[test]
fn tablet_auto_splits_on_bytes_with_skewed_value_sizes_over_seeds() {
    for i in 0..5 {
        run_tablet_auto_splits_on_bytes_with_skewed_value_sizes(0x9970_3000 + i);
    }
}
