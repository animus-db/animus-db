//! ADR 0075's 2026-10-21 amendment: **per-replica read-capacity overrides** on
//! an MREC global table, over the DynamoDB wire between two real 3-node
//! `SimCluster`s in one `SimWorld` (the harness of `sim_world_mrec_saga_tests`).
//!
//! Cluster A (Region `a`) holds a `PROVISIONED` table and receives every wire
//! call; B (Region `b`) is created by the saga as a twin. Per seed:
//!
//! 1. validation: an override on a `PAY_PER_REQUEST` table, an unknown
//!    `IndexName`, an `Update` naming no setting, and the settings whose
//!    underlying table feature does not exist are each a named
//!    `ValidationException`; with the override gate **closed** (cluster
//!    version 3) the surface is refused exactly as before it existed;
//! 2. `Create` with `ProvisionedThroughputOverride` for `b`: both clusters'
//!    `DescribeTable` report the same `Replicas[]` override, B's reads of a
//!    large item hit `ProvisionedThroughputExceededException` at the 1-RCU
//!    override while A (the table's own budget) never does;
//! 3. **fault**: the clusters are partitioned while an `Update` (one call, two
//!    actions: `b` raised, `a` — the owner's own Region — lowered to 1 RCU) is
//!    taken on A. A enforces its own override at once, B keeps the old record;
//!    after the heal both converge on the same `Replicas[]`, B is no longer
//!    throttled and A is;
//! 4. a clear (`ProvisionedThroughputOverride: {}`) converges as a tombstone
//!    (no `ProvisionedThroughputOverride` anywhere) and B returns to its own
//!    budget.
//!
//! `ANIMUS_MREC_WORLD_SEEDS=K` sets the depth (>= 20 seeds), `ANIMUS_SEED=<s>`
//! replays one.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use animus_control::version::{MAX_SUPPORTED, VersionRange};

use super::sim_cluster::SimCluster;
use super::sim_world::{LinkConfig, PeerHandler, SimWorld};
use super::sim_world_mrec_saga_tests::{S, cfg, create_body, key_body, saga_seeds};
use super::sim_world_mrec_tests::{A, B, LAT, TABLE, handler, open_mrec_gate, poll};

/// Finalize the cluster from the MREC gate's version (3, `open_mrec_gate`) up
/// to this binary's `MAX_SUPPORTED` through the real admin path, one step at a
/// time, so `Gate::MrecReplicaOverrides` is open.
fn open_overrides_gate(c: &mut SimCluster, seed: u64) {
    let n = c.node_count() as u64;
    c.set_all_node_versions(Some(VersionRange::new(1, MAX_SUPPORTED)));
    poll(c, "every node's full-range version record", seed, |c| {
        (0..n).all(|i| {
            let m = c.metadata(i);
            m.node_versions.len() == n as usize
                && m.node_versions
                    .values()
                    .all(|v| v.range.max == MAX_SUPPORTED)
        })
    });
    for target in 4..=MAX_SUPPORTED {
        let idx = c.control_leader_index();
        let leader = c.control_node_id(idx);
        let body = format!(r#"{{"to":{target},"expected":{}}}"#, target - 1);
        let (status, v) = c.admin(
            leader,
            "POST",
            "/admin/cluster-version/finalize",
            "",
            body.as_bytes(),
        );
        assert_eq!(status, 200, "seed={seed}: finalize ->{target}: {v}");
        poll(c, "the new cluster version on every node", seed, |c| {
            (0..n).all(|i| c.features(i).cluster_version() == target)
        });
    }
}

/// `S::new` with the gate ladder opened *before* the table exists: a cluster
/// that idles for a long finalize ladder after its table was created needs
/// many client calls to wake its quiesced groups, which is noise here.
fn world(seed: u64, overrides_gate: bool) -> S {
    let mut w = SimWorld::new(seed, 2, 3, 3);
    for c in [A, B] {
        open_mrec_gate(&mut w.clusters[c], seed);
        if overrides_gate {
            open_overrides_gate(&mut w.clusters[c], seed);
        }
    }
    w.clusters[A].create_table(TABLE);
    w.sync_clocks();
    w.bridge().set_default_link(LinkConfig::new(LAT));
    let cfgs = [cfg("a", &["pad", "b"]), cfg("b", &["a"])];
    for c in [A, B] {
        w.clusters[c].handle().set_mrec_config(cfgs[c].clone());
    }
    let entry = [Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0))];
    let ha: PeerHandler = handler(cfgs[A].as_ref().clone(), entry[A].clone());
    let hb: PeerHandler = handler(cfgs[B].as_ref().clone(), entry[B].clone());
    w.set_handler(A, ha);
    w.set_handler(B, hb);
    S {
        w,
        cfgs,
        seed,
        down: Vec::new(),
        entry,
    }
}

fn msg(r: &str) -> String {
    serde_json::from_str::<serde_json::Value>(r).expect("json")["message"]
        .as_str()
        .unwrap_or("")
        .to_owned()
}

fn update_body(actions: &str) -> String {
    format!(r#"{{"TableName":"{TABLE}","ReplicaUpdates":[{actions}]}}"#)
}

fn create_with(region: &str, rcu: u64) -> String {
    update_body(&format!(
        r#"{{"Create":{{"RegionName":"{region}","ProvisionedThroughputOverride":{{"ReadCapacityUnits":{rcu}}}}}}}"#
    ))
}

fn update_with(region: &str, rcu: u64) -> String {
    format!(
        r#"{{"Update":{{"RegionName":"{region}","ProvisionedThroughputOverride":{{"ReadCapacityUnits":{rcu}}}}}}}"#
    )
}

fn provision(s: &mut S, rcu: u64) {
    s.ok(
        A,
        "UpdateTable",
        &format!(
            r#"{{"TableName":"{TABLE}","BillingMode":"PROVISIONED","ProvisionedThroughput":{{"ReadCapacityUnits":{rcu},"WriteCapacityUnits":1000}}}}"#
        ),
    );
}

/// `region -> (table override, index overrides)` from `DescribeTable` on `c`.
fn overrides(s: &mut S, c: usize) -> BTreeMap<String, (Option<u64>, BTreeMap<String, u64>)> {
    let (st, r) = s.call(c, "DescribeTable", &format!(r#"{{"TableName":"{TABLE}"}}"#));
    let mut out = BTreeMap::new();
    if st != 200 {
        return out;
    }
    let v: serde_json::Value = serde_json::from_str(&r).expect("json");
    for e in v["Table"]["Replicas"]
        .as_array()
        .cloned()
        .unwrap_or_default()
    {
        let table = e["ProvisionedThroughputOverride"]["ReadCapacityUnits"].as_u64();
        let idx: BTreeMap<String, u64> = e["GlobalSecondaryIndexes"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .iter()
            .map(|g| {
                (
                    g["IndexName"].as_str().unwrap_or("").to_owned(),
                    g["ProvisionedThroughputOverride"]["ReadCapacityUnits"]
                        .as_u64()
                        .unwrap_or(0),
                )
            })
            .collect();
        if table.is_some() || !idx.is_empty() {
            out.insert(
                e["RegionName"].as_str().unwrap_or("").to_owned(),
                (table, idx),
            );
        }
    }
    out
}

fn want(entries: &[(&str, Option<u64>)]) -> BTreeMap<String, (Option<u64>, BTreeMap<String, u64>)> {
    entries
        .iter()
        .filter(|(_, o)| o.is_some())
        .map(|(r, o)| ((*r).to_owned(), (*o, BTreeMap::new())))
        .collect()
}

/// Strongly consistent reads of the 256 KiB item (64 RCU each) on cluster `c`
/// until one is throttled; `Some(n)` = `n` admitted before the refusal, `None`
/// = none of `limit` reads was refused.
fn reads_until_throttled(s: &mut S, c: usize, limit: usize) -> Option<usize> {
    // Production's metadata-watch loop refreshes each node's throttle fast-path
    // flag every tick; this harness does not run it.
    s.w.clusters[c].recompute_throttle_flags();
    for i in 0..limit {
        let (st, r) = s.call(c, "GetItem", &key_body("big", true));
        if st == 200 {
            continue;
        }
        assert!(
            r.contains("ProvisionedThroughputExceededException"),
            "seed={}: read {i} on {c} failed with something else: {st} {r}",
            s.seed
        );
        return Some(i);
    }
    None
}

#[test]
fn per_replica_read_overrides_replicate_enforce_and_converge_across_a_partition() {
    // Each seed moves ~100 MB of 256 KiB reads through two clusters, so this
    // one runs a smaller floor than the file's 20 (`ANIMUS_SEED` still replays).
    for seed in saga_seeds().into_iter().take(4) {
        let mut s = world(seed, true);

        // -- validation, on a PAY_PER_REQUEST table -------------------------
        let (st, r) = s.call(A, "UpdateTable", &create_with("b", 5));
        assert_eq!(st, 400, "seed={seed}: {r}");
        assert!(msg(&r).contains("is PAY_PER_REQUEST"), "seed={seed}: {r}");
        assert!(
            s.w.clusters[A].metadata(0).table_global(TABLE).is_none(),
            "seed={seed}: a refused request converted the table"
        );
        provision(&mut s, 1000);
        // The table's own Region is not a peer: Create refuses it, Update needs a replica.
        let (st, r) = s.call(A, "UpdateTable", &update_body(&update_with("b", 5)));
        assert_eq!(st, 400, "seed={seed}: {r}");
        assert!(
            msg(&r).contains("is not a multi-Region eventually consistent table"),
            "seed={seed}: {r}"
        );
        // Unknown IndexName.
        let body = update_body(
            r#"{"Create":{"RegionName":"b","GlobalSecondaryIndexes":[
                {"IndexName":"nope","ProvisionedThroughputOverride":{"ReadCapacityUnits":5}}]}}"#,
        );
        let (st, r) = s.call(A, "UpdateTable", &body);
        assert_eq!(st, 400, "seed={seed}: {r}");
        assert!(
            msg(&r).contains("has no global secondary index `nope`"),
            "seed={seed}: {r}"
        );
        // The settings whose table feature does not exist.
        for (field, feature) in [
            ("KMSMasterKeyId", "SSESpecification"),
            ("OnDemandThroughputOverride", "OnDemandThroughput"),
            ("TableClassOverride", "TableClass"),
        ] {
            let body = update_body(&format!(
                r#"{{"Create":{{"RegionName":"b","{field}":"x"}}}}"#
            ));
            let (st, r) = s.call(A, "UpdateTable", &body);
            assert_eq!(st, 400, "seed={seed}: {r}");
            assert!(msg(&r).contains(feature), "seed={seed}: {field}: {r}");
        }

        // -- create with an override ---------------------------------------
        s.put(A, "small", "x");
        let big = "x".repeat(256 * 1024);
        s.put(A, "big", &big);
        s.ok(A, "UpdateTable", &create_with("b", 1));
        s.until("both sides ACTIVE with the override on both", |s| {
            s.both_active()
                && overrides(s, A) == want(&[("b", Some(1))])
                && overrides(s, B) == want(&[("b", Some(1))])
        });
        // An Update on a replica that does not exist, and one naming no setting.
        let (st, r) = s.call(A, "UpdateTable", &update_body(&update_with("zz", 5)));
        assert_eq!(st, 400, "seed={seed}: {r}");
        assert!(
            msg(&r).contains("Region `zz` is not a replica of table"),
            "seed={seed}: {r}"
        );
        let (st, r) = s.call(
            A,
            "UpdateTable",
            &update_body(r#"{"Update":{"RegionName":"b"}}"#),
        );
        assert_eq!(st, 400, "seed={seed}: {r}");
        assert!(
            msg(&r).contains("names no setting to change"),
            "seed={seed}: {r}"
        );

        s.until("the big item on B", |s| s.read(B, "big").is_some());
        // B enforces its 1 RCU/s override (a 300-unit burst): it refuses within
        // a handful of 64-RCU reads. A's own budget (1000 RCU/s) never does.
        let on_b = reads_until_throttled(&mut s, B, 80);
        assert!(
            on_b.is_some_and(|n| n >= 1),
            "seed={seed}: B never throttled at its 1-RCU override ({on_b:?})"
        );
        assert_eq!(
            reads_until_throttled(&mut s, A, 40),
            None,
            "seed={seed}: A was throttled below its own table budget"
        );

        // -- fault: partition during an Update ------------------------------
        s.w.bridge().partition(A, B);
        let both = update_body(&format!(
            "{},{}",
            update_with("b", 100_000),
            update_with("a", 1)
        ));
        s.ok(A, "UpdateTable", &both);
        for _ in 0..8 {
            s.step_both();
        }
        // A took both at once and enforces its own; B still has the old record
        // (the push cannot cross the partition) and still throttles at 1.
        assert_eq!(
            overrides(&mut s, A),
            want(&[("a", Some(1)), ("b", Some(100_000))]),
            "seed={seed}"
        );
        assert_eq!(overrides(&mut s, B), want(&[("b", Some(1))]), "seed={seed}");
        s.w.run_for(std::time::Duration::from_secs(400));
        assert!(
            reads_until_throttled(&mut s, A, 80).is_some(),
            "seed={seed}: A did not enforce its own (local) override during the partition"
        );
        assert!(
            reads_until_throttled(&mut s, B, 80).is_some(),
            "seed={seed}: B changed behaviour before the heal"
        );

        // -- heal and converge ---------------------------------------------
        s.w.bridge().heal();
        s.until("identical Replicas[] overrides on both clusters", |s| {
            let w = want(&[("a", Some(1)), ("b", Some(100_000))]);
            overrides(s, A) == w && overrides(s, B) == w
        });
        s.w.run_for(std::time::Duration::from_secs(400));
        assert_eq!(
            reads_until_throttled(&mut s, B, 40),
            None,
            "seed={seed}: B is still throttled after its override was raised"
        );
        assert!(
            reads_until_throttled(&mut s, A, 80).is_some(),
            "seed={seed}: A lost its lowered override"
        );

        // -- clear ----------------------------------------------------------
        let clear = update_body(
            r#"{"Update":{"RegionName":"a","ProvisionedThroughputOverride":{}}},
               {"Update":{"RegionName":"b","ProvisionedThroughputOverride":{}}}"#,
        );
        s.ok(A, "UpdateTable", &clear);
        s.until("every override cleared on both clusters", |s| {
            let w = want(&[("a", None), ("b", None)]);
            overrides(s, A) == w && overrides(s, B) == w
        });
        // Several more passes: the tombstone is never resurrected by a stale push.
        for _ in 0..6 {
            s.step_both();
        }
        let w = want(&[("a", None), ("b", None)]);
        assert_eq!(overrides(&mut s, A), w, "seed={seed}");
        assert_eq!(overrides(&mut s, B), w, "seed={seed}");
        // One read per side adopts the restored rate (the bucket re-rates on its
        // next check); the burst then refills at the table's own budget.
        let _ = (
            reads_until_throttled(&mut s, A, 1),
            reads_until_throttled(&mut s, B, 1),
        );
        s.w.run_for(std::time::Duration::from_secs(400));
        assert_eq!(
            reads_until_throttled(&mut s, A, 40),
            None,
            "seed={seed}: A did not return to its table budget"
        );
        assert_eq!(
            reads_until_throttled(&mut s, B, 40),
            None,
            "seed={seed}: B did not return to its table budget"
        );
    }
}

/// With the override gate closed (cluster version 3: the MREC gate is open, the
/// override gate is not) the override surface is refused exactly as before it
/// existed, and nothing is converted.
#[test]
fn a_closed_overrides_gate_refuses_the_surface_and_leaves_replica_creation_alone() {
    for seed in [0x4A33_0001u64, 0x4A33_0002] {
        let mut s = world(seed, false);
        provision(&mut s, 1000);
        let (st, r) = s.call(A, "UpdateTable", &create_with("b", 5));
        assert_eq!(st, 400, "seed={seed}: {r}");
        assert_eq!(
            msg(&r),
            "UpdateTable: ReplicaUpdates Update and per-replica ProvisionedThroughputOverride \
             are not supported",
            "seed={seed}"
        );
        assert!(
            s.w.clusters[A].metadata(0).table_global(TABLE).is_none(),
            "seed={seed}"
        );
        // A plain Create still works at this cluster version.
        s.ok(A, "UpdateTable", &create_body("b"));
        s.until("both sides ACTIVE", |s| s.both_active());
        let (st, r) = s.call(A, "UpdateTable", &update_body(&update_with("b", 5)));
        assert_eq!(st, 400, "seed={seed}: {r}");
        assert!(msg(&r).contains("are not supported"), "seed={seed}: {r}");
        assert!(overrides(&mut s, A).values().all(|(t, _)| t.is_none()));
    }
}
