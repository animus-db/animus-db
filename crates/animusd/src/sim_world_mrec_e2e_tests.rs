//! G-d M4c: end-to-end MREC over the wire in `SimWorld`: a split of the source
//! tablet while shipping, a source-leader failover mid-ship, and a three-region
//! full mesh with concurrent conflicting writes. All at >= 20 seeds.
//!
//! Split lineage (plan risk R1): a split child starts with no cursor, so it
//! ships by an **unfiltered scan** of its current rows (the inherited-floor
//! optimisation was not built; see `mrec_shipper`'s module doc). These tests
//! are the correctness proof for that fallback: every row reaches the peer, a
//! delete is never resurrected, and a write after the split still replicates.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use animus_tablet::TabletState;

use super::AutoSplitThresholds;
use super::config::PeerCluster;
use super::mrec_peer::{MrecConfig, PeerClient};
use super::mrec_saga::mrec_saga_table;
use super::mrec_shipper::mrec_ship_table;
use super::sim_world::{LinkConfig, PeerHandler, SimWorld};
use super::sim_world_mrec_saga_tests::{
    NODES, S, cfg, create_body, item_body, key_body, saga_seeds,
};
use super::sim_world_mrec_tests::{A, B, LAT, TABLE, handler, open_mrec_gate};

const PAD: usize = 300;

fn pad(tag: &str) -> String {
    format!("{tag}-{}", "x".repeat(PAD))
}

fn active_tablets(s: &mut S, c: usize) -> usize {
    let n = s.up(c);
    s.w.clusters[c]
        .metadata(n)
        .tablets_for_table(TABLE)
        .filter(|(_, t)| t.state == TabletState::Active)
        .count()
}

fn drive_splits(s: &mut S, c: usize) {
    for n in 0..NODES {
        if !s.down.contains(&(c, n as usize)) {
            s.w.clusters[c].drive_inplace_split_cutover(n);
        }
    }
}

/// A write that rides out the split's freeze window (503, retryable).
fn put_retry(s: &mut S, k: &str, v: &str) {
    retry(s, "PutItem", &item_body(k, v, ""));
}

fn retry(s: &mut S, op: &str, body: &str) {
    for _ in 0..200 {
        let (st, r) = s.call(A, op, body);
        if st == 200 {
            return;
        }
        assert_eq!(st, 503, "seed={}: {op}: {r}", s.seed);
        drive_splits(s, A);
        s.step_both();
    }
    panic!("seed={}: {op} never got through the split", s.seed);
}

#[test]
fn a_split_of_the_source_tablet_keeps_shipping_every_row() {
    for seed in saga_seeds() {
        let mut s = S::new(seed, true);
        s.make_active(0);
        for i in 0..6 {
            s.put(A, &format!("k{i:02}"), &pad("v1"));
        }
        s.until("the first rows on B", |s| s.read(B, "k05").is_some());
        // Arm the bytes trigger and keep writing/deleting while the split and
        // the shipper race.
        s.w.clusters[A].set_auto_split_thresholds(AutoSplitThresholds {
            bytes: Some(2_000),
            change_rate: None,
            ops_rate: None,
            tablet_capacity_ceilings: Default::default(),
        });
        for i in 6..12 {
            put_retry(&mut s, &format!("k{i:02}"), &pad("v1"));
            if i == 8 {
                retry(&mut s, "DeleteItem", &key_body("k01", false));
            }
            s.step_both();
        }
        let mut guard = 0;
        while active_tablets(&mut s, A) < 2 {
            drive_splits(&mut s, A);
            s.step_both();
            guard += 1;
            assert!(guard < 200, "seed={seed}: the source never split");
        }
        // After the split: new writes, an overwrite and a delete on each side
        // of the key space.
        for i in 0..12 {
            if i == 1 {
                continue; // deleted before the split: must stay deleted
            }
            if i == 3 {
                retry(&mut s, "DeleteItem", &key_body("k03", false));
            } else {
                put_retry(&mut s, &format!("k{i:02}"), &pad("v2"));
            }
        }
        put_retry(&mut s, "zz-after", &pad("v2"));
        s.until("every row on B, every delete honoured", |s| {
            (0..12).all(|i| {
                let got = s.read(B, &format!("k{i:02}"));
                match i {
                    1 | 3 => got.is_none(),
                    _ => got.as_deref() == Some(pad("v2").as_str()),
                }
            }) && s.read(B, "zz-after").is_some()
        });
        // The deleted keys stay deleted (no resurrection) after more passes.
        for _ in 0..4 {
            s.step_both();
        }
        assert!(
            s.read(B, "k01").is_none() && s.read(B, "k03").is_none(),
            "seed={seed}"
        );
        assert!(
            s.read(A, "k01").is_none() && s.read(A, "k03").is_none(),
            "seed={seed}"
        );
    }
}

#[test]
fn a_source_leader_failover_mid_ship_loses_nothing() {
    for seed in saga_seeds() {
        let mut s = S::new(seed, true);
        s.make_active(0);
        for i in 0..30 {
            s.put(A, &format!("a{i:02}"), "one");
        }
        // Part of the batch is shipped when the leader dies.
        for _ in 0..(seed % 2) {
            s.step(A);
        }
        let n = s.up(A);
        let tablet = s.w.clusters[A]
            .metadata(n)
            .tablets_for_table(TABLE)
            .map(|(id, _)| *id)
            .min()
            .expect("tablet");
        let victim = s.w.clusters[A].leader_index_of(tablet).expect("leader");
        s.crash(A, victim);
        for i in 0..30 {
            s.put(A, &format!("b{i:02}"), "two");
        }
        s.until("every row on B after the failover", |s| {
            (0..30).all(|i| {
                s.read(B, &format!("a{i:02}")).as_deref() == Some("one")
                    && s.read(B, &format!("b{i:02}")).as_deref() == Some("two")
            })
        });
    }
}

// ---------------------------------------------------------------------------
// Three-region full mesh.

struct M {
    w: SimWorld,
    cfgs: Vec<Arc<MrecConfig>>,
    seed: u64,
}

const REGIONS: [&str; 3] = ["a", "b", "c"];

fn mesh_cfg(i: usize) -> Arc<MrecConfig> {
    // Sender view: peer index == the bridge's cluster index (self is `pad`).
    let peers: Vec<&str> = (0..3)
        .map(|j| if j == i { "pad" } else { REGIONS[j] })
        .collect();
    let mut c = cfg(REGIONS[i], &peers).as_ref().clone();
    c.peers = c
        .peers
        .iter()
        .map(|p| PeerCluster {
            region: p.region.clone(),
            endpoints: p.endpoints.clone(),
            tls_ca: None,
        })
        .collect();
    Arc::new(c)
}

impl M {
    fn new(seed: u64) -> M {
        let mut w = SimWorld::new(seed, 3, 3, 3);
        for c in 0..3 {
            open_mrec_gate(&mut w.clusters[c], seed);
        }
        w.clusters[A].create_table(TABLE);
        w.sync_clocks();
        w.bridge().set_default_link(LinkConfig::new(LAT));
        let cfgs: Vec<Arc<MrecConfig>> = (0..3).map(mesh_cfg).collect();
        for (c, cfg) in cfgs.iter().enumerate() {
            w.clusters[c].handle().set_mrec_config(cfg.clone());
            let h: PeerHandler = handler(cfg.as_ref().clone(), Arc::new(AtomicU64::new(0)));
            w.set_handler(c, h);
        }
        M { w, cfgs, seed }
    }

    fn call(&mut self, c: usize, op: &str, body: &str) -> (u16, String) {
        self.w.dynamo(c, 0, op, body)
    }

    fn put(&mut self, c: usize, k: &str, v: &str) {
        let (st, r) = self.call(c, "PutItem", &item_body(k, v, ""));
        assert_eq!(st, 200, "seed={}: {r}", self.seed);
    }

    fn read(&mut self, c: usize, k: &str) -> Option<String> {
        let (st, r) = self.call(c, "GetItem", &key_body(k, true));
        if st != 200 {
            return None;
        }
        let v: serde_json::Value = serde_json::from_str(&r).expect("json");
        v["Item"]["v"]["S"].as_str().map(str::to_owned)
    }

    fn step(&mut self) {
        for c in 0..3 {
            for node in 0..NODES {
                let mut ctx = self.w.clusters[c].handle().node_ctx(node);
                ctx.mrec = self.cfgs[c].clone();
                let client = self.w.peer_client(c);
                let seed = self.seed;
                self.w
                    .drive(c, node, Duration::from_secs(90), async move {
                        let _ = mrec_saga_table(&ctx, TABLE, &client as &dyn PeerClient).await;
                        let _ = mrec_ship_table(&ctx, TABLE, &client as &dyn PeerClient).await;
                    })
                    .unwrap_or_else(|| panic!("seed={seed}: step {c}/{node} did not finish"));
            }
        }
        self.w.run_for(Duration::from_millis(250));
    }

    fn until(&mut self, what: &str, cond: impl Fn(&mut M) -> bool) {
        for _ in 0..300 {
            if cond(self) {
                return;
            }
            self.step();
        }
        panic!("seed={}: timed out waiting for {what}", self.seed);
    }

    fn replica_count(&mut self, c: usize) -> (usize, bool) {
        let (st, r) = self.call(c, "DescribeTable", &format!(r#"{{"TableName":"{TABLE}"}}"#));
        if st != 200 {
            return (0, false);
        }
        let v: serde_json::Value = serde_json::from_str(&r).expect("json");
        let l = v["Table"]["Replicas"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        let all = l.iter().all(|e| e["ReplicaStatus"] == "ACTIVE");
        (l.len(), all)
    }
}

#[test]
fn three_regions_form_a_full_mesh_and_converge_on_conflicting_writes() {
    for seed in saga_seeds() {
        let mut m = M::new(seed);
        for i in 0..5 {
            m.put(A, &format!("pre{i}"), "old");
        }
        m.call(A, "UpdateTable", &create_body("b"));
        m.until("A+B active", |m| m.replica_count(A) == (2, true));
        m.call(A, "UpdateTable", &create_body("c"));
        m.until("the three-way mesh", |m| {
            (0..3).all(|c| m.replica_count(c) == (3, true))
        });
        for i in 0..5 {
            for c in [B, 2] {
                assert_eq!(
                    m.read(c, &format!("pre{i}")).as_deref(),
                    Some("old"),
                    "seed={seed}"
                );
            }
        }
        // Concurrent conflicting writes to one key from all three regions in
        // the same virtual millisecond, plus per-region keys.
        m.put(A, "race", "from-a");
        m.put(B, "race", "from-b");
        m.put(2, "race", "from-c");
        m.put(A, "only-a", "1");
        m.put(B, "only-b", "2");
        m.put(2, "only-c", "3");
        m.until("identical state everywhere", |m| {
            let r: Vec<_> = (0..3).map(|c| m.read(c, "race")).collect();
            let ok = r.iter().all(|x| x.is_some() && *x == r[0]);
            ok && (0..3).all(|c| {
                m.read(c, "only-a").is_some()
                    && m.read(c, "only-b").is_some()
                    && m.read(c, "only-c").is_some()
            })
        });
        // And it stays put (no flip-flop) after more passes.
        let w = m.read(A, "race");
        for _ in 0..6 {
            m.step();
        }
        for c in 0..3 {
            assert_eq!(m.read(c, "race"), w, "seed={seed}: region {c} flipped");
        }
    }
}
