//! G-01 stage G-d M4b: the MREC **replica lifecycle** over the DynamoDB wire,
//! between two real 3-node `SimCluster`s in one `SimWorld` (`UpdateTable`
//! `ReplicaUpdates` -> saga -> shipper scan -> ACTIVE; `Delete`).
//!
//! Cluster A holds the table (with rows) and receives the wire calls; B starts
//! with **no** table: the saga makes the peer create its twin. The saga and
//! the shipper are driven by hand (`step`), each side's receiver is the real
//! `handle_mrec_apply` (which routes lifecycle messages to `handle_control`).
//!
//! `ANIMUS_MREC_WORLD_SEEDS=K` sets the depth (>= 20 seeds on this file's own
//! floor), `ANIMUS_SEED=<s>` replays one.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use animus_control::schema::MrecReplicaStatus;

use super::config::PeerCluster;
use super::mrec_peer::{MrecConfig, PeerClient};
use super::mrec_saga::mrec_saga_table;
use super::mrec_shipper::mrec_ship_table;
use super::sim_world::{LinkConfig, PeerHandler, SimWorld};
use super::sim_world_mrec_tests::{A, B, LAT, TABLE, handler, open_mrec_gate, seeds};

pub(crate) const NODES: u64 = 3;
const MIN_SEEDS: u64 = 20;

pub(crate) fn saga_seeds() -> Vec<u64> {
    let mut v = seeds();
    let mut i = 0u64;
    while (v.len() as u64) < MIN_SEEDS && std::env::var("ANIMUS_SEED").is_err() {
        v.push(0x4A00_0000 + i);
        i += 1;
    }
    v
}

pub(crate) fn cfg(local: &str, peers: &[&str]) -> Arc<MrecConfig> {
    Arc::new(MrecConfig {
        region: Some(local.into()),
        peers: peers
            .iter()
            .map(|r| PeerCluster {
                region: (*r).into(),
                endpoints: vec![format!("{r}.invalid:7000")],
                tls_ca: None,
            })
            .collect(),
        allow_insecure: true,
        max_clock_skew_ms: 500,
        inflight: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        max_backlog: Duration::from_secs(3600),
        node_tls: None,
        health: Arc::default(),
    })
}

pub(crate) struct S {
    pub(crate) w: SimWorld,
    pub(crate) cfgs: [Arc<MrecConfig>; 2],
    pub(crate) seed: u64,
    /// Nodes (of either cluster, by node index) that are crashed: skipped by `step`.
    pub(crate) down: Vec<(usize, usize)>,
    /// Each cluster's peer-handler entry node.
    pub(crate) entry: [Arc<AtomicU64>; 2],
}

pub(crate) fn item_body(k: &str, v: &str, extra: &str) -> String {
    format!(
        r#"{{"TableName":"{TABLE}","Item":{{"pk":{{"S":"{k}"}},"sk":{{"S":"s"}},"v":{{"S":"{v}"}}{extra}}}}}"#
    )
}

pub(crate) fn key_body(k: &str, consistent: bool) -> String {
    let cr = if consistent {
        r#","ConsistentRead":true"#
    } else {
        ""
    };
    format!(r#"{{"TableName":"{TABLE}","Key":{{"pk":{{"S":"{k}"}},"sk":{{"S":"s"}}}}{cr}}}"#)
}

pub(crate) fn create_body(region: &str) -> String {
    format!(
        r#"{{"TableName":"{TABLE}","ReplicaUpdates":[{{"Create":{{"RegionName":"{region}"}}}}]}}"#
    )
}

pub(crate) fn delete_body(region: &str) -> String {
    format!(
        r#"{{"TableName":"{TABLE}","ReplicaUpdates":[{{"Delete":{{"RegionName":"{region}"}}}}]}}"#
    )
}

impl S {
    /// `gate`: finalize both clusters to the MREC gate. Table on A only.
    pub(crate) fn new(seed: u64, gate: bool) -> S {
        let mut w = SimWorld::new(seed, 2, 3, 3);
        if gate {
            for c in [A, B] {
                open_mrec_gate(&mut w.clusters[c], seed);
            }
        }
        w.clusters[A].create_table(TABLE);
        w.sync_clocks();
        w.bridge().set_default_link(LinkConfig::new(LAT));
        // Sender view: peer index == the bridge's cluster index (hence `pad`).
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

    pub(crate) fn call(&mut self, c: usize, op: &str, body: &str) -> (u16, String) {
        let n = self.up(c);
        self.w.dynamo(c, n, op, body)
    }

    /// Crash node `n` of cluster `c`; peer traffic and calls move to a live node.
    pub(crate) fn crash(&mut self, c: usize, n: u64) {
        self.w.clusters[c].crash(n);
        self.down.push((c, n as usize));
        let up = self.up(c);
        self.entry[c].store(up, std::sync::atomic::Ordering::SeqCst);
    }

    /// The first node of cluster `c` that is not crashed.
    pub(crate) fn up(&self, c: usize) -> u64 {
        (0..NODES)
            .find(|n| !self.down.contains(&(c, *n as usize)))
            .expect("a live node")
    }

    pub(crate) fn ok(&mut self, c: usize, op: &str, body: &str) -> String {
        let (s, r) = self.call(c, op, body);
        assert_eq!(s, 200, "seed={}: {op} on {c}: {r}", self.seed);
        r
    }

    pub(crate) fn put(&mut self, c: usize, k: &str, v: &str) {
        self.ok(c, "PutItem", &item_body(k, v, ""));
    }

    pub(crate) fn read(&mut self, c: usize, k: &str) -> Option<String> {
        let (s, r) = self.call(c, "GetItem", &key_body(k, true));
        if s != 200 {
            return None; // table not there (yet)
        }
        let v: serde_json::Value = serde_json::from_str(&r).expect("json");
        v["Item"]["v"]["S"].as_str().map(str::to_owned)
    }

    /// One saga + shipper pass of cluster `c` on every node.
    pub(crate) fn step(&mut self, c: usize) {
        for node in 0..NODES {
            if self.down.contains(&(c, node as usize)) {
                continue;
            }
            let mut ctx = self.w.clusters[c].handle().node_ctx(node);
            ctx.mrec = self.cfgs[c].clone();
            let client = self.w.peer_client(c);
            let seed = self.seed;
            self.w
                .drive(c, node, Duration::from_secs(90), async move {
                    let _ = mrec_saga_table(&ctx, TABLE, &client as &dyn PeerClient).await;
                    let _ = mrec_ship_table(&ctx, TABLE, &client as &dyn PeerClient).await;
                })
                .unwrap_or_else(|| panic!("seed={seed}: step on {c}/{node} did not finish"));
        }
        self.w.run_for(Duration::from_millis(250));
    }

    pub(crate) fn step_both(&mut self) {
        self.step(A);
        self.step(B);
    }

    pub(crate) fn until(&mut self, what: &str, cond: impl Fn(&mut S) -> bool) {
        for _ in 0..240 {
            if cond(self) {
                return;
            }
            self.step_both();
        }
        let (ra, rb) = (self.spec_replicas(A), self.spec_replicas(B));
        let n = self.up(A);
        let copied = self.w.clusters[A].metadata(n).table_global(TABLE).map(|g| {
            g.replicas
                .iter()
                .map(|r| r.copied.clone())
                .collect::<Vec<_>>()
        });
        panic!(
            "seed={}: timed out waiting for {what}; A spec {ra:?} copied {copied:?}; B spec {rb:?}",
            self.seed
        );
    }

    /// `region -> status` from `DescribeTable` on cluster `c` (empty when the
    /// table is absent or carries no replicas).
    pub(crate) fn replicas(&mut self, c: usize) -> BTreeMap<String, String> {
        let (s, r) = self.call(c, "DescribeTable", &format!(r#"{{"TableName":"{TABLE}"}}"#));
        let mut out = BTreeMap::new();
        if s != 200 {
            return out;
        }
        let v: serde_json::Value = serde_json::from_str(&r).expect("json");
        if let Some(list) = v["Table"]["Replicas"].as_array() {
            for e in list {
                out.insert(
                    e["RegionName"].as_str().unwrap_or("").to_owned(),
                    e["ReplicaStatus"].as_str().unwrap_or("").to_owned(),
                );
            }
        }
        out
    }

    pub(crate) fn both_active(&mut self) -> bool {
        let want: BTreeMap<String, String> = [("a", "ACTIVE"), ("b", "ACTIVE")]
            .iter()
            .map(|(a, b)| ((*a).to_owned(), (*b).to_owned()))
            .collect();
        self.replicas(A) == want && self.replicas(B) == want
    }

    pub(crate) fn spec_replicas(&mut self, c: usize) -> Vec<(String, MrecReplicaStatus)> {
        let n = self.up(c);
        self.w.clusters[c]
            .metadata(n)
            .table_global(TABLE)
            .map(|g| {
                g.replicas
                    .iter()
                    .map(|r| (r.region.clone(), r.status))
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(crate) fn make_active(&mut self, pre: usize) {
        for i in 0..pre {
            self.put(A, &format!("pre{i:02}"), "old");
        }
        self.ok(A, "UpdateTable", &create_body("b"));
        self.until("both sides ACTIVE", |s| s.both_active());
    }
}

#[test]
fn create_replica_on_a_populated_table_copies_it_and_goes_active() {
    for seed in saga_seeds() {
        let mut s = S::new(seed, true);
        for i in 0..6 {
            s.put(A, &format!("pre{i:02}"), "old");
        }
        let r = s.ok(A, "UpdateTable", &create_body("b"));
        let v: serde_json::Value = serde_json::from_str(&r).expect("json");
        assert_eq!(
            v["TableDescription"]["MultiRegionConsistency"], "EVENTUAL",
            "seed={seed}: {r}"
        );
        // Synchronous and local: the new peer is CREATING right after the call.
        assert_eq!(
            s.replicas(A).get("b").map(String::as_str),
            Some("CREATING"),
            "seed={seed}"
        );
        s.until("both sides ACTIVE", |s| s.both_active());
        for i in 0..6 {
            assert_eq!(
                s.read(B, &format!("pre{i:02}")).as_deref(),
                Some("old"),
                "seed={seed}"
            );
        }
        // Both directions converge.
        s.put(A, "ka", "from-a");
        s.put(B, "kb", "from-b");
        s.until("both writes everywhere", |s| {
            s.read(B, "ka").as_deref() == Some("from-a")
                && s.read(A, "kb").as_deref() == Some("from-b")
        });
        // A same-key race resolves to one winner on both sides.
        s.put(A, "race", "a-wins?");
        s.put(B, "race", "b-wins?");
        s.until("the race converged", |s| {
            let (x, y) = (s.read(A, "race"), s.read(B, "race"));
            x.is_some() && x == y
        });
        let g = s.w.clusters[A].metadata(0).table_stream(TABLE).cloned();
        assert!(g.is_some(), "seed={seed}: the stream was forced on");
    }
}

#[test]
fn a_transaction_and_a_ttl_delete_replicate() {
    for seed in saga_seeds() {
        let mut s = S::new(seed, true);
        s.ok(
            A,
            "UpdateTimeToLive",
            &format!(
                r#"{{"TableName":"{TABLE}","TimeToLiveSpecification":{{"Enabled":true,"AttributeName":"ttl"}}}}"#
            ),
        );
        s.make_active(0);
        // A region-local transaction (V14).
        let txn = format!(
            r#"{{"TransactItems":[{{"Put":{{"TableName":"{TABLE}","Item":{{"pk":{{"S":"t1"}},"sk":{{"S":"s"}},"v":{{"S":"x"}}}}}}}},{{"Put":{{"TableName":"{TABLE}","Item":{{"pk":{{"S":"t2"}},"sk":{{"S":"s"}},"v":{{"S":"y"}}}}}}}}]}}"#
        );
        s.ok(A, "TransactWriteItems", &txn);
        s.until("the transaction on B", |s| {
            s.read(B, "t1").as_deref() == Some("x") && s.read(B, "t2").as_deref() == Some("y")
        });
        // TTL: an item expiring ~40 virtual seconds on ships first, then
        // A's own reaper loop deletes it and the delete reaches B.
        s.ok(
            A,
            "PutItem",
            &item_body("gone", "soon", r#","ttl":{"N":"1577836870"}"#),
        );
        s.until("the item on B", |s| s.read(B, "gone").is_some());
        for _ in 0..10 {
            if s.read(B, "gone").is_none() {
                break;
            }
            s.w.run_for(Duration::from_secs(10));
            s.step_both();
        }
        s.until("the TTL delete on B", |s| s.read(B, "gone").is_none());
        assert_eq!(s.read(A, "gone"), None, "seed={seed}");
    }
}

#[test]
fn deleting_a_replica_stops_shipping_and_leaves_the_peer_standalone() {
    for seed in saga_seeds() {
        let mut s = S::new(seed, true);
        s.make_active(3);
        s.ok(A, "UpdateTable", &delete_body("b"));
        s.until("both sides standalone", |s| {
            s.spec_replicas(A).iter().all(|(r, _)| r == "a")
                && s.spec_replicas(B).iter().all(|(r, _)| r == "b")
        });
        assert!(
            s.replicas(A).is_empty() && s.replicas(B).is_empty(),
            "seed={seed}"
        );
        // B keeps what it had and is a working standalone table.
        assert_eq!(s.read(B, "pre00").as_deref(), Some("old"), "seed={seed}");
        s.put(A, "after", "x");
        for _ in 0..8 {
            s.step_both();
        }
        assert_eq!(
            s.read(B, "after"),
            None,
            "seed={seed}: shipped after Delete"
        );
        s.put(B, "b-only", "y");
        for _ in 0..8 {
            s.step_both();
        }
        assert_eq!(s.read(A, "b-only"), None, "seed={seed}");
        // The pair can be re-created.
        s.ok(A, "UpdateTable", &create_body("b"));
        s.until("ACTIVE again", |s| s.both_active());
        s.until("the missed row copied", |s| {
            s.read(B, "after").as_deref() == Some("x")
        });
    }
}

#[test]
fn validation_errors_are_exact() {
    let seed = 0x4A11_0001;
    let mut s = S::new(seed, true);
    let msg = |r: &str| -> String {
        serde_json::from_str::<serde_json::Value>(r).expect("json")["message"]
            .as_str()
            .unwrap_or("")
            .to_owned()
    };
    let (st, r) = s.call(A, "UpdateTable", &create_body("zz"));
    assert_eq!(st, 400, "{r}");
    assert_eq!(
        msg(&r),
        "UpdateTable: Region `zz` is not a configured peer of this cluster (configured peers: pad, b)"
    );
    let (st, r) = s.call(A, "UpdateTable", &create_body("a"));
    assert_eq!(st, 400, "{r}");
    assert_eq!(
        msg(&r),
        "UpdateTable: Region `a` is this cluster's own Region and cannot be named in \
         ReplicaUpdates Create"
    );
    let body = format!(
        r#"{{"TableName":"{TABLE}","ReplicaUpdates":[{{"Create":{{"RegionName":"b","KMSMasterKeyId":"k"}}}}]}}"#
    );
    let (st, r) = s.call(A, "UpdateTable", &body);
    assert_eq!(st, 400, "{r}");
    assert_eq!(
        msg(&r),
        "UpdateTable: ReplicaUpdates Create does not support `KMSMasterKeyId` (per-replica \
         overrides are not supported)"
    );
    let (st, r) = s.call(A, "UpdateTable", &delete_body("b"));
    assert_eq!(st, 400, "{r}");
    assert_eq!(
        msg(&r),
        "UpdateTable: table `tbl` is not a multi-Region eventually consistent table, so it has \
         no replica to delete"
    );
    s.make_active(0);
    let (st, r) = s.call(A, "UpdateTable", &create_body("b"));
    assert_eq!(st, 400, "{r}");
    assert_eq!(
        msg(&r),
        "UpdateTable: table `tbl` already has a replica in Region `b`"
    );
    let (st, r) = s.call(A, "UpdateTable", &delete_body("zz"));
    assert_eq!(st, 400, "{r}");
    assert_eq!(
        msg(&r),
        "UpdateTable: Region `zz` is not a replica of table `tbl`"
    );
    // V13: the stream cannot be turned off under a replicated table.
    let (st, r) = s.call(
        A,
        "UpdateTable",
        &format!(r#"{{"TableName":"{TABLE}","StreamSpecification":{{"StreamEnabled":false}}}}"#),
    );
    assert_eq!(st, 400, "{r}");
    assert!(msg(&r).contains("cannot be disabled"), "{r}");
}

#[test]
fn a_closed_gate_keeps_the_old_rejection() {
    for seed in [0x4A22_0001u64, 0x4A22_0002] {
        let mut s = S::new(seed, false);
        let body = format!(
            r#"{{"TableName":"{TABLE}","ReplicaUpdates":[{{"Create":{{"RegionName":"b"}}}}],"MultiRegionConsistency":"EVENTUAL"}}"#
        );
        let (st, r) = s.call(A, "UpdateTable", &body);
        assert_eq!(st, 400, "seed={seed}: {r}");
        assert!(
            r.contains("UpdateTable: ReplicaUpdates is not supported"),
            "seed={seed}: {r}"
        );
        assert!(
            s.w.clusters[A].metadata(0).table_global(TABLE).is_none(),
            "seed={seed}"
        );
    }
}

#[test]
fn a_peer_table_of_another_shape_is_refused_for_good() {
    for seed in saga_seeds() {
        let mut s = S::new(seed, true);
        // B already holds `tbl` with a different key schema.
        let body = format!(
            r#"{{"TableName":"{TABLE}","AttributeDefinitions":[{{"AttributeName":"id","AttributeType":"N"}}],"KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}],"BillingMode":"PAY_PER_REQUEST"}}"#
        );
        s.ok(B, "CreateTable", &body);
        s.ok(A, "UpdateTable", &create_body("b"));
        s.until("CREATION_FAILED", |s| {
            s.replicas(A).get("b").map(String::as_str) == Some("CREATION_FAILED")
        });
    }
}

#[test]
fn the_admin_global_tables_view_reports_mrec_replicas_and_shipper_health() {
    for seed in saga_seeds() {
        let mut s = S::new(seed, true);
        s.make_active(4);
        s.put(A, "k", "v");
        s.until("the write reached b", |s| s.read(B, "k").as_deref() == Some("v"));
        s.step_both();
        let mut ctx = s.w.clusters[A].handle().node_ctx(0);
        ctx.mrec = s.cfgs[A].clone();
        let v = crate::global_tables::admin_global_tables_view(&ctx);
        let t = v["tables"]
            .as_array()
            .and_then(|a| a.iter().find(|t| t["table"] == TABLE))
            .unwrap_or_else(|| panic!("seed={seed}: no MREC row in {v}"));
        assert_eq!(t["consistency"], "EVENTUAL", "seed={seed}: {t}");
        assert_eq!(t["replica_status"]["a"], "ACTIVE", "seed={seed}: {t}");
        assert_eq!(t["replica_status"]["b"], "ACTIVE", "seed={seed}: {t}");
        let reps = t["replicas"].as_array().expect("replicas");
        assert_eq!(reps.len(), 2, "seed={seed}: {t}");
        assert!(
            reps.iter().all(|r| r["tablets_copied"].is_u64() && r["tablets_total"].is_u64()),
            "seed={seed}: {t}"
        );
        let sh = t["shippers"].as_array().expect("shippers");
        assert!(!sh.is_empty(), "seed={seed}: a shipper row per led (tablet, peer): {t}");
        assert!(
            sh.iter().all(|r| r["peer"] == "b" && r["last_error"].is_null()),
            "seed={seed}: {t}"
        );
        assert!(
            sh.iter().any(|r| r["last_ack_age_ms"].is_u64() && r["shipped_rows"].as_u64() > Some(0)),
            "seed={seed}: some tablet acked rows: {t}"
        );
    }
}
