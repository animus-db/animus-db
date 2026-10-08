//! G-01 stage G-d M4a: the MREC **shipper** tick (`mrec_shipper`) driven
//! through `SimWorld` between two real 3-node clusters, both made MREC with
//! the M1 `MetaCommand`s after finalizing to cluster version 3. The tick is
//! driven directly (`mrec_ship_table` on every node; only the tablet's leader
//! acts), the peer client is the `PeerBridge`, each side's receiver is the
//! real `handle_mrec_apply`.
//!
//! `ANIMUS_MREC_WORLD_SEEDS=K` (default 4; the ship tests below run >= 20
//! seeds on their own floor) sets the depth, `ANIMUS_SEED=<s>` replays one.

use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use animus_control::MetaCommand;
use animus_control::schema::mrec_region_id;
use animus_cp_data::hlc::HlcTimestamp;
use animus_cp_data::{KIND_CURSOR, cursor};
use animus_env::{Env, Metric};

use super::config::PeerCluster;
use super::mrec_peer::{MrecConfig, PeerClient};
use super::mrec_shipper::{ShipOutcome, cursor_tag, mrec_ship_table, scan_tag, trim_term};
use super::sim_world::{LinkConfig, PeerHandler, SimWorld};
use super::sim_world_mrec_tests::{A, B, LAT, TABLE, handler, open_mrec_gate, propose, seeds};

const NODES: u64 = 3;
const MIN_SEEDS: u64 = 20;

fn ship_seeds() -> Vec<u64> {
    let mut v = seeds();
    let mut i = 0u64;
    while (v.len() as u64) < MIN_SEEDS && std::env::var("ANIMUS_SEED").is_err() {
        v.push(0x3E00_0000 + i);
        i += 1;
    }
    v
}

fn cfg(local: &str, peers: &[&str], max_backlog: Duration) -> Arc<MrecConfig> {
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
        max_backlog,
        node_tls: None,
        health: Arc::default(),
    })
}

struct W {
    w: SimWorld,
    /// `[A's, B's]` ctx-side config (the sender view; peer index == bridge index).
    cfgs: [Arc<MrecConfig>; 2],
    seed: u64,
}

fn put_body(k: &str, v: &str) -> String {
    format!(
        r#"{{"TableName":"{TABLE}","Item":{{"pk":{{"S":"{k}"}},"sk":{{"S":"s"}},"v":{{"S":"{v}"}}}}}}"#
    )
}

fn key_body(k: &str) -> String {
    format!(
        r#"{{"TableName":"{TABLE}","Key":{{"pk":{{"S":"{k}"}},"sk":{{"S":"s"}}}},"ConsistentRead":true}}"#
    )
}

impl W {
    /// Both clusters at version 3 with `pre` rows on A **before** it becomes MREC.
    fn new(seed: u64, pre: usize, max_backlog: Duration) -> W {
        let mut w = SimWorld::new(seed, 2, 3, 3);
        for c in [A, B] {
            open_mrec_gate(&mut w.clusters[c], seed);
            w.clusters[c].create_table(TABLE);
        }
        w.sync_clocks();
        w.bridge().set_default_link(LinkConfig::new(LAT));
        let mut me = W {
            w,
            // Sender view: peer index = the bridge's cluster index.
            cfgs: [
                cfg("a", &["pad", "b"], max_backlog),
                cfg("b", &["a"], max_backlog),
            ],
            seed,
        };
        for i in 0..pre {
            me.put(A, &format!("pre{i:02}"), "old");
        }
        for (c, local, peer) in [(A, "a", "b"), (B, "b", "a")] {
            propose(
                &mut me.w.clusters[c],
                MetaCommand::ConvertTableToMrec {
                    table: TABLE.into(),
                    local_region: local.into(),
                    region_id: mrec_region_id(local),
                },
                seed,
            );
            propose(
                &mut me.w.clusters[c],
                MetaCommand::AddMrecReplica {
                    table: TABLE.into(),
                    region: peer.into(),
                    region_id: mrec_region_id(peer),
                },
                seed,
            );
        }
        me.w.run_for(Duration::from_secs(2));
        for c in [A, B] {
            let ok = (0..NODES).all(|n| {
                me.w.clusters[c]
                    .metadata(n)
                    .table_global(TABLE)
                    .is_some_and(|g| g.is_mrec() && g.replicas.len() == 2)
            });
            assert!(ok, "seed={seed}: MREC spec not replicated on cluster {c}");
        }
        me.w.sync_clocks();
        // Receivers: each side's real handler, entry node 0.
        let ha: PeerHandler = handler(
            cfg("a", &["b"], max_backlog).as_ref().clone(),
            Arc::new(AtomicU64::new(0)),
        );
        let hb: PeerHandler = handler(
            cfg("b", &["a"], max_backlog).as_ref().clone(),
            Arc::new(AtomicU64::new(0)),
        );
        me.w.set_handler(A, ha);
        me.w.set_handler(B, hb);
        me
    }

    fn put(&mut self, c: usize, k: &str, v: &str) {
        let (s, r) = self.w.dynamo(c, 0, "PutItem", &put_body(k, v));
        assert_eq!(s, 200, "seed={}: PutItem {k} on {c}: {r}", self.seed);
    }

    fn delete(&mut self, c: usize, k: &str) {
        let (s, r) = self.w.dynamo(
            c,
            0,
            "DeleteItem",
            &key_body(k).replace(r#","ConsistentRead":true"#, ""),
        );
        assert_eq!(s, 200, "seed={}: DeleteItem {k} on {c}: {r}", self.seed);
    }

    fn read(&mut self, c: usize, k: &str) -> Option<String> {
        let (s, r) = self.w.dynamo(c, 0, "GetItem", &key_body(k));
        assert_eq!(s, 200, "seed={}: GetItem {k} on {c}: {r}", self.seed);
        let v: serde_json::Value = serde_json::from_str(&r).expect("json");
        v["Item"]["v"]["S"].as_str().map(str::to_owned)
    }

    /// One tick of cluster `c`'s shipper on every node (only a leader acts).
    fn tick(&mut self, c: usize) -> ShipOutcome {
        let peer_cfg = self.cfgs[c].clone();
        let mut out = ShipOutcome::Idle;
        for node in 0..NODES {
            let mut ctx = self.w.clusters[c].handle().node_ctx(node);
            ctx.mrec = peer_cfg.clone();
            let client = self.w.peer_client(c);
            let r = self
                .w
                .drive(c, node, Duration::from_secs(8), async move {
                    mrec_ship_table(&ctx, TABLE, &client as &dyn PeerClient).await
                })
                .unwrap_or_else(|| panic!("seed={}: tick on {c}/{node} did not finish", self.seed));
            if r != ShipOutcome::Idle && out != ShipOutcome::Waiting {
                out = r;
            }
        }
        out
    }

    /// Tick `c` until two consecutive ticks are `Idle` (caught up).
    fn settle(&mut self, c: usize) {
        let mut idle = 0;
        for _ in 0..80 {
            if self.tick(c) == ShipOutcome::Idle {
                idle += 1;
                if idle >= 2 {
                    return;
                }
            } else {
                idle = 0;
            }
            self.w.run_for(Duration::from_millis(250));
        }
        panic!("seed={}: shipper on {c} never settled", self.seed);
    }

    fn shipped(&self, c: usize) -> u64 {
        self.cfgs[c]
            .health
            .lock()
            .expect("health")
            .values()
            .map(|h| h.shipped_rows)
            .sum()
    }

    /// `(watermark, scan present)` of cluster `c`'s leader tablet for `peer`.
    fn cursors(&mut self, c: usize, peer: &str) -> (Option<HlcTimestamp>, bool) {
        let (tag, stag) = (cursor_tag(peer), scan_tag(peer));
        for node in 0..NODES {
            let ctx = self.w.clusters[c].handle().node_ctx(node);
            let (tag, stag) = (tag.clone(), stag.clone());
            let r = self
                .w
                .drive(c, node, Duration::from_secs(4), async move {
                    for (_, g) in ctx.edge.hosted_groups() {
                        if !g.is_leader() {
                            continue;
                        }
                        let start = g.scope_range().start;
                        let wm = g
                            .local_get_kind(KIND_CURSOR, &cursor::cursor_key(&start, &tag))
                            .await
                            .and_then(|b| cursor::decode_watermark(&b));
                        let sc = g
                            .local_get_kind(KIND_CURSOR, &cursor::cursor_key(&start, &stag))
                            .await
                            .is_some();
                        return Some((wm, sc));
                    }
                    None
                })
                .flatten();
            if let Some(x) = r {
                return x;
            }
        }
        panic!("seed={}: no leader tablet on cluster {c}", self.seed);
    }
}

fn run_steady_and_loop_prevention(seed: u64) {
    let mut f = W::new(seed, 0, Duration::from_secs(3600));
    f.settle(A); // empty initial scan finishes
    f.settle(B);
    for i in 0..5 {
        f.put(A, &format!("k{i}"), "v1");
    }
    f.settle(A);
    for i in 0..5 {
        assert_eq!(
            f.read(B, &format!("k{i}")).as_deref(),
            Some("v1"),
            "seed={seed}"
        );
    }
    f.put(A, "k0", "v2");
    f.delete(A, "k1");
    f.settle(A);
    assert_eq!(f.read(B, "k0").as_deref(), Some("v2"), "seed={seed}");
    assert_eq!(f.read(B, "k1"), None, "seed={seed}: tombstone shipped");
    let a_shipped = f.shipped(A);
    assert!(a_shipped >= 7, "seed={seed}: A shipped {a_shipped}");

    // Loop prevention: B holds A's rows (stamped `a`); B must ship none back.
    f.settle(B);
    assert_eq!(
        f.shipped(B),
        0,
        "seed={seed}: B shipped back rows A originated"
    );
    // B's own write goes to A, and A then does not echo it.
    f.put(B, "kb", "from-b");
    f.settle(B);
    assert_eq!(f.read(A, "kb").as_deref(), Some("from-b"), "seed={seed}");
    assert_eq!(f.shipped(B), 1, "seed={seed}");
    f.settle(A);
    assert_eq!(
        f.shipped(A),
        a_shipped,
        "seed={seed}: A echoed B's row back"
    );
}

#[test]
fn steady_state_ships_a_to_b_and_never_loops_back() {
    for seed in ship_seeds() {
        run_steady_and_loop_prevention(seed);
    }
}

fn run_scan_initial_copy(seed: u64) {
    let mut f = W::new(seed, 12, Duration::from_secs(3600));
    f.settle(A);
    for i in 0..12 {
        assert_eq!(
            f.read(B, &format!("pre{i:02}")).as_deref(),
            Some("old"),
            "seed={seed}"
        );
    }
    let (wm, scanning) = f.cursors(A, "b");
    assert!(
        wm.is_some() && !scanning,
        "seed={seed}: scan cursor must be gone"
    );
    // The log mode resumes: a later write ships.
    f.put(A, "post", "new");
    f.settle(A);
    assert_eq!(f.read(B, "post").as_deref(), Some("new"), "seed={seed}");
    // The initial copy was marked in the catalog.
    f.w.run_for(Duration::from_secs(1));
    let copied = f.w.clusters[A]
        .metadata(0)
        .table_global(TABLE)
        .is_some_and(|g| {
            g.replicas
                .iter()
                .any(|r| r.region == "b" && !r.copied.is_empty())
        });
    assert!(copied, "seed={seed}: MarkMrecCopied not applied");
}

#[test]
fn scan_mode_copies_pre_existing_rows_then_resumes_the_log() {
    for seed in ship_seeds() {
        run_scan_initial_copy(seed);
    }
}

fn run_cursor_after_ack(seed: u64) {
    let mut f = W::new(seed, 0, Duration::from_secs(3600));
    f.settle(A);
    let (wm0, _) = f.cursors(A, "b");
    for i in 0..4 {
        f.put(A, &format!("c{i}"), "v");
    }
    f.w.bridge().partition(A, B);
    let r = f.tick(A);
    assert_eq!(r, ShipOutcome::Waiting, "seed={seed}: unreachable peer");
    let (wm1, _) = f.cursors(A, "b");
    assert_eq!(wm0, wm1, "seed={seed}: cursor advanced without an ack");
    assert_eq!(f.read(B, "c0"), None, "seed={seed}");
    f.w.bridge().heal();
    f.w.run_for(Duration::from_secs(8)); // backoff elapses
    f.settle(A);
    let (wm2, _) = f.cursors(A, "b");
    assert!(
        wm2 > wm1,
        "seed={seed}: cursor did not advance after the ack"
    );
    for i in 0..4 {
        assert_eq!(
            f.read(B, &format!("c{i}")).as_deref(),
            Some("v"),
            "seed={seed}"
        );
    }
}

#[test]
fn the_cursor_advances_only_after_the_peer_acknowledged() {
    for seed in ship_seeds() {
        run_cursor_after_ack(seed);
    }
}

fn run_backlog_cap_resync(seed: u64) {
    let mut f = W::new(seed, 0, Duration::from_secs(5));
    f.settle(A);
    f.put(A, "before", "v");
    f.settle(A);
    f.w.bridge().partition(A, B);
    f.put(A, "during", "v");
    // Past the retention cap with the peer unreachable: the janitor term drops
    // the cursor (resync) instead of holding the log.
    f.w.run_for(Duration::from_secs(8));
    let cfg_a = f.cfgs[A].clone();
    let mut held = Some(0u64);
    for node in 0..NODES {
        let ctx = f.w.clusters[A].handle().node_ctx(node);
        let cfg_a = cfg_a.clone();
        let r =
            f.w.drive(A, node, Duration::from_secs(4), async move {
                for (_, g) in ctx.edge.hosted_groups() {
                    if g.is_leader() {
                        let meta = ctx.effective_metadata();
                        return Some(trim_term(&ctx.env, &cfg_a, &meta, TABLE, &g).await);
                    }
                }
                None
            })
            .flatten();
        if let Some(t) = r {
            held = t;
        }
    }
    assert_eq!(
        held, None,
        "seed={seed}: a capped-out peer must not hold the log"
    );
    let (wm, _) = f.cursors(A, "b");
    assert!(wm.is_none(), "seed={seed}: the cursor must be dropped");
    f.w.bridge().heal();
    f.w.run_for(Duration::from_secs(8));
    f.settle(A);
    assert_eq!(
        f.read(B, "during").as_deref(),
        Some("v"),
        "seed={seed}: resync scan"
    );
    assert_eq!(f.read(B, "before").as_deref(), Some("v"), "seed={seed}");
    let resyncs: u64 = (0..NODES)
        .map(|n| {
            f.w.clusters[A]
                .handle()
                .env(n)
                .metrics()
                .get(Metric::MrecResyncTotal)
        })
        .sum();
    assert!(resyncs >= 1, "seed={seed}: resync metric");
}

#[test]
fn past_the_backlog_cap_the_cursor_is_dropped_and_the_peer_resynced_by_scan() {
    for seed in ship_seeds() {
        run_backlog_cap_resync(seed);
    }
}

#[test]
fn a_cluster_below_the_gate_emits_nothing() {
    let seed = seeds()[0];
    let mut w = SimWorld::new(seed, 2, 3, 3);
    w.clusters[A].create_table(TABLE);
    w.sync_clocks();
    w.bridge().set_default_link(LinkConfig::new(LAT));
    let ctx = {
        let mut c = w.clusters[A].handle().node_ctx(0);
        c.mrec = cfg("a", &["pad", "b"], Duration::from_secs(3600));
        c
    };
    let client = w.peer_client(A);
    let r = w
        .drive(A, 0, Duration::from_secs(4), async move {
            mrec_ship_table(&ctx, TABLE, &client as &dyn PeerClient).await
        })
        .expect("tick");
    assert_eq!(r, ShipOutcome::Idle);
    assert!(w.bridge_log().is_empty(), "nothing may cross the WAN");
}
