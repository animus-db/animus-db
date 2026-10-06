//! G-01 stage G-d M3: the MREC **receiver** driven through `SimWorld`'s
//! `PeerBridge` (cluster A is the sender, cluster B the receiver).
//!
//! Both clusters are 3-node SimClusters finalized to cluster version 3 through
//! the real admin path; B's table is made MREC directly with the M1
//! `MetaCommand`s (no wire surface exists until M4), and B's receiver is the
//! very `mrec_receiver::handle_mrec_apply` a real node serves, run as the
//! `PeerBridge` handler on a chosen entry node (so a non-leader entry exercises
//! the normal hinted-forward leader path).
//!
//! Cases per seed: LWW (fresh, newer wins, older Superseded, equal re-delivery
//! Superseded), tombstone no-resurrection, skew rejected then accepted once
//! local time catches up, partition then heal, duplicated delivery, a
//! follower-connected (forwarding) entry, a table split into two tablets
//! (routing by the receiver's own layout), a randomized permutation/duplication
//! run against a model, refusals (gate, TLS policy, unknown peer, unknown
//! table, malformed record), and the two M2-left writer guards. A negative
//! control (`>=` instead of `>` is M2's; here: skew bound removed) shows the
//! skew assertion can fail.
//!
//! `ANIMUS_MREC_WORLD_SEEDS=K` sets the depth (default 4); `ANIMUS_SEED=<s>`
//! replays one seed.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use animus_control::schema::mrec_region_id;
use animus_control::version::{Gate, VersionRange};
use animus_control::{MetaCommand, ProposeResult};
use animus_dynamo::AttributeValue;
use animus_env::Metric;
use animus_item::MrecVersion;
use animus_node::{
    MREC_PROTO, MrecAnswer, MrecApplyRequest, MrecApplyResponse, MrecRecord, TxnTableWrite,
};

use super::config::PeerCluster;
use super::mrec_peer::{MrecConfig, PeerClient, PeerError};
use super::mrec_receiver::handle_mrec_apply;
use super::sim_cluster::SimCluster;
use super::sim_world::{LinkConfig, PeerHandler, SimWorld};

pub(crate) const A: usize = 0;
pub(crate) const B: usize = 1;
pub(crate) const TABLE: &str = "tbl";
pub(crate) const LAT: Duration = Duration::from_millis(40);
pub(crate) const TIMEOUT: Duration = Duration::from_millis(800);
pub(crate) const SKEW_MS: u64 = 500;

pub(crate) fn seeds() -> Vec<u64> {
    if let Some(s) = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        return vec![s];
    }
    let k: u64 = std::env::var("ANIMUS_MREC_WORLD_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4);
    (0..k).map(|i| 0x3D00_0000 + i).collect()
}

pub(crate) fn splitmix(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

// ---------------------------------------------------------------------------
// Setup

pub(crate) fn poll(
    c: &mut SimCluster,
    what: &str,
    seed: u64,
    cond: impl Fn(&mut SimCluster) -> bool,
) {
    for _ in 0..600 {
        if cond(c) {
            return;
        }
        c.run_for(Duration::from_millis(100));
    }
    panic!("seed={seed}: timed out waiting for {what}");
}

/// Era on, every node reporting `[1,3]`, then the real admin finalize 1 -> 2 -> 3.
pub(crate) fn open_mrec_gate(c: &mut SimCluster, seed: u64) {
    let n = c.node_count() as u64;
    c.set_all_node_versions(Some(VersionRange::new(1, 3)));
    poll(c, "the era and every version record", seed, |c| {
        (0..n)
            .all(|i| c.features(i).era_active() && c.metadata(i).node_versions.len() == n as usize)
    });
    for (expected, target) in [(1u32, 2u32), (2, 3)] {
        let idx = c.control_leader_index();
        let leader = c.control_node_id(idx);
        let body = format!(r#"{{"to":{target},"expected":{expected}}}"#);
        let (status, v) = c.admin(
            leader,
            "POST",
            "/admin/cluster-version/finalize",
            "",
            body.as_bytes(),
        );
        assert_eq!(
            status, 200,
            "seed={seed}: finalize {expected}->{target}: {v}"
        );
        poll(c, "the new cluster version on every node", seed, |c| {
            (0..n).all(|i| c.features(i).cluster_version() == target)
        });
    }
    assert!((0..n).all(|i| c.features(i).is_open(Gate::MrecReplication)));
}

pub(crate) fn propose(c: &mut SimCluster, cmd: MetaCommand, seed: u64) {
    match c.propose_meta(cmd.clone()) {
        ProposeResult::Accepted { .. } => {}
        other => panic!("seed={seed}: {cmd:?} not accepted: {other:?}"),
    }
}

/// B's table becomes MREC with A as its peer (the M1 commands, post-finalize).
fn make_mrec(c: &mut SimCluster, seed: u64) {
    c.create_table(TABLE);
    propose(
        c,
        MetaCommand::ConvertTableToMrec {
            table: TABLE.into(),
            local_region: "b".into(),
            region_id: mrec_region_id("b"),
        },
        seed,
    );
    propose(
        c,
        MetaCommand::AddMrecReplica {
            table: TABLE.into(),
            region: "a".into(),
            region_id: mrec_region_id("a"),
        },
        seed,
    );
    let n = c.node_count() as u64;
    poll(c, "the MREC spec on every node", seed, |c| {
        (0..n).all(|i| {
            c.metadata(i)
                .table_global(TABLE)
                .is_some_and(|g| g.is_mrec() && g.replicas.len() == 2)
        })
    });
}

fn receiver_cfg(allow_insecure: bool) -> MrecConfig {
    MrecConfig {
        region: Some("b".into()),
        peers: vec![PeerCluster {
            region: "a".into(),
            endpoints: vec!["a.invalid:7000".into()],
            tls_ca: None,
        }],
        allow_insecure,
        max_clock_skew_ms: SKEW_MS,
        inflight: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        max_backlog: Duration::from_secs(3600),
        node_tls: None,
        health: Arc::default(),
    }
}

/// B's `PeerHandler`: the real receiver on the entry node `entry` names.
pub(crate) fn handler(cfg: MrecConfig, entry: Arc<AtomicU64>) -> PeerHandler {
    Arc::new(move |handle, payload: Vec<u8>| {
        let cfg = cfg.clone();
        let node = entry.load(Ordering::SeqCst);
        Box::pin(async move {
            let ctx = handle.node_ctx(node);
            let resp = match serde_json::from_slice::<MrecApplyRequest>(&payload) {
                Ok(req) => handle_mrec_apply(&ctx, &cfg, req).await,
                Err(e) => MrecApplyResponse::Refused {
                    message: format!("malformed: {e}"),
                    retryable: false,
                },
            };
            serde_json::to_vec(&resp).expect("encodes")
        })
    })
}

struct Fixture {
    w: SimWorld,
    entry: Arc<AtomicU64>,
    seed: u64,
}

fn fixture(seed: u64, cfg: MrecConfig) -> Fixture {
    let mut w = SimWorld::new(seed, 2, 3, 3);
    for c in [A, B] {
        open_mrec_gate(&mut w.clusters[c], seed);
    }
    make_mrec(&mut w.clusters[B], seed);
    w.sync_clocks();
    let entry = Arc::new(AtomicU64::new(0));
    w.set_handler(B, handler(cfg, entry.clone()));
    w.bridge().set_default_link(LinkConfig::new(LAT));
    Fixture { w, entry, seed }
}

// ---------------------------------------------------------------------------
// Wire helpers

fn pk(k: &str) -> AttributeValue {
    AttributeValue::S(k.into())
}

fn sk() -> AttributeValue {
    AttributeValue::S("s".into())
}

fn item(k: &str, v: &str) -> animus_dynamo::Item {
    let mut it = BTreeMap::new();
    it.insert("pk".to_string(), pk(k));
    it.insert("sk".to_string(), sk());
    it.insert("v".to_string(), AttributeValue::S(v.into()));
    it
}

fn ver(wall_ms: u64, logical: u32) -> MrecVersion {
    MrecVersion {
        wall_ms,
        logical,
        region_id: mrec_region_id("a"),
    }
}

fn put(k: &str, v: &str, ver: MrecVersion) -> MrecRecord {
    MrecRecord {
        pk: pk(k),
        sk: Some(sk()),
        item: Some(item(k, v)),
        ver,
    }
}

fn del(k: &str, ver: MrecVersion) -> MrecRecord {
    MrecRecord {
        pk: pk(k),
        sk: Some(sk()),
        item: None,
        ver,
    }
}

fn request(records: Vec<MrecRecord>) -> MrecApplyRequest {
    MrecApplyRequest {
        proto: MREC_PROTO,
        from_region: "a".into(),
        table: TABLE.into(),
        records,
        control: None,
    }
}

impl Fixture {
    /// B's current wall clock in ms (the handler env shares its simulator).
    fn b_now_ms(&self) -> u64 {
        use animus_env::Clock;
        self.w.clusters[B].handle().env(0).wall_now().0
    }

    fn call(&mut self, req: &MrecApplyRequest) -> Result<MrecApplyResponse, PeerError> {
        let payload = serde_json::to_vec(req).expect("encodes");
        let (r, _) = self.w.peer_call(A, B, &payload, TIMEOUT);
        r.map(|bytes| serde_json::from_slice(&bytes).expect("response decodes"))
    }

    fn answers(&mut self, records: Vec<MrecRecord>) -> Vec<MrecAnswer> {
        match self.call(&request(records)).expect("delivered") {
            MrecApplyResponse::Answers(a) => a,
            other => panic!("seed={}: refused: {other:?}", self.seed),
        }
    }

    /// B's value of `k` through its own DynamoDB wire (strongly consistent).
    fn read(&mut self, k: &str) -> Option<String> {
        let body = format!(
            r#"{{"TableName":"{TABLE}","Key":{{"pk":{{"S":"{k}"}},"sk":{{"S":"s"}}}},"ConsistentRead":true}}"#
        );
        let (s, r) = self.w.dynamo(B, 0, "GetItem", &body);
        assert_eq!(s, 200, "seed={}: GetItem {k}: {r}", self.seed);
        let v: serde_json::Value = serde_json::from_str(&r).expect("json");
        v["Item"]["v"]["S"].as_str().map(str::to_owned)
    }

    fn skew_rejections(&self, node: u64) -> u64 {
        use animus_env::Env;
        self.w.clusters[B]
            .handle()
            .env(node)
            .metrics()
            .get(Metric::MrecSkewRejectedTotal)
    }
}

// ---------------------------------------------------------------------------
// Cases

fn run_lww_and_tombstones(seed: u64) {
    let mut f = fixture(seed, receiver_cfg(true));
    let t = f.b_now_ms().saturating_sub(1_000);

    // Fresh, then newer wins.
    assert_eq!(
        f.answers(vec![put("k", "v1", ver(t, 0))]),
        [MrecAnswer::Applied]
    );
    assert_eq!(f.read("k").as_deref(), Some("v1"), "seed={seed}");
    assert_eq!(
        f.answers(vec![put("k", "v2", ver(t + 10, 0))]),
        [MrecAnswer::Applied]
    );
    assert_eq!(f.read("k").as_deref(), Some("v2"));
    // Older: Superseded, value unchanged. Equal stamp (idempotent re-delivery):
    // Superseded too. A same-ms tie from a *lower* region id would also lose,
    // but only A's stamps are accepted from A, so the tie-break is M2's proptest.
    assert_eq!(
        f.answers(vec![put("k", "old", ver(t + 5, 0))]),
        [MrecAnswer::Superseded]
    );
    assert_eq!(
        f.answers(vec![put("k", "v2", ver(t + 10, 0))]),
        [MrecAnswer::Superseded]
    );
    assert_eq!(
        f.answers(vec![put("k", "x", ver(t + 10, 0))]),
        [MrecAnswer::Superseded]
    );
    assert_eq!(f.read("k").as_deref(), Some("v2"));
    // Within one batch: ascending both apply, descending loses the older.
    assert_eq!(
        f.answers(vec![
            put("b", "lo", ver(t, 1)),
            put("b", "hi", ver(t + 1, 0)),
            put("b", "lower", ver(t, 5)),
        ]),
        [
            MrecAnswer::Applied,
            MrecAnswer::Applied,
            MrecAnswer::Superseded
        ]
    );
    assert_eq!(f.read("b").as_deref(), Some("hi"));

    // Tombstone: applies, hides the item, and an older put cannot resurrect it.
    assert_eq!(
        f.answers(vec![del("k", ver(t + 20, 0))]),
        [MrecAnswer::Applied]
    );
    assert_eq!(f.read("k"), None, "seed={seed}: deleted");
    assert_eq!(
        f.answers(vec![put("k", "zombie", ver(t + 15, 0))]),
        [MrecAnswer::Superseded]
    );
    assert_eq!(
        f.read("k"),
        None,
        "seed={seed}: no resurrection by an older write"
    );
    assert_eq!(
        f.answers(vec![del("k", ver(t + 20, 0))]),
        [MrecAnswer::Superseded]
    );
    // A genuinely newer put re-creates it.
    assert_eq!(
        f.answers(vec![put("k", "back", ver(t + 30, 0))]),
        [MrecAnswer::Applied]
    );
    assert_eq!(f.read("k").as_deref(), Some("back"));
    // A delete of a never-seen key is a real (tombstone) write: it blocks an
    // older put arriving later (reordering across the WAN).
    assert_eq!(
        f.answers(vec![del("ghost", ver(t + 50, 0))]),
        [MrecAnswer::Applied]
    );
    assert_eq!(
        f.answers(vec![put("ghost", "late", ver(t + 40, 0))]),
        [MrecAnswer::Superseded]
    );
    assert_eq!(f.read("ghost"), None);
}

#[test]
fn lww_newer_wins_older_and_equal_are_superseded_tombstones_never_resurrect() {
    for seed in seeds() {
        run_lww_and_tombstones(seed);
    }
}

fn run_skew(seed: u64) {
    let mut f = fixture(seed, receiver_cfg(true));
    let before = f.skew_rejections(0);
    let ahead = f.b_now_ms() + 3_000;
    // Far-future stamp: Retry (never applied), counted, the key untouched.
    assert_eq!(
        f.answers(vec![put("s", "future", ver(ahead, 0))]),
        [MrecAnswer::Retry]
    );
    assert_eq!(f.read("s"), None);
    assert!(
        f.skew_rejections(0) > before,
        "seed={seed}: mrec_skew_rejected_total"
    );
    // Within the bound is accepted straight away.
    let near = f.b_now_ms() + SKEW_MS / 2;
    assert_eq!(
        f.answers(vec![put("n", "near", ver(near, 0))]),
        [MrecAnswer::Applied]
    );
    // Once local time catches up the very same record is accepted.
    f.w.run_for(Duration::from_secs(3));
    assert_eq!(
        f.answers(vec![put("s", "future", ver(ahead, 0))]),
        [MrecAnswer::Applied]
    );
    assert_eq!(f.read("s").as_deref(), Some("future"), "seed={seed}");
    // Mixed batch: one skewed record does not hold back its siblings.
    let far = f.b_now_ms() + 60_000;
    let t = f.b_now_ms();
    assert_eq!(
        f.answers(vec![
            put("m1", "a", ver(t, 0)),
            put("m2", "b", ver(far, 0)),
            put("m3", "c", ver(t, 0))
        ]),
        [MrecAnswer::Applied, MrecAnswer::Retry, MrecAnswer::Applied]
    );
    assert_eq!(f.read("m2"), None);
}

#[test]
fn a_far_future_stamp_is_retried_until_local_time_catches_up() {
    for seed in seeds() {
        run_skew(seed);
    }
}

/// Negative control: with the skew bound removed (a huge limit) the same
/// far-future stamp is applied at once, so the skew test above can fail.
#[test]
fn negative_control_without_the_skew_bound_a_future_stamp_wins() {
    let seed = seeds()[0];
    let mut cfg = receiver_cfg(true);
    cfg.max_clock_skew_ms = u64::MAX / 4;
    let mut f = fixture(seed, cfg);
    let ahead = f.b_now_ms() + 3_000_000;
    assert_eq!(
        f.answers(vec![put("s", "future", ver(ahead, 0))]),
        [MrecAnswer::Applied]
    );
    // ... and now every honest write to the key loses for ~35 days.
    let honest = f.b_now_ms();
    assert_eq!(
        f.answers(vec![put("s", "honest", ver(honest, 0))]),
        [MrecAnswer::Superseded]
    );
}

fn run_partition_heal_and_duplication(seed: u64) {
    let mut f = fixture(seed, receiver_cfg(true));
    let t = f.b_now_ms().saturating_sub(1_000);
    // Partition: the call times out, nothing applied.
    f.w.bridge().partition(A, B);
    let r = f.call(&request(vec![put("p", "cut", ver(t, 0))]));
    assert_eq!(r, Err(PeerError::Timeout), "seed={seed}");
    f.w.run_for(Duration::from_millis(300));
    assert_eq!(
        f.read("p"),
        None,
        "seed={seed}: nothing applied across a partition"
    );
    // One-way (A->B ok, B->A cut): the request is applied but the sender sees a
    // timeout; resending is safe (Superseded) and converges.
    f.w.bridge().heal();
    f.w.bridge().partition_one_way(B, A);
    let r = f.call(&request(vec![put("p", "cut", ver(t, 0))]));
    assert_eq!(r, Err(PeerError::Timeout), "seed={seed}: response lost");
    f.w.bridge().heal();
    assert_eq!(
        f.read("p").as_deref(),
        Some("cut"),
        "seed={seed}: applied despite the lost ack"
    );
    assert_eq!(
        f.answers(vec![put("p", "cut", ver(t, 0))]),
        [MrecAnswer::Superseded]
    );

    // Every delivery duplicated (request and response).
    f.w.bridge().set_default_link(LinkConfig {
        dup_permille: 1000,
        ..LinkConfig::new(LAT)
    });
    let a = f.answers(vec![
        put("d", "once", ver(t, 0)),
        put("e", "once", ver(t, 0)),
    ]);
    // The two deliveries of the request race: the sender sees whichever answer
    // arrives first (`Applied` from the winner, or `Superseded` from the copy
    // that ran after it) — both mean "committed", never a second application.
    assert!(
        a.iter()
            .all(|x| matches!(x, MrecAnswer::Applied | MrecAnswer::Superseded)),
        "seed={seed}: {a:?}"
    );
    f.w.run_for(Duration::from_secs(1));
    assert_eq!(f.read("d").as_deref(), Some("once"));
    assert_eq!(f.read("e").as_deref(), Some("once"));
    let dup =
        f.w.bridge_log()
            .iter()
            .filter(|l| l.contains("delivered") && l.contains("req "))
            .count();
    assert!(
        dup >= 2,
        "seed={seed}: the duplicate request really was delivered twice"
    );
    f.w.bridge().set_default_link(LinkConfig::new(LAT));
    assert_eq!(
        f.answers(vec![put("d", "once", ver(t, 0))]),
        [MrecAnswer::Superseded]
    );
}

#[test]
fn partition_then_heal_and_duplicated_delivery_converge_idempotently() {
    for seed in seeds() {
        run_partition_heal_and_duplication(seed);
    }
}

fn run_follower_entry(seed: u64) {
    let mut f = fixture(seed, receiver_cfg(true));
    let tablet = f.w.clusters[B].tablet_of(TABLE).expect("tablet");
    let leader = f.w.clusters[B]
        .handle()
        .leader_index_of(tablet)
        .expect("leader");
    // Enter through a node that does not lead the tablet: the receiver routes
    // through the normal hinted-forward path (a `KindWriteBatch` with
    // replicate ops inside `Forwarded`), gate content-checked on the way.
    let follower = (leader + 1) % 3;
    f.entry.store(follower, Ordering::SeqCst);
    let t = f.b_now_ms().saturating_sub(1_000);
    assert_eq!(
        f.answers(vec![put("fw", "via-follower", ver(t, 0))]),
        [MrecAnswer::Applied]
    );
    assert_eq!(f.read("fw").as_deref(), Some("via-follower"), "seed={seed}");
    assert_eq!(
        f.answers(vec![put("fw", "older", ver(t - 1, 0))]),
        [MrecAnswer::Superseded]
    );
    assert_eq!(
        f.answers(vec![del("fw", ver(t + 1, 0))]),
        [MrecAnswer::Applied]
    );
    assert_eq!(f.read("fw"), None);
    // Every entry node behaves identically.
    for node in 0..3u64 {
        f.entry.store(node, Ordering::SeqCst);
        let k = format!("n{node}");
        assert_eq!(
            f.answers(vec![put(&k, "x", ver(t + 100 + node, 0))]),
            [MrecAnswer::Applied],
            "seed={seed} entry {node}"
        );
        assert_eq!(f.read(&k).as_deref(), Some("x"));
    }
}

#[test]
fn a_follower_connected_entry_routes_to_the_leader_and_applies() {
    for seed in seeds() {
        run_follower_entry(seed);
    }
}

/// A randomized permutation/duplication run against a model: records for a few
/// keys with random stamps (ties across batches, deletes), shipped in random
/// batches with random re-sends. Final state equals the max-stamp record per
/// key, and the per-record answers are exactly `Applied` iff it beat the
/// stored stamp at that moment.
fn run_model(seed: u64) {
    let mut f = fixture(seed, receiver_cfg(true));
    let mut rng = seed ^ 0x77AA;
    let t = f.b_now_ms().saturating_sub(5_000);
    let keys = ["m0", "m1", "m2", "m3", "m4", "m5"];
    // The stamp determines the content, so an equal stamp is an identical record.
    let mk = |k: &str, w: u64, l: u32| -> MrecRecord {
        if w.is_multiple_of(5) {
            del(k, ver(t + w, l))
        } else {
            put(k, &format!("{k}@{w}.{l}"), ver(t + w, l))
        }
    };
    let mut model: BTreeMap<&str, MrecVersion> = BTreeMap::new();
    let mut all: Vec<MrecRecord> = Vec::new();
    for _ in 0..30 {
        let k = keys[(splitmix(&mut rng) % keys.len() as u64) as usize];
        all.push(mk(
            k,
            splitmix(&mut rng) % 40,
            (splitmix(&mut rng) % 3) as u32,
        ));
    }
    // duplicate a third of them, then shuffle (Fisher-Yates) into batches
    let dups: Vec<MrecRecord> = all.iter().step_by(3).cloned().collect();
    all.extend(dups);
    for i in (1..all.len()).rev() {
        let j = (splitmix(&mut rng) % (i as u64 + 1)) as usize;
        all.swap(i, j);
    }
    for chunk in all.chunks(7) {
        let answers = f.answers(chunk.to_vec());
        for (rec, ans) in chunk.iter().zip(&answers) {
            let key = keys.iter().find(|k| pk(k) == rec.pk).copied().expect("key");
            let cur = model.get(key).copied().unwrap_or(MrecVersion::ZERO);
            let want = if rec.ver > cur {
                model.insert(key, rec.ver);
                MrecAnswer::Applied
            } else {
                MrecAnswer::Superseded
            };
            assert_eq!(ans, &want, "seed={seed}: {rec:?} against stored {cur:?}");
        }
    }
    // Final state: each key holds exactly its max-stamp record.
    for k in keys {
        let want = all
            .iter()
            .filter(|r| pk(k) == r.pk)
            .max_by_key(|r| r.ver)
            .and_then(|r| r.item.as_ref())
            .and_then(|i| match i.get("v") {
                Some(AttributeValue::S(s)) => Some(s.clone()),
                _ => None,
            });
        assert_eq!(f.read(k), want, "seed={seed}: key {k}");
    }
}

#[test]
fn a_shuffled_duplicated_stream_converges_to_the_max_stamp_per_key() {
    for seed in seeds() {
        run_model(seed);
    }
}

/// Loss + duplication + jitter on the link, the sender resending a batch until
/// every record is `Applied`/`Superseded`: the final state is the model's.
fn run_lossy_resend(seed: u64) {
    let mut f = fixture(seed, receiver_cfg(true));
    f.w.bridge().set_default_link(LinkConfig {
        jitter: Duration::from_millis(30),
        loss_permille: 300,
        dup_permille: 300,
        ..LinkConfig::new(LAT)
    });
    let t = f.b_now_ms().saturating_sub(1_000);
    let records: Vec<MrecRecord> = (0..8u64)
        .map(|i| put(&format!("l{i}"), &format!("v{i}"), ver(t + i, 0)))
        .collect();
    let mut done = false;
    for _ in 0..40 {
        if let Ok(MrecApplyResponse::Answers(a)) = f.call(&request(records.clone())) {
            assert!(
                a.iter()
                    .all(|x| matches!(x, MrecAnswer::Applied | MrecAnswer::Superseded)),
                "seed={seed}: {a:?}"
            );
            done = true;
            break;
        }
    }
    assert!(
        done,
        "seed={seed}: never got an answer through 30% loss in 40 tries"
    );
    f.w.bridge().set_default_link(LinkConfig::new(LAT));
    for i in 0..8u64 {
        assert_eq!(
            f.read(&format!("l{i}")).as_deref(),
            Some(format!("v{i}").as_str()),
            "seed={seed}"
        );
    }
}

#[test]
fn resending_through_a_lossy_duplicating_link_converges() {
    for seed in seeds() {
        run_lossy_resend(seed);
    }
}

/// The receiver groups by **its own** tablet layout: split B's table into two
/// tablets, then ship a batch spanning both.
fn run_two_tablets(seed: u64) {
    let mut f = fixture(seed, receiver_cfg(true));
    let t = f.b_now_ms().saturating_sub(1_000);
    let keys: Vec<String> = (0..24).map(|i| format!("key{i:02}")).collect();
    let recs: Vec<MrecRecord> = keys.iter().map(|k| put(k, "v1", ver(t, 0))).collect();
    assert!(
        f.answers(recs).iter().all(|a| *a == MrecAnswer::Applied),
        "seed={seed}"
    );
    // Split the (only) tablet through the real admin trigger.
    let parent = f.w.clusters[B].tablet_of(TABLE).expect("tablet");
    // An 8-byte token boundary (0x40, 0, ...) in ADR 0022's token space.
    let body = format!(
        r#"{{"tablet":{},"split_key":"@\u0000\u0000\u0000\u0000\u0000\u0000\u0000"}}"#,
        parent.0
    );
    let w = &mut f.w;
    let (status, v) = w.clusters[B].admin(0, "POST", "/admin/tablet/split", "", body.as_bytes());
    assert_eq!(status, 200, "seed={seed}: split: {v}");
    let mut converged = false;
    for _ in 0..40 {
        for node in 0..3u64 {
            w.clusters[B].drive_inplace_split_cutover(node);
        }
        let m = w.clusters[B].metadata(0);
        if !m.tablets.contains_key(&parent) && m.tablets.len() == 2 {
            converged = true;
            break;
        }
        w.clusters[B].run_for(Duration::from_millis(200));
    }
    assert!(converged, "seed={seed}: split never cut over");
    w.sync_clocks();
    // The keys really do straddle the two children.
    let meta = w.clusters[B].metadata(0);
    let owners: std::collections::BTreeSet<_> = keys
        .iter()
        .filter_map(|k| {
            crate::topology::tablet_for_key(
                meta.tablets_for_table(TABLE),
                &crate::dynamo::item_key(&pk(k), Some(&sk())),
            )
        })
        .collect();
    assert_eq!(owners.len(), 2, "seed={seed}: keys must span both children");
    // A batch spanning both children: newer for every key, plus an older record
    // for every key to prove LWW per child tablet.
    let mut recs: Vec<MrecRecord> = keys.iter().map(|k| put(k, "v2", ver(t + 10, 0))).collect();
    recs.extend(keys.iter().map(|k| put(k, "stale", ver(t + 5, 0))));
    let a = f.answers(recs);
    let n = keys.len();
    assert!(
        a[..n].iter().all(|x| *x == MrecAnswer::Applied),
        "seed={seed}: {a:?}"
    );
    assert!(
        a[n..].iter().all(|x| *x == MrecAnswer::Superseded),
        "seed={seed}: {a:?}"
    );
    for k in &keys {
        assert_eq!(f.read(k).as_deref(), Some("v2"), "seed={seed}: {k}");
    }
}

#[test]
fn a_batch_is_grouped_by_the_receivers_own_tablet_layout() {
    for seed in seeds().into_iter().take(2) {
        run_two_tablets(seed);
    }
}

fn run_refusals(seed: u64) {
    let t0;
    {
        // Plaintext node without allow_insecure_peers: refused, retryable=false.
        let mut f = fixture(seed, receiver_cfg(false));
        t0 = f.b_now_ms();
        let r = f
            .call(&request(vec![put("x", "v", ver(t0 - 100, 0))]))
            .expect("delivered");
        assert!(
            matches!(&r, MrecApplyResponse::Refused { message, retryable: false } if message.contains("TLS")),
            "seed={seed}: {r:?}"
        );
        assert_eq!(f.read("x"), None);
    }
    let mut f = fixture(seed, receiver_cfg(true));
    let t = f.b_now_ms() - 100;
    let refused = |r: &MrecApplyResponse, needle: &str, retryable: bool| {
        assert!(
            matches!(r, MrecApplyResponse::Refused { message, retryable: rt }
                if message.contains(needle) && *rt == retryable),
            "seed={seed}: expected refusal `{needle}`: {r:?}"
        );
    };
    let mut req = request(vec![put("x", "v", ver(t, 0))]);
    req.proto = 99;
    refused(&f.call(&req).unwrap(), "protocol", false);
    let mut req = request(vec![put("x", "v", ver(t, 0))]);
    req.from_region = "stranger".into();
    refused(&f.call(&req).unwrap(), "not a configured peer", false);
    let mut req = request(vec![put("x", "v", ver(t, 0))]);
    req.table = "nope".into();
    refused(&f.call(&req).unwrap(), "not an MREC global table", true);
    // Per-record rejections: a stamp the sender does not own, a zero stamp, a
    // key that disagrees with the item, a missing sort key; siblings unaffected.
    let mut foreign = put("f", "v", ver(t, 0));
    foreign.ver.region_id = mrec_region_id("b");
    let mut bad_key = put("g", "v", ver(t, 0));
    bad_key.item = Some(item("other", "v"));
    let mut no_sk = put("h", "v", ver(t, 0));
    no_sk.sk = None;
    let a = f.answers(vec![
        foreign,
        put("z", "v", MrecVersion::ZERO),
        bad_key,
        no_sk,
        put("ok", "v", ver(t, 0)),
    ]);
    for (i, x) in a[..4].iter().enumerate() {
        assert!(
            matches!(x, MrecAnswer::Rejected { .. }),
            "seed={seed}: record {i}: {a:?}"
        );
    }
    assert_eq!(a[4], MrecAnswer::Applied);
    assert_eq!(f.read("ok").as_deref(), Some("v"));
    for k in ["f", "g", "h", "z"] {
        assert_eq!(f.read(k), None, "seed={seed}: {k}");
    }
}

#[test]
fn refusals_and_per_record_rejections_are_named() {
    for seed in seeds().into_iter().take(2) {
        run_refusals(seed);
    }
}

/// While the receiving cluster's MREC gate is closed (cluster version 2) the
/// receiver refuses by name, retryably, and applies nothing.
#[test]
fn a_receiver_below_the_gate_refuses_retryably() {
    let seed = seeds()[0];
    let mut w = SimWorld::new(seed, 2, 3, 3);
    // B: finalize to 2 only.
    {
        let c = &mut w.clusters[B];
        let n = c.node_count() as u64;
        c.set_all_node_versions(Some(VersionRange::new(1, 3)));
        poll(c, "era", seed, |c| {
            (0..n).all(|i| {
                c.features(i).era_active() && c.metadata(i).node_versions.len() == n as usize
            })
        });
        let idx = c.control_leader_index();
        let leader = c.control_node_id(idx);
        let (s, v) = c.admin(
            leader,
            "POST",
            "/admin/cluster-version/finalize",
            "",
            br#"{"to":2,"expected":1}"#,
        );
        assert_eq!(s, 200, "{v}");
        poll(c, "v2", seed, |c| {
            (0..n).all(|i| c.features(i).cluster_version() == 2)
        });
        c.create_table(TABLE);
    }
    w.sync_clocks();
    w.set_handler(B, handler(receiver_cfg(true), Arc::new(AtomicU64::new(0))));
    w.bridge().set_default_link(LinkConfig::new(LAT));
    let req = request(vec![put("x", "v", ver(1, 0))]);
    let (r, _) = w.peer_call(A, B, &serde_json::to_vec(&req).unwrap(), TIMEOUT);
    let resp: MrecApplyResponse = serde_json::from_slice(&r.expect("delivered")).unwrap();
    assert!(
        matches!(&resp, MrecApplyResponse::Refused { retryable: true, message } if message.contains("MrecReplication")),
        "{resp:?}"
    );
}

/// The whole receiver run is a pure function of the seed (same fingerprint
/// twice, distinct seeds differ).
fn fingerprint_run(seed: u64) -> u64 {
    let mut f = fixture(seed, receiver_cfg(true));
    f.w.bridge().set_default_link(LinkConfig {
        jitter: Duration::from_millis(20),
        dup_permille: 200,
        ..LinkConfig::new(LAT)
    });
    let t = f.b_now_ms().saturating_sub(1_000);
    for i in 0..6u64 {
        let _ = f.call(&request(vec![
            put(&format!("d{}", i % 3), "v", ver(t + i, 0)),
            del("d0", ver(t + i * 2, 1)),
        ]));
    }
    f.w.run_for(Duration::from_secs(1));
    f.w.fingerprint()
}

#[test]
fn the_receiver_run_is_seed_deterministic() {
    let seed = seeds()[0];
    assert_eq!(fingerprint_run(seed), fingerprint_run(seed), "seed={seed}");
    assert_ne!(fingerprint_run(seed), fingerprint_run(seed + 1));
}

/// The `PeerBridge` and the real client share the `PeerClient` shape: a
/// bridge client is usable as a `dyn PeerClient`.
#[test]
fn the_bridge_client_is_a_peer_client() {
    let w = SimWorld::new(seeds()[0], 2, 3, 3);
    let _c: Box<dyn PeerClient> = Box::new(w.peer_client(A));
}

// ---------------------------------------------------------------------------
// The two M2-left writer guards, driven on a real node of an MREC table.

fn run_writer_guards(seed: u64) {
    let mut c = SimCluster::new(seed, 3, 3);
    open_mrec_gate(&mut c, seed);
    make_mrec(&mut c, seed);
    let ctx = c.handle().node_ctx(0);
    let key = crate::dynamo::item_key(&pk("g"), Some(&sk()));
    // `marker_batch_write_raw`: refused before anything is proposed.
    let rows = vec![(
        key.clone(),
        Some(b"raw".to_vec()),
        (b"mk".to_vec(), b"mv".to_vec()),
    )];
    let r = c
        .spawn_and_capture_fast(0, {
            let ctx = ctx.clone();
            async move { crate::dynamo::marker_batch_write_raw(&ctx, TABLE, rows, false).await }
        })
        .expect("completed");
    let e = r.expect_err("an MREC table refuses a raw base-row write");
    assert!(e.contains("MREC global table"), "seed={seed}: {e}");
    // `cp_txn`: a plain (edge-valued) transaction write is refused.
    let r = c
        .spawn_and_capture_fast(0, {
            let ctx = ctx.clone();
            let key = key.clone();
            async move {
                ctx.cp_txn(
                    vec![TxnTableWrite::plain(
                        TABLE.into(),
                        key,
                        Some(b"raw".to_vec()),
                    )],
                    vec![],
                    vec![],
                )
                .await
            }
        })
        .expect("completed");
    match r {
        Err(crate::TxnAbortReason::Other(m)) => assert!(m.contains("MREC"), "seed={seed}: {m}"),
        other => panic!("seed={seed}: expected a refusal, got {other:?}"),
    }
}

#[test]
fn raw_and_edge_valued_writers_refuse_an_mrec_table() {
    for seed in seeds().into_iter().take(2) {
        run_writer_guards(seed);
    }
}
