//! G-01 stage G-d M5: the **multi-cluster fault-injection corpus** (ADR 0075
//! section 4.9) over [`SimWorld`]: 2 and 3 real 3-node `SimCluster`s (LSM
//! backend, finalized to cluster version 3) joined into one MREC global table
//! through the real `UpdateTable` saga, driven with random wire workloads
//! (puts, updates, deletes, conditional writes, region-local transactions,
//! TTL) while faults are injected (WAN partition / one-way partition / heal,
//! loss, duplication and reordering, node crash and restart on either side
//! including the shipping and the receiving leader, tablet split while
//! shipping, clock skew within and beyond `max_clock_skew`, a peer down past
//! `max_backlog` so it is resynced by scan).
//!
//! Oracles, after heal + quiescence (`check`):
//! 1. **convergence**: every region holds the identical `(item, version)` for
//!    every key, tombstones included;
//! 2. **LWW**: the final version of a key is >= every version a write of it
//!    was *observed* to take (read back from the origin right after the ack),
//!    and where it equals one, the value is that write's value; the final
//!    value is one that was attempted;
//! 3. **no acked write lost / no resurrection**: every key with an acked write
//!    is present (value or tombstone) and a delete's version is never beaten
//!    by an older put (subsumed by 2: tombstone versions are floors too);
//! 4. **no re-replication**: a record whose stamp names another region than
//!    its sender is never sent (checked on the receiving side of the WAN), and
//!    on fault-free cells the shipped-record count is bounded;
//! 5. **stream parity**: each region's stream holds an event carrying the
//!    converged value (a `REMOVE` for a tombstone); coalescing is allowed;
//! 6. same seed, same final state (`determinism` test).
//!
//! Knobs: `ANIMUS_MREC_SEEDS=K` (default 1), `ANIMUS_MREC_CELL=<substring>`,
//! `ANIMUS_SEED=<s>` replays one seed. A failing seed is printed
//! (`sim_world_mrec_corpus: FAILED cell=... seed=...`). One `#[test]` per cell.
//!
//! Negative controls (each must FAIL its oracle): LWW by arrival order, the
//! cursor advancing before the peer's ack, loop prevention off, and an oracle
//! fed a wrong expectation.

use std::collections::{BTreeMap, BTreeSet};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::mrec_region_id;
use animus_control::sim_versions::BinaryProfile;
use animus_dynamo::AttributeValue;
use animus_env::{Env, Metric};
use animus_item::MrecVersion;
use animus_node::MrecApplyRequest;

use super::AutoSplitThresholds;
use super::mrec_peer::{MrecConfig, PeerClient};
use super::mrec_saga::mrec_saga_table;
use super::mrec_shipper::{INITIAL_COPY_WALL_MS, mrec_ship_table, neg, trim_term};
use super::sim_world::{LinkConfig, PeerHandler, SimWorld};
use super::sim_world_mrec_saga_tests::{NODES, cfg, create_body, item_body, key_body};
use super::sim_world_mrec_tests::{LAT, TABLE, handler, open_mrec_gate, splitmix};

const REGIONS: [&str; 3] = ["a", "b", "c"];
const KEYS: [&str; 6] = ["k0", "k1", "k2", "k3", "k4", "k5"];
const PRE: usize = 3;

// ---------------------------------------------------------------------------
// Knobs
// ---------------------------------------------------------------------------

fn seeds() -> Vec<u64> {
    if let Some(s) = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
    {
        return vec![s];
    }
    let k = animus_test::corpus::seeds_from_env("ANIMUS_MREC_SEEDS") as u64;
    (0..k).map(|i| 0x4D52_0000 + i).collect()
}

fn cell_selected(name: &str) -> bool {
    std::env::var("ANIMUS_MREC_CELL")
        .ok()
        .is_none_or(|f| f.is_empty() || name.contains(&f))
}

// ---------------------------------------------------------------------------
// Plans
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
enum Fault {
    Partition(usize, usize),
    OneWay(usize, usize),
    Heal,
    Lossy(bool),
    CrashLeader(usize),
    RestartAll,
    Skew(usize, i64),
    Split,
    /// Keep stepping the shippers for this long (virtual seconds) with no ops.
    Wait(u64),
    /// `UpdateTable Create` of region index `n` on cluster 0 (create-under-load).
    CreateReplica(usize),
}

#[derive(Clone, Debug)]
struct Plan {
    name: &'static str,
    regions: usize,
    ops: usize,
    /// Weights: put, update, delete, cond-put, cond-update, txn, ttl-put.
    mix: [u32; 7],
    keys: usize,
    faults: Vec<(usize, Fault)>,
    ttl: bool,
    pad: bool,
    backlog_secs: u64,
    /// Ops only in region 0 (the loop-prevention control).
    only_a: bool,
    /// Skip the up-front mesh formation (create-under-load).
    form_late: bool,
    /// No pause between ops (same-millisecond stamps).
    burst: bool,
    /// Feed the oracle a wrong expectation (negative control).
    bite: bool,
    /// Fault-free: the shipped-record bound applies.
    clean: bool,
    /// The cell must actually exercise its fault (else it proves nothing).
    expect: Expect,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Expect {
    Nothing,
    /// At least one past-retention resync happened.
    Resync,
    /// The table ended with at least two active tablets.
    Split,
    /// At least one node was crashed and restarted.
    Crash,
}

impl Plan {
    fn base(name: &'static str, regions: usize, ops: usize) -> Plan {
        Plan {
            name,
            regions,
            ops,
            mix: [40, 15, 15, 10, 5, 10, 0],
            keys: KEYS.len(),
            faults: Vec::new(),
            ttl: false,
            pad: false,
            backlog_secs: 3600,
            only_a: false,
            form_late: false,
            burst: false,
            bite: false,
            clean: false,
            expect: Expect::Nothing,
        }
    }
}

fn plans() -> Vec<Plan> {
    let mut v = Vec::new();
    let mut p = Plan::base("steady_two_regions", 2, 50);
    p.clean = true;
    v.push(p);
    let mut p = Plan::base("steady_three_regions_full_mesh", 3, 60);
    p.clean = true;
    v.push(p);
    let mut p = Plan::base("partition_heal_two_regions", 2, 50);
    p.faults = vec![
        (10, Fault::Partition(0, 1)),
        (35, Fault::Wait(6)),
        // No heal before the last op: quiescence heals, so nothing written
        // during the partition is ever re-sent by a later write.
    ];
    v.push(p);
    let mut p = Plan::base("asymmetric_partition_heal", 3, 60);
    p.faults = vec![
        (8, Fault::OneWay(0, 1)),
        (14, Fault::OneWay(2, 0)),
        (40, Fault::Wait(5)),
        (45, Fault::Heal),
    ];
    v.push(p);
    let mut p = Plan::base("loss_dup_reorder", 3, 60);
    p.faults = vec![(0, Fault::Lossy(true))];
    v.push(p);
    let mut p = Plan::base("crash_restart_shipping_leader", 2, 60);
    p.faults = vec![
        (15, Fault::CrashLeader(0)),
        (30, Fault::RestartAll),
        (40, Fault::CrashLeader(0)),
        (50, Fault::RestartAll),
    ];
    p.expect = Expect::Crash;
    v.push(p);
    let mut p = Plan::base("crash_restart_receiving_leader", 2, 60);
    p.faults = vec![
        (15, Fault::CrashLeader(1)),
        (30, Fault::RestartAll),
        (40, Fault::CrashLeader(1)),
        (50, Fault::RestartAll),
    ];
    p.expect = Expect::Crash;
    v.push(p);
    let mut p = Plan::base("leader_churn_three_regions", 3, 70);
    p.faults = vec![
        (10, Fault::CrashLeader(0)),
        (20, Fault::RestartAll),
        (25, Fault::CrashLeader(1)),
        (35, Fault::RestartAll),
        (40, Fault::CrashLeader(2)),
        (50, Fault::RestartAll),
    ];
    p.expect = Expect::Crash;
    v.push(p);
    let mut p = Plan::base("region_down_past_retention_resync", 2, 50);
    p.backlog_secs = 6;
    p.faults = vec![
        (8, Fault::Partition(0, 1)),
        (14, Fault::Wait(14)),
        (30, Fault::Wait(10)),
        (36, Fault::Heal),
    ];
    p.expect = Expect::Resync;
    v.push(p);
    let mut p = Plan::base("concurrent_conflicting_writes", 3, 45);
    p.burst = true;
    p.faults = vec![(0, Fault::Lossy(true))];
    v.push(p);
    let mut p = Plan::base("delete_put_race", 2, 60);
    p.keys = 2;
    p.mix = [40, 5, 40, 5, 0, 10, 0];
    p.burst = true;
    v.push(p);
    let mut p = Plan::base("ttl_expiry_two_regions", 2, 40);
    p.ttl = true;
    p.mix = [25, 10, 5, 5, 0, 5, 50];
    v.push(p);
    let mut p = Plan::base("hlc_skew_within_bound", 2, 50);
    p.faults = vec![(0, Fault::Skew(0, -200)), (0, Fault::Skew(1, 200))];
    v.push(p);
    let mut p = Plan::base("hlc_skew_beyond_bound", 2, 40);
    p.faults = vec![(0, Fault::Skew(1, 2_500))];
    v.push(p);
    let mut p = Plan::base("split_while_shipping", 2, 60);
    p.pad = true;
    p.faults = vec![(10, Fault::Split)];
    p.expect = Expect::Split;
    v.push(p);
    let mut p = Plan::base("add_replica_under_load", 2, 60);
    p.form_late = true;
    p.faults = vec![(20, Fault::CreateReplica(1))];
    v.push(p);
    let mut p = Plan::base("txn_region_local", 2, 50);
    p.mix = [10, 5, 5, 0, 0, 80, 0];
    v.push(p);
    v
}

// ---------------------------------------------------------------------------
// The model the oracle checks against
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Eq)]
struct Row {
    val: Option<String>,
    item: String,
    ver: MrecVersion,
}

type Dump = BTreeMap<String, Row>;

#[derive(Default)]
struct Model {
    attempts: BTreeMap<String, BTreeSet<String>>,
    acked_keys: BTreeSet<String>,
    floor: BTreeMap<String, MrecVersion>,
    exact: BTreeMap<String, BTreeMap<MrecVersion, Option<String>>>,
    attempted_by: Vec<u64>,
    seq: u64,
}

/// What the peer-side spy saw cross the WAN.
#[derive(Default)]
struct Wan {
    /// Records received per sending region index.
    records_from: BTreeMap<usize, u64>,
    /// Records whose stamp names a region other than their sender's.
    foreign: Vec<String>,
}

struct W {
    w: SimWorld,
    cfgs: Vec<Arc<MrecConfig>>,
    entry: Vec<Arc<AtomicU64>>,
    down: Vec<(usize, usize)>,
    seed: u64,
    n: usize,
    rng: u64,
    m: Model,
    wan: Arc<Mutex<Wan>>,
    split: bool,
    crashes: u32,
}

fn mesh_cfg(i: usize, n: usize, backlog_secs: u64) -> Arc<MrecConfig> {
    // Sender view: peer index == the bridge's cluster index (self is `pad`).
    let peers: Vec<&str> = (0..n)
        .map(|j| if j == i { "pad" } else { REGIONS[j] })
        .collect();
    let mut c = cfg(REGIONS[i], &peers).as_ref().clone();
    c.max_backlog = Duration::from_secs(backlog_secs);
    Arc::new(c)
}

fn pad_of(v: &str, pad: bool) -> String {
    if pad {
        format!("{v}-{}", "x".repeat(300))
    } else {
        v.to_owned()
    }
}

impl W {
    fn new(seed: u64, plan: &Plan) -> W {
        let n = plan.regions;
        let mut w = SimWorld::new_lsm(seed, n, NODES as usize, 3);
        for c in 0..n {
            open_mrec_gate(&mut w.clusters[c], seed);
            for node in 0..NODES {
                w.clusters[c].set_binary_profile(node, BinaryProfile::Release(3));
            }
        }
        w.clusters[0].create_table(TABLE);
        w.sync_clocks();
        w.bridge().set_default_link(LinkConfig::new(LAT));
        let cfgs: Vec<Arc<MrecConfig>> =
            (0..n).map(|i| mesh_cfg(i, n, plan.backlog_secs)).collect();
        let wan = Arc::new(Mutex::new(Wan::default()));
        let mut entry = Vec::new();
        for (c, cf) in cfgs.iter().enumerate() {
            w.clusters[c].handle().set_mrec_config(cf.clone());
            let e = Arc::new(AtomicU64::new(0));
            let inner: PeerHandler = handler(cf.as_ref().clone(), e.clone());
            let spy_wan = wan.clone();
            let spy: PeerHandler = Arc::new(move |h, payload: Vec<u8>| {
                if let Ok(req) = serde_json::from_slice::<MrecApplyRequest>(&payload) {
                    let from = REGIONS.iter().position(|r| *r == req.from_region);
                    let mut s = spy_wan.lock().expect("wan");
                    for r in &req.records {
                        if r.ver.region_id != mrec_region_id(&req.from_region) {
                            s.foreign.push(format!(
                                "region {} sent a record stamped by region_id {} (pk {:?})",
                                req.from_region, r.ver.region_id, r.pk
                            ));
                        }
                    }
                    if let Some(f) = from {
                        *s.records_from.entry(f).or_default() += req.records.len() as u64;
                    }
                }
                inner(h, payload)
            });
            w.set_handler(c, spy);
            entry.push(e);
        }
        W {
            w,
            cfgs,
            entry,
            down: Vec::new(),
            seed,
            n,
            rng: seed ^ 0x6D72_6563_636F_7270,
            m: Model {
                attempted_by: vec![0; n],
                ..Model::default()
            },
            wan,
            split: false,
            crashes: 0,
        }
    }

    fn rnd(&mut self, below: u64) -> u64 {
        splitmix(&mut self.rng) % below.max(1)
    }

    fn up(&self, c: usize) -> u64 {
        (0..NODES)
            .find(|n| !self.down.contains(&(c, *n as usize)))
            .expect("a live node")
    }

    fn call(&mut self, c: usize, op: &str, body: &str) -> (u16, String) {
        let n = self.up(c);
        self.w.dynamo(c, n, op, body)
    }

    // ---- stepping ---------------------------------------------------------

    fn step_cluster(&mut self, c: usize) {
        for node in 0..NODES {
            if self.down.contains(&(c, node as usize)) {
                continue;
            }
            let mut ctx = self.w.clusters[c].handle().node_ctx(node);
            ctx.mrec = self.cfgs[c].clone();
            let cf = self.cfgs[c].clone();
            let client = self.w.peer_client(c);
            let seed = self.seed;
            self.w
                .drive(c, node, Duration::from_secs(90), async move {
                    let _ = mrec_saga_table(&ctx, TABLE, &client as &dyn PeerClient).await;
                    let _ = mrec_ship_table(&ctx, TABLE, &client as &dyn PeerClient).await;
                    // The retention janitor's MREC term (index_drain's
                    // trim_janitor in production).
                    let meta = ctx.effective_metadata();
                    for (_, g) in ctx.edge.hosted_groups() {
                        if g.is_leader() {
                            let _ = trim_term(&ctx.env, &cf, &meta, TABLE, &g).await;
                        }
                    }
                })
                .unwrap_or_else(|| panic!("seed={seed}: step on {c}/{node} did not finish"));
            if self.split {
                self.w.clusters[c].drive_inplace_split_cutover(node);
            }
        }
    }

    fn step_all(&mut self) {
        for c in 0..self.n {
            self.step_cluster(c);
        }
        self.w.run_for(Duration::from_millis(250));
    }

    // ---- reads of the engine ---------------------------------------------

    fn decode(&self, c: usize, v: &[u8]) -> Option<(String, Row)> {
        let rid = mrec_region_id(REGIONS[c]);
        let (item, ver) = animus_item::decode_stored_item_versioned(v).ok()?;
        let ver = ver.unwrap_or(MrecVersion {
            wall_ms: INITIAL_COPY_WALL_MS,
            logical: 0,
            region_id: rid,
        });
        match item {
            Some(it) => {
                let AttributeValue::S(pk) = it.get("pk")?.clone() else {
                    return None;
                };
                let val = match it.get("v") {
                    Some(AttributeValue::S(s)) => Some(s.clone()),
                    _ => None,
                };
                Some((
                    pk,
                    Row {
                        val,
                        item: format!("{it:?}"),
                        ver,
                    },
                ))
            }
            None => {
                let (AttributeValue::S(pk), _) = animus_item::decode_tombstone_key(v)? else {
                    return None;
                };
                Some((
                    pk,
                    Row {
                        val: None,
                        item: "TOMBSTONE".into(),
                        ver,
                    },
                ))
            }
        }
    }

    /// Every base row (tombstones included) of cluster `c`, from its leaders.
    fn dump(&mut self, c: usize) -> Dump {
        let mut out = Dump::new();
        for node in 0..NODES {
            if self.down.contains(&(c, node as usize)) {
                continue;
            }
            let ctx = self.w.clusters[c].handle().node_ctx(node);
            let seed = self.seed;
            let rows = self
                .w
                .drive(c, node, Duration::from_secs(30), async move {
                    let mut rows = Vec::new();
                    for (_, g) in ctx.edge.hosted_groups() {
                        if !g.is_leader() {
                            continue;
                        }
                        let (r, _) = g.local_scan_for_ship(&[], 100_000).await;
                        rows.extend(r.into_iter().map(|(_, v)| v));
                    }
                    rows
                })
                .unwrap_or_else(|| panic!("seed={seed}: dump of {c}/{node} did not finish"));
            for v in rows {
                if let Some((pk, row)) = self.decode(c, &v) {
                    match out.get(&pk) {
                        Some(old) if old.ver >= row.ver => {}
                        _ => {
                            out.insert(pk, row);
                        }
                    }
                }
            }
        }
        out
    }

    /// One key's stored row on cluster `c` (None when absent / no leader).
    fn stored(&mut self, c: usize, key: &str) -> Option<Row> {
        for node in 0..NODES {
            if self.down.contains(&(c, node as usize)) {
                continue;
            }
            let ctx = self.w.clusters[c].handle().node_ctx(node);
            let k = crate::dynamo::item_key(
                &AttributeValue::S(key.to_owned()),
                Some(&AttributeValue::S("s".to_owned())),
            );
            let raw = self
                .w
                .drive(c, node, Duration::from_secs(10), async move {
                    for (_, g) in ctx.edge.hosted_groups() {
                        if g.is_leader()
                            && let Some(v) = g.local_get(&k).await
                        {
                            return Some(v);
                        }
                    }
                    None
                })
                .flatten();
            if let Some(v) = raw {
                return self.decode(c, &v).map(|(_, r)| r);
            }
        }
        None
    }

    // ---- workload ---------------------------------------------------------

    fn note_attempt(&mut self, key: &str, val: &str) {
        self.m
            .attempts
            .entry(key.to_owned())
            .or_default()
            .insert(val.to_owned());
    }

    /// Record an acked write's observed stamp (read back from its origin).
    fn observe(&mut self, c: usize, key: &str, mine: Option<&str>) {
        self.m.acked_keys.insert(key.to_owned());
        if let Some(st) = self.stored(c, key) {
            let f = self.m.floor.entry(key.to_owned()).or_insert(st.ver);
            *f = (*f).max(st.ver);
            let ours = st.ver.region_id == mrec_region_id(REGIONS[c]);
            if ours && st.val.as_deref() == mine {
                self.m
                    .exact
                    .entry(key.to_owned())
                    .or_default()
                    .insert(st.ver, st.val.clone());
            }
        }
    }

    fn wall_secs(&self, c: usize) -> u64 {
        use animus_env::Clock;
        self.w.clusters[c].handle().env(0).wall_now().0 / 1000
    }

    fn one_op(&mut self, plan: &Plan, i: usize) {
        let c = if plan.only_a {
            0
        } else {
            self.rnd(self.n as u64) as usize
        };
        self.m.attempted_by[c] += 1;
        let key = if plan.burst {
            // A round of concurrent writers shares one key.
            KEYS[(i / plan.regions) % plan.keys]
        } else {
            KEYS[self.rnd(plan.keys as u64) as usize]
        };
        self.m.seq += 1;
        let val = pad_of(&format!("{}{}", REGIONS[c], self.m.seq), plan.pad);
        let total: u32 = plan.mix.iter().sum();
        let mut pick = self.rnd(u64::from(total)) as u32;
        let mut kind = 0;
        for (i, wgt) in plan.mix.iter().enumerate() {
            if pick < *wgt {
                kind = i;
                break;
            }
            pick -= *wgt;
        }
        let put = |k: &str| vec![(k.to_owned(), Some(val.clone()))];
        let (op, body, effects): (&str, String, Vec<(String, Option<String>)>) = match kind {
            0 => ("PutItem", item_body(key, &val, ""), put(key)),
            1 => ("UpdateItem", upd_body(key, &val, None), put(key)),
            2 => (
                "DeleteItem",
                key_body(key, false),
                vec![(key.to_owned(), None)],
            ),
            3 => ("PutItem", cond_put(key, &val), put(key)),
            4 => (
                "UpdateItem",
                upd_body(key, &val, Some("attribute_exists(pk)")),
                put(key),
            ),
            5 => {
                let k2 = KEYS[self.rnd(plan.keys as u64) as usize];
                if k2 == key {
                    ("PutItem", item_body(key, &val, ""), put(key))
                } else {
                    (
                        "TransactWriteItems",
                        txn_body(key, &val, k2),
                        vec![(key.to_owned(), Some(val.clone())), (k2.to_owned(), None)],
                    )
                }
            }
            _ => {
                let ttl = self.wall_secs(c) + 3 + self.rnd(10);
                (
                    "PutItem",
                    item_body(key, &val, &format!(r#","ttl":{{"N":"{ttl}"}}"#)),
                    put(key),
                )
            }
        };
        for (k, v) in &effects {
            if let Some(v) = v {
                self.note_attempt(k, v);
            }
        }
        let (st, r) = self.call(c, op, &body);
        if st != 200 && std::env::var("ANIMUS_MREC_DEBUG").is_ok() {
            eprintln!(
                "DEBUG op {op} on region {c} -> {st}: {}",
                r.chars().take(200).collect::<String>()
            );
        }
        if st == 200 {
            for (k, v) in effects {
                self.observe(c, &k, v.as_deref());
            }
        }
    }

    // ---- faults -----------------------------------------------------------

    fn leader_node(&mut self, c: usize) -> Option<u64> {
        for node in 0..NODES {
            if self.down.contains(&(c, node as usize)) {
                continue;
            }
            let ctx = self.w.clusters[c].handle().node_ctx(node);
            let led = self
                .w
                .drive(c, node, Duration::from_secs(4), async move {
                    ctx.edge.hosted_groups().iter().any(|(_, g)| g.is_leader())
                })
                .unwrap_or(false);
            if led {
                return Some(node);
            }
        }
        None
    }

    fn fault(&mut self, f: &Fault) {
        match f {
            Fault::Partition(a, b) => self.w.bridge().partition(*a, *b),
            Fault::OneWay(a, b) => self.w.bridge().partition_one_way(*a, *b),
            Fault::Heal => self.w.bridge().heal(),
            Fault::Lossy(on) => {
                let link = if *on {
                    let mut l = LinkConfig::new(LAT);
                    l.jitter = Duration::from_millis(70);
                    l.loss_permille = 120;
                    l.dup_permille = 150;
                    l
                } else {
                    LinkConfig::new(LAT)
                };
                self.w.bridge().set_default_link(link);
            }
            Fault::CrashLeader(c) => {
                if self.down.iter().any(|(cc, _)| cc == c) {
                    return; // never take a second node of one cluster down
                }
                if let Some(n) = self.leader_node(*c) {
                    self.w.clusters[*c].crash(n);
                    self.down.push((*c, n as usize));
                    self.crashes += 1;
                    let up = self.up(*c);
                    self.entry[*c].store(up, Ordering::SeqCst);
                }
            }
            Fault::RestartAll => self.restart_all(),
            Fault::Skew(c, ms) => {
                let sim = self.w.clusters[*c].simulator();
                for node in 0..NODES {
                    sim.set_clock_skew_for(animus_env::nid(node), ms * 1_000_000);
                }
            }
            Fault::Split => {
                self.split = true;
                for c in 0..self.n {
                    self.w.clusters[c].set_auto_split_thresholds(AutoSplitThresholds {
                        bytes: Some(2_000),
                        change_rate: None,
                        ops_rate: None,
                        tablet_capacity_ceilings: Default::default(),
                    });
                }
            }
            Fault::Wait(secs) => {
                let until = self.w.now_ms() + secs * 1000;
                while self.w.now_ms() < until {
                    self.step_all();
                }
            }
            Fault::CreateReplica(i) => {
                let _ = self.call(0, "UpdateTable", &create_body(REGIONS[*i]));
            }
        }
    }

    fn restart_all(&mut self) {
        let down = std::mem::take(&mut self.down);
        for (c, node) in down {
            self.w.clusters[c].restart(node as u64);
            self.w.clusters[c]
                .handle()
                .set_mrec_config(self.cfgs[c].clone());
        }
        for e in &self.entry {
            e.store(0, Ordering::SeqCst);
        }
        self.w.run_for(Duration::from_millis(500));
    }

    // ---- bring-up and quiescence -----------------------------------------

    fn active_count(&mut self, c: usize) -> (usize, bool) {
        let (st, r) = self.call(c, "DescribeTable", &format!(r#"{{"TableName":"{TABLE}"}}"#));
        if st != 200 {
            return (0, false);
        }
        let v: serde_json::Value = serde_json::from_str(&r).expect("json");
        let l = v["Table"]["Replicas"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        (l.len(), l.iter().all(|e| e["ReplicaStatus"] == "ACTIVE"))
    }

    fn until(&mut self, what: &str, cond: impl Fn(&mut W) -> bool) {
        for _ in 0..300 {
            if cond(self) {
                return;
            }
            self.step_all();
        }
        panic!("seed={}: timed out waiting for {what}", self.seed);
    }

    /// Pre-load region 0, then grow the table into every region.
    fn form(&mut self, plan: &Plan) {
        if plan.ttl {
            let (st, r) = self.call(
                0,
                "UpdateTimeToLive",
                &format!(
                    r#"{{"TableName":"{TABLE}","TimeToLiveSpecification":{{"Enabled":true,"AttributeName":"ttl"}}}}"#
                ),
            );
            assert_eq!(st, 200, "seed={}: ttl: {r}", self.seed);
        }
        for i in 0..PRE {
            let (k, v) = (format!("pre{i}"), "old".to_owned());
            self.note_attempt(&k, &v);
            let (st, r) = self.call(0, "PutItem", &item_body(&k, &v, ""));
            assert_eq!(st, 200, "seed={}: pre put: {r}", self.seed);
            self.m.acked_keys.insert(k);
        }
        if plan.form_late {
            return;
        }
        for i in 1..self.n {
            let (st, r) = self.call(0, "UpdateTable", &create_body(REGIONS[i]));
            assert_eq!(st, 200, "seed={}: create {i}: {r}", self.seed);
            let n = i + 1;
            self.until("the mesh to form", |w| {
                (0..n).all(|c| w.active_count(c) == (n, true))
            });
        }
    }

    fn quiesce(&mut self, plan: &Plan) {
        self.w.bridge().heal();
        self.w.bridge().set_default_link(LinkConfig::new(LAT));
        self.restart_all();
        if plan.ttl {
            // Let every TTL attribute expire and the reapers run.
            for _ in 0..8 {
                self.w.run_for(Duration::from_secs(5));
                self.step_all();
            }
        }
        if plan.form_late {
            let n = self.n;
            self.until("the late replicas to be ACTIVE", |w| {
                (0..n).all(|c| w.active_count(c) == (n, true))
            });
        }
        let mut stable = 0;
        for i in 0..500 {
            self.step_all();
            let d0 = self.dump(0);
            let mut equal = true;
            for c in 1..self.n {
                if self.dump(c) != d0 {
                    equal = false;
                }
            }
            stable = if equal { stable + 1 } else { 0 };
            if stable >= 8 {
                return;
            }
            if i == 499 {
                let mut msg = String::new();
                for c in 0..self.n {
                    msg.push_str(&format!("\n  region {c}: {:?}", self.dump(c)));
                }
                panic!(
                    "seed={}: ORACLE-convergence: regions never converged{msg}",
                    self.seed
                );
            }
        }
    }

    /// The cell really exercised the fault it names.
    fn expectations(&mut self, plan: &Plan) {
        match plan.expect {
            Expect::Nothing => {}
            Expect::Resync => {
                let mut total = 0;
                for c in 0..self.n {
                    for node in 0..NODES {
                        total += self.w.clusters[c]
                            .handle()
                            .env(node)
                            .metrics()
                            .get(Metric::MrecResyncTotal);
                    }
                }
                assert!(
                    total >= 1,
                    "seed={}: no past-retention resync happened: the cell proves nothing",
                    self.seed
                );
            }
            Expect::Split => {
                let n = self.up(0);
                let active = self.w.clusters[0]
                    .metadata(n)
                    .tablets_for_table(TABLE)
                    .filter(|(_, t)| t.state == animus_tablet::TabletState::Active)
                    .count();
                assert!(
                    active >= 2,
                    "seed={}: the table never split: the cell proves nothing",
                    self.seed
                );
            }
            Expect::Crash => assert!(
                self.crashes >= 1,
                "seed={}: no node was ever crashed: the cell proves nothing",
                self.seed
            ),
        }
    }

    // ---- the oracle -------------------------------------------------------

    fn check(&mut self, plan: &Plan) -> Dump {
        let dumps: Vec<Dump> = (0..self.n).map(|c| self.dump(c)).collect();
        for c in 1..self.n {
            assert_eq!(
                dumps[c], dumps[0],
                "seed={}: ORACLE-convergence: region {c} differs from region 0",
                self.seed
            );
        }
        let fin = dumps[0].clone();
        for k in &self.m.acked_keys {
            assert!(
                fin.contains_key(k),
                "seed={}: ORACLE-loss: acked key {k} is absent from the converged state",
                self.seed
            );
        }
        for (k, row) in &fin {
            let attempted = self.m.attempts.get(k);
            if let Some(v) = &row.val {
                assert!(
                    attempted.is_some_and(|a| a.contains(v)),
                    "seed={}: ORACLE-provenance: {k} holds {v}, a value nobody wrote",
                    self.seed
                );
            }
            if let Some(f) = self.m.floor.get(k) {
                let mut floor = *f;
                if plan.bite {
                    floor.wall_ms += 1; // the negative control: a wrong expectation
                }
                assert!(
                    row.ver >= floor,
                    "seed={}: ORACLE-floor: {k} ends at {:?}, below an observed acked stamp {floor:?}",
                    self.seed,
                    row.ver
                );
            }
            if let Some(val) = self.m.exact.get(k).and_then(|m| m.get(&row.ver)) {
                assert_eq!(
                    &row.val, val,
                    "seed={}: ORACLE-lww: {k} at stamp {:?} holds a different value than the write that took it",
                    self.seed, row.ver
                );
            }
        }
        if plan.ttl {
            for (k, row) in &fin {
                assert!(
                    !row.item.contains("\"ttl\""),
                    "seed={}: ORACLE-ttl: {k} still holds an expired TTL item",
                    self.seed
                );
            }
        }
        // No re-replication.
        {
            let wan = self.wan.lock().expect("wan");
            assert!(
                wan.foreign.is_empty(),
                "seed={}: ORACLE-echo: {} record(s) shipped toward a region that is not their origin, e.g. {:?}",
                self.seed,
                wan.foreign.len(),
                wan.foreign.first()
            );
            if plan.clean {
                for (c, sent) in &wan.records_from {
                    let own = self.m.attempted_by[*c] + (PRE + plan.keys) as u64;
                    let bound = own * (self.n as u64 - 1) * (self.n as u64 + 1);
                    assert!(
                        *sent <= bound,
                        "seed={}: ORACLE-amplification: region {c} sent {sent} records, bound {bound}",
                        self.seed
                    );
                }
            }
        }
        if self.split {
            // The fixture spawns no seal loop: seal the split parents' stream
            // shards by hand (quiesced: no bridge traffic is in flight), then
            // re-align the world clock the member simulators drove past.
            for c in 0..self.n {
                for node in 0..NODES {
                    self.w.clusters[c].drive_stream_seal(node);
                }
            }
            self.w.sync_clocks();
        }
        for c in 0..self.n {
            self.check_stream(c, &fin);
        }
        fin
    }

    /// Stream parity: the region's stream carries an event with every
    /// converged value (a `REMOVE` for a tombstone). Coalescing is allowed.
    fn check_stream(&mut self, c: usize, fin: &Dump) {
        let node = self.up(c);
        let handle = self.w.clusters[c].handle();
        let seed = self.seed;
        let events = self
            .w
            .drive(c, node, Duration::from_secs(120), async move {
                let jv = |s: &str| serde_json::from_str::<serde_json::Value>(s).expect("json");
                let (st, r) = handle
                    .dynamo(
                        node,
                        "DynamoDB_20120810.DescribeTable",
                        format!(r#"{{"TableName":"{TABLE}"}}"#).as_bytes(),
                    )
                    .await;
                if st != 200 { return Err(format!("DescribeTable: {r}")); }
                let arc = jv(&r)["Table"]["LatestStreamArn"]
                    .as_str()
                    .expect("stream arn")
                    .to_owned();
                let (st, r) = handle
                    .dynamo_streams(
                        node,
                        "DynamoDBStreams_20120810.DescribeStream",
                        format!(r#"{{"StreamArn":"{arc}"}}"#).as_bytes(),
                    )
                    .await;
                if st != 200 { return Err(format!("DescribeStream: {r}")); }
                let shards: Vec<String> = jv(&r)["StreamDescription"]["Shards"]
                    .as_array()
                    .cloned()
                    .unwrap_or_default()
                    .iter()
                    .map(|s| s["ShardId"].as_str().expect("shard").to_owned())
                    .collect();
                let mut ev: Vec<(String, String, Option<String>)> = Vec::new();
                if std::env::var("ANIMUS_MREC_DEBUG").is_ok() { eprintln!("DEBUG shards {shards:?}"); }
                for sh in shards {
                    let (st, r) = handle
                        .dynamo_streams(
                            node,
                            "DynamoDBStreams_20120810.GetShardIterator",
                            format!(
                                r#"{{"StreamArn":"{arc}","ShardId":"{sh}","ShardIteratorType":"TRIM_HORIZON"}}"#
                            )
                            .as_bytes(),
                        )
                        .await;
                    if st != 200 { return Err(format!("GetShardIterator {sh}: {r}")); }
                    let mut it = jv(&r)["ShardIterator"].as_str().map(str::to_owned);
                    if std::env::var("ANIMUS_MREC_DEBUG").is_ok() { eprintln!("DEBUG shard {sh} iter {it:?}"); }
                    while let Some(i) = it.take() {
                        let (st, r) = handle
                            .dynamo_streams(
                                node,
                                "DynamoDBStreams_20120810.GetRecords",
                                format!(r#"{{"ShardIterator":"{i}","Limit":1000}}"#).as_bytes(),
                            )
                            .await;
                        if st != 200 { return Err(format!("GetRecords: {r}")); }
                        let v = jv(&r);
                        let recs = v["Records"].as_array().cloned().unwrap_or_default();
                        if recs.is_empty() {
                            break;
                        }
                        for rec in recs {
                            let d = &rec["dynamodb"];
                            ev.push((
                                rec["eventName"].as_str().unwrap_or("").to_owned(),
                                d["Keys"]["pk"]["S"].as_str().unwrap_or("").to_owned(),
                                d["NewImage"]["v"]["S"].as_str().map(str::to_owned),
                            ));
                        }
                        it = v["NextShardIterator"].as_str().map(str::to_owned);
                    }
                }
                Ok::<_, String>(ev)
            })
            .unwrap_or_else(|| panic!("seed={seed}: stream read on {c} did not finish"))
            .unwrap_or_else(|e| panic!("seed={seed}: stream read on {c}: {e}"));
        for (k, row) in fin {
            if row.ver.wall_ms == INITIAL_COPY_WALL_MS {
                continue; // written before the table was global: no stream event
            }
            let ok = match &row.val {
                Some(v) => events
                    .iter()
                    .any(|(_, pk, nv)| pk == k && nv.as_deref() == Some(v.as_str())),
                // A tombstone: a REMOVE, unless this stream never saw the key
                // as a live row at all (the delete arrived for a missing row).
                None => {
                    events.iter().any(|(e, pk, _)| pk == k && e == "REMOVE")
                        || !events.iter().any(|(_, pk, _)| pk == k)
                }
            };
            assert!(
                ok,
                "seed={}: ORACLE-stream: region {c}'s stream has no event for {k} = {:?}",
                self.seed, row.val
            );
        }
    }
}

fn upd_body(k: &str, v: &str, cond: Option<&str>) -> String {
    let c = cond
        .map(|c| format!(r#","ConditionExpression":"{c}""#))
        .unwrap_or_default();
    format!(
        r#"{{"TableName":"{TABLE}","Key":{{"pk":{{"S":"{k}"}},"sk":{{"S":"s"}}}},"UpdateExpression":"SET v = :v","ExpressionAttributeValues":{{":v":{{"S":"{v}"}}}}{c}}}"#
    )
}

fn cond_put(k: &str, v: &str) -> String {
    format!(
        r#"{{"TableName":"{TABLE}","Item":{{"pk":{{"S":"{k}"}},"sk":{{"S":"s"}},"v":{{"S":"{v}"}}}},"ConditionExpression":"attribute_not_exists(pk)"}}"#
    )
}

fn txn_body(k1: &str, v: &str, k2: &str) -> String {
    format!(
        r#"{{"TransactItems":[{{"Put":{{"TableName":"{TABLE}","Item":{{"pk":{{"S":"{k1}"}},"sk":{{"S":"s"}},"v":{{"S":"{v}"}}}}}}}},{{"Delete":{{"TableName":"{TABLE}","Key":{{"pk":{{"S":"{k2}"}},"sk":{{"S":"s"}}}}}}}}]}}"#
    )
}

// ---------------------------------------------------------------------------
// One run
// ---------------------------------------------------------------------------

fn run(seed: u64, plan: &Plan) -> (Dump, u64) {
    let mut w = W::new(seed, plan);
    w.form(plan);
    for i in 0..plan.ops {
        let due: Vec<Fault> = plan
            .faults
            .iter()
            .filter(|(at, _)| *at == i)
            .map(|(_, f)| f.clone())
            .collect();
        for f in due {
            w.fault(&f);
        }
        w.one_op(plan, i);
        if plan.burst {
            // A round: one write per region before anything ships, so the
            // regions' writes are genuinely concurrent.
            if (i + 1) % plan.regions == 0 {
                w.step_all();
            }
            continue;
        }
        if w.rnd(3) == 0 {
            w.step_all();
        } else {
            let ms = w.rnd(120);
            w.w.run_for(Duration::from_millis(ms));
        }
    }
    w.quiesce(plan);
    if std::env::var("ANIMUS_MREC_DEBUG").is_ok() {
        for c in 0..w.n {
            eprintln!("DEBUG region {c}: {:?}", w.dump(c));
        }
        let wan = w.wan.lock().expect("wan");
        eprintln!(
            "DEBUG records_from {:?} floor {:?}",
            wan.records_from, w.m.floor
        );
    }
    let fin = w.check(plan);
    w.expectations(plan);
    let fp = w.w.fingerprint();
    (fin, fp)
}

fn plan_named(n: &str) -> Plan {
    plans()
        .into_iter()
        .find(|p| p.name == n)
        .unwrap_or_else(|| panic!("no cell {n}"))
}

fn run_cell(name: &str) {
    if !cell_selected(name) {
        return;
    }
    let plan = plan_named(name);
    for seed in seeds() {
        eprintln!("sim_world_mrec_corpus: cell {name} seed {seed}");
        if let Err(e) = catch_unwind(AssertUnwindSafe(|| {
            run(seed, &plan);
        })) {
            eprintln!("sim_world_mrec_corpus: FAILED cell={name} seed={seed}");
            std::panic::resume_unwind(e);
        }
    }
}

macro_rules! cell_tests {
    ($($test:ident => $label:literal),+ $(,)?) => {
        $(
            #[test]
            fn $test() {
                run_cell($label);
            }
        )+
    };
}

cell_tests! {
    sim_world_mrec_corpus_steady_two_regions => "steady_two_regions",
    sim_world_mrec_corpus_steady_three_regions_full_mesh => "steady_three_regions_full_mesh",
    sim_world_mrec_corpus_partition_heal_two_regions => "partition_heal_two_regions",
    sim_world_mrec_corpus_asymmetric_partition_heal => "asymmetric_partition_heal",
    sim_world_mrec_corpus_loss_dup_reorder => "loss_dup_reorder",
    sim_world_mrec_corpus_crash_restart_shipping_leader => "crash_restart_shipping_leader",
    sim_world_mrec_corpus_crash_restart_receiving_leader => "crash_restart_receiving_leader",
    sim_world_mrec_corpus_leader_churn_three_regions => "leader_churn_three_regions",
    sim_world_mrec_corpus_region_down_past_retention_resync => "region_down_past_retention_resync",
    sim_world_mrec_corpus_concurrent_conflicting_writes => "concurrent_conflicting_writes",
    sim_world_mrec_corpus_delete_put_race => "delete_put_race",
    sim_world_mrec_corpus_ttl_expiry_two_regions => "ttl_expiry_two_regions",
    sim_world_mrec_corpus_hlc_skew_within_bound => "hlc_skew_within_bound",
    sim_world_mrec_corpus_hlc_skew_beyond_bound => "hlc_skew_beyond_bound",
    sim_world_mrec_corpus_split_while_shipping => "split_while_shipping",
    sim_world_mrec_corpus_add_replica_under_load => "add_replica_under_load",
    sim_world_mrec_corpus_txn_region_local => "txn_region_local",
}

/// A seed run twice yields the identical final state and the identical
/// simulator fingerprint (LWW determinism, oracle 6).
#[test]
fn sim_world_mrec_corpus_determinism() {
    if !cell_selected("determinism") {
        return;
    }
    let plan = plan_named("loss_dup_reorder");
    let seed = seeds()[0];
    let (d1, f1) = run(seed, &plan);
    let (d2, f2) = run(seed, &plan);
    assert_eq!(d1, d2, "seed={seed}: final states differ between two runs");
    assert_eq!(f1, f2, "seed={seed}: fingerprints differ between two runs");
}

// ---------------------------------------------------------------------------
// Negative controls
// ---------------------------------------------------------------------------

/// Resets every negative-control switch when a run ends, panic included.
struct Switches;
impl Switches {
    fn none() -> Self {
        Switches
    }
    fn lww_by_arrival() -> Self {
        animus_cp_data::mrec_test_switch::set_lww_by_arrival(true);
        Switches
    }
    fn advance_before_ack() -> Self {
        neg::set_advance_before_ack(true);
        Switches
    }
    fn ship_foreign() -> Self {
        neg::set_ship_foreign(true);
        Switches
    }
}
impl Drop for Switches {
    fn drop(&mut self) {
        animus_cp_data::mrec_test_switch::set_lww_by_arrival(false);
        neg::set_advance_before_ack(false);
        neg::set_ship_foreign(false);
    }
}

/// Run `plan` over the seeds with a mechanism disabled; every seed must trip
/// the oracle named by `tag`.
fn must_fail(plan: &Plan, tag: &str, guard: impl Fn() -> Switches) {
    for seed in seeds() {
        let _g = guard();
        let r = catch_unwind(AssertUnwindSafe(|| {
            run(seed, plan);
        }));
        let Err(e) = r else {
            panic!(
                "seed={seed}: the oracle did not notice the disabled mechanism in {}",
                plan.name
            );
        };
        let msg = e
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| e.downcast_ref::<&str>().map(|s| (*s).to_owned()))
            .unwrap_or_default();
        let head: String = msg.chars().take(260).collect();
        eprintln!("sim_world_mrec_corpus: negative control failed as required: {head}");
        assert!(
            msg.contains(tag),
            "seed={seed}: failed, but not through {tag}: {head}"
        );
    }
}

/// Last-delivered-wins instead of last-writer-wins: concurrent conflicting
/// writes over a reordering, duplicating WAN leave the regions disagreeing.
#[test]
fn mrec_negative_lww_by_arrival() {
    must_fail(
        &plan_named("concurrent_conflicting_writes"),
        "ORACLE-",
        Switches::lww_by_arrival,
    );
}

/// The cursor advances even when the peer did not acknowledge the batch: rows
/// shipped into a partition are never re-sent and the regions never converge.
#[test]
fn mrec_negative_cursor_before_ack() {
    must_fail(
        &plan_named("partition_heal_two_regions"),
        "ORACLE-convergence",
        Switches::advance_before_ack,
    );
}

/// Loop prevention off: a region ships rows another region originated.
#[test]
fn mrec_negative_loop_prevention_off() {
    let mut p = plan_named("steady_two_regions");
    p.only_a = true;
    must_fail(&p, "ORACLE-echo", Switches::ship_foreign);
}

/// The oracle itself bites: a wrong expectation fails a healthy run.
#[test]
fn mrec_negative_oracle_bites() {
    let mut p = plan_named("steady_two_regions");
    p.bite = true;
    must_fail(&p, "ORACLE-floor", Switches::none);
}
