//! `sim_cluster_mrsc_corpus` — G-01 stage G-c, M4: the seed-reproducible
//! fault-injecting **cluster-tier corpus of MRSC stretch tables** over
//! [`SimCluster`] (ADR 0075 sections 3, 5, 6; plan section 4).
//!
//! Six combined nodes, two per Region (`topology.kubernetes.io/region` =
//! `r-a`/`r-b`/`r-c`, node ids `n/2` so the first two ids share a Region — a
//! naive "first RF by id" placement would put two replicas in `r-a`), every
//! inter-Region link a WAN link (60/75/90 ms one-way, jitter and a heavy tail,
//! `SimCluster::set_link_net_config`). The cluster is rolled to cluster
//! version 2 through the real `cluster-version/finalize` admin path, then a
//! table is created and converted over the real DynamoDB wire edge
//! (`UpdateTable` `MultiRegionConsistency: STRONG` + `ReplicaUpdates`), so the
//! pinned placement, the preferred-leader reconciler step and witness hiding
//! are what is under test — not a hand-seeded tablet.
//!
//! Cells (each a fixed script, seed-expanded by `ANIMUS_MRSC_SEEDS`,
//! `ANIMUS_MRSC_CELL=<substring>` narrows, `ANIMUS_SEED=<seed>` replays one):
//!
//! - `steady`: converges (one replica per Region, `ACTIVE`, leader in the
//!   preferred Region); acked writes and `ConsistentRead: true` reads from a
//!   client on **every** node (forwarded writes); the preferred Region is moved
//!   twice (`SetGlobalPreferredLeader`) and the leader follows.
//! - `region_loss_leader_region`: both nodes of the leader's (preferred)
//!   Region die. The majority Regions keep committing, **no cross-Region
//!   repair** happens (the strict pin: the lost Region's replica waits for its
//!   Region), and when the Region returns the leader goes home.
//! - `region_loss_follower_region`: the same for a Region holding a follower.
//! - `region_partition_heal`: the leader's Region is cut off from the other
//!   two (its nodes still see each other). The isolated side cannot ack a
//!   strong write, the majority side keeps committing, and after the heal the
//!   leader returns to the preferred Region.
//! - `split_under_mrsc`: auto-split mid-run; every child keeps one replica per
//!   Region, the pinned policy and the preferred leader.
//! - `in_region_node_replacement`: a node holding a replica dies for good;
//!   repair re-places the replica on **the other node of the same Region**
//!   (still one per Region), never in another Region.
//! - `witness_form_region_loss`: the two-replicas-plus-witness form: the
//!   witness Region never keeps the leader in steady state (even when the
//!   preferred Region is lost and the witness is the only other voter in
//!   reach), and losing the other full-replica Region keeps a quorum.
//! - `drain_last_node_of_region_refused`: the decommission guard (plan D10).
//!
//! Oracles, all converged-or-timeout (never a one-shot): every acked write is
//! read back by a `ConsistentRead: true` read; the `ConsistentRead: true`
//! register never goes backwards or returns a value never written
//! ([`Register`]); the placement invariant (exactly one replica in each of the
//! three Regions) holds at every phase boundary; the leader converges to the
//! preferred Region; replicas catch up to their commit index.
//!
//! **Negative controls** (each must fail the oracle it names; they run in the
//! default suite):
//! - `mrsc_negative_preferred_leader_disabled`: the preferred-leader step fed
//!   nothing leaves the leader outside the preferred Region;
//! - `mrsc_negative_unpinned_policy`: a plain (never converted) table over the
//!   same cluster violates the one-replica-per-Region oracle and loses its
//!   quorum when a Region dies;
//! - `mrsc_negative_checker_bites`: the register checker rejects a stale read
//!   and a never-written value.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use animus_control::Metadata;
use animus_control::sim_versions::BinaryProfile;
use animus_control::version::VersionRange;
use animus_env::{NodeId, nid};
use animus_placement::REGION_LABEL;
use animus_sim::NetConfig;
use animus_tablet::TabletId;
use serde_json::Value;

use super::AutoSplitThresholds;
use super::sim_cluster::{SimCluster, set_preferred_leader_disabled};

const REGIONS: [&str; 3] = ["r-a", "r-b", "r-c"];
const NODES: u64 = 6;
/// Virtual-time budget of every converged-or-timeout poll.
const CONVERGE: Duration = Duration::from_secs(240);
const JITTER: Duration = Duration::from_millis(30);
const TAIL_JITTER: Duration = Duration::from_millis(250);
const TAIL_PROB: f64 = 0.10;

/// Every seed of the run (`ANIMUS_SEED` replay, else the depth knob), before
/// `ANIMUS_SEED_SHARD` takes its slice.
fn all_seeds() -> Vec<u64> {
    if let Some(s) = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
    {
        return vec![s];
    }
    let k = animus_test::corpus::seeds_from_env("ANIMUS_MRSC_SEEDS") as u64;
    (0..k).map(|i| 0x6D00_0000 + i).collect()
}

/// This worker's seeds (`ANIMUS_SEED_SHARD`; a replay is never sharded).
fn seeds() -> Vec<u64> {
    if std::env::var("ANIMUS_SEED").is_ok() {
        return all_seeds();
    }
    animus_test::corpus::shard_retain(all_seeds())
}

fn cell_selected(name: &str) -> bool {
    std::env::var("ANIMUS_MRSC_CELL")
        .ok()
        .is_none_or(|f| f.is_empty() || name.contains(&f))
}

fn region_of_node(n: u64) -> &'static str {
    REGIONS[(n / 2) as usize]
}

fn nodes_of_region(r: &str) -> Vec<u64> {
    (0..NODES).filter(|n| region_of_node(*n) == r).collect()
}

fn node_index(id: &NodeId) -> u64 {
    id.to_string()
        .trim_start_matches('n')
        .parse()
        .expect("nN node id")
}

fn labels() -> Vec<BTreeMap<String, String>> {
    (0..NODES)
        .map(|n| BTreeMap::from([(REGION_LABEL.to_owned(), region_of_node(n).to_owned())]))
        .collect()
}

/// The link config between two Regions: 60/75/90 ms one-way, symmetric.
fn wan_cfg(a: &str, b: &str) -> NetConfig {
    let ms = match (a.min(b), a.max(b)) {
        ("r-a", "r-b") => 60,
        ("r-a", "r-c") => 75,
        _ => 90,
    };
    let mut cfg = NetConfig::default();
    cfg.base_delay = Duration::from_millis(ms);
    cfg.max_jitter = JITTER;
    cfg.heavy_tail_max_jitter = TAIL_JITTER;
    cfg.set_heavy_tail_prob(TAIL_PROB);
    cfg
}

fn apply_wan(cluster: &mut SimCluster) {
    for i in 0..NODES {
        for j in 0..NODES {
            let (ri, rj) = (region_of_node(i), region_of_node(j));
            if ri != rj {
                cluster.set_link_net_config(i, j, wan_cfg(ri, rj));
            }
        }
    }
}

/// Resets the thread-local negative-control switch when a run ends, panic
/// included, so one cell can never leak it into the next.
struct PreferredLeaderOff;
impl PreferredLeaderOff {
    fn new() -> Self {
        set_preferred_leader_disabled(true);
        Self
    }
}
impl Drop for PreferredLeaderOff {
    fn drop(&mut self) {
        set_preferred_leader_disabled(false);
    }
}

// ---------------------------------------------------------------------------
// The register checker (the linearizability oracle for the hot key)
// ---------------------------------------------------------------------------

/// A single hot register written with monotonically numbered values.
///
/// A `ConsistentRead: true` read must return a value that was **attempted**
/// (never an invented one) and whose number is **at least the last acked
/// one**, unless it is the number of an attempt that was never acked (an
/// in-flight timed-out write may land at any point: it is concurrent with
/// everything after it). Reading nothing after an acked write is a lost write.
#[derive(Default)]
struct Register {
    last_acked: Option<u64>,
    attempted: BTreeSet<u64>,
    unacked: BTreeSet<u64>,
}

impl Register {
    fn attempt(&mut self, n: u64) {
        self.attempted.insert(n);
        self.unacked.insert(n);
    }
    fn acked(&mut self, n: u64) {
        self.unacked.remove(&n);
        if self.last_acked.is_none_or(|l| n > l) {
            self.last_acked = Some(n);
        }
    }
    fn check_read(&self, read: Option<u64>) -> Result<(), String> {
        match (read, self.last_acked) {
            (None, None) => Ok(()),
            (None, Some(a)) => Err(format!("read nothing after write {a} was acked")),
            (Some(r), _) if !self.attempted.contains(&r) => {
                Err(format!("read {r}, which was never written"))
            }
            (Some(r), Some(a)) if r < a && !self.unacked.contains(&r) => {
                Err(format!("stale read {r} after write {a} was acked"))
            }
            _ => Ok(()),
        }
    }
}

// ---------------------------------------------------------------------------
// The run: a cluster plus the client-visible history
// ---------------------------------------------------------------------------

struct Run {
    c: SimCluster,
    seed: u64,
    dead: BTreeSet<u64>,
    /// (table, key) -> value, for every ACKED write of a unique key.
    acked: BTreeMap<(String, String), String>,
    register: BTreeMap<String, Register>,
    next: u64,
}

fn call(c: &mut SimCluster, node: u64, op: &str, body: &str) -> (u16, Value) {
    let (status, resp) = c.dynamo_fast(node, &format!("DynamoDB_20120810.{op}"), body.as_bytes());
    let v = serde_json::from_str(&resp).unwrap_or(Value::String(resp));
    (status, v)
}

impl Run {
    fn live(&self) -> Vec<u64> {
        (0..NODES).filter(|n| !self.dead.contains(n)).collect()
    }

    /// Converged-or-timeout over virtual time.
    fn poll(
        &mut self,
        what: &str,
        mut done: impl FnMut(&mut SimCluster) -> Result<(), String>,
    ) -> Result<(), String> {
        let _ = what;
        let mut waited = Duration::ZERO;
        loop {
            match done(&mut self.c) {
                Ok(()) => return Ok(()),
                Err(e) if waited >= CONVERGE => return Err(e),
                Err(_) => {
                    self.c.run_for(Duration::from_millis(500));
                    waited += Duration::from_millis(500);
                }
            }
        }
    }

    fn must(&mut self, what: &str, done: impl FnMut(&mut SimCluster) -> Result<(), String>) {
        let seed = self.seed;
        if let Err(e) = self.poll(what, done) {
            let dump = self.dump();
            panic!("seed={seed}: {what} never converged: {e}\n{dump}");
        }
    }

    /// Each node's own view (the convergence-timeout lesson: a poll that
    /// gives up must show every replica's Raft state, not just one).
    fn dump(&mut self) -> String {
        let mut out = String::new();
        for n in 0..NODES {
            let meta = self.c.metadata(n);
            let tablets: Vec<String> = meta
                .tablets
                .iter()
                .map(|(i, t)| {
                    let rs: Vec<u64> = t.replicas.iter().map(node_index).collect();
                    let leaders: Vec<u64> = (0..NODES)
                        .filter(|m| self.c.is_leader_local(*m, *i))
                        .collect();
                    format!("{i:?}:{:?}:{rs:?} led-by {leaders:?}", t.state)
                })
                .collect();
            out.push_str(&format!(
                "  n{n} dead={} control_raft={:?} members={} tablets={tablets:?} groups={:?}\n",
                self.dead.contains(&n),
                self.c.control_raft_indices(n),
                meta.members.len(),
                self.c.group_states(n),
            ));
        }
        out
    }

    fn crash_region(&mut self, region: &str) {
        for n in nodes_of_region(region) {
            self.c.crash(n);
            self.dead.insert(n);
        }
    }

    fn restart_region(&mut self, region: &str) {
        for n in nodes_of_region(region) {
            self.c.restart(n);
            self.dead.remove(&n);
        }
    }

    // ---- bring-up ---------------------------------------------------------

    fn boot(seed: u64) -> Run {
        Run::boot_at(seed, 2)
    }

    /// [`boot`](Self::boot) rolled to cluster `version` (every node a
    /// `Release(version)` binary, finalized one step at a time through the
    /// real admin path): `2` is the G-c surface every pre-existing cell runs
    /// on, `7` adds `Gate::MrscReplicaRemoval` (the replica-removal cells).
    fn boot_at(seed: u64, version: u32) -> Run {
        let mut c = SimCluster::new_with_node_labels_lsm(seed, 3, labels());
        apply_wan(&mut c);
        let _ = c.control_leader_index();
        c.set_all_node_versions(Some(VersionRange::new(1, 2)));
        // A restart re-applies the node's recorded profile (default `Phase1`,
        // a binary that cannot decode the version-2 batches), so every node
        // is also given `Release(2)` — the current binary — or a restarted
        // node would never rejoin (harness bug found in M4, see the lessons
        // log).
        for n in 0..NODES {
            c.set_binary_profile(n, BinaryProfile::Release(2));
        }
        let mut run = Run {
            c,
            seed,
            dead: BTreeSet::new(),
            acked: BTreeMap::new(),
            register: BTreeMap::new(),
            next: 0,
        };
        run.must("the era", |c| {
            if (0..NODES).all(|n| {
                c.features(n).era_active() && c.metadata(n).node_versions.len() == NODES as usize
            }) {
                Ok(())
            } else {
                Err("era not active everywhere".into())
            }
        });
        for step in 2..=version {
            if step > 2 {
                // Roll the whole cluster to the next binary (the model's
                // `Release(n)` range is `[n-1, n]`, so it can only be played
                // once the cluster is at `n-1`), wait for every record, then
                // finalize.
                for n in 0..NODES {
                    run.c.set_binary_profile(n, BinaryProfile::Release(step));
                    run.c.set_node_version(n, Some(VersionRange::new(1, step)));
                }
                run.must(&format!("[1,{step}] records"), |c| {
                    if (0..NODES).all(|n| {
                        let m = c.metadata(n);
                        m.node_versions.len() == NODES as usize
                            && m.node_versions
                                .values()
                                .all(|v| v.range == VersionRange::new(1, step))
                    }) {
                        Ok(())
                    } else {
                        Err(format!("not every node recorded at [1,{step}]"))
                    }
                });
            }
            let leader = {
                let idx = run.c.control_leader_index();
                run.c.control_node_id(idx)
            };
            let body = format!(r#"{{"to":{step},"expected":{}}}"#, step - 1);
            let (s, b) = run.c.admin(
                leader,
                "POST",
                "/admin/cluster-version/finalize",
                "",
                body.as_bytes(),
            );
            assert_eq!(s, 200, "seed={seed}: finalize to {step}: {b}");
            run.must(&format!("cluster version {step}"), |c| {
                if (0..NODES).all(|n| c.features(n).cluster_version() == step) {
                    Ok(())
                } else {
                    Err(format!("not at version {step}"))
                }
            });
        }
        run
    }

    fn create_table(&mut self, table: &str) {
        let body = format!(
            r#"{{"TableName":"{table}",
                "KeySchema":[{{"AttributeName":"pk","KeyType":"HASH"}}],
                "AttributeDefinitions":[{{"AttributeName":"pk","AttributeType":"S"}}]}}"#
        );
        let (s, v) = call(&mut self.c, 0, "CreateTable", &body);
        assert_eq!(s, 200, "seed={}: CreateTable {table}: {v}", self.seed);
    }

    /// Convert `table` (created from node 0, so `r-a` is the preferred Region)
    /// over the wire: `witness` selects the two-plus-witness form
    /// (`r-c` witness).
    fn convert(&mut self, table: &str, witness: bool) {
        let rest = if witness {
            r#""ReplicaUpdates":[{"Create":{"RegionName":"r-b"}}],"GlobalTableWitnessUpdates":[{"Create":{"RegionName":"r-c"}}]"#
        } else {
            r#""ReplicaUpdates":[{"Create":{"RegionName":"r-b"}},{"Create":{"RegionName":"r-c"}}]"#
        };
        let body = format!(r#"{{"TableName":"{table}","MultiRegionConsistency":"STRONG",{rest}}}"#);
        let (s, v) = call(&mut self.c, 0, "UpdateTable", &body);
        assert_eq!(s, 200, "seed={}: convert {table}: {v}", self.seed);
    }

    /// A converted table: created, converted, and polled to the steady state.
    fn global_table(&mut self, table: &str, witness: bool) {
        self.create_table(table);
        self.convert(table, witness);
        self.converged(table, "r-a");
    }

    // ---- oracles ----------------------------------------------------------

    /// The steady state of `table`: placement invariant, `ACTIVE` replicas,
    /// and the leader of every tablet in the `preferred` Region.
    fn converged(&mut self, table: &str, preferred: &str) {
        let (t, p) = (table.to_owned(), preferred.to_owned());
        let dead = self.dead.clone();
        self.must(
            &format!("{table} steady state (leader in {preferred})"),
            move |c| {
                let live = (0..NODES).find(|n| !dead.contains(n)).expect("a live node");
                let meta = c.metadata(live);
                one_per_region(&meta, &t)?;
                leaders_in(c, &meta, &t, &dead, &p)
            },
        );
    }

    fn placement(&mut self, table: &str) {
        let live = self.live()[0];
        let meta = self.c.metadata(live);
        one_per_region(&meta, table)
            .unwrap_or_else(|e| panic!("seed={}: placement invariant {table}: {e}", self.seed));
    }

    // ---- workload ---------------------------------------------------------

    /// One put of a fresh unique key, retried across `via` nodes; records the
    /// ack. `true` iff acked.
    fn put_unique(&mut self, table: &str, via: &[u64], tries: usize) -> bool {
        self.next += 1;
        let (k, v) = (format!("k{}", self.next), format!("v{}", self.next));
        let body =
            format!(r#"{{"TableName":"{table}","Item":{{"pk":{{"S":"{k}"}},"v":{{"S":"{v}"}}}}}}"#);
        for i in 0..tries {
            let node = via[(self.next as usize + i) % via.len()];
            let r = call(&mut self.c, node, "PutItem", &body);
            if r.0 == 200 {
                self.acked.insert((table.to_owned(), k), v);
                return true;
            }
        }
        false
    }

    /// Acked writes from a client on every given node (each must ack).
    fn write_round(&mut self, table: &str, via: &[u64]) {
        for &n in via {
            assert!(
                self.put_unique(table, &[n], 30),
                "seed={}: a put on {table} via n{n} never acked",
                self.seed
            );
        }
    }

    fn strong_get(&mut self, node: u64, table: &str, k: &str) -> Option<String> {
        let body = format!(
            r#"{{"TableName":"{table}","Key":{{"pk":{{"S":"{k}"}}}},"ConsistentRead":true}}"#
        );
        let (s, v) = call(&mut self.c, node, "GetItem", &body);
        (s == 200)
            .then(|| v["Item"]["v"]["S"].as_str().map(str::to_owned))
            .flatten()
    }

    /// Every acked unique key reads back its value, via each of `via`.
    fn check_durable(&mut self, via: &[u64]) {
        let acked: Vec<_> = self.acked.clone().into_iter().collect();
        for ((t, k), v) in acked {
            for &n in via {
                let mut got = None;
                for _ in 0..30 {
                    got = self.strong_get(n, &t, &k);
                    if got.is_some() {
                        break;
                    }
                }
                if got.as_deref() != Some(v.as_str()) {
                    let body = format!(
                        r#"{{"TableName":"{t}","Key":{{"pk":{{"S":"{k}"}}}},"ConsistentRead":true}}"#
                    );
                    let last = call(&mut self.c, n, "GetItem", &body);
                    let (seed, dump) = (self.seed, self.dump());
                    panic!(
                        "seed={seed}: acked write {t}/{k} not read back via n{n} (got {got:?}, want {v}); last reply {last:?}\n{dump}"
                    );
                }
            }
        }
    }

    /// Write register value `n` (attempted whether or not it acks).
    fn register_write(&mut self, table: &str, via: &[u64]) -> bool {
        self.next += 1;
        let n = self.next;
        self.register
            .entry(table.to_owned())
            .or_default()
            .attempt(n);
        let body =
            format!(r#"{{"TableName":"{table}","Item":{{"pk":{{"S":"reg"}},"v":{{"S":"{n}"}}}}}}"#);
        for i in 0..via.len().max(1) * 3 {
            let node = via[i % via.len()];
            if call(&mut self.c, node, "PutItem", &body).0 == 200 {
                self.register.get_mut(table).expect("reg").acked(n);
                return true;
            }
        }
        false
    }

    fn register_read_check(&mut self, table: &str, via: &[u64]) {
        for &n in via {
            let mut read = Err("no answer".to_owned());
            for _ in 0..30 {
                let body = format!(
                    r#"{{"TableName":"{table}","Key":{{"pk":{{"S":"reg"}}}},"ConsistentRead":true}}"#
                );
                let (s, v) = call(&mut self.c, n, "GetItem", &body);
                if s == 200 {
                    read = Ok(v["Item"]["v"]["S"]
                        .as_str()
                        .and_then(|x| x.parse::<u64>().ok()));
                    break;
                }
            }
            let reg = self.register.entry(table.to_owned()).or_default();
            match read {
                Ok(r) => reg.check_read(r).unwrap_or_else(|e| {
                    panic!("seed={}: register {table} via n{n}: {e}", self.seed)
                }),
                Err(e) => panic!("seed={}: strong read {table} via n{n}: {e}", self.seed),
            }
        }
    }

    /// The full client workload against `via`: acked writes, the register,
    /// then every acked key read back.
    fn workload(&mut self, table: &str, via: &[u64]) {
        self.write_round(table, via);
        assert!(
            self.register_write(table, via),
            "seed={}: register write on {table} never acked",
            self.seed
        );
        self.register_read_check(table, via);
        self.check_durable(via);
    }

    fn caught_up(&mut self) {
        self.c.await_replicas_caught_up("mrsc corpus");
    }
}

/// The placement invariant: every tablet of `table` has exactly three replicas,
/// one in each Region, and the table is global with all three Regions `ACTIVE`.
fn one_per_region(meta: &Metadata, table: &str) -> Result<(), String> {
    let mut any = false;
    for (id, tab) in meta.tablets_for_table(table) {
        if !tab.is_routable() {
            continue;
        }
        any = true;
        let regions: Vec<String> = tab
            .replicas
            .iter()
            .map(|r| {
                meta.members
                    .get(r)
                    .and_then(|m| m.labels.get(REGION_LABEL).cloned())
                    .unwrap_or_default()
            })
            .collect();
        let set: BTreeSet<&str> = regions.iter().map(String::as_str).collect();
        if tab.replicas.len() != 3 || set.len() != 3 {
            return Err(format!(
                "{table} tablet {id:?}: replicas {:?} span Regions {regions:?}",
                tab.replicas
            ));
        }
    }
    if !any {
        return Err(format!("{table}: no routable tablet yet"));
    }
    if meta.table_global(table).is_none() {
        return Err(format!("{table}: not a global table"));
    }
    if meta.table_ready_regions(table).len() != 3 {
        return Err(format!(
            "{table}: Regions ready {:?}",
            meta.table_ready_regions(table)
        ));
    }
    Ok(())
}

/// The live leader of `tablet`, as `(node, region)`.
fn leader_of(
    c: &SimCluster,
    tablet: TabletId,
    dead: &BTreeSet<u64>,
) -> Option<(u64, &'static str)> {
    (0..NODES)
        .filter(|n| !dead.contains(n))
        .find(|n| c.is_leader_local(*n, tablet))
        .map(|n| (n, region_of_node(n)))
}

/// Every routable tablet of `table` is led from `preferred`.
fn leaders_in(
    c: &SimCluster,
    meta: &Metadata,
    table: &str,
    dead: &BTreeSet<u64>,
    preferred: &str,
) -> Result<(), String> {
    for (id, tab) in meta
        .tablets_for_table(table)
        .filter(|(_, t)| t.is_routable())
    {
        match leader_of(c, *id, dead) {
            Some((_, r)) if r == preferred => {}
            other => {
                return Err(format!(
                    "tablet {id:?} led from {other:?}, want {preferred}"
                ));
            }
        }
        let _ = tab;
    }
    Ok(())
}

/// Re-point the preferred Region through the real admin action
/// (`POST /admin/table/preferred-leader`), sent to a node that is neither
/// the control leader nor in the new Region where possible, so the relayed
/// proposal path is the one exercised.
fn set_preferred(run: &mut Run, table: &str, region: &str) {
    let via = *run.live().last().expect("a live node");
    let body = format!(r#"{{"table":"{table}","region":"{region}"}}"#);
    let (status, resp) = run.c.admin(
        via,
        "POST",
        "/admin/table/preferred-leader",
        "",
        body.as_bytes(),
    );
    assert_eq!(
        status, 200,
        "seed={}: preferred-leader via n{via}: {resp}",
        run.seed
    );
}

/// The admin surface of a converged global table: `/admin/global-tables`
/// and the refusals of `/admin/table/preferred-leader`.
fn check_admin_surface(run: &mut Run, table: &str, preferred: &str) {
    let seed = run.seed;
    let node = run.live()[1];
    let (s, body) = run.c.admin(node, "GET", "/admin/global-tables", "", b"");
    assert_eq!(s, 200, "seed={seed}: global-tables: {body}");
    let v: Value = serde_json::from_str(&body).expect("json");
    assert_eq!(v["enabled"], true, "seed={seed}: {v}");
    let t = v["tables"]
        .as_array()
        .and_then(|a| a.iter().find(|t| t["table"] == table))
        .unwrap_or_else(|| panic!("seed={seed}: {table} not in {v}"));
    assert_eq!(t["consistency"], "STRONG");
    assert_eq!(t["preferred_leader_region"], preferred, "seed={seed}: {t}");
    for r in REGIONS {
        assert_eq!(t["replica_status"][r], "ACTIVE", "seed={seed}: {t}");
    }
    for tab in t["tablets"].as_array().expect("tablets") {
        let regions: BTreeSet<String> = tab["replicas"]
            .as_array()
            .expect("replicas")
            .iter()
            .map(|r| r["region"].as_str().expect("region").to_owned())
            .collect();
        assert_eq!(regions.len(), 3, "seed={seed}: {tab}");
    }
    assert!(
        v["warnings"].as_array().is_some_and(Vec::is_empty),
        "seed={seed}: unexpected warnings {}",
        v["warnings"]
    );
    // Refusals are named, never a bare Rejected.
    for (body, want, why) in [
        (
            format!(r#"{{"table":"{table}","region":"r-z"}}"#),
            400,
            "unknown region",
        ),
        (
            r#"{"table":"nope-nope","region":"r-a"}"#.to_owned(),
            404,
            "not a global table",
        ),
    ] {
        let (s, resp) = run.c.admin(
            node,
            "POST",
            "/admin/table/preferred-leader",
            "",
            body.as_bytes(),
        );
        assert_eq!(s, want, "seed={seed}: {why}: {resp}");
    }
    // Idempotent: the current preferred Region is a no-op success.
    let body = format!(r#"{{"table":"{table}","region":"{preferred}"}}"#);
    let (s, resp) = run.c.admin(
        node,
        "POST",
        "/admin/table/preferred-leader",
        "",
        body.as_bytes(),
    );
    assert_eq!(s, 200, "seed={seed}: idempotent: {resp}");
}

fn all_nodes() -> Vec<u64> {
    (0..NODES).collect()
}

fn tablets_of(c: &mut SimCluster, node: u64, table: &str) -> Vec<TabletId> {
    c.metadata(node)
        .tablets_for_table(table)
        .filter(|(_, t)| t.is_routable())
        .map(|(id, _)| *id)
        .collect()
}

fn leader_region_of_first(run: &mut Run, table: &str) -> &'static str {
    let live = run.live()[0];
    let id = tablets_of(&mut run.c, live, table)[0];
    leader_of(&run.c, id, &run.dead).expect("a leader").1
}

// ---------------------------------------------------------------------------
// Cells
// ---------------------------------------------------------------------------

fn cell_steady(seed: u64) {
    let mut run = Run::boot(seed);
    run.global_table("glob1", false);
    run.workload("glob1", &all_nodes());
    check_admin_surface(&mut run, "glob1", "r-a");
    // The preferred Region moves, the leader follows, twice.
    for to in ["r-b", "r-a"] {
        set_preferred(&mut run, "glob1", to);
        run.converged("glob1", to);
        run.workload("glob1", &all_nodes());
        run.placement("glob1");
    }
    run.caught_up();
}

fn cell_region_loss_leader_region(seed: u64) {
    let mut run = Run::boot(seed);
    run.global_table("glob1", false);
    run.workload("glob1", &all_nodes());
    let before = run
        .c
        .metadata(0)
        .tablets_for_table("glob1")
        .map(|(i, t)| (*i, t.replicas.clone()))
        .collect::<Vec<_>>();
    run.crash_region("r-a");
    let live = run.live();
    // The majority Regions keep committing.
    run.workload("glob1", &live);
    // Long enough for failure detection plus several repair passes.
    run.c.run_for(Duration::from_secs(60));
    // The strict pin: no cross-Region repair, the lost Region's replica waits.
    let after = run
        .c
        .metadata(live[0])
        .tablets_for_table("glob1")
        .map(|(i, t)| (*i, t.replicas.clone()))
        .collect::<Vec<_>>();
    assert_eq!(
        before, after,
        "seed={seed}: a replica of the lost Region was moved to another Region"
    );
    run.workload("glob1", &live);
    run.restart_region("r-a");
    run.converged("glob1", "r-a");
    run.workload("glob1", &all_nodes());
    run.placement("glob1");
    run.caught_up();
}

fn cell_region_loss_follower_region(seed: u64) {
    let mut run = Run::boot(seed);
    run.global_table("glob1", false);
    run.workload("glob1", &all_nodes());
    run.crash_region("r-c");
    let live = run.live();
    run.workload("glob1", &live);
    // The leader stays in the preferred Region throughout.
    run.converged("glob1", "r-a");
    run.restart_region("r-c");
    run.converged("glob1", "r-a");
    run.workload("glob1", &all_nodes());
    run.placement("glob1");
    run.caught_up();
}

fn cell_region_partition_heal(seed: u64) {
    let mut run = Run::boot(seed);
    run.global_table("glob1", false);
    run.workload("glob1", &all_nodes());
    let cut = nodes_of_region("r-a");
    let rest: Vec<u64> = (0..NODES).filter(|n| !cut.contains(n)).collect();
    for a in &cut {
        for b in &rest {
            run.c.partition(*a, *b);
        }
    }
    // The majority side keeps committing (a new leader outside r-a).
    run.workload("glob1", &rest);
    // The isolated side cannot ack a strong write.
    let acked_isolated = run.put_unique("glob1", &cut, 1);
    assert!(
        !acked_isolated,
        "seed={seed}: a strong write acked from a Region cut off from the quorum"
    );
    // Heal: every link back, the WAN profile re-applied.
    let sim = run.c.simulator();
    for a in &cut {
        for b in &rest {
            sim.heal(nid(*a), nid(*b));
        }
    }
    apply_wan(&mut run.c);
    run.converged("glob1", "r-a");
    run.workload("glob1", &all_nodes());
    run.placement("glob1");
    run.caught_up();
}

fn cell_split_under_mrsc(seed: u64) {
    let mut run = Run::boot(seed);
    run.global_table("glob1", false);
    run.workload("glob1", &all_nodes());
    run.c.set_auto_split_thresholds(AutoSplitThresholds {
        bytes: Some(2_000),
        change_rate: None,
        ops_rate: None,
        tablet_capacity_ceilings: Default::default(),
    });
    // Pad writes until the table has split.
    let pad = "x".repeat(300);
    let mut i = 0u64;
    run.must("the table splits", |c| {
        // The SimCluster fixture has no background split driver: the cutover
        // of a `Splitting` parent is driven by hand on every node each tick
        // (a no-op on a node leading no `Splitting` tablet).
        for node in 0..NODES {
            c.drive_inplace_split_cutover(node);
        }
        let meta = c.metadata(0);
        let splitting = meta
            .tablets_for_table("glob1")
            .filter(|(_, t)| t.state == animus_tablet::TabletState::Splitting)
            .count();
        let n = tablets_of(c, 0, "glob1").len();
        if n >= 2 && splitting == 0 {
            return Ok(());
        }
        i += 1;
        let body = format!(
            r#"{{"TableName":"glob1","Item":{{"pk":{{"S":"pad{i}"}},"v":{{"S":"{pad}"}}}}}}"#
        );
        let _ = call(c, i % NODES, "PutItem", &body);
        Err(format!("{n} tablet(s)"))
    });
    run.converged("glob1", "r-a");
    // Every child carries the region pin.
    let meta = run.c.metadata(0);
    for (id, _) in meta
        .tablets_for_table("glob1")
        .filter(|(_, t)| t.is_routable())
    {
        let p = meta.policies.get(id).expect("child policy");
        assert!(
            p.allowed_values.contains_key(REGION_LABEL),
            "seed={seed}: child {id:?} lost the Region pin: {p:?}"
        );
    }
    run.workload("glob1", &all_nodes());
    run.caught_up();
}

fn cell_in_region_node_replacement(seed: u64) {
    let mut run = Run::boot(seed);
    run.global_table("glob1", false);
    run.workload("glob1", &all_nodes());
    // A replica-holding node of r-c dies for good.
    let meta = run.c.metadata(0);
    let (_, tab) = meta.tablets_for_table("glob1").next().expect("tablet");
    let victim = tab
        .replicas
        .iter()
        .map(node_index)
        .find(|n| region_of_node(*n) == "r-c")
        .expect("a replica in r-c");
    let sibling = nodes_of_region("r-c")
        .into_iter()
        .find(|n| *n != victim)
        .expect("sibling");
    run.c.crash(victim);
    run.dead.insert(victim);
    run.must("repair within the Region", |c| {
        let meta = c.metadata(0);
        one_per_region(&meta, "glob1")?;
        let (_, tab) = meta.tablets_for_table("glob1").next().ok_or("no tablet")?;
        let ids: BTreeSet<u64> = tab.replicas.iter().map(node_index).collect();
        if ids.contains(&victim) || !ids.contains(&sibling) {
            return Err(format!(
                "replicas {ids:?}, want {sibling} instead of {victim}"
            ));
        }
        Ok(())
    });
    let live = run.live();
    run.workload("glob1", &live);
    run.converged("glob1", "r-a");
    run.caught_up();
}

fn cell_witness_form_region_loss(seed: u64) {
    let mut run = Run::boot(seed);
    run.global_table("glob1", true);
    run.workload("glob1", &all_nodes());
    // Steady state: the leader is in the preferred Region, never the witness.
    assert_eq!(
        leader_region_of_first(&mut run, "glob1"),
        "r-a",
        "seed={seed}"
    );
    // The other full-replica Region dies: preferred + witness keep a quorum.
    run.crash_region("r-b");
    let live = run.live();
    run.workload("glob1", &live);
    run.converged("glob1", "r-a");
    run.restart_region("r-b");
    run.converged("glob1", "r-a");
    // The preferred Region dies: the witness is the only other voter in reach
    // with r-b, so it may transiently lead, but the reconciler must hand the
    // leadership to the full replica in r-b and keep it off the witness.
    run.crash_region("r-a");
    let live = run.live();
    run.converged("glob1", "r-b");
    run.workload("glob1", &live);
    run.restart_region("r-a");
    run.converged("glob1", "r-a");
    run.workload("glob1", &all_nodes());
    run.placement("glob1");
    run.caught_up();
}

fn cell_drain_last_node_of_region_refused(seed: u64) {
    let mut run = Run::boot(seed);
    run.global_table("glob1", false);
    run.workload("glob1", &all_nodes());
    let drain = |run: &mut Run, node: u64, force: bool| -> (u16, String) {
        let leader = {
            let idx = run.c.control_leader_index();
            run.c.control_node_id(idx)
        };
        let body = if force {
            format!(r#"{{"node":"n{node}","force":true}}"#)
        } else {
            format!(r#"{{"node":"n{node}"}}"#)
        };
        run.c
            .admin(leader, "POST", "/admin/drain", "", body.as_bytes())
    };
    // n0 is not the last Active member of r-a (n1 is its twin): allowed, and
    // its replica is re-placed on n1, inside the Region.
    let (s, resp) = drain(&mut run, 0, false);
    assert_eq!(s, 200, "seed={seed}: drain n0: {resp}");
    run.must("n0 drained", |c| {
        let meta = c.metadata(2);
        if meta.tablets_referencing(&nid(0)) == 0 {
            Ok(())
        } else {
            Err("n0 still referenced".into())
        }
    });
    run.placement("glob1");
    // n1 is now the last Active member of r-a: refused, by name.
    let (s, resp) = drain(&mut run, 1, false);
    assert_eq!(s, 409, "seed={seed}: drain of the last r-a node: {resp}");
    assert!(
        resp.contains("last Active member of Region `r-a`") && resp.contains("glob1"),
        "seed={seed}: the refusal must name the Region and the table: {resp}"
    );
    let meta = run.c.metadata(2);
    assert_eq!(
        meta.members.get(&nid(1)).map(|m| m.status),
        Some(animus_control::NodeStatus::Active),
        "seed={seed}: the refused drain must leave n1 Active"
    );
    // `force` overrides the guard (an operator who accepts the stall) ...
    let (s, resp) = drain(&mut run, 1, true);
    assert_eq!(s, 200, "seed={seed}: forced drain: {resp}");
    // ... and the strict pin then holds the replica in r-a: nothing moves
    // across Regions.
    run.c.run_for(Duration::from_secs(60));
    run.placement("glob1");
    let live: Vec<u64> = vec![2, 3, 4, 5];
    run.workload("glob1", &live);
}

// ---------------------------------------------------------------------------
// Replica removal: an MRSC table reverts to a single-Region table (ADR 0075,
// 2026-10-21 amendment; `Gate::MrscReplicaRemoval`, cluster version 7)
// ---------------------------------------------------------------------------

/// The cluster version the replica-removal cells run at (`Gate::
/// MrscReplicaRemoval`).
const REMOVAL_VERSION: u32 = 7;

/// `UpdateTable` deleting `replicas` (and the witness, when `witness` names
/// one) of `table`, received by node `via`.
fn delete_regions(
    run: &mut Run,
    via: u64,
    table: &str,
    replicas: &[&str],
    witness: Option<&str>,
) -> (u16, Value) {
    let reps: Vec<String> = replicas
        .iter()
        .map(|r| format!(r#"{{"Delete":{{"RegionName":"{r}"}}}}"#))
        .collect();
    let mut body = format!(
        r#"{{"TableName":"{table}","ReplicaUpdates":[{}]"#,
        reps.join(",")
    );
    if let Some(w) = witness {
        body.push_str(&format!(
            r#","GlobalTableWitnessUpdates":[{{"Delete":{{"RegionName":"{w}"}}}}]"#
        ));
    }
    body.push('}');
    call(&mut run.c, via, "UpdateTable", &body)
}

/// The reverted-table invariant: no global spec, and every routable tablet
/// has exactly one replica on each node of `keep` (a two-node Region holds
/// two replicas), under a regional (unpinned) policy restricted to `keep`.
fn reverted_into(meta: &Metadata, table: &str, keep: &str) -> Result<(), String> {
    if meta.table_global(table).is_some() {
        return Err(format!("{table}: still a global table"));
    }
    let want: BTreeSet<u64> = nodes_of_region(keep).into_iter().collect();
    let mut any = false;
    for (id, tab) in meta
        .tablets_for_table(table)
        .filter(|(_, t)| t.is_routable())
    {
        any = true;
        let have: BTreeSet<u64> = tab.replicas.iter().map(node_index).collect();
        if have != want {
            return Err(format!(
                "{table} tablet {id:?}: replicas {have:?}, want {want:?}"
            ));
        }
        let p = meta.policies.get(id).ok_or("no policy")?;
        if p.is_pinned()
            || p.allowed_values.get(REGION_LABEL) != Some(&BTreeSet::from([keep.to_owned()]))
        {
            return Err(format!("{table} tablet {id:?}: policy {p:?}"));
        }
    }
    if any {
        Ok(())
    } else {
        Err(format!("{table}: no routable tablet"))
    }
}

impl Run {
    /// Converged-or-timeout: the table is a regional table in `keep`, every
    /// tablet is led from `keep`, and no live node outside `keep` still hosts
    /// a replica group of it (the removed replicas really left).
    fn reverted(&mut self, table: &str, keep: &str) {
        let (t, k) = (table.to_owned(), keep.to_owned());
        let dead = self.dead.clone();
        self.must(&format!("{table} reverted into {keep}"), move |c| {
            let live = (0..NODES).find(|n| !dead.contains(n)).expect("a live node");
            let meta = c.metadata(live);
            reverted_into(&meta, &t, &k)?;
            leaders_in(c, &meta, &t, &dead, &k)?;
            let ids: BTreeSet<TabletId> = meta.tablets_for_table(&t).map(|(i, _)| *i).collect();
            for n in (0..NODES).filter(|n| !dead.contains(n) && region_of_node(*n) != k) {
                if c.hosted_tablets(n).iter().any(|h| ids.contains(h)) {
                    return Err(format!("n{n} still hosts a replica of {t}"));
                }
            }
            Ok(())
        });
    }

    /// The control-plane half of [`reverted`](Self::reverted): the table is a
    /// regional table in `keep` with its tablets' replica sets moved there, as
    /// seen by a live node. The Raft groups themselves may still be moving.
    fn reverted_in_metadata(&mut self, table: &str, keep: &str) {
        let (t, k) = (table.to_owned(), keep.to_owned());
        let dead = self.dead.clone();
        self.must(
            &format!("{table} metadata reverted into {keep}"),
            move |c| {
                let live = (0..NODES).find(|n| !dead.contains(n)).expect("a live node");
                reverted_into(&c.metadata(live), &t, &k)
            },
        );
    }

    /// Acked writes through `via` while the cluster converges: `rounds` of one
    /// virtual second and one put each (every put must ack: a quorum survives
    /// every fault these cells inject).
    fn write_while_converging(&mut self, table: &str, via: &[u64], rounds: usize) {
        for _ in 0..rounds {
            self.c.run_for(Duration::from_secs(1));
            assert!(
                self.put_unique(table, via, 30),
                "seed={}: a put on {table} never acked while the table was converging",
                self.seed
            );
        }
    }
}

/// A single transactional put on `table` through `node`.
fn transact_put(c: &mut SimCluster, node: u64, table: &str, pk: &str) -> (u16, Value) {
    let body = format!(
        r#"{{"TransactItems":[{{"Put":{{"TableName":"{table}","Item":{{"pk":{{"S":"{pk}"}},"v":{{"S":"t"}}}}}}}}]}}"#
    );
    call(c, node, "TransactWriteItems", &body)
}

fn enable_ttl(c: &mut SimCluster, node: u64, table: &str) -> (u16, Value) {
    let body = format!(
        r#"{{"TableName":"{table}","TimeToLiveSpecification":{{"AttributeName":"ttl","Enabled":true}}}}"#
    );
    call(c, node, "UpdateTimeToLive", &body)
}

/// The MRSC restrictions hold while global and fall away on revert.
fn expect_mrsc_restrictions(run: &mut Run, table: &str) {
    let seed = run.seed;
    let (s, v) = transact_put(&mut run.c, 0, table, "t0");
    assert_eq!(s, 400, "seed={seed}: transaction on a global table: {v}");
    let (s, v) = enable_ttl(&mut run.c, 0, table);
    assert_eq!(s, 400, "seed={seed}: TTL on a global table: {v}");
}

/// The surface of a reverted table: `DescribeTable` carries no global
/// fields, `/admin/global-tables` does not list it, and the restrictions tied
/// to MRSC (transactions, TTL) no longer apply.
fn check_regional_surface(run: &mut Run, table: &str, via: u64) {
    let seed = run.seed;
    let body = format!(r#"{{"TableName":"{table}"}}"#);
    let (s, v) = call(&mut run.c, via, "DescribeTable", &body);
    assert_eq!(s, 200, "seed={seed}: DescribeTable: {v}");
    for key in [
        "GlobalTableVersion",
        "Replicas",
        "MultiRegionConsistency",
        "GlobalTableWitnesses",
    ] {
        assert!(
            v["Table"].get(key).is_none(),
            "seed={seed}: {key} on a reverted table: {}",
            v["Table"]
        );
    }
    assert_eq!(v["Table"]["TableStatus"], "ACTIVE", "seed={seed}: {v}");
    let (s, body) = run.c.admin(via, "GET", "/admin/global-tables", "", b"");
    assert_eq!(s, 200, "seed={seed}: global-tables: {body}");
    let g: Value = serde_json::from_str(&body).expect("json");
    assert!(
        g["tables"]
            .as_array()
            .is_none_or(|a| a.iter().all(|t| t["table"] != table)),
        "seed={seed}: {table} still listed: {g}"
    );
    let mut ok = false;
    for i in 0..30 {
        let (s, v) = transact_put(&mut run.c, via, table, &format!("t{i}"));
        if s == 200 {
            ok = true;
            break;
        }
        assert_ne!(s, 400, "seed={seed}: transaction still refused: {v}");
    }
    assert!(ok, "seed={seed}: a transaction never committed on {table}");
    let (s, v) = enable_ttl(&mut run.c, via, table);
    assert_eq!(s, 200, "seed={seed}: TTL after the revert: {v}");
}

/// Delete r-b and r-c through a node of r-a (the preferred Region): the table
/// becomes a regular table whose replicas all live in r-a; the writer keeps
/// acking throughout, no acked write is lost, transactions and TTL become
/// allowed, and the decommission guard no longer holds the old Regions.
fn cell_revert_keep_preferred(seed: u64) {
    let mut run = Run::boot_at(seed, REMOVAL_VERSION);
    run.global_table("glob1", false);
    run.workload("glob1", &all_nodes());
    expect_mrsc_restrictions(&mut run, "glob1");
    // A single Region, or the wrong shape, is refused by name and changes
    // nothing.
    for (reps, want) in [
        (
            vec!["r-b"],
            "single replica or the witness cannot be deleted",
        ),
        (vec!["r-b", "r-z"], "not a replica Region"),
        (vec!["r-a", "r-b", "r-c"], "all Regions"),
    ] {
        let (s, v) = delete_regions(&mut run, 0, "glob1", &reps, None);
        assert_eq!(s, 400, "seed={seed}: {reps:?}: {v}");
        assert!(
            v["message"].as_str().is_some_and(|m| m.contains(want)),
            "seed={seed}: {reps:?}: {v}"
        );
    }
    run.converged("glob1", "r-a");
    let (s, v) = delete_regions(&mut run, 0, "glob1", &["r-b", "r-c"], None);
    assert_eq!(s, 200, "seed={seed}: revert: {v}");
    assert!(
        v["TableDescription"].get("Replicas").is_none(),
        "seed={seed}: {v}"
    );
    let live = run.live();
    run.write_while_converging("glob1", &live, 5);
    run.reverted("glob1", "r-a");
    run.workload("glob1", &all_nodes());
    check_regional_surface(&mut run, "glob1", 0);
    // Idempotent: the same request on a regional table is refused by name (a
    // regional table has no replica to delete), nothing changes.
    let (s, _) = delete_regions(&mut run, 0, "glob1", &["r-b", "r-c"], None);
    assert_eq!(s, 400, "seed={seed}: a second revert must not be accepted");
    // The old Regions are free to drain: both nodes of r-c, the second of
    // which the guard refused while the table was global.
    for node in [4u64, 5] {
        let leader = {
            let idx = run.c.control_leader_index();
            run.c.control_node_id(idx)
        };
        let body = format!(r#"{{"node":"n{node}"}}"#);
        let (s, resp) = run
            .c
            .admin(leader, "POST", "/admin/drain", "", body.as_bytes());
        assert_eq!(
            s, 200,
            "seed={seed}: drain n{node} of a freed Region: {resp}"
        );
    }
    run.workload("glob1", &[0, 1, 2, 3]);
    run.caught_up();
}

/// Which node dies while the table converges.
#[derive(Clone, Copy)]
enum Kill {
    /// The control-plane leader (the proposer of every placement move).
    ControlLeader,
    /// The leader of the table's tablet (in the preferred Region, which is
    /// being deleted).
    TabletLeader,
    /// A node of the KEPT Region (issue #1271): one of the two desired
    /// replicas. The groups cannot finish moving while it is down (see
    /// `revert_with_kill`), so the cell asserts availability and the
    /// control-plane revert while it is down and full convergence after.
    KeptRegionNode,
}

/// Delete r-a (the preferred Region) and r-c through a node of r-b, keeping
/// r-b, and kill a node right after the request: the revert and the replica
/// moves converge under the new leader, nothing acked is lost, and the killed
/// node, restarted, releases its stale replica instead of resurrecting it.
fn revert_with_kill(seed: u64, kill: Kill) {
    let mut run = Run::boot_at(seed, REMOVAL_VERSION);
    run.global_table("glob1", false);
    run.workload("glob1", &all_nodes());
    let (s, v) = delete_regions(&mut run, 2, "glob1", &["r-a", "r-c"], None);
    assert_eq!(s, 200, "seed={seed}: revert: {v}");
    let victim = match kill {
        Kill::ControlLeader => {
            let idx = run.c.control_leader_index();
            run.c.control_node_id(idx)
        }
        Kill::TabletLeader => {
            let live = run.live()[0];
            let id = tablets_of(&mut run.c, live, "glob1")[0];
            leader_of(&run.c, id, &run.dead).expect("a leader").0
        }
        Kill::KeptRegionNode => nodes_of_region("r-b")[1],
    };
    run.c.crash(victim);
    run.dead.insert(victim);
    let live = run.live();
    run.write_while_converging("glob1", &live, 5);
    if region_of_node(victim) == "r-b" {
        // The victim is one of the two DESIRED replicas of the reverted
        // table. The reconfigure ladder drops an extra voter (and moves
        // leadership off a removed leader) only once every desired member is
        // caught up (`RaftKvNode::reconfigure_step` steps 5 and 6): dropping
        // to a two-voter group with one voter down would trade a working
        // quorum for none. So the removed replicas deliberately stay in the
        // group, and may still lead it, until the victim returns. Which node
        // is the control leader is seed-dependent, so seed 1828716545 (control
        // leader n2, a kept-Region node) failed the strict check below while
        // other seeds never reached this branch. The table must stay
        // available (the writes above acked) and be reverted in the metadata.
        run.reverted_in_metadata("glob1", "r-b");
        // Clients on a node that hosts no replica of the table reach a
        // removed-but-still-leading replica only by chasing the refusal's
        // leader hint, and `SimCluster` addresses (`n0`, no `:port`) fail
        // `parse_not_leader_refusal`'s `host:port` shape check, so the hint
        // is dropped there (production addresses pass it). Drive the workload
        // through the nodes that host the table, which find the leader by
        // their own local hint; everyone is exercised after the restart.
        let ids: BTreeSet<TabletId> = run
            .c
            .metadata(live[0])
            .tablets_for_table("glob1")
            .map(|(i, _)| *i)
            .collect();
        let hosts: Vec<u64> = live
            .iter()
            .copied()
            .filter(|n| run.c.hosted_tablets(*n).iter().any(|h| ids.contains(h)))
            .collect();
        run.workload("glob1", &hosts);
    } else {
        run.reverted("glob1", "r-b");
        run.workload("glob1", &live);
    }
    run.c.restart(victim);
    run.dead.remove(&victim);
    run.reverted("glob1", "r-b");
    run.workload("glob1", &all_nodes());
    check_regional_surface(&mut run, "glob1", 2);
    run.caught_up();
}

fn cell_revert_control_leader_kill(seed: u64) {
    revert_with_kill(seed, Kill::ControlLeader);
}

fn cell_revert_tablet_leader_kill(seed: u64) {
    revert_with_kill(seed, Kill::TabletLeader);
}

/// Deterministic sibling of the control-leader kill: the dead node is always a
/// kept-Region replica, never the removed Region's (issue #1271).
fn cell_revert_kept_region_node_kill(seed: u64) {
    revert_with_kill(seed, Kill::KeptRegionNode);
}

/// The witness form (r-c witness): delete the witness and r-b through a node
/// of r-a while r-b is partitioned from the rest during the convergence. The
/// majority side keeps committing and converges; after the heal the cut
/// Region's replica is gone too and no acked write was lost.
fn cell_revert_witness_form_partition(seed: u64) {
    let mut run = Run::boot_at(seed, REMOVAL_VERSION);
    run.global_table("glob1", true);
    run.workload("glob1", &all_nodes());
    // The witness cannot be the Region that remains.
    let (s, v) = delete_regions(&mut run, 0, "glob1", &["r-a", "r-b"], None);
    assert_eq!(s, 400, "seed={seed}: {v}");
    assert!(
        v["message"]
            .as_str()
            .is_some_and(|m| m.contains("must be a replica, not the witness")),
        "seed={seed}: {v}"
    );
    // A lone witness Delete is the AWS-refused single removal.
    let (s, v) = delete_regions(&mut run, 0, "glob1", &[], Some("r-c"));
    assert_eq!(s, 400, "seed={seed}: {v}");
    let cut = nodes_of_region("r-b");
    let rest: Vec<u64> = (0..NODES).filter(|n| !cut.contains(n)).collect();
    let (s, v) = delete_regions(&mut run, 0, "glob1", &["r-b"], Some("r-c"));
    assert_eq!(s, 200, "seed={seed}: revert: {v}");
    for a in &cut {
        for b in &rest {
            run.c.partition(*a, *b);
        }
    }
    run.write_while_converging("glob1", &rest, 8);
    let sim = run.c.simulator();
    for a in &cut {
        for b in &rest {
            sim.heal(nid(*a), nid(*b));
        }
    }
    apply_wan(&mut run.c);
    run.reverted("glob1", "r-a");
    run.workload("glob1", &all_nodes());
    check_regional_surface(&mut run, "glob1", 0);
    run.caught_up();
}

type CellFn = fn(u64);

/// Run one cell over every seed (`ANIMUS_MRSC_SEEDS`/`ANIMUS_SEED`), unless
/// `ANIMUS_MRSC_CELL` narrows the run to other cells.
fn run_cell(name: &str, cell: CellFn) {
    if !cell_selected(name) {
        return;
    }
    for seed in seeds() {
        eprintln!("sim_cluster_mrsc_corpus: cell {name} seed {seed}");
        cell(seed);
    }
}

/// One `#[test]` per cell, so the per-push nextest tier runs them in
/// parallel processes (a cell is a whole 6-node WAN cluster over the LSM
/// engine: serialised they would be the slowest test in the `animusd --lib`
/// tier). The `sim_cluster_mrsc_corpus_` prefix is what the deep step's
/// `cargo test ... sim_cluster_mrsc` filter selects.
macro_rules! cell_tests {
    ($($test:ident => ($label:literal, $cell:ident)),+ $(,)?) => {
        $(
            #[test]
            fn $test() {
                run_cell($label, $cell);
            }
        )+
    };
}

cell_tests! {
    sim_cluster_mrsc_corpus_steady => ("steady", cell_steady),
    sim_cluster_mrsc_corpus_region_loss_leader_region =>
        ("region_loss_leader_region", cell_region_loss_leader_region),
    sim_cluster_mrsc_corpus_region_loss_follower_region =>
        ("region_loss_follower_region", cell_region_loss_follower_region),
    sim_cluster_mrsc_corpus_region_partition_heal =>
        ("region_partition_heal", cell_region_partition_heal),
    sim_cluster_mrsc_corpus_split_under_mrsc =>
        ("split_under_mrsc", cell_split_under_mrsc),
    sim_cluster_mrsc_corpus_in_region_node_replacement =>
        ("in_region_node_replacement", cell_in_region_node_replacement),
    sim_cluster_mrsc_corpus_witness_form_region_loss =>
        ("witness_form_region_loss", cell_witness_form_region_loss),
    sim_cluster_mrsc_corpus_drain_last_node_of_region_refused =>
        ("drain_last_node_of_region_refused", cell_drain_last_node_of_region_refused),
    sim_cluster_mrsc_corpus_revert_keep_preferred =>
        ("revert_keep_preferred", cell_revert_keep_preferred),
    sim_cluster_mrsc_corpus_revert_control_leader_kill =>
        ("revert_control_leader_kill", cell_revert_control_leader_kill),
    sim_cluster_mrsc_corpus_revert_tablet_leader_kill =>
        ("revert_tablet_leader_kill", cell_revert_tablet_leader_kill),
    sim_cluster_mrsc_corpus_revert_kept_region_node_kill =>
        ("revert_kept_region_node_kill", cell_revert_kept_region_node_kill),
    sim_cluster_mrsc_corpus_revert_witness_form_partition =>
        ("revert_witness_form_partition", cell_revert_witness_form_partition),
}

// ---------------------------------------------------------------------------
// Negative controls
// ---------------------------------------------------------------------------

/// With the preferred-leader step fed nothing, a leader outside the preferred
/// Region stays there: the leader-locality oracle the positive cells use fails.
#[test]
fn mrsc_negative_preferred_leader_disabled() {
    for seed in seeds() {
        let _off = PreferredLeaderOff::new();
        let mut run = Run::boot(seed);
        run.create_table("glob1");
        run.convert("glob1", false);
        // Placement converges (not the leader's business), then the preferred
        // Region is set to one that does NOT hold the leader.
        run.must("placement", |c| one_per_region(&c.metadata(0), "glob1"));
        run.c.run_for(Duration::from_secs(20));
        let now = leader_region_of_first(&mut run, "glob1");
        let target = REGIONS.iter().find(|r| **r != now).expect("another Region");
        set_preferred(&mut run, "glob1", target);
        let (t, dead) = ("glob1", run.dead.clone());
        let r = run.poll("never", |c| {
            let meta = c.metadata(0);
            leaders_in(c, &meta, t, &dead, target)
        });
        assert!(
            r.is_err(),
            "seed={seed}: the leader reached {target} with the preferred-leader step off; the oracle proves nothing"
        );
    }
}

/// A plain table over the same cluster: the placement oracle names the
/// violation and a Region loss costs it its quorum.
#[test]
fn mrsc_negative_unpinned_policy() {
    // Aggregate over the whole seed set ("some seed doubles a Region"), so it
    // cannot be split by seed: shard 0 runs it in full, the others skip it.
    if !animus_test::corpus::owns_unsharded() {
        return;
    }
    let mut violated = 0usize;
    let mut quorum_lost = 0usize;
    let all = all_seeds();
    for seed in all.iter().copied() {
        let mut run = Run::boot(seed);
        run.create_table("plain");
        run.c.run_for(Duration::from_secs(20));
        let meta = run.c.metadata(0);
        // The invariant oracle rejects a table that is not global at all.
        assert!(one_per_region(&meta, "plain").is_err());
        let (_, tab) = meta.tablets_for_table("plain").next().expect("tablet");
        let mut per: BTreeMap<&str, usize> = BTreeMap::new();
        for r in &tab.replicas {
            *per.entry(region_of_node(node_index(r))).or_default() += 1;
        }
        if let Some((doubled, _)) = per.iter().find(|(_, n)| **n >= 2) {
            violated += 1;
            // Kill the Region holding two of the three replicas.
            let doubled = (*doubled).to_owned();
            assert!(
                run.put_unique("plain", &all_nodes(), 3),
                "seed={seed}: baseline"
            );
            run.crash_region(&doubled);
            let live = run.live();
            if !run.put_unique("plain", &live, 2) {
                quorum_lost += 1;
            }
        }
    }
    assert!(
        violated > 0,
        "the unpinned policy never put two replicas in one Region across {} seeds; the control proves nothing",
        all.len()
    );
    assert_eq!(
        violated, quorum_lost,
        "a Region holding two of three replicas must cost the quorum"
    );
}

/// The revert oracle is not vacuous: a table that was converted but never
/// reverted fails every clause of [`reverted_into`] (still global, replicas in
/// all three Regions), and a regional table in another Region fails the
/// kept-Region clause. Also pins the pre-revert restrictions so the
/// "transactions/TTL become allowed" assertions of the cells prove a change.
#[test]
fn mrsc_negative_revert_oracle_bites() {
    for seed in seeds() {
        let mut run = Run::boot_at(seed, REMOVAL_VERSION);
        run.global_table("glob1", false);
        let meta = run.c.metadata(0);
        let e = reverted_into(&meta, "glob1", "r-a").unwrap_err();
        assert!(e.contains("still a global table"), "seed={seed}: {e}");
        expect_mrsc_restrictions(&mut run, "glob1");
        // Below the gate's version nothing is accepted: the same request on a
        // version-2 cluster is the pre-amendment refusal.
        let mut old = Run::boot(seed);
        old.global_table("glob1", false);
        let (s, v) = delete_regions(&mut old, 0, "glob1", &["r-b", "r-c"], None);
        assert_eq!(s, 400, "seed={seed}: {v}");
        assert!(
            v["message"]
                .as_str()
                .is_some_and(|m| m.contains("ReplicaUpdates Delete is not supported")),
            "seed={seed}: the gate-closed text changed: {v}"
        );
        assert!(old.c.metadata(0).table_global("glob1").is_some());
    }
}

/// The register checker is not vacuous.
#[test]
fn mrsc_negative_checker_bites() {
    let mut r = Register::default();
    r.attempt(1);
    r.acked(1);
    r.attempt(2);
    r.acked(2);
    assert!(r.check_read(Some(2)).is_ok());
    assert!(r.check_read(Some(1)).unwrap_err().contains("stale"));
    assert!(r.check_read(None).unwrap_err().contains("acked"));
    assert!(r.check_read(Some(9)).unwrap_err().contains("never written"));
    // An in-flight, never-acked attempt may land at any time.
    r.attempt(3);
    assert!(r.check_read(Some(3)).is_ok());
}
