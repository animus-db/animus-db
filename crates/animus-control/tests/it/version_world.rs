//! Shared SimEnv harness for the ADR 0073 Phase 2 (P2-A) version-observation
//! and era-on tests (`version_observe_corpus`, `version_era_on`).
//!
//! A [`World`] is one seeded `Simulator` holding:
//! - **control voters** and **control learners** (`RaftNode`s over a retained
//!   `MemoryEngine`, so a restart is a real stop + fresh start on the same
//!   durable state). They never heartbeat: the leader can only learn their
//!   version from Raft traffic;
//! - **heartbeat-only members** (`heartbeat_loop` tasks, no Raft process),
//!   registered in `Metadata` via `UpsertMember`.
//!
//! Every node starts as a **Phase 1 binary** (empty handshake `ext`, and, for
//! control nodes, `set_own_version_range(None)`). [`World::flip`] upgrades one
//! node to the "B2" profile (`ext` = `encode_ext(Some((1, 1)), Some("b2"))`,
//! own range `Some`, optional restart).
//!
//! Everything is a pure function of the seed; assertion messages carry it.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use animus_control::meta::NodeAddrs;
use animus_control::node::heartbeat_loop;
use animus_control::raft::ProposeResult;
use animus_control::version::{VersionRange, own_range};
use animus_control::{MetaCommand, Metadata, NodeStatus, RaftNode};
use animus_env::handshake::encode_ext;
use animus_env::{EnvExt, NodeId, nid};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;

/// splitmix64.
pub struct Rng(pub u64);

impl Rng {
    pub fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    /// An independent stream derived from this one.
    pub fn fork(&mut self) -> Rng {
        Rng(self.next())
    }
    pub fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    pub fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

/// The B2 handshake `ext`.
pub fn b2_ext() -> Vec<u8> {
    encode_ext(Some((1, 1)), Some("b2"))
}

/// The seeds a corpus cell runs: `ANIMUS_SEED` replays one, else
/// `variants x ANIMUS_CONTROL_SEEDS` name-derived seeds.
pub fn seeds(name: &str, variants: usize) -> Vec<u64> {
    if let Some(seed) = std::env::var("ANIMUS_SEED").ok().and_then(|v| {
        v.parse::<u64>()
            .ok()
            .or_else(|| u64::from_str_radix(v.trim_start_matches("0x"), 16).ok())
    }) {
        return vec![seed];
    }
    let k = animus_test::corpus::seeds_from_env("ANIMUS_CONTROL_SEEDS");
    let mut out = Vec::new();
    animus_test::corpus::for_each_seed(name, variants * k, |s| out.push(s));
    out
}

fn addrs(n: u64, role: &str) -> NodeAddrs {
    NodeAddrs {
        internal: format!("127.0.0.1:{}", 9300 + n),
        client: format!("127.0.0.1:{}", 9000 + n),
        admin: format!("127.0.0.1:{}", 9500 + n),
        intra: format!("127.0.0.1:{}", 9600 + n),
        role: role.to_string(),
    }
}

/// One nemesis episode.
#[derive(Clone, Copy, Debug)]
pub enum Fault {
    LeaderKill,
    PartitionLeader,
    Lossy,
    CutMember,
}

pub struct World {
    pub seed: u64,
    pub sim: Simulator,
    pub voters: Vec<u64>,
    pub learners: Vec<u64>,
    pub members: Vec<u64>,
    pub nodes: BTreeMap<u64, RaftNode<SimEnv>>,
    engines: BTreeMap<u64, MemoryEngine>,
    /// Nodes currently upgraded to B2.
    pub flipped: BTreeSet<u64>,
    /// Per-node B2 range override (default [`own_range`]); set by
    /// [`World::flip_range`].
    pub ranges: BTreeMap<u64, (u32, u32)>,
    /// Control nodes restarted since start (their `Metadata` view resets).
    pub restarted: BTreeSet<u64>,
    pub rng: Rng,
}

impl World {
    /// Start the control nodes and member heartbeaters, all as Phase 1.
    pub fn new(seed: u64, voters: &[u64], learners: &[u64], members: &[u64]) -> Self {
        let sim = Simulator::new(seed);
        let mut w = Self {
            seed,
            sim,
            voters: voters.to_vec(),
            learners: learners.to_vec(),
            members: members.to_vec(),
            nodes: BTreeMap::new(),
            engines: BTreeMap::new(),
            flipped: BTreeSet::new(),
            ranges: BTreeMap::new(),
            restarted: BTreeSet::new(),
            rng: Rng(seed ^ 0xA5A5_5A5A_1234_4321),
        };
        for &id in voters.iter().chain(learners) {
            w.engines.insert(id, MemoryEngine::new());
            w.start_control(id);
        }
        for &id in members {
            w.start_heartbeat(id);
        }
        w
    }

    pub fn control_ids(&self) -> Vec<u64> {
        self.voters.iter().chain(&self.learners).copied().collect()
    }

    fn start_control(&mut self, id: u64) {
        let node = RaftNode::start(
            self.sim.env(nid(id)),
            self.voters.iter().copied().map(nid).collect(),
            self.engines[&id].clone(),
        );
        self.apply_profile(id, &node);
        self.nodes.insert(id, node);
    }

    fn apply_profile(&self, id: u64, node: &RaftNode<SimEnv>) {
        if self.flipped.contains(&id) {
            let r = self
                .ranges
                .get(&id)
                .map_or_else(own_range, |&(a, b)| VersionRange::new(a, b));
            node.set_own_version_range(Some(r));
            node.set_own_build("b2");
        } else {
            node.set_own_version_range(None);
        }
    }

    fn start_heartbeat(&self, id: u64) {
        let env = self.sim.env(nid(id));
        env.spawn_task(heartbeat_loop(
            env.clone(),
            self.voters.iter().copied().map(nid).collect(),
        ));
    }

    /// The unique live leader among control nodes that are not `down`.
    pub fn leader(&self) -> Option<u64> {
        let l: Vec<u64> = self
            .nodes
            .iter()
            .filter(|(_, n)| n.is_leader())
            .map(|(&i, _)| i)
            .collect();
        // Two nodes can transiently both claim leadership across terms; the
        // highest term is the real one.
        l.into_iter().max_by_key(|i| (self.nodes[i].term(), *i))
    }

    /// Run `dur`, calling `check` every 25 ms of virtual time.
    pub fn run(&mut self, dur: Duration, check: &mut dyn FnMut(&World)) {
        let step = Duration::from_millis(25);
        let mut left = dur;
        while !left.is_zero() {
            let d = left.min(step);
            self.sim.run_for(d);
            left -= d;
            check(self);
        }
    }

    /// Propose through the current leader, retrying until the command is
    /// visible via `visible`, within a budget; panics with the seed otherwise.
    pub fn propose_confirmed(
        &mut self,
        cmd: &MetaCommand,
        visible: &dyn Fn(&Metadata) -> bool,
        what: &str,
    ) {
        for _ in 0..200 {
            if let Some(l) = self.leader() {
                let _ = self.nodes[&l].propose(cmd.clone());
            }
            self.sim.run_for(Duration::from_millis(100));
            if self.nodes.values().any(|n| visible(&n.metadata())) {
                return;
            }
        }
        panic!("seed={}: {what} never became visible", self.seed);
    }

    /// Elect, register every node in `Metadata`, add the learners.
    pub fn bootstrap(&mut self) {
        self.sim.run_for(Duration::from_secs(2));
        for id in self.control_ids() {
            self.propose_confirmed(
                &MetaCommand::RegisterNode {
                    node: nid(id),
                    addrs: addrs(id, "control"),
                    labels: BTreeMap::new(),
                },
                &|m| m.node_addrs.contains_key(&nid(id)),
                "control registration",
            );
        }
        for id in self.members.clone() {
            self.propose_confirmed(
                &MetaCommand::UpsertMember {
                    node: nid(id),
                    labels: BTreeMap::new(),
                    status: NodeStatus::Active,
                },
                &|m| m.members.contains_key(&nid(id)),
                "member upsert",
            );
        }
        for id in self.learners.clone() {
            for _ in 0..100 {
                if let Some(l) = self.leader()
                    && (matches!(
                        self.nodes[&l].add_learner(nid(id)),
                        ProposeResult::Accepted { .. }
                    ) || self.nodes[&l].learners().contains(&nid(id)))
                {
                    break;
                }
                self.sim.run_for(Duration::from_millis(100));
            }
        }
        self.sim.run_for(Duration::from_secs(2));
    }

    /// Upgrade `id` to the B2 profile; a control node optionally restarts
    /// (stop + fresh start over the same durable state), a member's heartbeat
    /// task is restarted likewise.
    pub fn flip(&mut self, id: u64, restart: bool) {
        self.sim.set_network_ext_for(nid(id), b2_ext());
        self.flipped.insert(id);
        if restart {
            if self.nodes.contains_key(&id) {
                self.sim.stop(nid(id));
                self.restarted.insert(id);
                self.start_control(id);
            } else if self.members.contains(&id) {
                self.sim.stop(nid(id));
                self.start_heartbeat(id);
            }
        } else if let Some(n) = self.nodes.get(&id) {
            self.apply_profile(id, n);
        }
    }

    /// [`flip`](Self::flip) to a B2 profile with an explicit range `(min,
    /// max)` (advertised in the `ext` and used as the control node's own
    /// range).
    pub fn flip_range(&mut self, id: u64, range: (u32, u32), restart: bool) {
        self.ranges.insert(id, range);
        self.flip(id, restart);
        self.sim
            .set_network_ext_for(nid(id), encode_ext(Some(range), Some("b2")));
    }

    /// Stop and freshly start control node `id` over its retained durable
    /// state, then override its own range (`Some`/`None`) before any sim time
    /// passes (so the boot-time range check sees it).
    pub fn restart_with_own(&mut self, id: u64, own: Option<VersionRange>) {
        self.sim.stop(nid(id));
        self.restarted.insert(id);
        self.start_control(id);
        self.nodes[&id].set_own_version_range(own);
    }

    /// Advertise B2 on the wire for `id` without touching its own-range
    /// profile (isolates the proposer-side gate from the observation side).
    pub fn set_ext_only(&mut self, id: u64) {
        self.sim.set_network_ext_for(nid(id), b2_ext());
    }

    /// Apply one nemesis episode; returns what [`heal`](Self::heal) needs.
    pub fn inject(&mut self, fault: Fault) -> Option<Healer> {
        match fault {
            Fault::LeaderKill => {
                let l = self.leader()?;
                self.sim.crash(nid(l));
                Some(Healer::Restart(l))
            }
            Fault::PartitionLeader => {
                let l = self.leader()?;
                let ids = self.control_ids();
                let peers: Vec<u64> = ids.into_iter().filter(|&i| i != l).collect();
                let p = peers[self.rng.below(peers.len() as u64) as usize];
                self.sim.partition_pair(nid(l), nid(p));
                Some(Healer::Heal(l, p))
            }
            Fault::Lossy => {
                let mut cfg = animus_sim::NetConfig::default();
                cfg.set_drop_prob(0.05 + (self.rng.below(20) as f64) / 100.0);
                self.sim.set_net_config(cfg);
                Some(Healer::NetReset)
            }
            Fault::CutMember => {
                let m = *self
                    .members
                    .get(self.rng.below(self.members.len().max(1) as u64) as usize)?;
                for c in self.control_ids() {
                    self.sim.partition_pair(nid(m), nid(c));
                }
                Some(Healer::HealMember(m))
            }
        }
    }

    pub fn heal(&mut self, h: Healer) {
        match h {
            Healer::Restart(l) => self.sim.restart(nid(l)),
            Healer::Heal(a, b) => self.sim.heal(nid(a), nid(b)),
            Healer::NetReset => self.sim.set_net_config(animus_sim::NetConfig::default()),
            Healer::HealMember(m) => {
                for c in self.control_ids() {
                    self.sim.heal(nid(m), nid(c));
                }
            }
        }
    }

    /// Converged-or-timeout poll: run in 100 ms steps until `pred` holds,
    /// panicking with the seed once `budget` of virtual time has passed.
    pub fn poll(
        &mut self,
        budget: Duration,
        what: &str,
        check: &mut dyn FnMut(&World),
        pred: &dyn Fn(&World) -> bool,
    ) {
        let mut spent = Duration::ZERO;
        while !pred(self) {
            assert!(
                spent < budget,
                "seed={}: timed out after {budget:?} waiting for: {what}",
                self.seed
            );
            self.run(Duration::from_millis(100), check);
            spent += Duration::from_millis(100);
        }
    }

    /// The leader's applied `Metadata`.
    pub fn leader_meta(&self) -> Option<Metadata> {
        self.leader().map(|l| self.nodes[&l].metadata())
    }

    /// Stop every fault: heal the network, restart anything crashed.
    pub fn heal_all(&mut self) {
        self.sim.set_net_config(animus_sim::NetConfig::default());
        for a in self.control_ids().into_iter().chain(self.members.clone()) {
            for b in self.control_ids().into_iter().chain(self.members.clone()) {
                if a != b {
                    self.sim.heal(nid(a), nid(b));
                }
            }
        }
        for id in self.control_ids() {
            self.sim.restart(nid(id));
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Healer {
    Restart(u64),
    Heal(u64, u64),
    NetReset,
    HealMember(u64),
}

/// Whether `m` has the era on.
pub fn era(m: &Metadata) -> bool {
    m.versioning_active()
}

/// The `(range, build)` records of `m`.
pub fn records(m: &Metadata) -> BTreeMap<NodeId, (VersionRange, String)> {
    m.node_versions
        .iter()
        .map(|(k, v)| (k.clone(), (v.range, v.build.clone())))
        .collect()
}
