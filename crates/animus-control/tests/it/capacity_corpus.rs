//! Placement-capacity corpus (ADR 0079 section 1, step 7): Tier 1 cells 10-12
//! and negative controls (g) and (h).
//!
//! * **Cell 10, heterogeneous nodes.** Weights 100/100/200 (and seeded
//!   variants): tablets minted through the capacity-aware creation path, a node
//!   added, the leader's rebalance run to a fixed point. Final replica counts
//!   are proportional to weight within `1/min_weight`; then a lowered
//!   `max_replicas` drains the node, losing no replica. Control **(g)**: the
//!   same run with weights ignored (count balance) fails the proportionality
//!   bound.
//! * **Cell 11, capacity exhausted** (the real `reconcile_loop`). Every node at
//!   `max_replicas`: a new tablet is refused with `CapacityExhausted`; a node
//!   dies and repair is best effort, strictly bounded by the caps (no node
//!   ever above its ceiling), with `PlacementCapacityBlockedTablets` non-zero;
//!   raising the caps resolves it and the gauge returns to 0. Control **(h)**:
//!   best-effort over-placement on creation puts a node over its cap.
//! * **Cell 12, region-pinned capacity** (ADR 0075). A Region whose nodes are
//!   all at `max_replicas` is reported (the wire edge refuses the global
//!   conversion on it); once converted, a lost Region's replica waits and is
//!   never placed in another Region.
//!
//! Depth knob `ANIMUS_CAPACITY_SEEDS=K` (default 1; K seeds per cell), replay
//! with `ANIMUS_SEED=<seed>`.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use animus_control::node::heartbeat_loop;
use animus_control::raft::ProposeResult;
use animus_control::schema::{GlobalTableSpec, MultiRegionConsistency};
use animus_control::version::VersionRange;
use animus_control::{ApplyOutcome, Capacity, MetaCommand, Metadata, NodeStatus, RaftNode};
use animus_env::{EnvExt, Metric, MetricsHandle, NodeId, nid};
use animus_placement::{PlacementPolicy, REGION_LABEL};
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{KeyRange, TabletId};

fn seeds(base: u64) -> Vec<u64> {
    if let Ok(s) = std::env::var("ANIMUS_SEED") {
        return vec![s.parse().expect("ANIMUS_SEED")];
    }
    let k: u64 = std::env::var("ANIMUS_CAPACITY_SEEDS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    (0..k).map(|i| base + i * 0x9E37).collect()
}

/// splitmix64: a tiny seeded generator for the pure cells.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
}

fn cap(weight: u32, max: Option<u32>) -> Capacity {
    Capacity {
        weight,
        max_replicas: max,
    }
}

fn upsert(m: &mut Metadata, n: u64, labels: BTreeMap<String, String>, status: NodeStatus) {
    assert_eq!(
        m.apply(&MetaCommand::UpsertMember {
            node: nid(n),
            labels,
            status
        }),
        ApplyOutcome::Applied
    );
}

/// Report every node's version range and finalize up to the capacity gate.
fn open_gate(m: &mut Metadata, nodes: &[u64]) {
    for &n in nodes {
        assert_eq!(
            m.apply(&MetaCommand::ReportNodeVersion {
                node: nid(n),
                range: VersionRange::new(1, 5),
                build: "t".into(),
            }),
            ApplyOutcome::Applied
        );
    }
    for target in 2..=5 {
        assert_eq!(
            m.apply(&MetaCommand::FinalizeClusterVersion {
                expected: target - 1,
                target
            }),
            ApplyOutcome::Applied
        );
    }
}

fn counts(m: &Metadata) -> BTreeMap<NodeId, usize> {
    let mut out = BTreeMap::new();
    for t in m.tablets.values() {
        for r in &t.replicas {
            *out.entry(r.clone()).or_insert(0) += 1;
        }
    }
    out
}

/// Mint one RF-`rf` tablet through the capacity-aware creation path (what
/// `CreateTable` uses). `Ok(false)` = refused for lack of capacity.
fn create_tablet(
    m: &mut Metadata,
    id: u64,
    rf: usize,
) -> Result<(), animus_control::CapacityExhausted> {
    let policy = PlacementPolicy::simple("p", rf);
    let replicas = match m.capacity_initial_replicas(&policy) {
        Some(r) => r?,
        None => m
            .members
            .iter()
            .filter(|(_, mm)| mm.status == NodeStatus::Active)
            .map(|(n, _)| n.clone())
            .take(rf)
            .collect(),
    };
    assert_eq!(
        m.apply(&MetaCommand::CreateTablet {
            tablet: TabletId(id),
            table: None,
            range: KeyRange::whole(),
            replicas,
        }),
        ApplyOutcome::Applied
    );
    assert_eq!(
        m.apply(&MetaCommand::SetTabletPolicy {
            tablet: TabletId(id),
            policy: Some(policy),
        }),
        ApplyOutcome::Applied
    );
    Ok(())
}

/// Apply the leader's rebalance (and repair) moves to a fixed point.
fn converge(m: &mut Metadata) -> usize {
    let none_done: BTreeSet<TabletId> = BTreeSet::new();
    let none_down: BTreeSet<NodeId> = BTreeSet::new();
    let mut moves = 0;
    loop {
        let mut progressed = false;
        for cmd in m.reconcile(&none_done, &none_down) {
            assert_eq!(m.apply(&cmd), ApplyOutcome::Applied);
            moves += 1;
            progressed = true;
        }
        if let Some(cmd) = m.rebalance(&none_done, &none_down) {
            assert_eq!(m.apply(&cmd), ApplyOutcome::Applied);
            moves += 1;
            progressed = true;
        }
        if !progressed {
            return moves;
        }
        assert!(moves < 5000, "rebalance must terminate");
    }
}

/// ADR 0079 rule 4: `|r(n)/w(n) - r(m)/w(m)| <= 1/min_weight` for every pair,
/// cross-multiplied (exact integers).
fn proportional(m: &Metadata, nodes: &[(u64, u32)]) -> Result<(), String> {
    let c = counts(m);
    let min_w = u128::from(nodes.iter().map(|n| n.1).min().unwrap());
    for &(a, wa) in nodes {
        for &(b, wb) in nodes {
            let ra = c.get(&nid(a)).copied().unwrap_or(0) as u128;
            let rb = c.get(&nid(b)).copied().unwrap_or(0) as u128;
            let (wa, wb) = (u128::from(wa), u128::from(wb));
            let diff = (ra * wb).abs_diff(rb * wa);
            if diff * min_w > wa * wb {
                return Err(format!(
                    "node {a} (w{wa}, {ra} replicas) vs node {b} (w{wb}, {rb}): \
                     |{ra}/{wa} - {rb}/{wb}| > 1/{min_w}; counts {c:?}"
                ));
            }
        }
    }
    Ok(())
}

/// One run of cell 10. `honor_weights = false` is control (g): the declared
/// weights are never seen by placement (the gate stays closed), i.e. a plain
/// count balance.
fn run_cell10(seed: u64, honor_weights: bool) -> Result<(), String> {
    let mut rng = Rng(seed);
    // The ADR's shape first (100/100/200); later seeds draw other weights.
    let mut nodes: Vec<(u64, u32)> = if seed & 1 == 0 || seed.is_multiple_of(3) {
        vec![(1, 100), (2, 100), (3, 200)]
    } else {
        (1..=3)
            .map(|n| (n, 100 * (1 + rng.below(3) as u32)))
            .collect()
    };
    // Make sure the weights are not all equal (the cell is about unequal ones).
    if nodes.iter().all(|n| n.1 == nodes[0].1) {
        nodes[2].1 = nodes[0].1 * 2;
    }
    let rf = if rng.below(2) == 0 { 1 } else { 2 };
    let tablets = 60 + rng.below(40);
    let mut m = Metadata::default();
    for &(n, _) in &nodes {
        upsert(&mut m, n, BTreeMap::new(), NodeStatus::Active);
    }
    if honor_weights {
        open_gate(&mut m, &[1, 2, 3]);
    }
    for &(n, w) in &nodes {
        assert!(matches!(
            m.apply(&MetaCommand::SetNodeCapacity {
                node: nid(n),
                capacity: cap(w, None)
            }),
            ApplyOutcome::Applied | ApplyOutcome::NoOp
        ));
    }
    // "Create tables": every tablet minted through the creation path.
    for t in 1..=tablets {
        create_tablet(&mut m, t, rf).map_err(|e| e.to_string())?;
    }
    // "Add a node": a fresh member, then converge.
    let new_w = 100 * (1 + rng.below(3) as u32);
    nodes.push((4, new_w));
    upsert(&mut m, 4, BTreeMap::new(), NodeStatus::Active);
    assert!(matches!(
        m.apply(&MetaCommand::SetNodeCapacity {
            node: nid(4),
            capacity: cap(new_w, None)
        }),
        ApplyOutcome::Applied | ApplyOutcome::NoOp
    ));
    converge(&mut m);
    proportional(&m, &nodes).map_err(|e| format!("seed {seed:#x}: {e}"))?;
    if !honor_weights {
        return Ok(());
    }

    // A lowered max_replicas drains the heaviest node without losing a
    // replica: the total is unchanged, every tablet keeps `rf` distinct
    // replicas, and nothing is placed onto the node afterwards.
    let before = counts(&m);
    let total: usize = before.values().sum();
    let (heavy, held) = before.iter().max_by_key(|(_, c)| **c).unwrap();
    let held = *held;
    let lowered = held as u32 / 2;
    assert_eq!(
        m.apply(&MetaCommand::SetNodeCapacity {
            node: heavy.clone(),
            capacity: cap(
                nodes.iter().find(|n| nid(n.0) == *heavy).unwrap().1,
                Some(lowered)
            )
        }),
        ApplyOutcome::Applied
    );
    converge(&mut m);
    let after = counts(&m);
    if after.values().sum::<usize>() != total {
        return Err(format!(
            "seed {seed:#x}: replicas lost in the drain: {after:?}"
        ));
    }
    if after.get(heavy).copied().unwrap_or(0) > lowered as usize {
        return Err(format!(
            "seed {seed:#x}: {heavy} still above its lowered cap {lowered}: {after:?}"
        ));
    }
    for t in m.tablets.values() {
        let set: BTreeSet<&NodeId> = t.replicas.iter().collect();
        if set.len() != rf || t.replicas.len() != rf {
            return Err(format!(
                "seed {seed:#x}: tablet lost a replica: {:?}",
                t.replicas
            ));
        }
    }
    Ok(())
}

#[test]
fn cell10_heterogeneous_nodes_are_filled_in_proportion_and_drain() {
    for seed in seeds(0xCA10_0001) {
        run_cell10(seed, true).unwrap_or_else(|e| panic!("{e}"));
    }
}

#[test]
fn negative_control_g_count_balance_fails_the_proportionality_bound() {
    for seed in seeds(0xCA10_0002) {
        assert!(
            run_cell10(seed, false).is_err(),
            "seed {seed:#x}: a count balance satisfied the weighted bound"
        );
    }
}

// ---------------------------------------------------------------- cell 11

const CONTROL: [u64; 3] = [0, 1, 2];
const DATA: [u64; 4] = [10, 11, 12, 13];
const MAX: u32 = 3;

struct Rig {
    sim: Simulator,
    nodes: Vec<RaftNode<SimEnv>>,
    handles: Vec<MetricsHandle>,
    leader: usize,
}

impl Rig {
    fn propose(&mut self, cmd: MetaCommand) {
        assert!(
            matches!(
                self.nodes[self.leader].propose(cmd.clone()),
                ProposeResult::Accepted { .. }
            ),
            "{cmd:?} not accepted"
        );
        self.sim.run_for(Duration::from_millis(300));
    }
    fn meta(&self) -> Metadata {
        self.nodes[self.leader].metadata()
    }
    fn gauge(&self) -> u64 {
        self.handles[self.leader].get(Metric::PlacementCapacityBlockedTablets)
    }
}

fn rig(seed: u64) -> Rig {
    let mut sim = Simulator::new(seed);
    let handles: Vec<MetricsHandle> = CONTROL.iter().map(|_| MetricsHandle::recording()).collect();
    let nodes: Vec<RaftNode<SimEnv>> = CONTROL
        .iter()
        .enumerate()
        .map(|(i, &id)| {
            RaftNode::start_with_metrics(
                sim.env(nid(id)),
                CONTROL.iter().copied().map(nid).collect(),
                handles[i].clone(),
                MemoryEngine::new(),
            )
        })
        .collect();
    for id in DATA {
        let env = sim.env(nid(id));
        env.spawn_task(heartbeat_loop(
            env.clone(),
            CONTROL.iter().copied().map(nid).collect(),
        ));
    }
    // The version era starts once every required node is observed on a B2
    // handshake and a control node advertises its own range (as `animusd`
    // does); the data nodes are reported below.
    for id in CONTROL.iter().chain(&DATA) {
        sim.set_network_ext_for(
            nid(*id),
            animus_env::handshake::encode_ext(Some((1, 5)), Some("t")),
        );
    }
    for n in &nodes {
        n.set_own_version_range(Some(VersionRange::new(1, 5)));
        n.set_own_build("t");
    }
    sim.run_for(Duration::from_secs(3));
    let leader = (0..3).find(|&i| nodes[i].is_leader()).expect("a leader");
    let mut r = Rig {
        sim,
        nodes,
        handles,
        leader,
    };
    for id in DATA {
        r.propose(MetaCommand::UpsertMember {
            node: nid(id),
            labels: BTreeMap::new(),
            status: NodeStatus::Active,
        });
    }
    for _ in 0..100 {
        if r.meta().versioning_active() {
            break;
        }
        r.sim.run_for(Duration::from_millis(200));
    }
    assert!(
        r.meta().versioning_active(),
        "the version era never started"
    );
    for id in DATA {
        r.propose(MetaCommand::ReportNodeVersion {
            node: nid(id),
            range: VersionRange::new(1, 5),
            build: "t".into(),
        });
    }
    for target in 2..=5 {
        r.propose(MetaCommand::FinalizeClusterVersion {
            expected: target - 1,
            target,
        });
    }
    assert!(
        r.meta().capacity_gate_open(),
        "the gate must be open: version {} finalize said {:?}; reports {:?} members {:?}",
        r.meta().cluster_version(),
        r.meta()
            .clone()
            .apply(&MetaCommand::FinalizeClusterVersion {
                expected: r.meta().cluster_version(),
                target: r.meta().cluster_version() + 1
            }),
        r.meta().node_versions.keys().collect::<Vec<_>>(),
        r.meta().members.keys().collect::<Vec<_>>()
    );
    r
}

fn over_cap(m: &Metadata) -> Vec<(NodeId, usize, u32)> {
    let c = counts(m);
    m.node_capacity
        .iter()
        .filter_map(|(n, cp)| {
            let have = c.get(n).copied().unwrap_or(0);
            cp.max_replicas
                .filter(|&mx| have > mx as usize)
                .map(|mx| (n.clone(), have, mx))
        })
        .collect()
}

/// One run of cell 11. `overplace = true` is control (h): creation falls back
/// to best-effort over-placement past `max_replicas` when every node is full.
fn run_cell11(seed: u64, overplace: bool) -> Result<(), String> {
    let mut r = rig(seed);
    // Four RF3 tablets over four nodes, each node holding exactly MAX = 3.
    for id in DATA {
        r.propose(MetaCommand::SetNodeCapacity {
            node: nid(id),
            capacity: cap(100, Some(MAX)),
        });
    }
    let policy = PlacementPolicy::simple("rf3", 3);
    for i in 0..4u64 {
        let replicas: Vec<NodeId> = (0..3).map(|k| nid(DATA[((i + k) % 4) as usize])).collect();
        r.propose(MetaCommand::CreateTablet {
            tablet: TabletId(i + 1),
            table: None,
            range: KeyRange::whole(),
            replicas,
        });
        r.propose(MetaCommand::SetTabletPolicy {
            tablet: TabletId(i + 1),
            policy: Some(policy.clone()),
        });
    }
    let m = r.meta();
    if counts(&m).values().any(|&c| c != MAX as usize) {
        return Err(format!("setup: {:?}", counts(&m)));
    }

    // (1) Creation is refused: every node is at max_replicas.
    match m.capacity_initial_replicas(&policy) {
        Some(Err(e)) => {
            if e.active != DATA.len() {
                return Err(format!("refusal names {} active nodes", e.active));
            }
        }
        other if !overplace => return Err(format!("creation not refused: {other:?}")),
        _ => {}
    }
    if overplace {
        // Control (h): the pre-ADR first-N placement ignores the ceilings.
        r.propose(MetaCommand::CreateTablet {
            tablet: TabletId(99),
            table: None,
            range: KeyRange::whole(),
            replicas: DATA[..3].iter().map(|&n| nid(n)).collect(),
        });
    }
    let over = over_cap(&r.meta());
    if !over.is_empty() {
        return Err(format!("nodes above max_replicas after creation: {over:?}"));
    }

    // (2) A node dies. Repair is best effort and bounded by the caps.
    let dead = nid(DATA[0]);
    r.sim.crash(dead.clone());
    r.propose(MetaCommand::UpsertMember {
        node: dead.clone(),
        labels: BTreeMap::new(),
        status: NodeStatus::Down,
    });
    r.sim.run_for(Duration::from_secs(25));
    let m = r.meta();
    let over = over_cap(&m);
    if !over.is_empty() {
        return Err(format!("repair placed past max_replicas: {over:?}"));
    }
    let violating = m
        .tablets
        .values()
        .filter(|t| t.replicas.contains(&dead))
        .count();
    let live_short = m
        .tablets
        .values()
        .filter(|t| t.replicas.iter().filter(|n| **n != dead).count() < 3)
        .count();
    if live_short == 0 {
        return Err("no tablet is under-replicated: the caps did not bind".into());
    }
    if r.gauge() == 0 || r.gauge() as usize != live_short {
        return Err(format!(
            "blocked gauge {} but {live_short} tablets are capacity-blocked (V={violating})",
            r.gauge()
        ));
    }

    // (3) Raising the caps resolves it.
    for id in &DATA[1..] {
        r.propose(MetaCommand::SetNodeCapacity {
            node: nid(*id),
            capacity: cap(100, Some(6)),
        });
    }
    r.sim.run_for(Duration::from_secs(40));
    let m = r.meta();
    if m.tablets
        .values()
        .any(|t| t.replicas.len() != 3 || t.replicas.contains(&dead))
    {
        return Err(format!(
            "raising the cap did not restore RF3 on live nodes: {:?}",
            m.tablets
                .values()
                .map(|t| t.replicas.clone())
                .collect::<Vec<_>>()
        ));
    }
    if r.gauge() != 0 {
        return Err(format!(
            "gauge stuck at {} after the cap was raised",
            r.gauge()
        ));
    }
    Ok(())
}

#[test]
fn cell11_capacity_exhausted_refuses_creation_and_bounds_repair() {
    for seed in seeds(0xCA11_0001) {
        run_cell11(seed, false).unwrap_or_else(|e| panic!("seed {seed:#x}: {e}"));
    }
}

#[test]
fn negative_control_h_overplacing_creation_exceeds_a_cap() {
    for seed in seeds(0xCA11_0002) {
        let err = run_cell11(seed, true).expect_err("over-placement must violate the cap oracle");
        assert!(err.contains("above max_replicas"), "seed {seed:#x}: {err}");
    }
}

// ---------------------------------------------------------------- cell 12

fn region_world(seed: u64) -> Metadata {
    let mut rng = Rng(seed);
    let mut m = Metadata::default();
    let regions = [("a", [1, 2]), ("b", [3, 4]), ("c", [5, 6])];
    for (region, ids) in regions {
        for n in ids {
            upsert(
                &mut m,
                n,
                BTreeMap::from([(REGION_LABEL.to_string(), region.to_string())]),
                NodeStatus::Active,
            );
        }
    }
    open_gate(&mut m, &[1, 2, 3, 4, 5, 6]);
    // The (still plain) table that will become global: one RF3 tablet.
    assert_eq!(
        m.apply(&MetaCommand::CreateTableSchema {
            table: "t".into(),
            schema: animus_control::TableSchema::simple("id", animus_control::ColumnType::String),
        }),
        ApplyOutcome::Applied
    );
    assert_eq!(
        m.apply(&MetaCommand::CreateTablet {
            tablet: TabletId(1),
            table: Some("t".into()),
            range: KeyRange::whole(),
            replicas: vec![nid(1), nid(2), nid(5)],
        }),
        ApplyOutcome::Applied
    );
    assert_eq!(
        m.apply(&MetaCommand::SetTabletPolicy {
            tablet: TabletId(1),
            policy: Some(PlacementPolicy::simple("p", 3)),
        }),
        ApplyOutcome::Applied
    );
    // Plain background tablets that fill Region b exactly (each b node holds
    // `fill` replicas = its ceiling); a and c stay roomy.
    let fill = 2 + rng.below(3) as u32;
    let mut id = 100;
    for n in [3u64, 4] {
        for _ in 0..fill {
            id += 1;
            assert_eq!(
                m.apply(&MetaCommand::CreateTablet {
                    tablet: TabletId(id),
                    table: None,
                    range: KeyRange::whole(),
                    replicas: vec![nid(n)],
                }),
                ApplyOutcome::Applied
            );
            assert_eq!(
                m.apply(&MetaCommand::SetTabletPolicy {
                    tablet: TabletId(id),
                    policy: Some(PlacementPolicy::simple("bg", 1)),
                }),
                ApplyOutcome::Applied
            );
        }
        assert_eq!(
            m.apply(&MetaCommand::SetNodeCapacity {
                node: nid(n),
                capacity: cap(100, Some(fill)),
            }),
            ApplyOutcome::Applied
        );
    }
    m
}

fn regions() -> Vec<String> {
    vec!["a".into(), "b".into(), "c".into()]
}

#[test]
fn cell12_a_full_region_refuses_the_conversion_and_a_lost_region_waits() {
    for seed in seeds(0xCA12_0001) {
        let mut m = region_world(seed);
        // (1) Region b is full: the wire edge refuses the conversion on this.
        assert_eq!(
            m.regions_without_capacity(&regions()),
            vec!["b".to_string()],
            "seed {seed:#x}"
        );
        // ... and a roomy Region is not reported.
        assert!(
            m.regions_without_capacity(&["a".into(), "c".into()])
                .is_empty()
        );

        // (2) Raise b's ceilings: the conversion is allowed and converges to
        // one replica per Region, each Region node within its cap.
        for n in [3u64, 4] {
            assert_eq!(
                m.apply(&MetaCommand::SetNodeCapacity {
                    node: nid(n),
                    capacity: cap(100, Some(50)),
                }),
                ApplyOutcome::Applied
            );
        }
        assert!(m.regions_without_capacity(&regions()).is_empty());
        let spec = GlobalTableSpec {
            consistency: MultiRegionConsistency::Strong,
            regions: regions(),
            witness: None,
            preferred_leader_region: "a".into(),
            replicas: Vec::new(),
        };
        assert_eq!(
            m.apply(&MetaCommand::ConvertTableToGlobal {
                table: "t".into(),
                spec
            }),
            ApplyOutcome::Applied
        );
        converge(&mut m);
        let region_of = |m: &Metadata, n: &NodeId| m.members[n].labels[REGION_LABEL].clone();
        let t = &m.tablets[&TabletId(1)];
        let got: BTreeSet<String> = t.replicas.iter().map(|n| region_of(&m, n)).collect();
        assert_eq!(t.replicas.len(), 3, "seed {seed:#x}: {:?}", t.replicas);
        assert_eq!(got.len(), 3, "seed {seed:#x}: one replica per Region");

        // (3) Region c is lost entirely: its replica waits. Nothing is placed
        // in another Region (a and b have room), and the set keeps the dead
        // replica.
        let held = t.replicas.clone();
        for n in [5u64, 6] {
            upsert(
                &mut m,
                n,
                BTreeMap::from([(REGION_LABEL.to_string(), "c".into())]),
                NodeStatus::Down,
            );
        }
        let cmds = m.reconcile(&BTreeSet::new(), &BTreeSet::new());
        assert!(
            cmds.iter().all(|c| !matches!(
                c,
                MetaCommand::CasTabletReplicas { tablet, .. } if *tablet == TabletId(1)
            )),
            "seed {seed:#x}: a lost Region's replica must wait, got {cmds:?}"
        );
        assert_eq!(m.tablets[&TabletId(1)].replicas, held);
    }
}
