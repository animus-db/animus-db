//! ADR 0079 section 1 (implementation step 3): per-node declared capacity in
//! `Metadata` -- the apply matrix of `RegisterNode.capacity` and
//! `SetNodeCapacity`, the `Gate::NodeCapacity` classification, and the rule
//! that the control leader's placement passes read capacity only once the gate
//! is open (gate closed = today's count balance, exactly).

use std::collections::{BTreeMap, BTreeSet};

use animus_control::meta::{ApplyOutcome, NodeAddrs};
use animus_control::schema::{ColumnType, TableSchema};
use animus_control::version::{Gate, GatedCommand, VersionRange};
use animus_control::{Capacity, MetaCommand, Metadata, NodeStatus, PlacementPolicy};
use animus_env::{NodeId, nid};
use animus_tablet::{KeyRange, TabletId};

fn addrs(n: u16, role: &str) -> NodeAddrs {
    NodeAddrs {
        internal: format!("127.0.0.1:{}", 9400 + n),
        client: format!("127.0.0.1:{}", 9000 + n),
        intra: format!("127.0.0.1:{}", 9600 + n),
        admin: format!("127.0.0.1:{}", 9500 + n),
        role: role.to_string(),
    }
}

fn cap(weight: u32, max: Option<u32>) -> Capacity {
    Capacity {
        weight,
        max_replicas: max,
    }
}

fn register(n: u64, role: &str, capacity: Option<Capacity>) -> MetaCommand {
    MetaCommand::RegisterNode {
        node: nid(n),
        addrs: addrs(n as u16, role),
        labels: BTreeMap::new(),
        capacity,
    }
}

fn set(n: u64, capacity: Capacity) -> MetaCommand {
    MetaCommand::SetNodeCapacity {
        node: nid(n),
        capacity,
    }
}

fn apply(m: &mut Metadata, c: &MetaCommand) -> ApplyOutcome {
    m.apply(c)
}

/// The two rows of the gate table, plus the field-by-content rule: a plain
/// registration stays `Base` (its bytes are unchanged), one that carries a
/// capacity is `NodeCapacity`.
#[test]
fn required_gate_rows() {
    assert_eq!(Gate::NodeCapacity.version(), Some(5));
    assert_eq!(
        set(1, Capacity::DEFAULT).required_gate(),
        Gate::NodeCapacity
    );
    assert_eq!(register(1, "combined", None).required_gate(), Gate::Base);
    assert_eq!(
        register(1, "combined", Some(cap(200, None))).required_gate(),
        Gate::NodeCapacity
    );
}

/// ADR 0079 step 6: `RepairSettled` shares the v5 gate; its apply is an
/// epoch-CAS (stale and unknown rejected), idempotent (`NoOp`), and the mark
/// is the tablet's own `settled_epoch`.
#[test]
fn repair_settled_gate_and_apply() {
    use animus_tablet::Epoch;
    let rs = |t: u64, e: u64| MetaCommand::RepairSettled {
        tablet: TabletId(t),
        epoch: Epoch(e),
    };
    assert_eq!(rs(1, 1).required_gate(), Gate::NodeCapacity);
    let mut m = Metadata::default();
    assert!(matches!(
        apply(&mut m, &rs(1, 1)),
        ApplyOutcome::Rejected(_)
    ));
    apply(
        &mut m,
        &MetaCommand::CreateTablet {
            tablet: TabletId(1),
            table: None,
            range: KeyRange::whole(),
            replicas: vec![nid(1)],
        },
    );
    assert_eq!(apply(&mut m, &rs(1, 1)), ApplyOutcome::Applied);
    assert_eq!(apply(&mut m, &rs(1, 1)), ApplyOutcome::NoOp);
    // A placement change makes the old report stale and the mark inapplicable.
    apply(
        &mut m,
        &MetaCommand::CasTabletReplicas {
            tablet: TabletId(1),
            expected_epoch: Epoch(1),
            replicas: vec![nid(1), nid(2)],
        },
    );
    assert!(matches!(
        apply(&mut m, &rs(1, 1)),
        ApplyOutcome::Rejected(_)
    ));
    assert_ne!(
        m.tablets[&TabletId(1)].settled_epoch,
        Some(m.tablets[&TabletId(1)].epoch)
    );
    assert_eq!(apply(&mut m, &rs(1, 2)), ApplyOutcome::Applied);
    assert_eq!(m.tablets[&TabletId(1)].settled_epoch, Some(Epoch(2)));
}

/// `Metadata::apply` never reads a gate (ADR 0073 decision 4): both commands
/// apply on a pre-era metadata; only the *emitters* are gated.
#[test]
fn apply_matrix() {
    let mut m = Metadata::default();
    // A fresh data-plane claim fills the capacity; a default one stores nothing.
    assert_eq!(
        apply(&mut m, &register(1, "combined", Some(cap(300, Some(800))))),
        ApplyOutcome::Applied
    );
    assert_eq!(m.capacity_of(&nid(1)), cap(300, Some(800)));
    assert_eq!(
        apply(&mut m, &register(2, "combined", Some(Capacity::DEFAULT))),
        ApplyOutcome::Applied
    );
    assert!(!m.node_capacity.contains_key(&nid(2)));
    // A control-only registration claims no member and so records nothing.
    assert_eq!(
        apply(&mut m, &register(3, "control", Some(cap(400, None)))),
        ApplyOutcome::Applied
    );
    assert!(!m.node_capacity.contains_key(&nid(3)));
    // Fill-if-empty: a re-registration never overwrites an existing capacity,
    // and an identical one is a NoOp.
    assert_eq!(
        apply(&mut m, &register(1, "combined", Some(cap(999, None)))),
        ApplyOutcome::NoOp
    );
    assert_eq!(m.capacity_of(&nid(1)), cap(300, Some(800)));
    // ... but it does fill a node that has none (a restart with a new flag).
    assert_eq!(
        apply(&mut m, &register(2, "combined", Some(cap(50, None)))),
        ApplyOutcome::Applied
    );
    assert_eq!(m.capacity_of(&nid(2)), cap(50, None));
    // SetNodeCapacity: idempotent, replaces, rejects unknown / zero weight,
    // and the default removes the entry (the canonical form).
    assert_eq!(apply(&mut m, &set(2, cap(50, None))), ApplyOutcome::NoOp);
    assert_eq!(
        apply(&mut m, &set(2, cap(70, Some(5)))),
        ApplyOutcome::Applied
    );
    assert_eq!(m.capacity_of(&nid(2)), cap(70, Some(5)));
    assert!(matches!(
        apply(&mut m, &set(9, cap(70, None))),
        ApplyOutcome::Rejected(_)
    ));
    assert!(matches!(
        apply(&mut m, &set(2, cap(0, None))),
        ApplyOutcome::Rejected(_)
    ));
    assert!(matches!(
        apply(&mut m, &set(3, cap(70, None))),
        ApplyOutcome::Rejected(_)
    ));
    assert_eq!(
        apply(&mut m, &set(2, Capacity::DEFAULT)),
        ApplyOutcome::Applied
    );
    assert!(!m.node_capacity.contains_key(&nid(2)));
    assert_eq!(
        apply(&mut m, &set(2, Capacity::DEFAULT)),
        ApplyOutcome::NoOp
    );
    // RemoveMember prunes the entry (the member must be drained first).
    assert_eq!(
        apply(
            &mut m,
            &MetaCommand::UpsertMember {
                node: nid(1),
                labels: BTreeMap::new(),
                status: NodeStatus::Leaving
            }
        ),
        ApplyOutcome::Applied
    );
    assert_eq!(
        apply(&mut m, &MetaCommand::RemoveMember { node: nid(1) }),
        ApplyOutcome::Applied
    );
    assert!(m.node_capacity.is_empty());
}

/// Everything placement may see is empty until the gate opens, whatever was
/// declared; `placement_capacity` follows.
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
            ApplyOutcome::Applied,
            "finalize to {target}"
        );
    }
}

/// `n_nodes` Active members; `tablets` single-replica tablets, all on node 1
/// (the unbalanced start), each with an RF-1 policy.
fn crowded(n_nodes: u64, tablets: u64) -> Metadata {
    let mut m = Metadata::default();
    for n in 1..=n_nodes {
        assert_eq!(
            m.apply(&MetaCommand::UpsertMember {
                node: nid(n),
                labels: BTreeMap::new(),
                status: NodeStatus::Active
            }),
            ApplyOutcome::Applied
        );
    }
    for t in 1..=tablets {
        // A table starts with exactly one tablet: one table per tablet.
        assert_eq!(
            m.apply(&MetaCommand::CreateTableSchema {
                table: format!("t{t}"),
                schema: TableSchema::simple("id", ColumnType::String)
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::CreateTablet {
                tablet: TabletId(t),
                table: Some(format!("t{t}")),
                range: KeyRange::whole(),
                replicas: vec![nid(1)]
            }),
            ApplyOutcome::Applied
        );
        assert_eq!(
            m.apply(&MetaCommand::SetTabletPolicy {
                tablet: TabletId(t),
                policy: Some(PlacementPolicy::simple("p", 1))
            }),
            ApplyOutcome::Applied
        );
    }
    m
}

fn counts(m: &Metadata) -> BTreeMap<NodeId, usize> {
    let mut out = BTreeMap::new();
    for (_, t) in m.tablets.iter() {
        for r in &t.replicas {
            *out.entry(r.clone()).or_insert(0) += 1;
        }
    }
    out
}

/// Converge by repeatedly applying the leader's single rebalance move.
fn converge(m: &mut Metadata) -> usize {
    let none_done: BTreeSet<TabletId> = BTreeSet::new();
    let none_down: BTreeSet<NodeId> = BTreeSet::new();
    let mut moves = 0;
    while let Some(cmd) = m.rebalance(&none_done, &none_down) {
        assert_eq!(m.apply(&cmd), ApplyOutcome::Applied);
        moves += 1;
        assert!(moves < 1000, "rebalance must terminate");
    }
    moves
}

/// Gate closed: declared capacities are invisible, so the result is exactly the
/// count balance of a cluster that declared nothing. Gate open: the same
/// declarations shape the result in proportion to weight.
#[test]
fn placement_reads_capacity_only_when_the_gate_is_open() {
    let declare = |m: &mut Metadata| {
        assert_eq!(m.apply(&set(2, cap(300, None))), ApplyOutcome::Applied);
    };
    // Reference: nothing declared.
    let mut plain = crowded(3, 30);
    converge(&mut plain);
    let plain_counts = counts(&plain);
    assert!(plain_counts.values().all(|&c| c == 10), "{plain_counts:?}");

    // Declared, gate closed (no era at all): identical to the reference.
    let mut closed = crowded(3, 30);
    declare(&mut closed);
    assert!(!closed.capacity_gate_open());
    assert!(closed.effective_capacities().is_empty());
    assert_eq!(closed.placement_capacity(&nid(2)), Capacity::DEFAULT);
    converge(&mut closed);
    assert_eq!(counts(&closed), plain_counts);
    assert_eq!(closed.tablets, plain.tablets);

    // Declared, gate open: node 2 (weight 300) takes three times node 3's share.
    let mut open = crowded(3, 30);
    open_gate(&mut open, &[1, 2, 3]);
    declare(&mut open);
    assert!(open.capacity_gate_open());
    assert_eq!(open.placement_capacity(&nid(2)), cap(300, None));
    converge(&mut open);
    let c = counts(&open);
    // weights 100 : 300 : 100 over 30 replicas -> 6 : 18 : 6.
    assert_eq!((c[&nid(1)], c[&nid(2)], c[&nid(3)]), (6, 18, 6), "{c:?}");
}

/// `max_replicas` is a hard ceiling for rebalance destinations and an over-cap
/// node is a drain-only source -- once the gate is open.
#[test]
fn max_replicas_binds_only_when_open_and_over_cap_nodes_drain() {
    let mut m = crowded(3, 12);
    open_gate(&mut m, &[1, 2, 3]);
    assert_eq!(m.apply(&set(2, cap(100, Some(2)))), ApplyOutcome::Applied);
    converge(&mut m);
    let c = counts(&m);
    assert!(c[&nid(2)] <= 2, "node 2 capped at 2: {c:?}");
    assert_eq!(c.values().sum::<usize>(), 12);
    // Lower the cap below what it holds: it drains toward the others.
    assert_eq!(m.apply(&set(2, cap(100, Some(0)))), ApplyOutcome::Applied);
    converge(&mut m);
    assert_eq!(counts(&m).get(&nid(2)).copied().unwrap_or(0), 0);

    // The same ceiling with the gate closed is ignored.
    let mut closed = crowded(3, 12);
    assert_eq!(
        closed.apply(&set(2, cap(100, Some(0)))),
        ApplyOutcome::Applied
    );
    converge(&mut closed);
    assert_eq!(counts(&closed)[&nid(2)], 4);
}
