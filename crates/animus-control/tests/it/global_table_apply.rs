//! `MetaCommand::ConvertTableToGlobal` apply matrix (ADR 0075, G-01 stage
//! G-c, M1): accept, every state-based rejection, idempotence, the atomic
//! policy replacement, the reconcile pinned path (convergence from a
//! violating start), the gate row, and the pinned JSON shape. Pure
//! `Metadata` tests, no simulator.

use std::collections::BTreeMap;

use animus_control::schema::{GlobalTableSpec, MultiRegionConsistency};
use animus_control::version::{Gate, GatedCommand};
use animus_control::{
    ApplyOutcome, ColumnType, IndexDef, IndexKind, IndexProjection, IndexStatus, MetaCommand,
    Metadata, NodeStatus, PlacementPolicy, TableSchema, TtlSpec,
};
use animus_env::nid;
use animus_placement::REGION_LABEL;
use animus_tablet::{KeyRange, TabletId};

fn spec() -> GlobalTableSpec {
    GlobalTableSpec {
        consistency: MultiRegionConsistency::Strong,
        regions: vec!["a".into(), "b".into(), "c".into()],
        witness: None,
        preferred_leader_region: "a".into(),
    }
}

fn convert(table: &str, spec: GlobalTableSpec) -> MetaCommand {
    MetaCommand::ConvertTableToGlobal {
        table: table.to_string(),
        spec,
    }
}

/// Six `Active` members, two per region a/b/c (ids 1..=6), a table `t` with
/// one tablet whose replicas are `replicas` and a plain RF-3 policy.
fn world(replicas: &[u64]) -> Metadata {
    let mut m = Metadata::default();
    for (n, region) in [(1, "a"), (2, "a"), (3, "b"), (4, "b"), (5, "c"), (6, "c")] {
        let labels = BTreeMap::from([(REGION_LABEL.to_string(), region.to_string())]);
        assert_eq!(
            m.apply(&MetaCommand::UpsertMember {
                node: nid(n),
                labels,
                status: NodeStatus::Active,
            }),
            ApplyOutcome::Applied
        );
    }
    assert_eq!(
        m.apply(&MetaCommand::CreateTableSchema {
            table: "t".into(),
            schema: TableSchema::simple("id", ColumnType::String),
        }),
        ApplyOutcome::Applied
    );
    assert_eq!(
        m.apply(&MetaCommand::CreateTablet {
            tablet: TabletId(1),
            table: Some("t".into()),
            range: KeyRange::whole(),
            replicas: replicas.iter().map(|&n| nid(n)).collect(),
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
    m
}

#[test]
fn convert_sets_the_spec_and_pins_every_tablet_atomically() {
    let mut m = world(&[1, 3, 5]);
    assert_eq!(m.apply(&convert("t", spec())), ApplyOutcome::Applied);
    assert_eq!(m.schemas.get("t").unwrap().global, Some(spec()));
    let policy = &m.policies[&TabletId(1)];
    assert!(policy.is_pinned());
    assert_eq!(policy.replication_factor, 3);
    assert_eq!(
        policy.allowed_values[REGION_LABEL],
        ["a", "b", "c"].iter().map(|s| s.to_string()).collect()
    );
}

#[test]
fn identical_reapply_is_a_noop_and_a_different_spec_is_rejected() {
    let mut m = world(&[1, 3, 5]);
    assert_eq!(m.apply(&convert("t", spec())), ApplyOutcome::Applied);
    assert_eq!(m.apply(&convert("t", spec())), ApplyOutcome::NoOp);
    let mut other = spec();
    other.preferred_leader_region = "b".into();
    assert_eq!(
        m.apply(&convert("t", other)),
        ApplyOutcome::Rejected("table is already a global table")
    );
}

#[test]
fn rejections_are_named_and_leave_the_state_untouched() {
    // No schema.
    let mut m = world(&[1, 3, 5]);
    assert_eq!(
        m.apply(&convert("nope", spec())),
        ApplyOutcome::Rejected("no such table schema")
    );
    // Invalid spec (two regions only).
    let mut bad = spec();
    bad.regions.pop();
    let before = m.clone();
    assert!(matches!(
        m.apply(&convert("t", bad)),
        ApplyOutcome::Rejected(r) if r.contains("exactly three regions")
    ));
    assert_eq!(m, before);
    // TTL enabled.
    assert_eq!(
        m.apply(&MetaCommand::SetTableTtl {
            table: "t".into(),
            spec: Some(TtlSpec {
                attribute_name: "exp".into()
            }),
        }),
        ApplyOutcome::Applied
    );
    assert_eq!(
        m.apply(&convert("t", spec())),
        ApplyOutcome::Rejected("global table cannot have TTL enabled")
    );
    // LSI.
    let mut m = world(&[1, 3, 5]);
    assert_eq!(
        m.apply(&MetaCommand::CreateTableIndex {
            table: "t".into(),
            index: IndexDef {
                name: "lsi".into(),
                kind: IndexKind::Local,
                hash_attribute: "id".into(),
                sort_attribute: Some("s".into()),
                projection: IndexProjection::All,
                status: IndexStatus::Active,
                hash_attribute_type: None,
                sort_attribute_type: None,
            },
        }),
        ApplyOutcome::Applied
    );
    assert_eq!(
        m.apply(&convert("t", spec())),
        ApplyOutcome::Rejected("global table cannot have a local index")
    );
    // No tablets.
    let mut m = Metadata::default();
    m.apply(&MetaCommand::CreateTableSchema {
        table: "t".into(),
        schema: TableSchema::simple("id", ColumnType::String),
    });
    assert_eq!(
        m.apply(&convert("t", spec())),
        ApplyOutcome::Rejected("table has no tablets to convert")
    );
}

#[test]
fn a_global_table_refuses_ttl_and_lsi_afterwards() {
    let mut m = world(&[1, 3, 5]);
    assert_eq!(m.apply(&convert("t", spec())), ApplyOutcome::Applied);
    assert_eq!(
        m.apply(&MetaCommand::SetTableTtl {
            table: "t".into(),
            spec: Some(TtlSpec {
                attribute_name: "exp".into()
            }),
        }),
        ApplyOutcome::Rejected("global table cannot have TTL enabled")
    );
    // Disabling a (never-enabled) TTL stays a no-op.
    assert_eq!(
        m.apply(&MetaCommand::SetTableTtl {
            table: "t".into(),
            spec: None,
        }),
        ApplyOutcome::NoOp
    );
    assert_eq!(
        m.apply(&MetaCommand::CreateTableIndex {
            table: "t".into(),
            index: IndexDef {
                name: "lsi".into(),
                kind: IndexKind::Local,
                hash_attribute: "id".into(),
                sort_attribute: Some("s".into()),
                projection: IndexProjection::All,
                status: IndexStatus::Active,
                hash_attribute_type: None,
                sort_attribute_type: None,
            },
        }),
        ApplyOutcome::Rejected("global table cannot have a local index")
    );
}

/// The command is gated on `GlobalTables`; an exhaustive `required_gate`
/// row (mutation: downgrade it to `Base` and this fails).
#[test]
fn convert_requires_the_global_tables_gate() {
    assert_eq!(convert("t", spec()).required_gate(), Gate::GlobalTables);
    assert_eq!(MetaCommand::NoOp.required_gate(), Gate::Base);
    assert_eq!(Gate::GlobalTables.version(), Some(2));
}

/// The pinned JSON shape of the command and of the schema field (class G):
/// a field that moves would change what an older binary cannot decode.
#[test]
fn pinned_json_shapes() {
    let cmd = serde_json::to_string(&convert("t", spec())).unwrap();
    assert_eq!(
        cmd,
        r#"{"ConvertTableToGlobal":{"table":"t","spec":{"consistency":"Strong","regions":["a","b","c"],"preferred_leader_region":"a"}}}"#
    );
    let mut with_witness = spec();
    with_witness.witness = Some("c".into());
    let json = serde_json::to_string(&with_witness).unwrap();
    assert!(json.contains(r#""witness":"c""#), "{json}");
    // A table with no global spec never writes the key (byte-identical to
    // before the field existed).
    let plain = serde_json::to_string(&TableSchema::simple("id", ColumnType::String)).unwrap();
    assert!(!plain.contains("global"), "{plain}");
}

/// Reconcile's pinned path converges a **violating** start (two replicas in
/// region a) to one-per-region through ordinary `CasTabletReplicas`, and a
/// non-pinned policy on the same skewed set is left alone (documented
/// behaviour of plain `replan_repair`).
#[test]
fn reconcile_converges_a_skewed_pinned_tablet_and_leaves_an_unpinned_one() {
    let none = std::collections::BTreeSet::new();
    let no_nodes = std::collections::BTreeSet::new();
    // Unpinned: replicas {1,2,3} (a,a,b) satisfy RF 3, so repair proposes nothing.
    let m = world(&[1, 2, 3]);
    assert!(m.reconcile(&none, &no_nodes).is_empty());

    // Pinned: converge.
    let mut m = world(&[1, 2, 3]);
    assert_eq!(m.apply(&convert("t", spec())), ApplyOutcome::Applied);
    for round in 0..4 {
        let cmds = m.reconcile(&none, &no_nodes);
        if cmds.is_empty() {
            let reps = &m.tablets[&TabletId(1)].replicas;
            let mut regions: Vec<String> = reps
                .iter()
                .map(|n| m.members[n].labels[REGION_LABEL].clone())
                .collect();
            regions.sort();
            assert_eq!(regions, ["a", "b", "c"], "converged after {round} rounds");
            return;
        }
        for c in &cmds {
            assert!(matches!(c, MetaCommand::CasTabletReplicas { .. }));
            assert_eq!(m.apply(c), ApplyOutcome::Applied);
        }
    }
    panic!("pinned reconcile did not converge");
}

/// A pinned region with no live node yields no command at all (the replica
/// waits for its region; no cross-region repair), and a node of the same
/// region replaces a lost one in place.
#[test]
fn reconcile_never_repairs_across_regions_but_does_within_one() {
    let none = std::collections::BTreeSet::new();
    let no_nodes = std::collections::BTreeSet::new();
    let mut m = world(&[1, 3, 5]);
    assert_eq!(m.apply(&convert("t", spec())), ApplyOutcome::Applied);
    assert!(
        m.reconcile(&none, &no_nodes).is_empty(),
        "already compliant"
    );

    // Node 5 down: node 6 (same region c) takes over.
    m.apply(&MetaCommand::UpsertMember {
        node: nid(5),
        labels: m.members[&nid(5)].labels.clone(),
        status: NodeStatus::Down,
    });
    let cmds = m.reconcile(&none, &no_nodes);
    assert_eq!(cmds.len(), 1);
    let MetaCommand::CasTabletReplicas { replicas, .. } = &cmds[0] else {
        panic!("{cmds:?}")
    };
    assert_eq!(replicas, &vec![nid(1), nid(3), nid(6)]);

    // Both region-c nodes down: no command (never a different region).
    m.apply(&MetaCommand::UpsertMember {
        node: nid(6),
        labels: m.members[&nid(6)].labels.clone(),
        status: NodeStatus::Down,
    });
    assert!(m.reconcile(&none, &no_nodes).is_empty());
}

#[test]
fn global_spec_validation_matrix() {
    use animus_control::GlobalSpecError as E;
    assert_eq!(spec().validate(), Ok(()));
    let mut s = spec();
    s.regions = vec!["a".into(), "b".into(), "a".into()];
    assert_eq!(s.validate(), Err(E::DuplicateRegion));
    let mut s = spec();
    s.regions[1] = String::new();
    assert_eq!(s.validate(), Err(E::EmptyRegion));
    let mut s = spec();
    s.witness = Some("z".into());
    assert_eq!(s.validate(), Err(E::WitnessNotInRegions));
    let mut s = spec();
    s.preferred_leader_region = "z".into();
    assert_eq!(s.validate(), Err(E::PreferredNotInRegions));
    let mut s = spec();
    s.witness = Some("a".into());
    assert_eq!(s.validate(), Err(E::PreferredIsWitness));
    let mut s = spec();
    s.regions.push("d".into());
    assert_eq!(s.validate(), Err(E::WrongRegionCount));
}

fn set_preferred(table: &str, region: &str) -> MetaCommand {
    MetaCommand::SetGlobalPreferredLeader {
        table: table.to_string(),
        region: region.to_string(),
    }
}

/// `SetGlobalPreferredLeader` (ADR 0075 section 3.3): applies to a global
/// table, is idempotent, and every rejection is state-based and named.
#[test]
fn set_global_preferred_leader_apply_matrix() {
    let mut m = world(&[1, 3, 5]);
    // Not global yet.
    assert_eq!(
        m.apply(&set_preferred("t", "b")),
        ApplyOutcome::Rejected("table is not a global table")
    );
    assert_eq!(
        m.apply(&set_preferred("nope", "b")),
        ApplyOutcome::Rejected("no such table schema")
    );
    let mut with_witness = spec();
    with_witness.witness = Some("c".into());
    assert_eq!(m.apply(&convert("t", with_witness)), ApplyOutcome::Applied);
    // Same region: no-op.
    assert_eq!(m.apply(&set_preferred("t", "a")), ApplyOutcome::NoOp);
    // Unknown region, and the witness region: rejected, state untouched.
    assert_eq!(
        m.apply(&set_preferred("t", "z")),
        ApplyOutcome::Rejected("preferred-leader region is not one of the table's regions")
    );
    assert_eq!(
        m.apply(&set_preferred("t", "c")),
        ApplyOutcome::Rejected("preferred-leader region is the witness")
    );
    assert_eq!(
        m.schemas
            .get("t")
            .unwrap()
            .global
            .as_ref()
            .unwrap()
            .preferred_leader_region,
        "a"
    );
    // A valid move applies and leaves the placement policy alone.
    let policy_before = m.policies[&TabletId(1)].clone();
    assert_eq!(m.apply(&set_preferred("t", "b")), ApplyOutcome::Applied);
    assert_eq!(
        m.schemas
            .get("t")
            .unwrap()
            .global
            .as_ref()
            .unwrap()
            .preferred_leader_region,
        "b"
    );
    assert_eq!(m.policies[&TabletId(1)], policy_before);
}

/// Same single gate as the conversion (mutation: downgrade to `Base`), and the
/// pinned JSON shape.
#[test]
fn set_global_preferred_leader_gate_and_pinned_json() {
    assert_eq!(set_preferred("t", "b").required_gate(), Gate::GlobalTables);
    assert_eq!(
        serde_json::to_string(&set_preferred("t", "b")).unwrap(),
        r#"{"SetGlobalPreferredLeader":{"table":"t","region":"b"}}"#
    );
}
