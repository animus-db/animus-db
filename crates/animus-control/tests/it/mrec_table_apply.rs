//! MREC (multi-Region eventual-consistency) catalog commands (ADR 0075 section
//! 4, G-01 stage G-d, M1): `ConvertTableToMrec` / `AddMrecReplica` /
//! `RemoveMrecReplica` / `SetMrecReplicaStatus` apply matrix, every
//! state-based rejection, the `Gate::MrecReplication` rows (including the
//! content-dependent one for an `Eventual` `ConvertTableToGlobal`), the
//! MRSC-only restrictions staying MRSC-only, and the pinned JSON shapes. Pure
//! `Metadata` tests, no simulator. M1 emits none of these: the apply is real
//! but inert until the M4 wire surface.

use animus_control::schema::{
    GlobalSpecError, GlobalTableSpec, MrecReplica, MrecReplicaStatus, MultiRegionConsistency,
    mrec_region_id,
};
use animus_control::version::{Gate, GatedCommand};
use animus_control::{
    ApplyOutcome, ColumnType, IndexDef, IndexKind, IndexProjection, IndexStatus, MetaCommand,
    Metadata, TableSchema, TtlSpec,
};
use animus_tablet::{KeyRange, TabletId};

fn convert(table: &str, local: &str) -> MetaCommand {
    MetaCommand::ConvertTableToMrec {
        table: table.to_string(),
        local_region: local.to_string(),
        region_id: mrec_region_id(local),
    }
}

fn add(table: &str, region: &str) -> MetaCommand {
    MetaCommand::AddMrecReplica {
        table: table.to_string(),
        region: region.to_string(),
        region_id: mrec_region_id(region),
    }
}

fn status(table: &str, region: &str, s: MrecReplicaStatus) -> MetaCommand {
    MetaCommand::SetMrecReplicaStatus {
        table: table.to_string(),
        region: region.to_string(),
        status: s,
    }
}

fn world() -> Metadata {
    let mut m = Metadata::default();
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
            replicas: vec![animus_env::nid(1)],
        }),
        ApplyOutcome::Applied
    );
    m
}

fn rejected(m: &mut Metadata, c: &MetaCommand) -> &'static str {
    let before = m.clone();
    match m.apply(c) {
        ApplyOutcome::Rejected(why) => {
            assert_eq!(*m, before, "a rejection leaves the state untouched: {c:?}");
            why
        }
        other => panic!("expected a rejection of {c:?}, got {other:?}"),
    }
}

#[test]
fn convert_records_an_active_local_replica_and_leaves_placement_alone() {
    let mut m = world();
    let policies_before = m.policies.clone();
    assert_eq!(m.apply(&convert("t", "us")), ApplyOutcome::Applied);
    let g = m.table_global("t").expect("global spec");
    assert!(g.is_mrec() && !g.is_mrsc());
    assert_eq!(g.consistency, MultiRegionConsistency::Eventual);
    assert!(g.regions.is_empty() && g.witness.is_none() && g.preferred_leader_region.is_empty());
    assert_eq!(
        g.replicas,
        vec![MrecReplica {
            region: "us".into(),
            region_id: mrec_region_id("us"),
            status: MrecReplicaStatus::Active,
            local: true,
            copied: Default::default(),
        }]
    );
    assert_eq!(m.policies, policies_before, "MREC never touches placement");
    // Identical re-apply is a no-op; a different local region is rejected.
    assert_eq!(m.apply(&convert("t", "us")), ApplyOutcome::NoOp);
    assert_eq!(
        rejected(&mut m, &convert("t", "eu")),
        "table is already a global table"
    );
}

#[test]
fn convert_rejections_are_named() {
    let mut m = world();
    assert_eq!(
        rejected(&mut m, &convert("nope", "us")),
        "no such table schema"
    );
    let bad_id = MetaCommand::ConvertTableToMrec {
        table: "t".into(),
        local_region: "us".into(),
        region_id: mrec_region_id("us") ^ 1,
    };
    assert_eq!(
        rejected(&mut m, &bad_id),
        GlobalSpecError::MrecRegionId.message()
    );
    assert_eq!(
        rejected(&mut m, &convert("t", "")),
        GlobalSpecError::MrecReplicaName.message()
    );
    // A strong global table cannot also be converted.
    let mut strong = world();
    let strong_spec = GlobalTableSpec {
        consistency: MultiRegionConsistency::Strong,
        regions: vec!["a".into(), "b".into(), "c".into()],
        witness: None,
        preferred_leader_region: "a".into(),
        replicas: Vec::new(),
    };
    assert_eq!(
        strong.apply(&MetaCommand::ConvertTableToGlobal {
            table: "t".into(),
            spec: strong_spec,
        }),
        ApplyOutcome::Applied
    );
    assert_eq!(
        rejected(&mut strong, &convert("t", "us")),
        "table is already a global table"
    );
}

#[test]
fn replica_set_lifecycle() {
    let mut m = world();
    assert_eq!(
        rejected(&mut m, &add("t", "eu")),
        "table is not an MREC global table",
        "adding before converting"
    );
    assert_eq!(m.apply(&convert("t", "us")), ApplyOutcome::Applied);
    assert_eq!(m.apply(&add("t", "eu")), ApplyOutcome::Applied);
    assert_eq!(m.apply(&add("t", "eu")), ApplyOutcome::NoOp);
    let wrong = MetaCommand::AddMrecReplica {
        table: "t".into(),
        region: "eu".into(),
        region_id: 7,
    };
    assert_eq!(
        rejected(&mut m, &wrong),
        "replica already exists with a different region id"
    );
    let eu = |m: &Metadata| {
        m.table_global("t")
            .unwrap()
            .replicas
            .iter()
            .find(|r| r.region == "eu")
            .cloned()
            .unwrap()
    };
    assert_eq!(eu(&m).status, MrecReplicaStatus::Creating);
    assert!(!eu(&m).local);
    for s in [
        MrecReplicaStatus::Active,
        MrecReplicaStatus::Deleting,
        MrecReplicaStatus::CreationFailed,
    ] {
        assert_eq!(m.apply(&status("t", "eu", s)), ApplyOutcome::Applied);
        assert_eq!(eu(&m).status, s);
        assert_eq!(m.apply(&status("t", "eu", s)), ApplyOutcome::NoOp);
    }
    assert_eq!(
        rejected(&mut m, &status("t", "ap", MrecReplicaStatus::Active)),
        "no such MREC replica"
    );
    // The local replica cannot be removed; a peer can; removal is idempotent.
    let rm = |r: &str| MetaCommand::RemoveMrecReplica {
        table: "t".into(),
        region: r.into(),
    };
    assert_eq!(
        rejected(&mut m, &rm("us")),
        "cannot remove the local replica"
    );
    assert_eq!(m.apply(&rm("eu")), ApplyOutcome::Applied);
    assert_eq!(m.apply(&rm("eu")), ApplyOutcome::NoOp);
    assert_eq!(m.table_global("t").unwrap().replicas.len(), 1);
}

#[test]
fn replica_cap_and_collisions_are_enforced() {
    let mut m = world();
    assert_eq!(m.apply(&convert("t", "r0")), ApplyOutcome::Applied);
    for i in 1..GlobalTableSpec::MREC_MAX_REPLICAS {
        assert_eq!(m.apply(&add("t", &format!("r{i}"))), ApplyOutcome::Applied);
    }
    assert_eq!(
        rejected(&mut m, &add("t", "one-too-many")),
        GlobalSpecError::MrecReplicaCount.message()
    );
}

/// The MRSC-only restrictions (no TTL, no LSI) must not leak onto MREC tables:
/// the plan keeps LSIs and TTL working on an MREC table (ADR 0075 4.6/D9).
#[test]
fn an_mrec_table_keeps_ttl_and_lsi() {
    let mut m = world();
    assert_eq!(m.apply(&convert("t", "us")), ApplyOutcome::Applied);
    assert_eq!(
        m.apply(&MetaCommand::SetTableTtl {
            table: "t".into(),
            spec: Some(TtlSpec {
                attribute_name: "exp".into()
            }),
        }),
        ApplyOutcome::Applied
    );
    let lsi = IndexDef {
        name: "by_x".into(),
        kind: IndexKind::Local,
        hash_attribute: "id".into(),
        sort_attribute: Some("x".into()),
        projection: IndexProjection::All,
        status: IndexStatus::Active,
        hash_attribute_type: None,
        sort_attribute_type: None,
    };
    assert_eq!(
        m.apply(&MetaCommand::CreateTableIndex {
            table: "t".into(),
            index: lsi,
        }),
        ApplyOutcome::Applied
    );
}

/// `ConvertTableToGlobal` is the strong command: an `Eventual` spec through it
/// is a deterministic rejection AND is classified `MrecReplication` by content
/// (a Release(2) voter would fail to decode the variant of `consistency`).
#[test]
fn convert_to_global_refuses_an_eventual_spec_and_gates_it_by_content() {
    let eventual = GlobalTableSpec {
        consistency: MultiRegionConsistency::Eventual,
        regions: Vec::new(),
        witness: None,
        preferred_leader_region: String::new(),
        replicas: vec![MrecReplica {
            region: "us".into(),
            region_id: mrec_region_id("us"),
            status: MrecReplicaStatus::Active,
            local: true,
            copied: Default::default(),
        }],
    };
    let cmd = MetaCommand::ConvertTableToGlobal {
        table: "t".into(),
        spec: eventual,
    };
    assert_eq!(cmd.required_gate(), Gate::MrecReplication);
    let mut m = world();
    assert_eq!(
        rejected(&mut m, &cmd),
        "ConvertTableToGlobal takes a strong spec (use ConvertTableToMrec)"
    );
}

#[test]
fn mrec_commands_require_the_mrec_gate() {
    for c in [
        convert("t", "us"),
        add("t", "eu"),
        MetaCommand::RemoveMrecReplica {
            table: "t".into(),
            region: "eu".into(),
        },
        status("t", "eu", MrecReplicaStatus::Active),
    ] {
        assert_eq!(c.required_gate(), Gate::MrecReplication, "{c:?}");
    }
    assert_eq!(Gate::MrecReplication.version(), Some(3));
}

#[test]
fn spec_validation_matrix() {
    let ok = |region: &str, local: bool| MrecReplica {
        region: region.into(),
        region_id: mrec_region_id(region),
        status: MrecReplicaStatus::Active,
        local,
        copied: Default::default(),
    };
    let spec = |replicas: Vec<MrecReplica>| GlobalTableSpec {
        consistency: MultiRegionConsistency::Eventual,
        regions: Vec::new(),
        witness: None,
        preferred_leader_region: String::new(),
        replicas,
    };
    assert_eq!(spec(vec![ok("a", true)]).validate(), Ok(()));
    assert_eq!(spec(vec![ok("a", true), ok("b", false)]).validate(), Ok(()));
    assert_eq!(
        spec(vec![]).validate(),
        Err(GlobalSpecError::MrecReplicaCount)
    );
    assert_eq!(
        spec(vec![ok("a", true), ok("a", false)]).validate(),
        Err(GlobalSpecError::MrecReplicaName)
    );
    assert_eq!(
        spec(vec![ok("a", false)]).validate(),
        Err(GlobalSpecError::MrecLocalReplica)
    );
    assert_eq!(
        spec(vec![ok("a", true), ok("b", true)]).validate(),
        Err(GlobalSpecError::MrecLocalReplica)
    );
    let mut forged = ok("a", true);
    forged.region_id = 5;
    assert_eq!(
        spec(vec![forged]).validate(),
        Err(GlobalSpecError::MrecRegionId)
    );
    let mut mixed = spec(vec![ok("a", true)]);
    mixed.regions = vec!["x".into()];
    assert_eq!(mixed.validate(), Err(GlobalSpecError::ModeFieldsMixed));
    // A strong spec cannot carry replicas.
    let mut strong = GlobalTableSpec {
        consistency: MultiRegionConsistency::Strong,
        regions: vec!["a".into(), "b".into(), "c".into()],
        witness: None,
        preferred_leader_region: "a".into(),
        replicas: vec![ok("a", true)],
    };
    assert_eq!(strong.validate(), Err(GlobalSpecError::ModeFieldsMixed));
    strong.replicas.clear();
    assert_eq!(strong.validate(), Ok(()));
}

/// The pinned JSON shape of a converted MREC spec: field names and the
/// skip-when-default rules a Release(2) binary's strict decode depends on.
#[test]
fn pinned_json_shape() {
    let mut m = world();
    assert_eq!(m.apply(&convert("t", "us")), ApplyOutcome::Applied);
    assert_eq!(m.apply(&add("t", "eu")), ApplyOutcome::Applied);
    let g = serde_json::to_string(m.table_global("t").unwrap()).unwrap();
    let us = mrec_region_id("us");
    let eu = mrec_region_id("eu");
    assert_eq!(
        g,
        format!(
            "{{\"consistency\":\"Eventual\",\"regions\":[],\"preferred_leader_region\":\"\",\
             \"replicas\":[{{\"region\":\"us\",\"region_id\":{us},\"status\":\"Active\",\
             \"local\":true}},{{\"region\":\"eu\",\"region_id\":{eu},\"status\":\"Creating\"}}]}}"
        )
    );
}

/// The accessor split: nothing in the strong-table machinery may mistake an
/// MREC spec for a strong one (`is_mrsc` is what MRSC consumers must test).
#[test]
fn mrec_spec_is_not_mrsc() {
    let mut m = world();
    assert_eq!(m.apply(&convert("t", "us")), ApplyOutcome::Applied);
    let g = m.table_global("t").unwrap();
    assert!(!g.is_mrsc());
    assert_eq!(
        rejected(
            &mut m,
            &MetaCommand::SetGlobalPreferredLeader {
                table: "t".into(),
                region: "us".into(),
            }
        ),
        "preferred-leader region is not one of the table's regions"
    );
}

/// G-d M4: `MarkMrecCopied` records per-tablet initial-copy completion on a
/// replica (a set, so a repeat is a no-op), is rejected for an unknown replica
/// or a non-MREC table, and is gated like the other MREC commands.
#[test]
fn mark_mrec_copied_records_tablets_idempotently() {
    let mark = |region: &str, tablet: u64| MetaCommand::MarkMrecCopied {
        table: "t".into(),
        region: region.into(),
        tablet,
    };
    let mut m = world();
    assert_eq!(
        rejected(&mut m, &mark("eu", 1)),
        "table is not an MREC global table"
    );
    assert_eq!(m.apply(&convert("t", "us")), ApplyOutcome::Applied);
    assert_eq!(m.apply(&add("t", "eu")), ApplyOutcome::Applied);
    assert_eq!(rejected(&mut m, &mark("ap", 1)), "no such MREC replica");
    assert_eq!(m.apply(&mark("eu", 1)), ApplyOutcome::Applied);
    assert_eq!(m.apply(&mark("eu", 1)), ApplyOutcome::NoOp);
    assert_eq!(m.apply(&mark("eu", 7)), ApplyOutcome::Applied);
    let g = m.table_global("t").unwrap();
    let eu = g.replicas.iter().find(|r| r.region == "eu").unwrap();
    assert_eq!(eu.copied.iter().copied().collect::<Vec<_>>(), vec![1, 7]);
    assert!(
        g.replicas
            .iter()
            .find(|r| r.local)
            .unwrap()
            .copied
            .is_empty()
    );
    assert_eq!(mark("eu", 1).required_gate(), Gate::MrecReplication);
}
