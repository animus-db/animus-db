//! Legacy (version 2017.11.29) global tables, catalog side (ADR 0075's
//! 2026-10-21 amendment, `Gate::LegacyGlobalTables`, cluster version 9):
//! `ConvertTableToLegacyGlobal` / `SetLegacyGlobalSettings` apply matrix,
//! idempotency, every state-based rejection, the legacy spec dissolving when
//! its last remote replica is removed, the gate rows and a mirror round trip.
//! Pure `Metadata` tests, no simulator.

use animus_control::schema::{
    GlobalBilling, GlobalSpecError, GlobalTableSpec, MrecReplicaStatus, mrec_region_id,
};
use animus_control::version::{Gate, GatedCommand};
use animus_control::{
    ApplyOutcome, ColumnType, MetaCommand, Metadata, ProvisionedThroughput, TableSchema,
};
use animus_tablet::{KeyRange, TabletId};

fn convert(table: &str, local: &str, created_ms: u64) -> MetaCommand {
    MetaCommand::ConvertTableToLegacyGlobal {
        table: table.to_string(),
        local_region: local.to_string(),
        region_id: mrec_region_id(local),
        created_ms,
    }
}

fn settings(billing: GlobalBilling, stamp: u64, origin: u32) -> MetaCommand {
    MetaCommand::SetLegacyGlobalSettings {
        table: "t".into(),
        billing,
        stamp,
        origin,
    }
}

fn add(region: &str) -> MetaCommand {
    MetaCommand::AddMrecReplica {
        table: "t".into(),
        region: region.into(),
        region_id: mrec_region_id(region),
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

fn legacy_world() -> Metadata {
    let mut m = world();
    assert_eq!(m.apply(&convert("t", "us", 42)), ApplyOutcome::Applied);
    assert_eq!(m.apply(&add("eu")), ApplyOutcome::Applied);
    m
}

/// The conversion records an MREC spec with the legacy marker (and the
/// creation instant), is idempotent, and rejects a different local Region.
#[test]
fn convert_records_the_legacy_marker_and_is_idempotent() {
    let mut m = world();
    let policies_before = m.policies.clone();
    assert_eq!(m.apply(&convert("t", "us", 42)), ApplyOutcome::Applied);
    let g = m.table_global("t").expect("global spec");
    assert!(g.is_mrec() && g.is_legacy() && !g.is_mrsc());
    assert_eq!(g.legacy.as_ref().unwrap().created_ms, 42);
    assert!(g.legacy.as_ref().unwrap().billing.is_none());
    assert_eq!(g.replicas.len(), 1);
    assert!(g.replicas[0].local && g.replicas[0].status == MrecReplicaStatus::Active);
    assert_eq!(m.policies, policies_before, "never touches placement");
    assert_eq!(m.apply(&convert("t", "us", 42)), ApplyOutcome::NoOp);
    assert_eq!(
        m.apply(&convert("t", "us", 99)),
        ApplyOutcome::NoOp,
        "a re-sent conversion keeps the first creation instant"
    );
    assert_eq!(
        m.table_global("t")
            .unwrap()
            .legacy
            .as_ref()
            .unwrap()
            .created_ms,
        42
    );
    assert_eq!(
        rejected(&mut m, &convert("t", "eu", 42)),
        "table is already a global table"
    );
    assert_eq!(
        rejected(&mut world(), &convert("nope", "us", 1)),
        "no such table schema"
    );
}

/// A table that is already a global table of another kind is not converted,
/// except a standalone MREC table (only its own replica), which becomes
/// legacy.
#[test]
fn convert_refuses_other_global_tables_but_adopts_a_standalone_mrec_table() {
    let mut m = world();
    assert_eq!(
        m.apply(&MetaCommand::ConvertTableToMrec {
            table: "t".into(),
            local_region: "us".into(),
            region_id: mrec_region_id("us"),
        }),
        ApplyOutcome::Applied
    );
    assert_eq!(m.apply(&convert("t", "us", 7)), ApplyOutcome::Applied);
    assert!(m.table_global("t").unwrap().is_legacy());

    let mut with_peer = world();
    assert_eq!(
        with_peer.apply(&MetaCommand::ConvertTableToMrec {
            table: "t".into(),
            local_region: "us".into(),
            region_id: mrec_region_id("us"),
        }),
        ApplyOutcome::Applied
    );
    assert_eq!(with_peer.apply(&add("eu")), ApplyOutcome::Applied);
    assert_eq!(
        rejected(&mut with_peer, &convert("t", "us", 7)),
        "table is already a global table",
        "a 2019.11.21 table with peers is not converted to 2017.11.29"
    );
}

/// The settings record is last-writer-wins by `(stamp, origin)`, idempotent,
/// and sets this cluster's table throughput.
#[test]
fn settings_are_last_writer_wins_and_set_the_table_throughput() {
    let mut m = legacy_world();
    let w100 = settings(GlobalBilling::Provisioned { write_units: 100 }, 10, 1);
    assert_eq!(m.apply(&w100), ApplyOutcome::Applied);
    assert_eq!(m.apply(&w100), ApplyOutcome::NoOp, "idempotent");
    // A table that had no throughput gets read = write units.
    assert_eq!(
        m.table_throughput("t"),
        Some(&ProvisionedThroughput {
            read_units: 100,
            write_units: 100
        })
    );
    // A stale stamp (even with different content) loses.
    assert_eq!(
        m.apply(&settings(GlobalBilling::PayPerRequest, 9, 1)),
        ApplyOutcome::NoOp
    );
    assert_eq!(m.table_throughput("t").unwrap().write_units, 100);
    // Equal stamp: the higher origin wins, the lower loses.
    assert_eq!(
        m.apply(&settings(
            GlobalBilling::Provisioned { write_units: 5 },
            10,
            0
        )),
        ApplyOutcome::NoOp
    );
    assert_eq!(
        m.apply(&settings(
            GlobalBilling::Provisioned { write_units: 250 },
            10,
            2
        )),
        ApplyOutcome::Applied
    );
    // Read units of an already provisioned table are kept; only writes follow.
    assert_eq!(
        m.table_throughput("t"),
        Some(&ProvisionedThroughput {
            read_units: 100,
            write_units: 250
        })
    );
    // PAY_PER_REQUEST clears the throughput, keeping the record's stamp.
    assert_eq!(
        m.apply(&settings(GlobalBilling::PayPerRequest, 20, 1)),
        ApplyOutcome::Applied
    );
    assert_eq!(m.table_throughput("t"), None);
    let l = m.table_global("t").unwrap().legacy.clone().unwrap();
    assert_eq!(l.billing, Some(GlobalBilling::PayPerRequest));
    assert_eq!(l.order(), (20, 1));
    assert_eq!(
        m.apply(&settings(
            GlobalBilling::Provisioned { write_units: 100 },
            10,
            1
        )),
        ApplyOutcome::NoOp,
        "a stale push cannot resurrect an older setting"
    );
}

#[test]
fn settings_rejections_are_named_and_leave_state_untouched() {
    let mut plain = world();
    assert_eq!(
        rejected(&mut plain, &settings(GlobalBilling::PayPerRequest, 1, 1)),
        "table is not a legacy global table"
    );
    let mut mrec = world();
    assert_eq!(
        mrec.apply(&MetaCommand::ConvertTableToMrec {
            table: "t".into(),
            local_region: "us".into(),
            region_id: mrec_region_id("us"),
        }),
        ApplyOutcome::Applied
    );
    assert_eq!(
        rejected(&mut mrec, &settings(GlobalBilling::PayPerRequest, 1, 1)),
        "table is not a legacy global table",
        "a 2019.11.21 table has no legacy settings"
    );
    let mut m = legacy_world();
    assert_eq!(
        rejected(
            &mut m,
            &settings(GlobalBilling::Provisioned { write_units: 0 }, 1, 1)
        ),
        GlobalSpecError::LegacyWriteUnits.message()
    );
    let mut absent = world();
    let missing = MetaCommand::SetLegacyGlobalSettings {
        table: "nope".into(),
        billing: GlobalBilling::PayPerRequest,
        stamp: 1,
        origin: 1,
    };
    assert_eq!(rejected(&mut absent, &missing), "no such table schema");
}

/// Removing the last remote replica dissolves a legacy spec (the table is a
/// regular table again); a non-legacy MREC table keeps its single-replica
/// spec as before.
#[test]
fn removing_the_last_remote_replica_dissolves_a_legacy_spec_only() {
    let mut m = legacy_world();
    assert_eq!(m.apply(&add("ap")), ApplyOutcome::Applied);
    assert_eq!(
        m.apply(&MetaCommand::RemoveMrecReplica {
            table: "t".into(),
            region: "ap".into()
        }),
        ApplyOutcome::Applied
    );
    assert!(m.table_global("t").is_some_and(|g| g.is_legacy()));
    assert_eq!(
        m.apply(&MetaCommand::RemoveMrecReplica {
            table: "t".into(),
            region: "eu".into()
        }),
        ApplyOutcome::Applied
    );
    assert!(
        m.table_global("t").is_none(),
        "a one-Region legacy table is a regular table"
    );
    // It can be made a legacy global table again.
    assert_eq!(m.apply(&convert("t", "us", 50)), ApplyOutcome::Applied);

    let mut plain = world();
    for c in [
        MetaCommand::ConvertTableToMrec {
            table: "t".into(),
            local_region: "us".into(),
            region_id: mrec_region_id("us"),
        },
        add("eu"),
        MetaCommand::RemoveMrecReplica {
            table: "t".into(),
            region: "eu".into(),
        },
    ] {
        assert_eq!(plain.apply(&c), ApplyOutcome::Applied);
    }
    assert!(plain.table_global("t").is_some_and(|g| !g.is_legacy()));
}

/// `validate` keeps the marker out of MRSC specs and rejects zero write
/// capacity.
#[test]
fn spec_validation_covers_the_legacy_marker() {
    let mut m = legacy_world();
    let mut spec: GlobalTableSpec = m.table_global("t").unwrap().clone();
    assert_eq!(spec.validate(), Ok(()));
    spec.legacy.as_mut().unwrap().billing = Some(GlobalBilling::Provisioned { write_units: 0 });
    assert_eq!(spec.validate(), Err(GlobalSpecError::LegacyWriteUnits));
    let mut strong = GlobalTableSpec {
        consistency: animus_control::MultiRegionConsistency::Strong,
        regions: vec!["a".into(), "b".into(), "c".into()],
        witness: None,
        preferred_leader_region: "a".into(),
        replicas: Vec::new(),
        legacy: Some(Default::default()),
    };
    assert_eq!(strong.validate(), Err(GlobalSpecError::ModeFieldsMixed));
    strong.legacy = None;
    assert_eq!(strong.validate(), Ok(()));
    let _ = &mut m;
}

/// The gate rows, the content-independence of the apply (it never consults
/// the gate), and a mirror round trip: the schema row derived from each
/// command rebuilds the identical spec on a data-only node.
#[test]
fn legacy_gate_rows_and_mirror_round_trip() {
    use animus_control::mirror::{self, KeyWrite};
    let c = convert("t", "us", 42);
    let s = settings(GlobalBilling::Provisioned { write_units: 100 }, 10, 1);
    assert_eq!(c.required_gate(), Gate::LegacyGlobalTables);
    assert_eq!(s.required_gate(), Gate::LegacyGlobalTables);
    assert_eq!(
        Gate::LegacyGlobalTables.version(),
        Some(animus_control::version::LEGACY_GLOBAL_TABLES_VERSION)
    );
    assert!(Gate::LegacyGlobalTables.rank() > Gate::MrecReplicaOverrides.rank());
    let mut mirror_side = Metadata::default();
    let mut oracle = world();
    for cmd in [c, add("eu"), s] {
        let (outcome, writes) = mirror::apply_and_derive_mirror(&mut oracle, &cmd);
        assert_eq!(outcome, ApplyOutcome::Applied, "{cmd:?}");
        assert!(!writes.is_empty());
        for w in &writes {
            if let KeyWrite::Put(..) = w {
                mirror::apply_key_write(&mut mirror_side, w);
            }
        }
    }
    assert_eq!(
        mirror_side.schemas.get("t").and_then(|s| s.global.clone()),
        oracle.schemas.get("t").and_then(|s| s.global.clone()),
        "the mirrored schema row carries the legacy marker and settings"
    );
    assert_eq!(
        mirror_side.schemas.get("t").and_then(|s| s.throughput),
        oracle.schemas.get("t").and_then(|s| s.throughput),
        "the mirrored schema row carries the throughput the setting wrote"
    );
    assert!(oracle.table_global("t").unwrap().is_legacy());
}
