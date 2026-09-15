//! Deterministic, `SimCluster`-driven coverage for issue #856's second half:
//! `DeleteBackup` refuses a backup that is the source of a restore still
//! `Seeding` (`BackupInUseException`), and the identical delete succeeds
//! once that restore reaches a terminal state — ADR 0061 rung-style
//! coverage, added as a direct follow-up review fix rather than a numbered
//! rung.
//!
//! **Why this exists, and why it is deterministic where the wire-level
//! `dynamo_restore.rs::delete_backup_refuses_while_a_restore_is_in_progress_
//! then_succeeds` test used to race.** That `ProdEnv` test issued a real
//! `RestoreTableFromBackup` call and then, one `DeleteBackup` call later,
//! *hoped* to observe the restore still `Seeding` — `animusd::backup_
//! restore`'s own driver (`RESTORE_TICK_INTERVAL` = 200ms) could have
//! already finished seeding a small table well within that window on a
//! fast run, silently turning the `BackupInUseException` assertion into a
//! no-op (`if status == 400 { assert ... }`, never asserted unconditionally)
//! — a flake by construction, the exact class CLAUDE.md forbids.
//!
//! `SimCluster` never spawns `backup_restore::backup_restore_loop` as a
//! background task at all (see this crate's own `SimCluster` design-
//! decisions section, `crates/animusd/CLAUDE.md`) — nothing in this
//! fixture ever advances a restore past whatever state it was directly
//! proposed into. Minting a restore row via a direct
//! [`SimCluster::propose_meta`] `MetaCommand::BeginRestore` call (mirroring
//! `sim_cluster_backup_janitor.rs`'s own `complete_a_backup` idiom for
//! building an `Available` backup row) therefore leaves that restore
//! `Seeding` **forever**, deterministically, with zero race — no polling,
//! no "wait until Seeding" logic, no timing window of any kind: the state
//! is synchronously true the instant the propose commits. Moving it to a
//! terminal state (`MetaCommand::FailRestore`, chosen over `CompleteRestore`
//! since it needs no destination-tablet hosting/activation machinery this
//! fixture has no reason to stand up) is likewise a single direct propose.
//!
//! Two scenarios, both single-seed (nothing here depends on fault
//! injection — the property under test is a pure state-machine/dispatch
//! fact, not a fault-tolerance one):
//!
//! (a) [`delete_backup_refuses_while_a_restore_is_seeding_then_succeeds_
//! once_failed`] — the full life cycle: an `Available` backup, a `Seeding`
//! restore referencing it, `DeleteBackup` refused unconditionally with
//! `BackupInUseException`, the restore failed, then the identical
//! `DeleteBackup` call succeeds and reports `DELETED`.
//!
//! (b) [`delete_backup_succeeds_immediately_with_no_restore_at_all`] — the
//! negative control: an `Available` backup with no restore referencing it
//! at all deletes on the first call, proving the refusal above is
//! genuinely conditioned on the live restore and not some other gate.
//!
//! See `crates/animusd/tests/dynamo_restore.rs`'s own trimmed
//! `delete_backup_succeeds_after_restore_completes` for this mechanism's
//! real, wire-driven `RestoreTableFromBackup` complement (a real restore
//! run to completion, then `DeleteBackup` succeeds) — that test no longer
//! attempts to catch the in-flight window at all, deferring entirely to
//! this module for the deterministic proof.

use animus_control::{ColumnType, MetaCommand, ProposeResult, TableSchema};
use animus_tablet::TabletId;

use super::sim_cluster::SimCluster;

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn accepted(r: ProposeResult) -> bool {
    matches!(r, ProposeResult::Accepted { .. })
}

fn json(body: &str) -> serde_json::Value {
    serde_json::from_str(body).unwrap_or_else(|e| panic!("invalid JSON ({e}): {body}"))
}

fn error_code(body: &str) -> String {
    json(body)["__type"]
        .as_str()
        .map(|t| t.rsplit('#').next().unwrap_or(t).to_owned())
        .unwrap_or_else(|| panic!("no __type in error body: {body}"))
}

/// Drive a backup all the way to `Available` on `table` — mirrors
/// `sim_cluster_backup_janitor.rs::complete_a_backup`'s own shape exactly
/// (a single pinned tablet, one `RecordBackupTabletComplete` report),
/// proposing directly on the current control leader via
/// [`SimCluster::propose_meta`].
fn complete_a_backup(cluster: &mut SimCluster, table: &str, backup_id: &str) {
    let tablet = cluster
        .tablet_of(table)
        .unwrap_or_else(|| panic!("table {table} has no tablet"));
    assert!(
        accepted(cluster.propose_meta(MetaCommand::BeginBackup {
            backup_id: backup_id.to_owned(),
            table: table.to_owned(),
            created_wall_ms: 1_000,
            backup_name: "nightly".to_owned(),
            pitr_base: false,
        })),
        "BeginBackup rejected"
    );
    assert!(
        accepted(
            cluster.propose_meta(MetaCommand::RecordBackupTabletComplete {
                backup_id: backup_id.to_owned(),
                tablet,
                cut_version: 10,
                bytes: 100,
                chunk_count: 1,
            })
        ),
        "RecordBackupTabletComplete rejected"
    );
    assert!(
        accepted(cluster.propose_meta(MetaCommand::CompleteBackup {
            backup_id: backup_id.to_owned(),
        })),
        "CompleteBackup rejected"
    );
    cluster.run_for(std::time::Duration::from_millis(200));
}

/// Mint a `Seeding` restore referencing `backup_id`, targeting a brand-new
/// table name — no destination-tablet hosting/serving is ever needed, since
/// nothing in this test reads or writes through it; only the catalog row's
/// own `Seeding` status matters. Returns the minted `restore_id`.
///
/// The target table's own schema is proposed first, exactly as
/// `BeginRestore`'s own apply-arm precondition requires (mirroring
/// `animus-control`'s own `meta::tests::begin_restore_apply_arm`), and the
/// destination tablet id is drawn fresh from the control leader's own live
/// `Metadata::next_free_tablet_id()` — never a fixture-local counter (see
/// `sim_cluster.rs::create_table_with_replication`'s own doc for why a
/// fixture-local counter can collide with the live allocator the moment a
/// cluster mixes hand-hosted and wire-provisioned tablets, the identical
/// hazard a bare literal tablet id here would risk).
fn begin_a_seeding_restore(
    cluster: &mut SimCluster,
    source_table: &str,
    backup_id: &str,
    target_table: &str,
    restore_id: &str,
) {
    assert!(
        accepted(cluster.propose_meta(MetaCommand::CreateTableSchema {
            table: target_table.to_owned(),
            schema: TableSchema::simple("id", ColumnType::String),
        })),
        "CreateTableSchema (restore target) rejected"
    );
    // `propose_meta` only appends to the leader's own log and returns
    // `Accepted` (never "committed") — advance virtual time so this entry
    // (and, below, `BeginRestore`'s) actually commits and applies before
    // this fixture reads `Metadata` back on any node, mirroring
    // `sim_cluster_backup_janitor.rs::complete_a_backup`'s own
    // `run_for` calls after each propose.
    cluster.run_for(std::time::Duration::from_millis(200));
    let tablet = cluster.metadata(0).next_free_tablet_id();
    assert!(
        accepted(cluster.propose_meta(MetaCommand::BeginRestore {
            restore_id: restore_id.to_owned(),
            backup_id: backup_id.to_owned(),
            source_table: source_table.to_owned(),
            target_table: target_table.to_owned(),
            tablet,
            replicas: vec![animus_env::nid(0)],
            gsi_defs: Vec::new(),
            pitr: None,
        })),
        "BeginRestore rejected"
    );
    cluster.run_for(std::time::Duration::from_millis(200));
    assert_eq!(
        cluster
            .metadata(0)
            .restore(restore_id)
            .map(|r| r.status.clone()),
        Some(animus_control::RestoreStatus::Seeding),
        "restore must be Seeding immediately after BeginRestore commits — \
         SimCluster never spawns backup_restore_loop, so this state never \
         advances on its own"
    );
}

fn delete_backup(cluster: &mut SimCluster, node: u64, backup_arn: &str) -> (u16, String) {
    cluster.dynamo(
        node,
        "DynamoDB_20120810.DeleteBackup",
        format!(r#"{{"BackupArn":"{backup_arn}"}}"#).as_bytes(),
    )
}

/// Issue #856 (second half), deterministically: `DeleteBackup` refuses a
/// backup with a `Seeding` restore against it, unconditionally — no
/// `if status == 400` branch, since the restore's `Seeding` state is a
/// synchronous fact of this fixture, never a race — and the identical call
/// succeeds once that restore is failed.
fn run_delete_backup_refuses_while_seeding_then_succeeds_once_failed(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let tablet = cluster.create_table("orders");
    let _ = tablet;

    let backup_id = "arn:aws:dynamodb:animus:0:table/orders/backup/inflight";
    complete_a_backup(&mut cluster, "orders", backup_id);

    begin_a_seeding_restore(
        &mut cluster,
        "orders",
        backup_id,
        "orders_restored",
        "restore-inflight",
    );

    // Unconditional: this must be `BackupInUseException` every single time,
    // at every seed — there is no window here at all, since nothing in this
    // fixture ever advances the restore past `Seeding` on its own.
    let (status, body) = delete_backup(&mut cluster, 0, backup_id);
    assert_eq!(
        status, 400,
        "seed={seed}: DeleteBackup must refuse while the restore is Seeding: {body}"
    );
    assert_eq!(
        error_code(&body),
        "BackupInUseException",
        "seed={seed}: body: {body}"
    );

    // Move the restore to a terminal state — the block must lift, not stay
    // sticky.
    assert!(
        accepted(cluster.propose_meta(MetaCommand::FailRestore {
            restore_id: "restore-inflight".to_owned(),
            reason: "test: forced terminal".to_owned(),
        })),
        "seed={seed}: FailRestore rejected"
    );
    cluster.run_for(std::time::Duration::from_millis(200));
    assert!(
        matches!(
            cluster
                .metadata(0)
                .restore("restore-inflight")
                .map(|r| &r.status),
            Some(animus_control::RestoreStatus::Failed { .. })
        ),
        "seed={seed}: restore must be Failed"
    );

    let (status, body) = delete_backup(&mut cluster, 0, backup_id);
    assert_eq!(
        status, 200,
        "seed={seed}: DeleteBackup after the restore terminated must succeed: {body}"
    );
    assert_eq!(
        json(&body)["BackupDescription"]["BackupDetails"]["BackupStatus"],
        "DELETED",
        "seed={seed}: body: {body}"
    );
}

#[test]
fn delete_backup_refuses_while_a_restore_is_seeding_then_succeeds_once_failed() {
    run_delete_backup_refuses_while_seeding_then_succeeds_once_failed(env_seed(0xD856_0001));
}

#[test]
fn delete_backup_refuses_while_a_restore_is_seeding_then_succeeds_once_failed_over_seeds() {
    for i in 0..5 {
        run_delete_backup_refuses_while_seeding_then_succeeds_once_failed(0xD856_1000 + i);
    }
}

/// Negative control: an `Available` backup with no restore referencing it
/// at all deletes on the very first call — proving the refusal above is
/// genuinely conditioned on the live restore, not some other unrelated
/// gate (e.g. the backup's own status).
fn run_delete_backup_succeeds_immediately_with_no_restore(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let _tablet: TabletId = cluster.create_table("orders");

    let backup_id = "arn:aws:dynamodb:animus:0:table/orders/backup/untouched";
    complete_a_backup(&mut cluster, "orders", backup_id);

    let (status, body) = delete_backup(&mut cluster, 0, backup_id);
    assert_eq!(status, 200, "seed={seed}: body: {body}");
    assert_eq!(
        json(&body)["BackupDescription"]["BackupDetails"]["BackupStatus"],
        "DELETED",
        "seed={seed}: body: {body}"
    );
}

#[test]
fn delete_backup_succeeds_immediately_with_no_restore_at_all() {
    run_delete_backup_succeeds_immediately_with_no_restore(env_seed(0xD856_0002));
}

#[test]
fn delete_backup_succeeds_immediately_with_no_restore_at_all_over_seeds() {
    for i in 0..5 {
        run_delete_backup_succeeds_immediately_with_no_restore(0xD856_2000 + i);
    }
}
