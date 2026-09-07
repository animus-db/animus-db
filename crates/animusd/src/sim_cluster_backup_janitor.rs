//! `SimCluster`-driven deterministic coverage for the backup janitor's own
//! async loop (`animus_node::backup_janitor::backup_janitor_loop`) — C-04 /
//! ADR 0061 rung D4 PR 5.
//!
//! **Widening (`client_ctx_host.rs`, `backup_janitor.rs`)**: the loop itself
//! (`animus_node::backup_janitor::backup_janitor_loop<E, H>`) was already
//! `E: Env`-generic since ADR 0061 rung C2 — the one thing stopping it from
//! being spawnable under `SimEnv` was that `ClientCtx`'s own implementations
//! of the three host-capability traits it needs (`ControlLeaderHost<E>`/
//! `BackupObjectStore`/`BackupJanitorProgressHost`) were all pinned to the
//! concrete `ClientCtx` alias (`E = ProdEnv, R = AnimusdRelayClient`), and
//! `animusd::backup_janitor::backup_janitor_loop`'s own thin wrapper took a
//! concrete `ClientCtx` too. Both widened to `impl<E: Env, R: RelayClient>
//! .. for ClientCtx<E, R>`/`fn backup_janitor_loop<E: Env, R: RelayClient>`
//! — a pure signature change, zero new mechanism (every field/method each
//! impl delegates to was already `E`/`R`-agnostic or already generic). See
//! each file's own doc for the full account. `animus-node`'s own trait
//! definitions (`host.rs`) needed **no** change at all.
//!
//! **Store choice: ONE shared `SimSegmentStore`, wrapped in
//! `BackupStoreHandle::S3` on every node** (`sim_cluster.rs`'s own `SimCluster::new`
//! construction comment has the full reasoning) — not a per-node `Fs`/
//! `Cluster`-shaped local directory. Production's real `S3` variant already
//! holds `Arc<dyn SegmentStore>` specifically so a test can substitute a
//! fake transport (`lib.rs`'s own `s3_store_handle_tests` does the identical
//! thing over `animus_s3::fake::FakeS3`); a real S3 bucket has no per-node
//! locality at all, so one shared `SimSegmentStore` every node's own handle
//! wraps a clone of is the faithful sim analogue, not a simplification —
//! and it is what makes the leader-gating scenario below meaningful at all
//! (a per-node-local store would make "did a follower's janitor touch the
//! store" trivially true by construction, since it would have its own
//! private copy to *not* touch).
//!
//! **Scenarios** (seed-parameterized, replayed at 5 seeds each via a
//! `_over_seeds` sibling — `ANIMUS_SEED=<seed> cargo test -p animusd --lib
//! <test name>` replays any one, per the repo convention):
//!
//! (a) a completed (`Available`) backup marked deleted (`MarkBackupDeleted`
//!     → `Expired`) is reclaimed: its manifest + data chunk objects, seeded
//!     directly into the shared store, are gone, and the catalog row itself
//!     is gone (not merely `Expired`) — proven via [`JanitorProgress`]'s own
//!     `Idle → Reclaiming → RemovingRow → Idle` phase sequence (observed
//!     mid-flight by racing the poll against the janitor's own 200ms tick)
//!     as well as the converged end state;
//! (b) a `Failed` backup (the completion aggregator's own stuck-timeout
//!     shape, `FailBackup`) is reclaimed the identical way — no
//!     `MarkBackupDeleted` involved, since `Failed` is already one of the
//!     two reclaimable statuses;
//! (c) leader gating: (c1) a follower's own `JanitorProgress` never leaves
//!     `Idle` while the leader alone reclaims a deleted backup; (c2) a real
//!     `RaftCore::transfer_leadership` handoff issued right after
//!     `MarkBackupDeleted` commits still converges to exactly one reclaim
//!     with no error recorded on ANY node's own progress (whichever of the
//!     old/new leader's tick actually does the work, a second tick's own
//!     `list_local`/`delete_local`/`DeleteBackup` are all idempotent
//!     no-ops, never a surfaced error);
//! (d) the control-plane leader itself crashes right after
//!     `MarkBackupDeleted` commits and is restarted only once the survivors
//!     have already reclaimed the backup — the restarted node's own view
//!     converges too (issue #722's own "a node absent during the whole
//!     window still catches up" shape, one layer up from that fix's own
//!     tablet-engine-reclaim subject);
//! (e) a backup that is still `Available` (never marked deleted, never
//!     failed) is never touched, over a long window — every node's own
//!     `JanitorProgress.backups_seen` stays 0 and the store keeps every
//!     object.

use std::time::Duration;

use animus_control::{BackupStatus, MetaCommand, ProposeResult};
use animus_cp_data::backup as backup_codec;
use animus_node::backup_janitor::JanitorPhase;
use animus_tablet::TabletId;

use super::sim_cluster::SimCluster;

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// A single-hash-key (`pk`, string) `CreateTable`, issued from `node` —
/// mirrors every other `sim_cluster_*` module's identically-named helper.
fn create_table(cluster: &mut SimCluster, node: u64, table: &str) -> (u16, String) {
    let body = format!(
        r#"{{"TableName":"{table}",
            "KeySchema":[{{"AttributeName":"pk","KeyType":"HASH"}}],
            "AttributeDefinitions":[{{"AttributeName":"pk","AttributeType":"S"}}]}}"#
    );
    cluster.dynamo(node, "DynamoDB_20120810.CreateTable", body.as_bytes())
}

/// `table`'s own tablet id, per `node`'s own view of `Metadata` — mirrors
/// every other `sim_cluster_*` module's identically-named helper.
fn tablet_of(cluster: &SimCluster, node: u64, table: &str) -> TabletId {
    let meta = cluster.metadata(node);
    *meta
        .tablets_for_table(table)
        .next()
        .unwrap_or_else(|| panic!("table {table} has no tablet on node {node}'s own view"))
        .0
}

fn accepted(r: ProposeResult) -> bool {
    matches!(r, ProposeResult::Accepted { .. })
}

/// Drive a backup all the way to `Available` (one table, one pinned tablet)
/// — mirrors `animus_node::backup_janitor::tests::complete_a_backup`'s own
/// shape, proposing directly on the current control leader via
/// [`SimCluster::propose_meta`] rather than a bare `RaftNode` handle.
fn complete_a_backup(cluster: &mut SimCluster, table: &str, backup_id: &str) {
    let tablet = tablet_of(cluster, 0, table);
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
    cluster.run_for(Duration::from_millis(200));
}

/// Just `BeginBackup` — a `Creating` row, no completion — for scenario (b),
/// which fails a still-`Creating` backup (`FailBackup`'s own precondition:
/// rejected once `Available`/already `Expired`, so `Creating` is the shape
/// this scenario needs).
fn begin_a_backup(cluster: &mut SimCluster, table: &str, backup_id: &str) {
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
    cluster.run_for(Duration::from_millis(200));
}

/// Poll (never a fixed sleep) until `backup_id` has no catalog row at all
/// on EVERY id in `nodes` — the janitor's own finalize step
/// (`MetaCommand::DeleteBackup`) removes the row outright, so "gone" is the
/// converged end state, mirroring `dynamo_backup.rs`'s own converged-poll
/// idiom for the identical property against a real cluster.
fn poll_until_backup_row_gone(
    cluster: &mut SimCluster,
    backup_id: &str,
    nodes: &[u64],
    budget: Duration,
) {
    const STEP: Duration = Duration::from_millis(50);
    let seed = cluster.seed();
    let mut elapsed = Duration::ZERO;
    loop {
        if nodes
            .iter()
            .all(|&n| cluster.metadata(n).backup(backup_id).is_none())
        {
            return;
        }
        assert!(
            elapsed < budget,
            "backup {backup_id} row did not converge to removed within {budget:?} (seed={seed})"
        );
        cluster.run_for(STEP);
        elapsed += STEP;
    }
}

// ---------------------------------------------------------------------------
// Scenario (a): a completed backup marked deleted is reclaimed.
// ---------------------------------------------------------------------------

fn run_a_a_deleted_backup_is_reclaimed(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, body) = create_table(&mut cluster, 0, "orders");
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    let tablet = tablet_of(&cluster, 0, "orders");

    let backup_id = "arn:aws:dynamodb:animus:0:table/orders/backup/abc";
    complete_a_backup(&mut cluster, "orders", backup_id);

    let manifest_id = backup_codec::backup_manifest_object_id(backup_id);
    let chunk_id = backup_codec::backup_data_object_id(backup_id, tablet.0, 0);
    cluster.seed_backup_object(&manifest_id, b"manifest-bytes");
    cluster.seed_backup_object(&chunk_id, b"chunk-bytes");
    let stored = cluster.backup_store().stored_ids();
    assert!(
        stored.contains(&manifest_id) && stored.contains(&chunk_id),
        "seed={seed}: both objects must actually land before MarkBackupDeleted: {stored:?}"
    );

    assert!(
        accepted(cluster.propose_meta(MetaCommand::MarkBackupDeleted {
            backup_id: backup_id.to_owned(),
        })),
        "seed={seed}: MarkBackupDeleted rejected"
    );

    let nodes: Vec<u64> = (0..cluster.node_count() as u64).collect();
    poll_until_backup_row_gone(&mut cluster, backup_id, &nodes, Duration::from_secs(20));

    let stored = cluster.backup_store().stored_ids();
    assert!(
        !stored.contains(&manifest_id),
        "seed={seed}: manifest object must be reclaimed: {stored:?}"
    );
    assert!(
        !stored.contains(&chunk_id),
        "seed={seed}: data chunk object must be reclaimed: {stored:?}"
    );

    // Roadmap U-07: the leader's own progress ended `Idle` with at least
    // one backup seen and both objects reclaimed — the same `Idle →
    // Reclaiming → RemovingRow → Idle` sequence
    // `animus_node::backup_janitor::tests` pins at the primitive level,
    // now proven end to end through a real multi-node cluster.
    let leader = cluster.control_leader_index() as u64;
    let progress = cluster.backup_janitor_progress(leader);
    assert_eq!(progress.phase, JanitorPhase::Idle, "seed={seed}");
    assert!(
        progress.backups_seen >= 1,
        "seed={seed}: leader must have seen the backup: {progress:?}"
    );
    assert!(
        progress.objects_reclaimed >= 2,
        "seed={seed}: leader must have reclaimed both objects: {progress:?}"
    );
    assert_eq!(progress.last_error, None, "seed={seed}: {progress:?}");
}

#[test]
fn a_a_deleted_backup_is_reclaimed() {
    run_a_a_deleted_backup_is_reclaimed(env_seed(0xBAC7_0001));
}

#[test]
fn a_a_deleted_backup_is_reclaimed_over_seeds() {
    for i in 0..5 {
        run_a_a_deleted_backup_is_reclaimed(0xBAC7_1000 + i);
    }
}

// ---------------------------------------------------------------------------
// Scenario (b): a failed (stuck-`Creating`-timeout-shaped) backup is
// reclaimed the identical way.
// ---------------------------------------------------------------------------

fn run_b_a_failed_backup_is_reclaimed(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, body) = create_table(&mut cluster, 0, "orders");
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    let tablet = tablet_of(&cluster, 0, "orders");

    let backup_id = "arn:aws:dynamodb:animus:0:table/orders/backup/stuck";
    begin_a_backup(&mut cluster, "orders", backup_id);

    // A partial capture: only the manifest object landed before this
    // backup got stuck and was failed — the janitor's own local sweep
    // scans by prefix, so a partial object set reclaims exactly what's
    // actually there, nothing more.
    let manifest_id = backup_codec::backup_manifest_object_id(backup_id);
    cluster.seed_backup_object(&manifest_id, b"manifest-bytes");

    assert!(
        accepted(cluster.propose_meta(MetaCommand::FailBackup {
            backup_id: backup_id.to_owned(),
            reason: "stuck creating timeout".to_owned(),
        })),
        "seed={seed}: FailBackup rejected"
    );
    // A `Failed` row still records no `total_bytes`/pinned-tablet reads —
    // `tablet` is unused beyond proving the table/tablet setup above ran
    // (kept for symmetry with scenario (a) and any future extension that
    // wants to seed a second, per-tablet chunk object here too).
    let _ = tablet;

    let nodes: Vec<u64> = (0..cluster.node_count() as u64).collect();
    poll_until_backup_row_gone(&mut cluster, backup_id, &nodes, Duration::from_secs(20));

    assert!(
        !cluster.backup_store().stored_ids().contains(&manifest_id),
        "seed={seed}: the Failed backup's own manifest object must be reclaimed"
    );
}

#[test]
fn b_a_failed_backup_is_reclaimed() {
    run_b_a_failed_backup_is_reclaimed(env_seed(0xBAC7_0002));
}

#[test]
fn b_a_failed_backup_is_reclaimed_over_seeds() {
    for i in 0..5 {
        run_b_a_failed_backup_is_reclaimed(0xBAC7_2000 + i);
    }
}

// ---------------------------------------------------------------------------
// Scenario (c1): only the control-plane LEADER's janitor ever advances its
// own progress past `Idle` — a follower's copy never touches the store.
// ---------------------------------------------------------------------------

fn run_c1_only_the_leader_reclaims(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, body) = create_table(&mut cluster, 0, "orders");
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    let tablet = tablet_of(&cluster, 0, "orders");

    let backup_id = "arn:aws:dynamodb:animus:0:table/orders/backup/abc";
    complete_a_backup(&mut cluster, "orders", backup_id);
    let manifest_id = backup_codec::backup_manifest_object_id(backup_id);
    let chunk_id = backup_codec::backup_data_object_id(backup_id, tablet.0, 0);
    cluster.seed_backup_object(&manifest_id, b"manifest-bytes");
    cluster.seed_backup_object(&chunk_id, b"chunk-bytes");

    let leader = cluster.control_leader_index() as u64;
    assert!(
        accepted(cluster.propose_meta(MetaCommand::MarkBackupDeleted {
            backup_id: backup_id.to_owned(),
        })),
        "seed={seed}: MarkBackupDeleted rejected"
    );

    let nodes: Vec<u64> = (0..cluster.node_count() as u64).collect();
    poll_until_backup_row_gone(&mut cluster, backup_id, &nodes, Duration::from_secs(20));

    for &n in &nodes {
        if n == leader {
            continue;
        }
        let progress = cluster.backup_janitor_progress(n);
        assert_eq!(
            progress.phase,
            JanitorPhase::Idle,
            "seed={seed}: follower {n}'s own janitor must never leave Idle: {progress:?}"
        );
        assert_eq!(
            progress.backups_seen, 0,
            "seed={seed}: follower {n} must never have looked at any backup row: {progress:?}"
        );
        assert_eq!(
            progress.objects_reclaimed, 0,
            "seed={seed}: follower {n} must never have touched the store: {progress:?}"
        );
    }
}

#[test]
fn c1_only_the_leader_reclaims() {
    run_c1_only_the_leader_reclaims(env_seed(0xBAC7_0003));
}

#[test]
fn c1_only_the_leader_reclaims_over_seeds() {
    for i in 0..5 {
        run_c1_only_the_leader_reclaims(0xBAC7_3000 + i);
    }
}

// ---------------------------------------------------------------------------
// Scenario (c2): a leadership transfer right after `MarkBackupDeleted`
// commits still yields exactly one reclaim, with no error recorded on any
// node's own progress (idempotent whichever leader's tick actually does the
// work).
// ---------------------------------------------------------------------------

fn run_c2_leadership_transfer_yields_one_clean_reclaim(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, body) = create_table(&mut cluster, 0, "orders");
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    let tablet = tablet_of(&cluster, 0, "orders");

    let backup_id = "arn:aws:dynamodb:animus:0:table/orders/backup/abc";
    complete_a_backup(&mut cluster, "orders", backup_id);
    let manifest_id = backup_codec::backup_manifest_object_id(backup_id);
    let chunk_id = backup_codec::backup_data_object_id(backup_id, tablet.0, 0);
    cluster.seed_backup_object(&manifest_id, b"manifest-bytes");
    cluster.seed_backup_object(&chunk_id, b"chunk-bytes");

    assert!(
        accepted(cluster.propose_meta(MetaCommand::MarkBackupDeleted {
            backup_id: backup_id.to_owned(),
        })),
        "seed={seed}: MarkBackupDeleted rejected"
    );

    // Move leadership right away — before this fixture ever lets a
    // `BACKUP_JANITOR_INTERVAL` (200ms) tick fire, so whichever of the old
    // or new leader's own loop first observes the `Expired` row is
    // unpredictable from this test's own point of view (deliberately: the
    // property under test is that it doesn't matter which one does).
    let original_leader = cluster.control_leader_index() as u64;
    let target = (original_leader + 1) % cluster.node_count() as u64;
    cluster.transfer_control_leadership_to(target);

    let nodes: Vec<u64> = (0..cluster.node_count() as u64).collect();
    poll_until_backup_row_gone(&mut cluster, backup_id, &nodes, Duration::from_secs(20));

    let stored = cluster.backup_store().stored_ids();
    assert!(
        !stored.contains(&manifest_id) && !stored.contains(&chunk_id),
        "seed={seed}: both objects must be reclaimed exactly once regardless of the handoff: {stored:?}"
    );

    // No node's own progress ever recorded an error — a second tick's own
    // `list_local`/`delete_local` (an already-empty prefix) and a second
    // `DeleteBackup` propose (already-idempotent-NoOp on an unknown id) are
    // both silent no-ops, never a surfaced failure.
    for &n in &nodes {
        let progress = cluster.backup_janitor_progress(n);
        assert_eq!(
            progress.last_error, None,
            "seed={seed}: node {n} recorded an unexpected janitor error: {progress:?}"
        );
    }
}

#[test]
fn c2_leadership_transfer_yields_one_clean_reclaim() {
    run_c2_leadership_transfer_yields_one_clean_reclaim(env_seed(0xBAC7_0004));
}

#[test]
fn c2_leadership_transfer_yields_one_clean_reclaim_over_seeds() {
    for i in 0..5 {
        run_c2_leadership_transfer_yields_one_clean_reclaim(0xBAC7_4000 + i);
    }
}

// ---------------------------------------------------------------------------
// Scenario (d): the control-plane leader crashes right after
// `MarkBackupDeleted` commits and is restarted only once the survivors have
// already reclaimed the backup — the restarted node's own view converges
// too.
// ---------------------------------------------------------------------------

fn run_d_a_crashed_and_restarted_node_converges(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, body) = create_table(&mut cluster, 0, "orders");
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    let tablet = tablet_of(&cluster, 0, "orders");

    let backup_id = "arn:aws:dynamodb:animus:0:table/orders/backup/abc";
    complete_a_backup(&mut cluster, "orders", backup_id);
    let manifest_id = backup_codec::backup_manifest_object_id(backup_id);
    let chunk_id = backup_codec::backup_data_object_id(backup_id, tablet.0, 0);
    cluster.seed_backup_object(&manifest_id, b"manifest-bytes");
    cluster.seed_backup_object(&chunk_id, b"chunk-bytes");

    let victim = cluster.control_leader_index() as u64;
    assert!(
        accepted(cluster.propose_meta(MetaCommand::MarkBackupDeleted {
            backup_id: backup_id.to_owned(),
        })),
        "seed={seed}: MarkBackupDeleted rejected"
    );
    // `propose` only appends to the leader's own local log and returns
    // `Accepted` (never "committed") — let it actually replicate to a
    // majority and commit before crashing the leader, or the entry is
    // stranded unreplicated on the log this crash is about to mute,
    // silently lost rather than reclaimed by the survivors (the exact
    // "accepted but not yet committed" distinction root `CLAUDE.md`'s
    // durable-before-visible entry warns every proposer must respect).
    // Deliberately short — well under `BACKUP_JANITOR_INTERVAL` (200ms),
    // so the crash still lands before the leader's own janitor tick could
    // have finished the whole reclaim itself, keeping this scenario a
    // genuine proof that the SURVIVORS do the work.
    cluster.run_for(Duration::from_millis(50));

    // Crash the (former) leader before any janitor tick has had a real
    // chance to run — the survivors must elect a new leader and finish the
    // reclaim entirely on their own.
    cluster.crash(victim);

    let live_nodes: Vec<u64> = (0..cluster.node_count() as u64)
        .filter(|&n| n != victim)
        .collect();
    poll_until_backup_row_gone(
        &mut cluster,
        backup_id,
        &live_nodes,
        Duration::from_secs(20),
    );
    let stored = cluster.backup_store().stored_ids();
    assert!(
        !stored.contains(&manifest_id) && !stored.contains(&chunk_id),
        "seed={seed}: the survivors must reclaim both objects without the crashed node: {stored:?}"
    );

    // Restart only now — well after the survivors have already converged.
    cluster.restart(victim);
    cluster.run_for(Duration::from_secs(2));

    let all_nodes: Vec<u64> = (0..cluster.node_count() as u64).collect();
    poll_until_backup_row_gone(&mut cluster, backup_id, &all_nodes, Duration::from_secs(20));

    // The restarted node's own janitor never records a stale error from
    // whatever half-seen state it caught up through.
    let progress = cluster.backup_janitor_progress(victim);
    assert_eq!(
        progress.last_error, None,
        "seed={seed}: restarted node {victim} recorded an unexpected janitor error: {progress:?}"
    );
}

#[test]
fn d_a_crashed_and_restarted_node_converges() {
    run_d_a_crashed_and_restarted_node_converges(env_seed(0xBAC7_0005));
}

#[test]
fn d_a_crashed_and_restarted_node_converges_over_seeds() {
    for i in 0..5 {
        run_d_a_crashed_and_restarted_node_converges(0xBAC7_5000 + i);
    }
}

// ---------------------------------------------------------------------------
// Scenario (e): a backup that is still `Available` (never deleted, never
// failed) is never touched, over a long window.
// ---------------------------------------------------------------------------

fn run_e_an_available_backup_is_never_touched(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, body) = create_table(&mut cluster, 0, "orders");
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
    let tablet = tablet_of(&cluster, 0, "orders");

    let backup_id = "arn:aws:dynamodb:animus:0:table/orders/backup/abc";
    complete_a_backup(&mut cluster, "orders", backup_id);
    let manifest_id = backup_codec::backup_manifest_object_id(backup_id);
    let chunk_id = backup_codec::backup_data_object_id(backup_id, tablet.0, 0);
    cluster.seed_backup_object(&manifest_id, b"manifest-bytes");
    cluster.seed_backup_object(&chunk_id, b"chunk-bytes");

    // Several janitor ticks' worth of virtual time, never marking or
    // failing the backup.
    cluster.run_for(Duration::from_secs(5));

    let meta = cluster.metadata(0);
    assert_eq!(
        meta.backup(backup_id).map(|r| r.status.clone()),
        Some(BackupStatus::Available),
        "seed={seed}: an untouched backup must stay Available"
    );
    let stored = cluster.backup_store().stored_ids();
    assert!(
        stored.contains(&manifest_id) && stored.contains(&chunk_id),
        "seed={seed}: an Available backup's own objects must never be reclaimed: {stored:?}"
    );

    for n in 0..cluster.node_count() as u64 {
        let progress = cluster.backup_janitor_progress(n);
        assert_eq!(
            progress.backups_seen, 0,
            "seed={seed}: node {n} must never have looked at the Available backup: {progress:?}"
        );
    }
}

#[test]
fn e_an_available_backup_is_never_touched() {
    run_e_an_available_backup_is_never_touched(env_seed(0xBAC7_0006));
}

#[test]
fn e_an_available_backup_is_never_touched_over_seeds() {
    for i in 0..5 {
        run_e_an_available_backup_is_never_touched(0xBAC7_6000 + i);
    }
}
