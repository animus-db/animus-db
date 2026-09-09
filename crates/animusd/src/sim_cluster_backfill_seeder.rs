//! `SimCluster`-driven end-to-end tests of the secondary-index **backfill
//! seeder** (ADR 0045 §2, ADR 0061 rung J, C-10 PR 4) — converting 4 of the
//! 5 scenarios in `tests/backfill_seeder.rs` (real `ProdEnv`) into
//! deterministic, seed-replayable siblings here, driven through the same
//! primitives [`sim_cluster_index_ddl`](super::sim_cluster_index_ddl)'s own
//! PR 2 groundwork established: [`SimCluster::drive_backfill_seed`] (the
//! per-tablet seeding half, `index_drain::backfill_seed_tick`) and
//! [`SimCluster::drain_gsi`] (the ordinary GSI drain, which materializes
//! both seeded and live-write change records identically), alongside the
//! always-on `index_backfill::index_backfill_loop` completion aggregator
//! this fixture spawns unconditionally on every node since that PR.
//!
//! **`ANIMUS_SEED=<seed> cargo test -p animusd --lib <test name>` replays
//! any one** — every scenario below is seed-parameterized with a pinned
//! test plus a `_over_seeds` sibling at 5 seeds, mirroring every prior
//! `sim_cluster_*` module's own convention.
//!
//! # 5 → 4 mapping
//!
//! 1. `backfill_seeder_materializes_every_pre_existing_row_then_flips_active`
//!    → [`backfill_seeder_materializes_every_pre_existing_row_then_flips_active`]:
//!    populate a table, hand-drive `MetaCommand::CreateTableIndex{status:
//!    Creating}` (`SimCluster::propose_meta`, mirroring the real test's own
//!    `ClientRequest::ProposeSchema` — `UpdateTable`'s wire path for adding
//!    an index to a populated table wasn't reachable from `SimCluster` at
//!    the time this file was ported, the identical scope note the original
//!    file's own doc carried), converge to `ACTIVE` by looping
//!    `drive_backfill_seed`/`drain_gsi` across every node until a
//!    `DescribeTable` call reports it — never a single call assumed to
//!    finish a populated table's backfill in one pass — then assert every
//!    pre-existing row is queryable through the index.
//! 2. `live_writes_during_backfill_converge_to_the_correct_final_gsi` →
//!    [`live_writes_during_backfill_converge_to_the_correct_final_gsi`]:
//!    issue the same five-new/one-moved/one-deleted race writes the real
//!    test does, but sequenced (not raced) — right after the index commits
//!    `Creating` and **before any `drive_backfill_seed` round has run at
//!    all** (so the completion aggregator has nothing to observe and the
//!    index genuinely cannot have flipped `Active` yet), then converge.
//!    Every live write already carries a genuine change-log record
//!    unconditional on the index's status (ADR 0045 §2), so the ordinary
//!    drain materializes them identically to a truly concurrent write —
//!    this fixture's own deterministic single-threaded driving can't
//!    literally race two tasks against the seeder the way the real
//!    `tokio::spawn` writer does, so it instead proves the same "final
//!    state is correct regardless of write/seed interleaving" property the
//!    real test's own doc states is the actual load-bearing claim.
//! 3. `two_indexes_creating_simultaneously_converge_independently` →
//!    [`two_indexes_creating_simultaneously_converge_independently`]:
//!    unchanged in shape — two `Creating` indexes on the same table,
//!    converging to `ACTIVE` with correct, independent content
//!    (`drive_backfill_seed`/`drain_gsi` already process every `Creating`
//!    index of a table in one call, so nothing extra is needed to drive
//!    both at once).
//! 4. `a_crash_and_restart_mid_backfill_still_converges` →
//!    [`a_crash_and_restart_mid_backfill_still_converges`]: 300 rows (>
//!    the production `BACKFILL_SEED_BATCH`, 256, `index_drain.rs` — so one
//!    `drive_backfill_seed` round provably cannot finish sweeping this
//!    table), one partial seed round on the tablet's own leader (leaving
//!    the sweep genuinely mid-flight, confirmed `Creating` right before the
//!    crash), then `SimCluster::crash`/`restart` of that same leader node
//!    — a true process restart reusing the SAME `MemoryTabletEngines`
//!    handle (`animusd/CLAUDE.md`'s own `SimCluster::restart` doc), so the
//!    tablet's own durable `KIND_CURSOR` row (committed via the tablet's
//!    own Raft group before the crash, replicated to every replica, not
//!    driver-local state) survives. A further round of
//!    `drive_backfill_seed`/`drain_gsi` calls (possibly through a
//!    different leader, if the crash forced a re-election) resumes from
//!    that persisted cursor and converges to the correct final GSI.
//!
//! # Residual: `split_during_backfill_converges_with_correct_final_gsi`
//!
//! **Kept on `ProdEnv`, in `tests/backfill_seeder.rs`, unmodified** — the
//! opener's own license (ADR 0061's rung J opener) for exactly this case.
//! `SimCluster` spawns no `index_drain::change_consumer_loop` at all (see
//! this crate's own `SimCluster` module doc's "Design decisions" — every
//! per-tick arm that loop would run is instead something a test drives
//! on demand), so proving this scenario here would mean hand-interleaving
//! **three** separately-timed on-demand primitives every round —
//! [`SimCluster::drive_backfill_seed`]/[`SimCluster::drain_gsi`] (the
//! pre-cutover backfill/GSI vetoes `index_drain::inplace_split_driver_tick`
//! checks) and [`SimCluster::drive_inplace_split_cutover`] itself (the
//! cutover propose) — across every node, with no way to verify, in this
//! session (no `cargo` access), that the resulting sequencing doesn't let
//! the always-on completion aggregator observe the **parent**'s own
//! completed sweep and flip the index `Active` a beat before (or after)
//! the separate cutover propose actually commits, nor that the post-cutover
//! Fork-A per-child restart-from-scratch resweep (ADR 0045 §5 — "no
//! split-lineage cursor inheritance") converges within any round budget
//! this file could pick without ever having run it. Rather than ship an
//! unverified, potentially flaky or non-terminating conversion of the one
//! scenario in this file that exercises three interacting subsystems at
//! once, it stays exactly as it was.

use std::time::Duration;

use animus_control::{
    IndexDef, IndexKind, IndexProjection, IndexStatus, MetaCommand, ProposeResult,
};
use animus_dynamo::wire::BATCH_WRITE_MAX_ITEMS;

use super::sim_cluster::SimCluster;

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// A `Creating` GSI definition hashing on `hash_attribute` — duplicated from
/// `tests/backfill_seeder.rs`'s own identically-named helper (this crate's
/// own per-file-fixture convention).
fn creating_index(name: &str, hash_attribute: &str) -> IndexDef {
    IndexDef {
        name: name.to_owned(),
        kind: IndexKind::Global,
        hash_attribute: hash_attribute.to_owned(),
        sort_attribute: None,
        projection: IndexProjection::All,
        status: IndexStatus::Creating,
        hash_attribute_type: None,
        sort_attribute_type: None,
    }
}

/// Pull `(IndexStatus, Backfilling)` for `index` out of a `DescribeTable`
/// response body's `GlobalSecondaryIndexes` array — `None` if the index
/// isn't listed at all. Duplicated from `sim_cluster_index_ddl.rs`'s own
/// identically-named helper, per this crate's own per-file-fixture
/// convention.
fn index_status(body: &str, index: &str) -> Option<(String, bool)> {
    let v: serde_json::Value = serde_json::from_str(body).ok()?;
    let table = v.get("Table").or_else(|| v.get("TableDescription"))?;
    let gsis = table.get("GlobalSecondaryIndexes")?.as_array()?;
    let entry = gsis
        .iter()
        .find(|g| g.get("IndexName").and_then(|n| n.as_str()) == Some(index))?;
    let status = entry.get("IndexStatus")?.as_str()?.to_owned();
    let backfilling = entry
        .get("Backfilling")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    Some((status, backfilling))
}

/// Find the tablet leader of `table`'s (sole) tablet, reading `Metadata`
/// off `node`'s own view — mirrors `sim_cluster_index_ddl.rs`'s own
/// identically-named helper (duplicated, per convention).
fn leader_of_table(cluster: &SimCluster, node: u64, table: &str) -> u64 {
    let meta = cluster.metadata(node);
    let (tablet, _) = meta
        .tablets_for_table(table)
        .next()
        .unwrap_or_else(|| panic!("{table} has no tablet"));
    let tablet = *tablet;
    drop(meta);
    cluster
        .leader_index_of(tablet)
        .unwrap_or_else(|| panic!("{table} tablet has no leader"))
}

fn create_table_no_index(cluster: &mut SimCluster, node: u64, table: &str, seed: u64) {
    let (status, body) = cluster.dynamo(
        node,
        "DynamoDB_20120810.CreateTable",
        format!(
            r#"{{"TableName":"{table}",
                "AttributeDefinitions":[{{"AttributeName":"id","AttributeType":"S"}}],
                "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}]}}"#
        )
        .as_bytes(),
    );
    assert_eq!(status, 200, "seed={seed}: CreateTable failed: {body}");
}

fn put_item(
    cluster: &mut SimCluster,
    node: u64,
    table: &str,
    id: &str,
    attr: &str,
    value: &str,
    seed: u64,
) {
    let (status, body) = cluster.dynamo(
        node,
        "DynamoDB_20120810.PutItem",
        format!(
            r#"{{"TableName":"{table}","Item":{{"id":{{"S":"{id}"}},"{attr}":{{"S":"{value}"}}}}}}"#
        )
        .as_bytes(),
    );
    assert_eq!(status, 200, "seed={seed}: PutItem({id}) failed: {body}");
}

fn delete_item(cluster: &mut SimCluster, node: u64, table: &str, id: &str, seed: u64) {
    let (status, body) = cluster.dynamo(
        node,
        "DynamoDB_20120810.DeleteItem",
        format!(r#"{{"TableName":"{table}","Key":{{"id":{{"S":"{id}"}}}}}}"#).as_bytes(),
    );
    assert_eq!(status, 200, "seed={seed}: DeleteItem({id}) failed: {body}");
}

/// Propose `MetaCommand::CreateTableIndex{status: Creating}` directly on the
/// control leader (`SimCluster::propose_meta`, mirroring the real test's own
/// hand-driven `ClientRequest::ProposeSchema`) and converge-or-timeout poll
/// (`run_for`-only, never another op call) until every node's own
/// `effective_metadata()` shows the index. This is a plain local propose
/// (no commit-wait of its own — `propose_meta`'s own doc), so the caller
/// must confirm convergence itself before relying on any node's own view.
fn create_index_creating(
    cluster: &mut SimCluster,
    table: &str,
    index_name: &str,
    hash_attribute: &str,
    seed: u64,
) {
    let outcome = cluster.propose_meta(MetaCommand::CreateTableIndex {
        table: table.to_owned(),
        index: creating_index(index_name, hash_attribute),
    });
    assert!(
        matches!(outcome, ProposeResult::Accepted { .. }),
        "seed={seed}: CreateTableIndex must be accepted by the current control leader \
         (table={table})"
    );
    let n = cluster.node_count() as u64;
    for _ in 0..100 {
        if (0..n).all(|node| {
            cluster
                .metadata(node)
                .table_indexes(table)
                .iter()
                .any(|i| i.name == index_name)
        }) {
            return;
        }
        cluster.run_for(Duration::from_millis(100));
    }
    panic!("seed={seed}: CreateTableIndex({table}/{index_name}) did not converge within 10s");
}

/// Drive the backfill seeder + GSI drain to exhaustion across **every**
/// node each round (a leader-check no-op on any node that doesn't
/// currently lead a matching tablet — safe and cheap to call unconditionally),
/// polling a `DescribeTable` call each round (a real op call, which is what
/// actually gives the always-on `index_backfill_loop` completion
/// aggregator a chance to observe the freshly-reported tablet(s) and flip
/// every one of `indices` `Active`) — never a single round assumed to
/// finish. Mirrors `sim_cluster_index_ddl.rs::converge_gsi_active`,
/// generalized to every node (rather than one pinned leader) so a scenario
/// whose own leader can change mid-convergence — e.g. a crash/restart —
/// still converges correctly.
fn converge_indices_active(
    cluster: &mut SimCluster,
    table: &str,
    indices: &[&str],
    observer: u64,
    rounds: usize,
    seed: u64,
) {
    for _ in 0..rounds {
        let n = cluster.node_count() as u64;
        for node in 0..n {
            cluster.drive_backfill_seed(node, table);
            cluster.drain_gsi(node, table);
        }
        let (_, body) = cluster.dynamo(
            observer,
            "DynamoDB_20120810.DescribeTable",
            format!(r#"{{"TableName":"{table}"}}"#).as_bytes(),
        );
        if indices
            .iter()
            .all(|idx| index_status(&body, idx).map(|(s, _)| s) == Some("ACTIVE".to_owned()))
        {
            return;
        }
    }
    panic!(
        "seed={seed}: indices {indices:?} on `{table}` did not converge to ACTIVE within \
         {rounds} rounds"
    );
}

/// Live row count of `table`, via a real linearizable client-protocol scan
/// (`SimCluster::scan`) — decoded live items only, so a tombstone from a
/// `DeleteItem` is never counted. Mirrors `tests/backfill_seeder.rs`'s own
/// `row_count`/`await_row_count` shape.
fn row_count(cluster: &mut SimCluster, node: u64, table: &str, seed: u64) -> usize {
    let rows = cluster
        .scan(node, table, true)
        .unwrap_or_else(|e| panic!("seed={seed}: scan(`{table}`) on node {node} failed: {e}"));
    rows.iter()
        .filter(|(_, v)| matches!(animus_dynamo::wire::decode_stored_item(v), Ok(Some(_))))
        .count()
}

fn await_row_count(
    cluster: &mut SimCluster,
    node: u64,
    table: &str,
    want: usize,
    what: &str,
    seed: u64,
) {
    let mut last = 0;
    for _ in 0..50 {
        last = row_count(cluster, node, table, seed);
        if last == want {
            return;
        }
        cluster.run_for(Duration::from_millis(100));
    }
    panic!("seed={seed}: {what}: `{table}` never reached {want} rows (last saw {last})");
}

/// Poll a GSI `Query` until `accept` is satisfied (a GSI is eventually
/// consistent by contract — `ConsistentRead` is always `false`, the only
/// legal value against a GSI). Mirrors `tests/backfill_seeder.rs`'s own
/// `await_gsi_query`.
fn poll_gsi_query(
    cluster: &mut SimCluster,
    node: u64,
    body: &str,
    seed: u64,
    accept: impl Fn(&str) -> bool,
) {
    let mut last = String::new();
    for _ in 0..50 {
        let (status, got) = cluster.dynamo(node, "DynamoDB_20120810.Query", body.as_bytes());
        if status == 200 && accept(&got) {
            return;
        }
        last = got;
        cluster.run_for(Duration::from_millis(100));
    }
    panic!("seed={seed}: GSI query never converged (last saw: {last})");
}

#[allow(clippy::too_many_arguments)]
fn await_gsi_hit(
    cluster: &mut SimCluster,
    node: u64,
    table: &str,
    index: &str,
    hash: &str,
    value: &str,
    id: &str,
    seed: u64,
) {
    let body = format!(
        r#"{{"TableName":"{table}","IndexName":"{index}","ConsistentRead":false,
            "KeyConditionExpression":"{hash} = :v",
            "ExpressionAttributeValues":{{":v":{{"S":"{value}"}}}}}}"#
    );
    poll_gsi_query(cluster, node, &body, seed, |b| {
        b.contains("\"Count\":1") && b.contains(&format!(r#""id":{{"S":"{id}"}}"#))
    });
}

fn await_gsi_miss(
    cluster: &mut SimCluster,
    node: u64,
    table: &str,
    index: &str,
    hash: &str,
    value: &str,
    seed: u64,
) {
    let body = format!(
        r#"{{"TableName":"{table}","IndexName":"{index}","ConsistentRead":false,
            "KeyConditionExpression":"{hash} = :v",
            "ExpressionAttributeValues":{{":v":{{"S":"{value}"}}}}}}"#
    );
    poll_gsi_query(cluster, node, &body, seed, |b| b.contains("\"Count\":0"));
}

// ---------------------------------------------------------------------------
// Scenario 1: materializes every pre-existing row, then flips Active.
// ---------------------------------------------------------------------------

fn run_backfill_seeder_materializes_every_pre_existing_row_then_flips_active(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let table = "bf_seed";
    let index_table = "bf_seed$by-email";

    create_table_no_index(&mut cluster, 0, table, seed);
    let ids: Vec<String> = (0..12).map(|i| format!("p{i}")).collect();
    for id in &ids {
        put_item(
            &mut cluster,
            0,
            table,
            id,
            "email",
            &format!("{id}@x"),
            seed,
        );
    }

    create_index_creating(&mut cluster, table, "by-email", "email", seed);
    converge_indices_active(&mut cluster, table, &["by-email"], 0, 15, seed);

    await_row_count(
        &mut cluster,
        0,
        index_table,
        ids.len(),
        "after backfill converges",
        seed,
    );
    for id in &ids {
        await_gsi_hit(
            &mut cluster,
            0,
            table,
            "by-email",
            "email",
            &format!("{id}@x"),
            id,
            seed,
        );
    }
}

#[test]
fn backfill_seeder_materializes_every_pre_existing_row_then_flips_active() {
    run_backfill_seeder_materializes_every_pre_existing_row_then_flips_active(env_seed(
        0xBF5E_0001,
    ));
}

#[test]
fn backfill_seeder_materializes_every_pre_existing_row_then_flips_active_over_seeds() {
    for i in 0..5 {
        run_backfill_seeder_materializes_every_pre_existing_row_then_flips_active(0xBF5E_1000 + i);
    }
}

// ---------------------------------------------------------------------------
// Scenario 2: live writes issued while the index is still Creating converge
// to the correct final GSI regardless of write/seed interleaving.
// ---------------------------------------------------------------------------

fn run_live_writes_during_backfill_converge_to_the_correct_final_gsi(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let table = "bf_live";
    let index_table = "bf_live$by-g";

    create_table_no_index(&mut cluster, 0, table, seed);
    let pre_existing: Vec<String> = (0..20).map(|i| format!("p{i}")).collect();
    for id in &pre_existing {
        put_item(&mut cluster, 0, table, id, "g", &format!("g-{id}"), seed);
    }

    create_index_creating(&mut cluster, table, "by-g", "g", seed);

    // Issued right after the index commits `Creating` and before any
    // `drive_backfill_seed` round has run at all — the completion
    // aggregator has nothing to observe yet (no `MarkIndexBackfilled` has
    // been proposed for this tablet), so the index cannot have flipped
    // `Active` underneath these writes. Every live write already leaves a
    // genuine change-log record unconditional on the index's own status
    // (ADR 0045 §2), so the ordinary drain (below) materializes them
    // identically to how it would a truly concurrent write — this
    // fixture's own deterministic, single-threaded driving can't literally
    // race two tasks against the seeder the way the real `tokio::spawn`
    // writer does, so this instead proves the same "final state is correct
    // regardless of write/seed interleaving" property the real test's own
    // doc states is the actual load-bearing claim: five new items, one
    // moved attribute (p0), one deletion (p1).
    for i in 0..5 {
        put_item(
            &mut cluster,
            0,
            table,
            &format!("n{i}"),
            "g",
            &format!("g-n{i}"),
            seed,
        );
    }
    put_item(&mut cluster, 0, table, "p0", "g", "g-p0-moved", seed);
    delete_item(&mut cluster, 0, table, "p1", seed);

    converge_indices_active(&mut cluster, table, &["by-g"], 0, 15, seed);

    // 20 pre-existing - 1 deleted (p1) + 5 new = 24; p0 still counts once,
    // at its moved key.
    await_row_count(
        &mut cluster,
        0,
        index_table,
        24,
        "after backfill + concurrent writes",
        seed,
    );

    for i in 0..5 {
        await_gsi_hit(
            &mut cluster,
            0,
            table,
            "by-g",
            "g",
            &format!("g-n{i}"),
            &format!("n{i}"),
            seed,
        );
    }
    await_gsi_hit(
        &mut cluster,
        0,
        table,
        "by-g",
        "g",
        "g-p0-moved",
        "p0",
        seed,
    );
    await_gsi_miss(&mut cluster, 0, table, "by-g", "g", "g-p0", seed); // the old key is gone
    await_gsi_miss(&mut cluster, 0, table, "by-g", "g", "g-p1", seed); // deleted
    // An untouched pre-existing item is unaffected.
    await_gsi_hit(&mut cluster, 0, table, "by-g", "g", "g-p10", "p10", seed);
}

#[test]
fn live_writes_during_backfill_converge_to_the_correct_final_gsi() {
    run_live_writes_during_backfill_converge_to_the_correct_final_gsi(env_seed(0xBF5E_0002));
}

#[test]
fn live_writes_during_backfill_converge_to_the_correct_final_gsi_over_seeds() {
    for i in 0..5 {
        run_live_writes_during_backfill_converge_to_the_correct_final_gsi(0xBF5E_2000 + i);
    }
}

// ---------------------------------------------------------------------------
// Scenario 3: two indexes Creating simultaneously converge independently.
// ---------------------------------------------------------------------------

fn run_two_indexes_creating_simultaneously_converge_independently(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let table = "bf_multi";
    let idx1_table = "bf_multi$by-g1";
    let idx2_table = "bf_multi$by-g2";

    create_table_no_index(&mut cluster, 0, table, seed);
    let ids: Vec<String> = (0..10).map(|i| format!("m{i}")).collect();
    for id in &ids {
        let (status, body) = cluster.dynamo(
            0,
            "DynamoDB_20120810.PutItem",
            format!(
                r#"{{"TableName":"{table}","Item":{{"id":{{"S":"{id}"}},
                    "g1":{{"S":"g1-{id}"}},"g2":{{"S":"g2-{id}"}}}}}}"#
            )
            .as_bytes(),
        );
        assert_eq!(status, 200, "seed={seed}: PutItem({id}) failed: {body}");
    }

    create_index_creating(&mut cluster, table, "by-g1", "g1", seed);
    create_index_creating(&mut cluster, table, "by-g2", "g2", seed);

    converge_indices_active(&mut cluster, table, &["by-g1", "by-g2"], 0, 15, seed);

    await_row_count(
        &mut cluster,
        0,
        idx1_table,
        ids.len(),
        "by-g1 after convergence",
        seed,
    );
    await_row_count(
        &mut cluster,
        0,
        idx2_table,
        ids.len(),
        "by-g2 after convergence",
        seed,
    );
    for id in &ids {
        await_gsi_hit(
            &mut cluster,
            0,
            table,
            "by-g1",
            "g1",
            &format!("g1-{id}"),
            id,
            seed,
        );
        await_gsi_hit(
            &mut cluster,
            0,
            table,
            "by-g2",
            "g2",
            &format!("g2-{id}"),
            id,
            seed,
        );
    }
}

#[test]
fn two_indexes_creating_simultaneously_converge_independently() {
    run_two_indexes_creating_simultaneously_converge_independently(env_seed(0xBF5E_0003));
}

#[test]
fn two_indexes_creating_simultaneously_converge_independently_over_seeds() {
    for i in 0..5 {
        run_two_indexes_creating_simultaneously_converge_independently(0xBF5E_3000 + i);
    }
}

// ---------------------------------------------------------------------------
// Scenario 4: a crash and restart mid-backfill still converges, resuming
// from the persisted (durable) backfill cursor.
// ---------------------------------------------------------------------------

fn run_a_crash_and_restart_mid_backfill_still_converges(seed: u64) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let table = "bf_restart";
    let index_table = "bf_restart$by-email";

    create_table_no_index(&mut cluster, 0, table, seed);
    // 300 > the production `BACKFILL_SEED_BATCH` (256, `index_drain.rs`),
    // so the very first `drive_backfill_seed` round provably cannot finish
    // sweeping this table in one pass — the crash below genuinely lands
    // mid-backfill, with a durably-committed (Raft-replicated to every
    // replica of this tablet's own group) partial cursor, not a synthetic
    // "before it even started" case. `BatchWriteItem` in
    // `BATCH_WRITE_MAX_ITEMS`-sized chunks, not individual `PutItem`
    // round trips, mirroring the real test's own reasoning: this table is
    // still unindexed at population time, so it rides the fast marker
    // write path.
    let ids: Vec<String> = (0..300).map(|i| format!("r{i:04}")).collect();
    for chunk in ids.chunks(BATCH_WRITE_MAX_ITEMS) {
        let puts: Vec<String> = chunk
            .iter()
            .map(|id| {
                format!(
                    r#"{{"PutRequest":{{"Item":{{"id":{{"S":"{id}"}},"email":{{"S":"{id}@x"}}}}}}}}"#
                )
            })
            .collect();
        let body = format!(r#"{{"RequestItems":{{"{table}":[{}]}}}}"#, puts.join(","));
        let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.BatchWriteItem", body.as_bytes());
        assert_eq!(status, 200, "seed={seed}: BatchWriteItem failed: {resp}");
    }

    create_index_creating(&mut cluster, table, "by-email", "email", seed);

    let leader = leader_of_table(&cluster, 0, table);
    // ONE partial round: seeds up to 256 of the 300 partitions, leaving the
    // sweep short of the tablet's own range end.
    cluster.drive_backfill_seed(leader, table);
    let (_, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.DescribeTable",
        format!(r#"{{"TableName":"{table}"}}"#).as_bytes(),
    );
    assert_eq!(
        index_status(&body, "by-email").map(|(s, _)| s),
        Some("CREATING".to_owned()),
        "seed={seed}: the index must still be mid-backfill right before the crash: {body}"
    );

    cluster.crash(leader);
    cluster.restart(leader);

    // The always-on `index_backfill::index_backfill_loop` completion
    // aggregator and a fresh round of `drive_backfill_seed`/`drain_gsi`
    // calls (possibly through a different leader now, if the crash forced
    // a re-election) resume from the persisted cursor rather than
    // re-sweeping from scratch — the seeder's own durable-cursor contract,
    // proven end to end here rather than assumed. `converge_indices_active`
    // re-derives which node to call each round rather than pinning the
    // pre-crash leader, so it works correctly regardless of who now leads.
    converge_indices_active(&mut cluster, table, &["by-email"], 0, 30, seed);

    await_row_count(
        &mut cluster,
        0,
        index_table,
        ids.len(),
        "after restart recovery",
        seed,
    );
    // A spot-check across the id range (first, one past the round-1/round-2
    // numeric midpoint, and last) — `await_row_count` above already proves
    // every one of the 300 rows landed; the seeder sweeps in physical
    // (hash-token) key order, not id-string order, so these three don't
    // necessarily correspond to which literal round seeded them, only that
    // an arbitrary spread of ids is genuinely queryable post-resume.
    for id in [&ids[0], &ids[150], &ids[299]] {
        await_gsi_hit(
            &mut cluster,
            0,
            table,
            "by-email",
            "email",
            &format!("{id}@x"),
            id,
            seed,
        );
    }
}

#[test]
fn a_crash_and_restart_mid_backfill_still_converges() {
    run_a_crash_and_restart_mid_backfill_still_converges(env_seed(0xBF5E_0004));
}

#[test]
fn a_crash_and_restart_mid_backfill_still_converges_over_seeds() {
    for i in 0..5 {
        run_a_crash_and_restart_mid_backfill_still_converges(0xBF5E_4000 + i);
    }
}
