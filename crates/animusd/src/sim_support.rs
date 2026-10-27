//! Helpers shared by the `sim_*` test modules.
//!
//! A helper lands here once two or more `sim_*` files carried a
//! byte-identical copy of it; the `sim_*` files import it from here instead of
//! re-declaring it. A scenario-specific variant (a different table shape, key
//! name, assertion or panic message) stays local to its file: only move a
//! helper whose body is identical, because every body here is part of the
//! seed-reproducible virtual-time schedule (step sizes, timeouts, panic
//! messages) the scenarios rely on.

use std::time::Duration;

use animus_tablet::{TabletId, TabletState};

use super::sim_cluster::SimCluster;

/// The `ANIMUS_SEED` replay override: the env var's value when it parses as a
/// `u64`, else `default`. Single-sourced for every `sim_*` scenario.
pub(crate) fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// Parse a response body as JSON, panicking with the body on failure.
pub(crate) fn json(body: &str) -> serde_json::Value {
    serde_json::from_str(body).unwrap_or_else(|e| panic!("invalid JSON ({e}): {body}"))
}

/// Converged-or-timeout poll on `cond(cluster)`: advances virtual time in
/// 100 ms steps, panicking with `seed` and `what` once `budget` is spent.
/// (Not `SimCluster::poll_until`, a different private method with a 50 ms step.)
pub(crate) fn poll_until(
    cluster: &mut SimCluster,
    budget: Duration,
    seed: u64,
    what: &str,
    mut cond: impl FnMut(&mut SimCluster) -> bool,
) {
    const STEP: Duration = Duration::from_millis(100);
    let mut elapsed = Duration::ZERO;
    loop {
        if cond(cluster) {
            return;
        }
        assert!(
            elapsed < budget,
            "seed={seed}: {what} never converged within {budget:?}"
        );
        cluster.run_for(STEP);
        elapsed += STEP;
    }
}

/// Result slot of a detached driver tick.
pub(crate) type Tick = std::sync::Arc<std::sync::Mutex<Option<Result<(), String>>>>;

/// One DynamoDB wire `CreateTable`, issued from `node`, with a caller-supplied
/// body (for fixtures that need several distinct table shapes).
pub(crate) fn create_table_via_wire(
    cluster: &mut SimCluster,
    node: u64,
    body: &str,
) -> (u16, String) {
    cluster.dynamo(node, "DynamoDB_20120810.CreateTable", body.as_bytes())
}

/// A single-hash-key (`pk`, string) `CreateTable`, issued from `node` over the
/// DynamoDB wire.
pub(crate) fn create_table(cluster: &mut SimCluster, node: u64, table: &str) -> (u16, String) {
    let body = format!(
        r#"{{"TableName":"{table}",
            "KeySchema":[{{"AttributeName":"pk","KeyType":"HASH"}}],
            "AttributeDefinitions":[{{"AttributeName":"pk","AttributeType":"S"}}]}}"#
    );
    cluster.dynamo(node, "DynamoDB_20120810.CreateTable", body.as_bytes())
}

/// The same single-hash-key (`pk`, string) `CreateTable` as [`create_table`],
/// routed through [`create_table_via_wire`]. Callers import it
/// `as create_table`.
pub(crate) fn create_table_pk_via_wire(
    cluster: &mut SimCluster,
    node: u64,
    table: &str,
) -> (u16, String) {
    let body = format!(
        r#"{{"TableName":"{table}",
            "KeySchema":[{{"AttributeName":"pk","KeyType":"HASH"}}],
            "AttributeDefinitions":[{{"AttributeName":"pk","AttributeType":"S"}}]}}"#
    );
    create_table_via_wire(cluster, node, &body)
}

/// `table`'s own (sole) tablet id, read off node 0's own `Metadata`. Not
/// `SimCluster::tablet_of`, which only knows a hand-hosted table's tablet; a
/// table created through the real DynamoDB wire never gets an entry there.
pub(crate) fn first_tablet(cluster: &SimCluster, table: &str) -> TabletId {
    cluster
        .metadata(0)
        .tablets_for_table(table)
        .next()
        .unwrap_or_else(|| panic!("{table} has no tablet"))
        .0
        .to_owned()
}

/// The (sole, pre-split) tablet id hosting `table`, from `node`'s own view of
/// `Metadata`. A wire-provisioned table never has an entry in
/// `SimCluster::create_table_with_replication`'s hand-hosted bookkeeping, so
/// this reads live `Metadata` instead.
pub(crate) fn tablet_of(cluster: &SimCluster, node: u64, table: &str) -> TabletId {
    let meta = cluster.metadata(node);
    *meta
        .tablets_for_table(table)
        .next()
        .unwrap_or_else(|| panic!("table {table} has no tablet on node {node}'s own view"))
        .0
}

/// `table`'s own tablet id, resolved from the replicated catalog (node 0's
/// view). For a table created over the real wire, never via
/// `SimCluster::create_table`.
pub(crate) fn tablet_of_table(cluster: &SimCluster, table: &str) -> TabletId {
    *cluster
        .metadata(0)
        .tablets_for_table(table)
        .next()
        .unwrap_or_else(|| panic!("table `{table}` has no tablet"))
        .0
}

/// The node id currently leading `table`'s own tablet ([`tablet_of_table`]).
pub(crate) fn leader_of_table(cluster: &SimCluster, table: &str) -> u64 {
    let tablet = tablet_of_table(cluster, table);
    cluster
        .leader_index_of(tablet)
        .unwrap_or_else(|| panic!("tablet {} has no leader", tablet.0))
}

/// Like [`leader_of_table`], resolving the (sole) tablet inline with its own
/// panic messages. Callers import it `as leader_of_table`.
pub(crate) fn leader_of_sole_tablet(cluster: &SimCluster, table: &str) -> u64 {
    let meta = cluster.metadata(0);
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

/// Pull `(IndexStatus, Backfilling)` for `index` out of a `DescribeTable`/
/// `UpdateTable` response body's `GlobalSecondaryIndexes` array.
pub(crate) fn index_status(body: &str, index: &str) -> Option<(String, bool)> {
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

/// Poll `body`'s `Query` until `accept` holds, or panic after a bounded
/// number of attempts.
pub(crate) fn await_gsi_query(
    cluster: &mut SimCluster,
    node: u64,
    body: &str,
    accept: impl Fn(&str) -> bool,
) -> String {
    let mut last = String::new();
    for _ in 0..80 {
        let (status, resp) = cluster.dynamo(node, "DynamoDB_20120810.Query", body.as_bytes());
        if status == 200 && accept(&resp) {
            return resp;
        }
        last = resp;
        cluster.run_for(Duration::from_millis(100));
    }
    panic!("gsi query never converged (last saw: {last})");
}

/// Drive the backfill seeder + GSI drain to exhaustion, then poll
/// `DescribeTable` until `index` converges to `ACTIVE`. Resolves the leader
/// through [`leader_of_sole_tablet`].
pub(crate) fn converge_gsi_active(cluster: &mut SimCluster, table: &str, index: &str) {
    let leader = leader_of_table(cluster, table);
    for _ in 0..10 {
        cluster.drive_backfill_seed(leader, table);
        cluster.drain_gsi(leader, table);
        let (_, body) = cluster.dynamo(
            0,
            "DynamoDB_20120810.DescribeTable",
            format!(r#"{{"TableName":"{table}"}}"#).as_bytes(),
        );
        if index_status(&body, index).map(|(s, _)| s) == Some("ACTIVE".to_owned()) {
            return;
        }
    }
    panic!("index `{index}` on `{table}` did not converge to ACTIVE within 10 rounds");
}

/// One `inplace_split_driver_tick` per `Splitting` tablet `node` leads (what
/// `change_consumer_loop` does), without blocking the simulator.
pub(crate) fn spawn_driver_ticks(cluster: &SimCluster, node: u64) -> Tick {
    cluster.spawn_detached(node, move |ctx| async move {
        let meta = ctx.effective_metadata();
        for (tablet, group) in ctx.edge.hosted_groups() {
            if !group.is_leader() {
                continue;
            }
            let splitting = meta
                .tablets
                .get(&tablet)
                .is_some_and(|t| t.state == TabletState::Splitting);
            if splitting {
                crate::index_drain::inplace_split_driver_tick(&ctx, &meta, tablet, &group).await?;
            }
        }
        Ok(())
    })
}

/// Parse an error body's `__type` field, panicking when the body is not JSON
/// or carries no `__type`.
pub(crate) fn error_type(body: &str) -> String {
    let json: serde_json::Value =
        serde_json::from_str(body).unwrap_or_else(|e| panic!("body is not JSON ({e}): {body}"));
    json["__type"]
        .as_str()
        .unwrap_or_else(|| panic!("no __type in: {body}"))
        .to_string()
}

/// Like [`error_type`], but yields an empty string instead of panicking.
/// Callers import it `as error_type`.
pub(crate) fn error_type_lenient(body: &str) -> String {
    serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|j| j["__type"].as_str().map(str::to_owned))
        .unwrap_or_default()
}

/// A `PutItem` body for `table` with an `id` (S) key and a `v` (S) attribute.
pub(crate) fn put_body(table: &str, id: &str, value: &str) -> String {
    format!(r#"{{"TableName":"{table}","Item":{{"id":{{"S":"{id}"}},"v":{{"S":"{value}"}}}}}}"#)
}

/// The message of a caught panic payload.
pub(crate) fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|m| (*m).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "<non-string panic payload>".to_string())
}
