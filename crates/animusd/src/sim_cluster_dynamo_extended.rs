//! `SimCluster`-driven end-to-end test of the extended DynamoDB JSON wire
//! surface (ADR 0061 rung D3 PR 1) — replaces the
//! `concurrent_conditional_puts_one_wins` test from the real-socket
//! `ProdEnv` binary `crates/animusd/tests/dynamo_extended.rs`. That file's
//! sibling test, `create_table_query_and_conditional_writes`, exercises
//! wire-level `CreateTable` (`TableStatus`, `ResourceInUseException` on a
//! duplicate) — `dynamo::dispatch_item_op` does not cover `CreateTable` at
//! all (`SimCluster::create_table` seeds a table by proposing
//! `CreateTableSchema`/`CreateTablet` directly on the control leader,
//! bypassing the wire entirely), so that test **stays on `ProdEnv`** and is
//! left in the original file — a fixture-PR candidate for whenever
//! `dispatch_item_op` grows `CreateTable` support.
//!
//! `concurrent_conditional_puts_one_wins` itself needs no wire `CreateTable`
//! — the `ProdEnv` original relies on `dynamo::legacy_register`'s
//! auto-registration of an unrecognized table on its first `PutItem`
//! instead. **That specific path is a second `SimCluster` capability gap,
//! found running this conversion**: `ClientCtx::provision_tablet` (the
//! auto-provision every first write to a brand-new table goes through)
//! picks the tablet's initial replica set from `Metadata::members` — the
//! *node-registration* catalog a real deployment's `MetaCommand::
//! RegisterNode` populates, which `SimCluster::new` never proposes (its own
//! nodes are wired directly into `ClusterEdgeState`, never registered into
//! `Metadata`). A legacy-registered table's auto-provision therefore mints
//! a tablet with an **empty** replica set — nobody ever hosts it, and the
//! write times out waiting for a group that will never form. This uses
//! `SimCluster::create_table` (the fixture's own supported table-creation
//! path, which picks replicas directly rather than through `Metadata::
//! members`) instead of relying on legacy auto-registration — a fixture
//! data-shape change only (a real `pk`/`sk` composite schema instead of a
//! legacy one, so every item needs an explicit `sk`), not a change to what
//! the race itself proves. Widening `SimCluster::new` to also register
//! nodes is left for the fixture PR the D3 task names, not attempted here.
//!
//! Seed replay: `ANIMUS_SEED=<seed> cargo test -p animusd --lib <test name>`.

use super::sim_cluster::SimCluster;

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

/// Regression: **concurrent** conditional puts are serialized correctly at
/// apply time (ADR 0054): two simultaneous `attribute_not_exists(pk)`
/// `PutItem`s on the same key must yield exactly one success and one
/// `ConditionalCheckFailedException` — without that, both could read
/// "absent" and both succeed (a lost update / double create). Runs several
/// rounds on distinct keys so a lucky interleaving can't mask the race.
/// `SimCluster::dynamo_concurrent` spawns both requests before the one
/// shared `Simulator::run_for` that drives them, so they genuinely race the
/// same key the way the original `tokio::join!` pair did.
#[test]
fn concurrent_conditional_puts_one_wins() {
    let seed = env_seed(0xE475_0001);
    let mut cluster = SimCluster::new(seed, 1, 1);
    cluster.create_table("claims");

    for round in 0..10u32 {
        let body = format!(
            r#"{{"TableName":"claims","Item":{{"pk":{{"S":"claim-{round}"}},"sk":{{"S":"k"}},
                "owner":{{"S":"me"}}}},
                "ConditionExpression":"attribute_not_exists(pk)"}}"#
        );
        let body_bytes = body.as_bytes();
        let results = cluster.dynamo_concurrent(&[
            (0, "DynamoDB_20120810.PutItem", body_bytes),
            (0, "DynamoDB_20120810.PutItem", body_bytes),
        ]);
        let [a, b] = <[(u16, String); 2]>::try_from(results).expect("two results");
        let outcomes = [&a, &b];
        let wins = outcomes.iter().filter(|(s, _)| *s == 200).count();
        assert_eq!(
            wins, 1,
            "round {round}: exactly one conditional put must win, got {a:?} / {b:?} (seed={seed})"
        );
        let loser = outcomes.iter().find(|(s, _)| *s != 200).unwrap();
        assert_eq!(
            loser.0, 400,
            "round {round} seed={seed}: loser status: {loser:?}"
        );
        assert!(
            loser.1.contains("ConditionalCheckFailedException"),
            "round {round} seed={seed}: loser must fail the condition, got: {}",
            loser.1
        );
    }
}
