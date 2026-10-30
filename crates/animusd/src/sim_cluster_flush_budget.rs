//! Issue #1389 (ADR 0079 amendment): the node memtable budget over `SimCluster`.
//!
//! Real `LsmEngine<SimEnv>` tablet engines, many tablets hosted per node (a
//! batch of tables, three replicas on three nodes), a per-node
//! [`animus_storage::FlushBudget`] shared by every tablet engine on the node
//! (the scaled-down 64 KiB budget, 1 KiB floor, 16 KiB cap), and a steady write
//! load spread over all of them through the DynamoDB wire.
//!
//! Oracles (after a warm-up in which every tablet has written):
//!
//! - **within budget**: the node's total memtable bytes, sampled throughout the
//!   run, stay within the budget plus one write of slack per active tablet
//!   (each engine flushes at its share, `budget / active`);
//! - **it is the budget doing it**: the same run with the budget absent (every
//!   engine at the fixed 64 KiB threshold) exceeds the budget (negative
//!   control), so the first oracle measures something;
//! - **acked survives**: every acked write reads back.
//!
//! Knobs: `ANIMUS_FLUSH_BUDGET_SEEDS=K` (default 1), `ANIMUS_SEED` replays.

use std::time::Duration;

use super::sim_cluster::SimCluster;
use super::sim_cluster_lsm::SimFlushBudget;

const TABLES: usize = 8;
const BUDGET: usize = 64 << 10;
const FLOOR: usize = 1 << 10;
const CAP: usize = 16 << 10;
const VALUE: usize = 600;
/// Per-write slack: value + key + framing, rounded up.
const WRITE_SLACK: usize = 1 << 10;

fn seeds() -> Vec<u64> {
    if let Some(s) = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        return vec![s];
    }
    let k: u64 = std::env::var("ANIMUS_FLUSH_BUDGET_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1)
        .max(1);
    animus_test::corpus::shard_retain((0..k).map(|i| 0x1389_0000 + i).collect())
}

fn put_body(table: usize, id: usize) -> String {
    format!(
        r#"{{"TableName":"fb{table}","Item":{{"id":{{"S":"k{id}"}},"v":{{"S":"{}"}}}}}}"#,
        "x".repeat(VALUE)
    )
}

struct Outcome {
    /// Max over samples and nodes of total memtable bytes.
    peak: usize,
    /// Max over samples of the allowed bound (budget + slack per active group).
    bound: usize,
    hosted: usize,
    violations: Vec<String>,
}

fn run(seed: u64, with_budget: bool) -> Outcome {
    let _guard = with_budget.then(|| {
        SimFlushBudget {
            budget: BUDGET,
            floor: FLOOR,
            cap: CAP,
        }
        .install()
    });
    let mut cluster =
        SimCluster::new_with_lsm_engines(seed, &[crate::config::NodeRole::Both; 3], 3, None);
    for t in 0..TABLES {
        let body = format!(
            r#"{{"TableName":"fb{t}",
            "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}],
            "AttributeDefinitions":[{{"AttributeName":"id","AttributeType":"S"}}]}}"#
        );
        let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.CreateTable", body.as_bytes());
        assert_eq!(status, 200, "CreateTable fb{t}: {resp}");
    }
    cluster.run_for(Duration::from_secs(5));

    let mut out = Outcome {
        peak: 0,
        bound: 0,
        hosted: 0,
        violations: Vec::new(),
    };
    let mut acked = Vec::new();
    let mut n = 0usize;
    for round in 0..24 {
        let bodies: Vec<(usize, usize, String)> = (0..TABLES * 3)
            .map(|i| {
                n += 1;
                (i % TABLES, n, put_body(i % TABLES, n))
            })
            .collect();
        let reqs: Vec<(u64, &str, &[u8])> = bodies
            .iter()
            .map(|(_, _, b)| (0u64, "DynamoDB_20120810.PutItem", b.as_bytes()))
            .collect();
        for (i, (status, body)) in cluster.dynamo_concurrent(&reqs).iter().enumerate() {
            if *status == 200 {
                acked.push((bodies[i].0, bodies[i].1));
            } else if *status != 400 && *status != 503 {
                out.violations.push(format!(
                    "write {}: unexpected {status}: {body}",
                    bodies[i].1
                ));
            }
        }
        if round >= 6 {
            // Warm-up over: every tablet has written and the shares converged.
            for node in 0..3u64 {
                let (groups, dirty, bytes) = cluster.lsm_memtable_totals(node);
                out.hosted = out.hosted.max(groups);
                out.peak = out.peak.max(bytes);
                out.bound = out.bound.max(BUDGET + dirty * WRITE_SLACK);
                if with_budget && bytes > BUDGET + dirty * WRITE_SLACK {
                    out.violations.push(format!(
                        "round {round} node {node}: {bytes} memtable bytes over {dirty} dirty of \
                         {groups} hosted groups exceeds budget {BUDGET} + slack"
                    ));
                }
            }
        }
    }
    cluster.run_for(Duration::from_secs(10));
    for (t, i) in acked.iter().step_by(acked.len().div_ceil(48).max(1)) {
        let body = format!(
            r#"{{"TableName":"fb{t}","Key":{{"id":{{"S":"k{i}"}}}},"ConsistentRead":true}}"#
        );
        let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.GetItem", body.as_bytes());
        if status != 200 || !resp.contains(&format!("\"k{i}\"")) {
            out.violations
                .push(format!("acked write fb{t}/k{i} lost: {status} {resp}"));
        }
    }
    out
}

#[test]
fn sim_cluster_hosted_tablets_keep_memtables_within_the_node_budget() {
    for seed in seeds() {
        let on = run(seed, true);
        eprintln!(
            "FLUSHBUDGET seed={seed:#x} hosted/node={} peak={} bound={}",
            on.hosted, on.peak, on.bound
        );
        assert!(
            on.violations.is_empty(),
            "seed={seed:#x} (replay ANIMUS_SEED={seed}):\n  {}",
            on.violations.join("\n  ")
        );
        assert!(
            on.hosted * FLOOR <= BUDGET,
            "seed={seed:#x}: {} hosted groups at the {FLOOR} B floor exceed the budget; \
             the scenario no longer exercises the budget split",
            on.hosted
        );

        // Negative control: without the budget (fixed 64 KiB threshold) the
        // same load holds more than the budget in memtables.
        let off = run(seed, false);
        eprintln!("FLUSHBUDGET seed={seed:#x} control peak={}", off.peak);
        assert!(
            off.peak > BUDGET,
            "seed={seed:#x}: control peak {} did not exceed the budget {BUDGET}, so the bound \
             above proves nothing",
            off.peak
        );
        assert!(on.peak < off.peak, "seed={seed:#x}");
    }
}
