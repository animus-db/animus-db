//! Issue #1229: an in-place split child whose replicas ALL move to other
//! nodes via directed Placing (ADR 0062) must keep its pre-split rows.
//!
//! **Root cause (fixed)**: a split child's engine is *cloned* from its
//! parent's (ADR 0058), so its pre-fork rows are in no entry of the child's
//! Raft log. A learner recruited by Placing was replicated the child's log
//! from entry 1 (the leader's `snapshot_index` is still 0 until the first
//! compaction), caught up "completely" on post-fork writes only, was
//! promoted, and the old homes then reclaimed the only copies of the
//! pre-fork rows — acked data lost. `RaftCore::log_omits_base` (set by the
//! data-plane driver from the durable split-trim marker) now makes the
//! leader ship such a learner the engine image instead.
//!
//! Cells: `memory` (plain `MemoryEngine`) and `lsm_restarts` (real
//! `LsmEngine`s, plus a crash/restart of a rotating node during the settle
//! window, so the leader/learner restart paths are exercised — a restart
//! here is a true process restart over the retained disk).
//!
//! Depth: `ANIMUS_SPLIT_RELOCATION_SEEDS=K` (default 1); `ANIMUS_SEED=<s>`
//! replays exactly one seed (both cells).

use std::time::Duration;

use super::AutoSplitThresholds;
use super::sim_cluster::SimCluster;
use crate::config::NodeRole;

fn call(c: &mut SimCluster, node: u64, op: &str, body: &str) -> (u16, serde_json::Value) {
    let (s, r) = c.dynamo_fast(node, &format!("DynamoDB_20120810.{op}"), body.as_bytes());
    (
        s,
        serde_json::from_str(&r).unwrap_or(serde_json::Value::String(r)),
    )
}

fn seeds() -> Vec<u64> {
    if let Some(s) = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
    {
        return vec![s];
    }
    let k = animus_test::corpus::seeds_from_env("ANIMUS_SPLIT_RELOCATION_SEEDS") as u64;
    (0..k).map(|i| 5 + i).collect()
}

fn run_cell(seed: u64, lsm: bool) {
    let mut c = if lsm {
        SimCluster::new_with_lsm_engines(seed, &[NodeRole::Both; 6], 3, None)
    } else {
        SimCluster::new(seed, 6, 3)
    };
    let _ = c.control_leader_index();
    c.run_for(Duration::from_secs(10));
    let (s, v) = call(
        &mut c,
        0,
        "CreateTable",
        r#"{"TableName":"tbl","KeySchema":[{"AttributeName":"pk","KeyType":"HASH"}],"AttributeDefinitions":[{"AttributeName":"pk","AttributeType":"S"}]}"#,
    );
    assert_eq!(s, 200, "seed {seed}: {v}");
    c.run_for(Duration::from_secs(10));
    for i in 0..20 {
        let (s, v) = call(
            &mut c,
            (i % 6) as u64,
            "PutItem",
            &format!(r#"{{"TableName":"tbl","Item":{{"pk":{{"S":"old{i}"}},"v":{{"S":"x"}}}}}}"#),
        );
        assert_eq!(s, 200, "seed {seed}: {v}");
    }
    c.set_auto_split_thresholds(AutoSplitThresholds {
        bytes: Some(2_000),
        change_rate: None,
        ops_rate: None,
        tablet_capacity_ceilings: Default::default(),
    });
    let pad = "x".repeat(300);
    let mut i = 0usize;
    for _ in 0..4800 {
        for n in 0..6 {
            c.drive_inplace_split_cutover(n);
        }
        let meta = c.metadata(0);
        let routable = meta
            .tablets_for_table("tbl")
            .filter(|(_, t)| t.is_routable())
            .count();
        let splitting = meta
            .tablets_for_table("tbl")
            .filter(|(_, t)| t.state == animus_tablet::TabletState::Splitting)
            .count();
        if routable >= 2 && splitting == 0 {
            break;
        }
        i += 1;
        let _ = call(
            &mut c,
            (i % 6) as u64,
            "PutItem",
            &format!(
                r#"{{"TableName":"tbl","Item":{{"pk":{{"S":"pad{i}"}},"v":{{"S":"{pad}"}}}}}}"#
            ),
        );
        c.run_for(Duration::from_millis(500));
    }
    if lsm {
        // Placing is converging now: bounce one node at a time (a real
        // process restart over its retained disk).
        for k in 0..6u64 {
            let n = (seed + k) % 6;
            c.crash(n);
            c.run_for(Duration::from_secs(2));
            c.restart(n);
            c.run_for(Duration::from_secs(15));
        }
    }
    c.run_for(Duration::from_secs(150));
    let meta = c.metadata(0);
    let placement: Vec<_> = meta
        .tablets_for_table("tbl")
        .map(|(id, t)| (*id, t.state, t.replicas.clone()))
        .collect();
    let mut missing = vec![];
    for j in 0..20 {
        let mut ok = false;
        for _ in 0..30 {
            let (s, v) = call(
                &mut c,
                0,
                "GetItem",
                &format!(
                    r#"{{"TableName":"tbl","Key":{{"pk":{{"S":"old{j}"}}}},"ConsistentRead":true}}"#
                ),
            );
            if s == 200 {
                ok = v.get("Item").is_some();
                break;
            }
            c.run_for(Duration::from_millis(500));
        }
        if !ok {
            missing.push(j);
        }
    }
    assert!(
        missing.is_empty(),
        "seed {seed} lsm={lsm}: lost pre-split keys {missing:?}; placement {placement:?}"
    );
}

#[test]
fn split_child_relocated_wholesale_keeps_pre_split_rows() {
    for seed in seeds() {
        run_cell(seed, false);
    }
}

#[test]
fn split_child_relocated_wholesale_keeps_pre_split_rows_across_restarts() {
    for seed in seeds() {
        run_cell(seed, true);
    }
}
