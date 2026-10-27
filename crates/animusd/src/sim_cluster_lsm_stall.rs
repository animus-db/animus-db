//! ADR 0081 Tier 2: an LSM write stall over `SimCluster` and the DynamoDB wire.
//!
//! Real `LsmEngine<SimEnv>` tablet engines with the L0 stall on (3T slowdown,
//! 5T stop) and a shared compaction wedge that withholds every compaction. An
//! open-loop burst against a small propose bound while the wedge holds must see
//! only the documented answers (`200`, `400 ThrottlingException`, `503`; never a
//! `500`), the sim must never panic (an `Err` out of a stalled write used to
//! take the apply task down), every `200` must be durable once the wedge lifts,
//! and a fresh write must succeed afterwards.
//!
//! Control: with `StallFault::OldGate` (the pre-ADR gate, post-append, ending in
//! `Err` after 10 s) the same run must violate the no-panic oracle.
//!
//! Knobs: `ANIMUS_LSM_STALL_SEEDS=K` (default 1), `ANIMUS_SEED` replays.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use super::sim_cluster::{SimCluster, SimOverload};
use super::sim_cluster_lsm::SimStall;
use super::sim_support::error_type_lenient as error_type;
use animus_cp_data::admission::BacklogLimits;
use animus_storage::{LsmOptions, StallFault};

fn seeds() -> Vec<u64> {
    if let Some(s) = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
    {
        return vec![s];
    }
    let k: u64 = std::env::var("ANIMUS_LSM_STALL_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1)
        .max(1);
    (0..k).map(|i| 0x0081_2000 + i).collect()
}

fn put_body(id: &str) -> String {
    format!(
        r#"{{"TableName":"tab_s","Item":{{"id":{{"S":"{id}"}},"v":{{"S":"{}"}}}}}}"#,
        "x".repeat(700)
    )
}

fn opts(fault: StallFault) -> LsmOptions {
    LsmOptions {
        flush_threshold_bytes: 1024,
        compaction_trigger: 2,
        target_table_bytes: 4096,
        level_fanout: 2,
        background_maintenance: true,
        l0_slowdown_factor: 3,
        l0_stop_factor: 5,
        l0_slowdown_write_rate: 32 << 10,
        l0_min_write_rate: 4 << 10,
        stall_fault: fault,
        ..LsmOptions::default()
    }
}

fn run(seed: u64, fault: StallFault) -> (Vec<String>, usize) {
    let mut v = Vec::new();
    let wedge = Arc::new(AtomicBool::new(false));
    let _stall = SimStall {
        opts: opts(fault),
        wedge: wedge.clone(),
    }
    .install();
    let _ov = SimOverload::with_backlog(
        BacklogLimits {
            max_entries: 16,
            max_bytes: 0,
        },
        0,
    )
    .install();
    let r = catch_unwind(AssertUnwindSafe(|| {
        let mut cluster =
            SimCluster::new_with_lsm_engines(seed, &[crate::config::NodeRole::Both; 3], 3, None);
        let body = r#"{"TableName":"tab_s",
            "KeySchema":[{"AttributeName":"id","KeyType":"HASH"}],
            "AttributeDefinitions":[{"AttributeName":"id","AttributeType":"S"}]}"#;
        let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.CreateTable", body.as_bytes());
        assert_eq!(status, 200, "CreateTable: {resp}");
        cluster.run_for(Duration::from_secs(3));

        wedge.store(true, Ordering::Release);
        let (mut throttled, mut acked) = (0usize, Vec::new());
        let mut vv = Vec::new();
        let mut n = 0usize;
        for _round in 0..5 {
            let bodies: Vec<String> = (0..48)
                .map(|_| {
                    n += 1;
                    put_body(&format!("w{n}"))
                })
                .collect();
            let first = n - 47;
            let reqs: Vec<(u64, &str, &[u8])> = bodies
                .iter()
                .map(|b| (0u64, "DynamoDB_20120810.PutItem", b.as_bytes()))
                .collect();
            for (i, (status, body)) in cluster.dynamo_concurrent(&reqs).iter().enumerate() {
                match *status {
                    200 => acked.push(first + i),
                    400 if error_type(body).ends_with("ThrottlingException") => throttled += 1,
                    503 => {}
                    other => vv.push(format!("write {}: unexpected {other}: {body}", first + i)),
                }
            }
        }
        wedge.store(false, Ordering::Release);
        cluster.run_for(Duration::from_secs(40));
        for i in &acked {
            let body = format!(
                r#"{{"TableName":"tab_s","Key":{{"id":{{"S":"w{i}"}}}},"ConsistentRead":true}}"#
            );
            let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.GetItem", body.as_bytes());
            if status != 200 || !resp.contains(&format!("\"w{i}\"")) {
                vv.push(format!("acked write w{i} is not durable: {status} {resp}"));
            }
        }
        let (status, resp) =
            cluster.dynamo(0, "DynamoDB_20120810.PutItem", put_body("fresh").as_bytes());
        if status != 200 {
            vv.push(format!("fresh write after the stall: {status} {resp}"));
        }
        (vv, throttled)
    }));
    match r {
        Ok((vv, throttled)) => {
            v.extend(vv);
            (v, throttled)
        }
        Err(e) => {
            let m = e
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| e.downcast_ref::<&str>().map(|s| (*s).to_string()))
                .unwrap_or_default();
            v.push(format!("no-panic: the sim panicked: {m}"));
            (v, 0)
        }
    }
}

#[test]
fn sim_cluster_lsm_stall_wire_sees_only_documented_refusals_and_acked_writes_survive() {
    for seed in seeds() {
        let (v, throttled) = run(seed, StallFault::None);
        assert!(v.is_empty(), "seed={seed}:\n  {}", v.join("\n  "));
        eprintln!("LSMSTALL throttled={throttled}");
        assert!(
            throttled > 0,
            "seed={seed}: the burst never reached the propose bound"
        );
    }
}

#[test]
fn sim_cluster_lsm_stall_control_old_gate_takes_the_apply_task_down() {
    for seed in seeds() {
        let (v, _) = run(seed, StallFault::OldGate);
        assert!(
            v.iter().any(|m| m.starts_with("no-panic")),
            "seed={seed}: the old gate must violate no-panic, got {v:?}"
        );
    }
}
