//! ADR 0080 Tier 2: overload admission over `SimCluster` and the DynamoDB wire.
//!
//! Three cells, each with a seed (`ANIMUS_SEED` replays one) and a negative
//! control that must fail its oracle:
//!
//! - **leaderless fast-fail** (ADR 0080 2b; Tier 1 cells 5 and 6): a node cut
//!   off from a group's other replicas refuses requests for it with a `503
//!   ServiceUnavailable` / `NoLeader:` once the group has been leaderless for
//!   `leaderless_wait`, *at once* (no slot held, no park to `CLIENT_TIMEOUT`),
//!   healthy groups keep their rate, and the first request after the leader is
//!   reachable again is served. Control (d): fast-fail off makes every request
//!   park for the whole client budget.
//! - **the `Busy` vocabulary** (ADR 0080 1 and the refusal table): an
//!   open-loop burst against a tiny propose bound sees only `200`, `400
//!   ThrottlingException` or `503`, never a `500`, and every `200` is durable.
//!   Control: the bound off sees no refusal at all (the oracle that a refusal
//!   happened bites).
//!
//! Metric lines are printed `C17`-style (`OVERLOAD metric=value ...`).

use super::sim_support::env_seed;
use std::time::Duration;

use super::sim_cluster::{SimCluster, SimOverload};
use animus_cp_data::admission::BacklogLimits;

fn create_table(cluster: &mut SimCluster, node: u64, table: &str) {
    let body = format!(
        r#"{{"TableName":"{table}",
            "KeySchema":[{{"AttributeName":"id","KeyType":"HASH"}}],
            "AttributeDefinitions":[{{"AttributeName":"id","AttributeType":"S"}}]}}"#
    );
    let (status, resp) = cluster.dynamo(node, "DynamoDB_20120810.CreateTable", body.as_bytes());
    assert_eq!(status, 200, "CreateTable({table}) failed: {resp}");
}

fn put_body(table: &str, id: &str) -> String {
    format!(r#"{{"TableName":"{table}","Item":{{"id":{{"S":"{id}"}},"v":{{"S":"x"}}}}}}"#)
}

fn error_type(body: &str) -> String {
    serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|j| j["__type"].as_str().map(str::to_owned))
        .unwrap_or_default()
}

const STEP: Duration = Duration::from_millis(50);
const MAX_STEPS: usize = 400; // 20 s of virtual time

/// The leaderless cell. Returns the violations; `fast_fail` is the control
/// switch (`false` = a 60 s wait, i.e. the old behaviour).
fn run_leaderless(seed: u64, fast_fail: bool) -> Vec<String> {
    let mut v = Vec::new();
    let _guard = SimOverload {
        leaderless_wait: if fast_fail {
            Duration::from_secs(2)
        } else {
            Duration::from_secs(60)
        },
        ..SimOverload::with_backlog(BacklogLimits::off(), 0)
    }
    .install();
    let mut cluster = SimCluster::new(seed, 3, 3);
    create_table(&mut cluster, 0, "tab_a");
    create_table(&mut cluster, 0, "tab_b");
    cluster.run_for(Duration::from_secs(3));

    // Cut node 2 off from the other replicas: its own view of every group is
    // leaderless (the groups keep their majority on {0, 1}).
    let victim = 2u64;
    cluster.partition(victim, 0);
    cluster.partition(victim, 1);
    cluster.run_for(Duration::from_millis(800));

    // The first request waits at most `leaderless_wait` (plus one poll) ...
    let (first, resp) = cluster.dynamo_timed(
        victim,
        "DynamoDB_20120810.PutItem",
        put_body("tab_a", "k0").as_bytes(),
        STEP,
        MAX_STEPS,
    );
    let (status, body) = resp.unwrap_or((0, "unresolved".into()));
    eprintln!(
        "OVERLOAD leaderless first_ms={} status={status}",
        first.as_millis()
    );
    if fast_fail {
        if status != 503 || !body.contains("NoLeader") {
            v.push(format!("first: expected 503 NoLeader, got {status} {body}"));
        }
        if first > Duration::from_millis(2_600) {
            v.push(format!(
                "first: refused after {first:?}, bound is leaderless_wait + one RTT"
            ));
        }
    }

    // ... and every later request is refused at once, holding no slot.
    let mut slowest = Duration::ZERO;
    for i in 0..20 {
        let (took, resp) = cluster.dynamo_timed(
            victim,
            "DynamoDB_20120810.PutItem",
            put_body("tab_a", &format!("k{}", i + 1)).as_bytes(),
            STEP,
            MAX_STEPS,
        );
        slowest = slowest.max(took);
        let (status, body) = resp.unwrap_or((0, "unresolved".into()));
        if fast_fail && (status != 503 || !body.contains("NoLeader")) {
            v.push(format!(
                "storm {i}: expected 503 NoLeader, got {status} {body}"
            ));
        }
        if fast_fail && took > Duration::from_millis(300) {
            v.push(format!(
                "storm {i}: refused after {took:?}, expected at once"
            ));
        }
    }
    eprintln!(
        "OVERLOAD leaderless storm_slowest_ms={}",
        slowest.as_millis()
    );
    if !fast_fail && slowest < Duration::from_secs(5) {
        v.push(format!(
            "fast-fail-off: the slowest request returned in {slowest:?}, expected a park of seconds"
        ));
    }

    // Healthy groups keep their rate: node 0 still serves table `b`.
    if fast_fail {
        for i in 0..10 {
            let (took, resp) = cluster.dynamo_timed(
                0,
                "DynamoDB_20120810.PutItem",
                put_body("tab_b", &format!("h{i}")).as_bytes(),
                STEP,
                MAX_STEPS,
            );
            let (status, body) = resp.unwrap_or((0, "unresolved".into()));
            if status != 200 || took > Duration::from_secs(2) {
                v.push(format!("healthy {i}: {status} in {took:?}: {body}"));
            }
        }
    }

    // Recovery (Tier 1 cell 6): the first request after the leader is
    // reachable again is served.
    cluster.heal_all();
    cluster.run_for(Duration::from_secs(4));
    if fast_fail {
        let (took, resp) = cluster.dynamo_timed(
            victim,
            "DynamoDB_20120810.PutItem",
            put_body("tab_a", "after").as_bytes(),
            STEP,
            MAX_STEPS,
        );
        let (status, body) = resp.unwrap_or((0, "unresolved".into()));
        if status != 200 || took > Duration::from_secs(3) {
            v.push(format!("recovery: {status} in {took:?}: {body}"));
        }
    }
    v
}

#[test]
fn leaderless_group_is_refused_at_once_healthy_groups_unaffected_and_it_recovers() {
    for seed in [env_seed(0x0ad2_0001), env_seed(0x0ad2_0002)] {
        let v = run_leaderless(seed, true);
        assert!(v.is_empty(), "seed={seed}:\n  {}", v.join("\n  "));
    }
}

#[test]
fn control_d_without_the_fast_fail_every_request_parks_for_the_client_budget() {
    let seed = env_seed(0x0ad2_0003);
    let v = run_leaderless(seed, false);
    assert!(
        v.is_empty(),
        "seed={seed}: the control's own oracle failed:\n  {}",
        v.join("\n  ")
    );
}

/// An open-loop burst against a tiny propose bound: only the documented
/// refusal codes ever appear, and every acknowledged write is durable.
fn run_busy_vocabulary(seed: u64, bound_on: bool) -> (Vec<String>, usize) {
    let mut v = Vec::new();
    let backlog = if bound_on {
        BacklogLimits {
            max_entries: 2,
            max_bytes: 0,
        }
    } else {
        BacklogLimits::off()
    };
    let _guard = SimOverload::with_backlog(backlog, 0).install();
    let mut cluster = SimCluster::new(seed, 3, 3);
    create_table(&mut cluster, 0, "tab_t");
    cluster.run_for(Duration::from_secs(3));

    let bodies: Vec<String> = (0..96)
        .map(|i| put_body("tab_t", &format!("w{i}")))
        .collect();
    let reqs: Vec<(u64, &str, &[u8])> = bodies
        .iter()
        .map(|b| (0u64, "DynamoDB_20120810.PutItem", b.as_bytes()))
        .collect();
    let results = cluster.dynamo_concurrent(&reqs);
    let (mut ok, mut throttled, mut unavailable) = (0usize, 0usize, 0usize);
    let mut acked = Vec::new();
    for (i, (status, body)) in results.iter().enumerate() {
        match *status {
            200 => {
                ok += 1;
                acked.push(i);
            }
            400 if error_type(body).ends_with("ThrottlingException") => throttled += 1,
            503 => unavailable += 1,
            other => v.push(format!("request {i}: unexpected {other}: {body}")),
        }
    }
    eprintln!(
        "OVERLOAD busy bound_on={bound_on} ok={ok} throttled={throttled} unavailable={unavailable}"
    );
    for i in acked {
        let body = format!(
            r#"{{"TableName":"tab_t","Key":{{"id":{{"S":"w{i}"}}}},"ConsistentRead":true}}"#
        );
        let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.GetItem", body.as_bytes());
        if status != 200 || !resp.contains(&format!("\"w{i}\"")) {
            v.push(format!("acked write w{i} is not durable: {status} {resp}"));
        }
    }
    (v, throttled)
}

#[test]
fn a_burst_against_a_tiny_propose_bound_sees_only_documented_refusals_and_acked_writes_survive() {
    let seed = env_seed(0x0ad2_0010);
    let (v, throttled) = run_busy_vocabulary(seed, true);
    assert!(v.is_empty(), "seed={seed}:\n  {}", v.join("\n  "));
    assert!(
        throttled > 0,
        "seed={seed}: the burst never hit the propose bound (no ThrottlingException)"
    );
}

#[test]
fn control_without_the_bound_the_burst_is_never_refused() {
    let seed = env_seed(0x0ad2_0011);
    let (_, throttled) = run_busy_vocabulary(seed, false);
    assert_eq!(
        throttled, 0,
        "seed={seed}: nothing may be refused with the bound off"
    );
}
