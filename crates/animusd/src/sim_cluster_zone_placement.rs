//! `sim_cluster_zone_placement` — G-01 stage G-a: a seed-reproducible
//! fault-injecting placement corpus over [`SimCluster`] with zone-labelled
//! members.
//!
//! Six combined nodes, two per zone (`topology.kubernetes.io/zone` = `a`/`b`/
//! `c`, node ids interleaved so the first three ids share a zone — a naive
//! "first RF by id" pick would put two replicas in zone `a`), RF 3. Per seed:
//!
//! 1. wire `CreateTable` x2 (the real `ClientCtx::provision_tablet`, so the
//!    zone-spread policy and the zone-aware initial replica pick are what is
//!    under test, not a hand-seeded tablet);
//! 2. **placement**: converged-or-timeout poll until every tablet's replica
//!    set spans 3 distinct zones and its recorded policy carries the zone
//!    spread; acked wire `PutItem`s on both tables;
//! 3. **zone loss**: crash both nodes of a seed-chosen zone (a majority of
//!    each tablet's replicas and of the 6-voter control group survive: 4/6,
//!    2/3);
//! 4. writes and `ConsistentRead: true` reads keep working through the
//!    survivors (converged-or-timeout, never one-shot), **every acked write
//!    — before and after the loss — reads back**, and the repair pass
//!    re-converges every tablet to 3 live replicas (doubling up in a
//!    surviving zone: the spread is best-effort by design).
//!
//! Depth: `ANIMUS_ZONE_PLACEMENT_SEEDS=K` (default 1) seeds; `ANIMUS_SEED=<s>`
//! replays exactly one. Every assertion message carries the seed.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use animus_env::nid;
use animus_placement::ZONE_LABEL;

use super::sim_cluster::SimCluster;

const ZONES: [&str; 3] = ["a", "b", "c"];
const TABLES: [&str; 2] = ["zp0", "zp1"];

fn seeds() -> Vec<u64> {
    if let Some(s) = std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
    {
        return vec![s];
    }
    let k = animus_test::corpus::seeds_from_env("ANIMUS_ZONE_PLACEMENT_SEEDS") as u64;
    (0..k).map(|i| 7_100 + i).collect()
}

fn zone_of_node(n: u64) -> &'static str {
    ZONES[(n / 2) as usize]
}

fn labels() -> Vec<BTreeMap<String, String>> {
    (0..6u64)
        .map(|n| BTreeMap::from([(ZONE_LABEL.to_owned(), zone_of_node(n).to_owned())]))
        .collect()
}

/// Converged-or-timeout over virtual time: `Ok` once `done` holds, else the
/// last observed `Err` after `budget`.
fn poll(
    cluster: &mut SimCluster,
    budget: Duration,
    mut done: impl FnMut(&SimCluster) -> Result<(), String>,
) -> Result<(), String> {
    let mut waited = Duration::ZERO;
    loop {
        match done(cluster) {
            Ok(()) => return Ok(()),
            Err(e) if waited >= budget => return Err(e),
            Err(_) => {
                cluster.run_for(Duration::from_secs(1));
                waited += Duration::from_secs(1);
            }
        }
    }
}

fn zones_of(replicas: &[animus_env::NodeId]) -> BTreeSet<&'static str> {
    replicas
        .iter()
        .map(|r| {
            let n: u64 = r
                .to_string()
                .trim_start_matches('n')
                .parse()
                .expect("nN id");
            zone_of_node(n)
        })
        .collect()
}

fn put(cluster: &mut SimCluster, node: u64, table: &str, k: &str, v: &str) -> bool {
    let body =
        format!(r#"{{"TableName":"{table}","Item":{{"pk":{{"S":"{k}"}},"v":{{"S":"{v}"}}}}}}"#);
    cluster
        .dynamo(node, "DynamoDB_20120810.PutItem", body.as_bytes())
        .0
        == 200
}

fn get(cluster: &mut SimCluster, node: u64, table: &str, k: &str) -> Option<String> {
    let body =
        format!(r#"{{"TableName":"{table}","Key":{{"pk":{{"S":"{k}"}}}},"ConsistentRead":true}}"#);
    let (status, resp) = cluster.dynamo(node, "DynamoDB_20120810.GetItem", body.as_bytes());
    if status != 200 {
        return None;
    }
    let v: serde_json::Value = serde_json::from_str(&resp).ok()?;
    v["Item"]["v"]["S"].as_str().map(str::to_owned)
}

fn run_seed(seed: u64) {
    let mut cluster = SimCluster::new_with_node_labels(seed, 3, labels());
    for t in TABLES {
        let body = format!(
            r#"{{"TableName":"{t}",
                "KeySchema":[{{"AttributeName":"pk","KeyType":"HASH"}}],
                "AttributeDefinitions":[{{"AttributeName":"pk","AttributeType":"S"}}]}}"#
        );
        let (status, resp) = cluster.dynamo(0, "DynamoDB_20120810.CreateTable", body.as_bytes());
        assert_eq!(status, 200, "seed={seed}: CreateTable {t}: {resp}");
    }

    // 2. Placement: every tablet spans 3 distinct zones, policy is the spread.
    poll(&mut cluster, Duration::from_secs(60), |c| {
        let meta = c.metadata(1);
        for t in TABLES {
            let Some((id, tab)) = meta.tablets_for_table(t).next() else {
                return Err(format!("{t}: no tablet yet"));
            };
            let zones = zones_of(&tab.replicas);
            if tab.replicas.len() != 3 || zones.len() != 3 {
                return Err(format!("{t}: replicas {:?} zones {zones:?}", tab.replicas));
            }
            match meta.policies.get(id).and_then(|p| p.spread.as_ref()) {
                Some(sp) if sp.domain == ZONE_LABEL && !sp.strict => {}
                other => return Err(format!("{t}: policy spread {other:?}")),
            }
        }
        Ok(())
    })
    .unwrap_or_else(|e| panic!("seed={seed}: placement did not converge to 3 zones: {e}"));

    // Acked writes before the fault.
    let mut acked: Vec<(&str, String, String)> = Vec::new();
    for t in TABLES {
        for i in 0..6 {
            let (k, v) = (format!("k{i}"), format!("pre-{t}-{i}"));
            let mut ok = false;
            for _ in 0..5 {
                if put(&mut cluster, (i % 6) as u64, t, &k, &v) {
                    ok = true;
                    break;
                }
            }
            assert!(ok, "seed={seed}: pre-fault put {t}/{k} never acked");
            acked.push((t, k, v));
        }
    }

    // 3. Kill a whole zone.
    let dead_zone = ZONES[(seed % 3) as usize];
    let dead: Vec<u64> = (0..6).filter(|n| zone_of_node(*n) == dead_zone).collect();
    for n in &dead {
        cluster.crash(*n);
    }
    let live: Vec<u64> = (0..6).filter(|n| !dead.contains(n)).collect();

    // 4. Writes/reads keep working through survivors; acked writes survive.
    let mut post_acked = 0usize;
    for t in TABLES {
        for i in 0..3 {
            let (k, v) = (format!("post{i}"), format!("post-{t}-{i}"));
            let mut ok = false;
            for attempt in 0..30 {
                if put(&mut cluster, live[(i + attempt) % live.len()], t, &k, &v) {
                    ok = true;
                    break;
                }
            }
            assert!(
                ok,
                "seed={seed}: post-fault put {t}/{k} never acked with zone {dead_zone} down"
            );
            acked.push((t, k, v));
            post_acked += 1;
        }
    }
    assert!(post_acked > 0, "seed={seed}: vacuous");
    for (t, k, v) in &acked {
        let mut got = None;
        for attempt in 0..30 {
            got = get(&mut cluster, live[attempt % live.len()], t, k);
            if got.is_some() {
                break;
            }
        }
        assert_eq!(
            got.as_deref(),
            Some(v.as_str()),
            "seed={seed}: acked write {t}/{k} lost after zone {dead_zone} died"
        );
    }

    // The repair pass re-converges every tablet to 3 live replicas.
    let live_ids: BTreeSet<_> = live.iter().map(|n| nid(*n)).collect();
    poll(&mut cluster, Duration::from_secs(180), |c| {
        let meta = c.metadata(live[0]);
        for t in TABLES {
            let (_, tab) = meta
                .tablets_for_table(t)
                .next()
                .ok_or_else(|| format!("{t}: tablet vanished"))?;
            if tab.replicas.len() != 3 || !tab.replicas.iter().all(|r| live_ids.contains(r)) {
                return Err(format!("{t}: replicas {:?}", tab.replicas));
            }
        }
        Ok(())
    })
    .unwrap_or_else(|e| {
        panic!("seed={seed}: tablets did not re-converge to 3 live replicas after zone {dead_zone} loss: {e}")
    });
}

#[test]
fn zone_placement_corpus() {
    for seed in seeds() {
        run_seed(seed);
    }
}
