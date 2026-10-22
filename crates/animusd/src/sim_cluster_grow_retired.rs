//! `SimCluster` regression for issue #1345 (the sim half of #454): a stream
//! grow walk (`ClientCtx::grow_stream`) whose later tablet is retired by an
//! in-place split before the walk reaches it.
//!
//! # The bug
//!
//! `grow_stream` snapshots a table's tablets, then calls `grow_stream_tablet`
//! on each in turn. #454's `classify_grow_response` folds the `"no such
//! tablet"` lookup miss into a skip, but under an in-place split that shape
//! only exists for the instant between the metadata flip and the group's
//! teardown. A tablet retired earlier has no leader anywhere: the per-tablet
//! call kept answering not-leader, so `forward_to_tablet_leader` chased it
//! for the whole ~35 s budget and returned `"forward to tablet leader: budget
//! exhausted chasing the leader ...; retry"`, a non-skip error.
//!
//! # The window (built deliberately)
//!
//! Two live tablets `A < B` (one earlier in-place split of the root). The
//! walk visits `A` first, then `B`:
//!
//! - `A`'s leader is moved off the walker `W` and partitioned from it, so
//!   `W`'s call for `A` cannot finish (the control leader is kept off `A`'s
//!   leader, so `W` still learns metadata);
//! - meanwhile `B` is split by its own leader and driven through cutover
//!   until `B` is gone from `W`'s metadata; only then is the partition
//!   healed and `A`'s call completes, so the walk reaches the retired `B`.
//!
//! Two shapes: `Local` (the walker `W` is also `B`'s old leader, so a stale
//! local group could answer) and `Remote` (`W` is neither tablet's leader).
//!
//! Oracles: the walk resolves well under the 35 s chase; no entry is a
//! non-skip error; every live tablet descends from the root through
//! `split_lineage`; and writes after the walk reach the stream (every key
//! appears in some shard).
//!
//! Replay one seed with `ANIMUS_SEED=<seed>`.

use std::collections::BTreeSet;
use std::time::Duration;

use animus_env::nid;
use animus_tablet::{TabletId, TabletState};

use super::sim_cluster::SimCluster;
use crate::ClientResponse;

const TABLE: &str = "growret";
const STEP: Duration = Duration::from_millis(50);
/// The product's chase budget is `SCHEMA_COMMIT_TIMEOUT + SPLIT_WAIT_BOUND`
/// (about 35 s); after the heal the walk must finish far inside it.
const WALK_BOUND: Duration = Duration::from_secs(15);

fn env_seed(default: u64) -> u64 {
    std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn live_tablets(cluster: &SimCluster, node: u64) -> Vec<(TabletId, TabletState)> {
    cluster
        .metadata(node)
        .tablets_for_table(TABLE)
        .map(|(id, t)| (*id, t.state))
        .collect()
}

fn leader_node(cluster: &SimCluster, tablet: TabletId) -> u64 {
    (0..cluster.node_count() as u64)
        .find(|&n| cluster.is_leader_local(n, tablet))
        .unwrap_or_else(|| panic!("seed={}: {tablet:?} has no leader", cluster.seed()))
}

/// Move `tablet`'s leadership onto `to` (test-only: matches the private
/// `CpGroup` enum), waiting until it holds.
fn move_leader(cluster: &mut SimCluster, tablet: TabletId, to: u64) {
    let seed = cluster.seed();
    for _ in 0..200 {
        if cluster.is_leader_local(to, tablet) {
            return;
        }
        let from = leader_node(cluster, tablet);
        let slot = cluster.spawn_detached(from, move |ctx| async move {
            let (_, group) = ctx
                .edge
                .hosted_groups()
                .into_iter()
                .find(|(t, _)| *t == tablet)
                .expect("leader hosts the tablet");
            match group {
                crate::CpGroup::Lsm(g) => g.transfer_leadership(nid(to)),
                crate::CpGroup::Mem(g) => g.transfer_leadership(nid(to)),
            }
        });
        for _ in 0..20 {
            cluster.run_for(Duration::from_millis(100));
            if cluster.is_leader_local(to, tablet) {
                break;
            }
        }
        drop(slot);
    }
    assert!(
        cluster.is_leader_local(to, tablet),
        "seed={seed}: could not move {tablet:?} leadership to node {to}"
    );
}

type Tick = std::sync::Arc<std::sync::Mutex<Option<Result<(), String>>>>;

/// One `inplace_split_driver_tick` per `Splitting` tablet `node` leads
/// (what `change_consumer_loop` does), without blocking the simulator.
fn spawn_driver_ticks(cluster: &SimCluster, node: u64) -> Tick {
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

/// Advance `STEP` at a time, keeping one driver tick per node in flight,
/// until `done` holds; panics with the seed past `budget`.
fn drive_until(
    cluster: &mut SimCluster,
    budget: Duration,
    what: &str,
    mut done: impl FnMut(&SimCluster) -> bool,
) -> Duration {
    let seed = cluster.seed();
    let mut ticks: Vec<Tick> = Vec::new();
    let mut elapsed = Duration::ZERO;
    loop {
        ticks.retain(|t| t.lock().expect("tick slot").is_none());
        if ticks.is_empty() {
            for n in 0..cluster.node_count() as u64 {
                ticks.push(spawn_driver_ticks(cluster, n));
            }
        }
        if done(cluster) {
            return elapsed;
        }
        assert!(
            elapsed < budget,
            "seed={seed}: {what} did not hold within {budget:?}"
        );
        cluster.run_for(STEP);
        elapsed += STEP;
    }
}

#[derive(Clone, Copy, Debug)]
enum Shape {
    /// The walker is also the victim's old leader.
    Local,
    /// The walker leads neither tablet.
    Remote,
}

fn run(seed: u64, shape: Shape) {
    let mut cluster = SimCluster::new(seed, 3, 3);
    let (status, body) = cluster.dynamo(
        0,
        "DynamoDB_20120810.CreateTable",
        format!(
            r#"{{"TableName":"{TABLE}",
                "KeySchema":[{{"AttributeName":"pk","KeyType":"HASH"}}],
                "AttributeDefinitions":[{{"AttributeName":"pk","AttributeType":"S"}}],
                "StreamSpecification":{{"StreamEnabled":true,"StreamViewType":"NEW_AND_OLD_IMAGES"}}}}"#
        )
        .as_bytes(),
    );
    assert_eq!(status, 200, "seed={seed} {shape:?}: CreateTable: {body}");
    let root = live_tablets(&cluster, 0)[0].0;
    let put = |cluster: &mut SimCluster, pk: &str| {
        let body = format!(r#"{{"TableName":"{TABLE}","Item":{{"pk":{{"S":"{pk}"}}}}}}"#);
        let (s, b) = cluster.dynamo(0, "DynamoDB_20120810.PutItem", body.as_bytes());
        assert_eq!(s, 200, "seed={seed} {shape:?}: put {pk}: {b}");
    };
    for i in 0..16 {
        put(&mut cluster, &format!("pre{i:03}"));
    }

    // One ordinary split of the root: two live tablets A < B.
    let first = cluster.grow_stream(0, TABLE);
    assert!(
        matches!(first[0].1, ClientResponse::PutOk),
        "seed={seed} {shape:?}: the setup split must succeed: {first:?}"
    );
    drive_until(
        &mut cluster,
        Duration::from_secs(120),
        "root retired into two Active children",
        |c| {
            let t = live_tablets(c, 0);
            t.len() == 2 && t.iter().all(|(_, s)| *s == TabletState::Active)
        },
    );
    cluster.run_for(Duration::from_secs(3));
    let live = live_tablets(&cluster, 0);
    let (a, b) = (live[0].0, live[1].0);
    assert!(a < b, "seed={seed} {shape:?}: walk order is by tablet id");

    // Roles. Local: the walker is B's old leader; Remote: it is not. A's
    // leader is never the walker.
    let lb0 = leader_node(&cluster, b);
    let walker = match shape {
        Shape::Local => lb0,
        Shape::Remote => (lb0 + 1) % 3,
    };
    if cluster.is_leader_local(walker, a) {
        move_leader(&mut cluster, a, (walker + 1) % 3);
    }
    let la = leader_node(&cluster, a);
    assert_ne!(
        la, walker,
        "seed={seed} {shape:?}: A must be remote to the walker"
    );
    // The control leader is the third node, reachable from the walker and
    // from A's leader alike (B's leader may be A's leader).
    let third = (0..3u64)
        .find(|&n| n != walker && n != la)
        .expect("third node");
    if cluster.control_leader_index() as u64 != third {
        cluster.transfer_control_leadership_to(third);
    }
    let lb = leader_node(&cluster, b);
    // The walker cannot reach A's leader: its call for A (the walk's first
    // tablet) is stuck until the test heals the link. B is split on fast
    // links in the meantime.
    cluster.partition(walker, la);
    cluster.run_for(Duration::from_millis(200));

    // The walk starts: it snapshots {A, B} (both Active) on its first poll.
    let walk = cluster.spawn_detached(walker, |ctx| async move { ctx.grow_stream(TABLE).await });
    cluster.run_for(Duration::from_millis(1));
    let split_b = cluster.spawn_detached(
        lb,
        move |ctx| async move { ctx.grow_stream_tablet(b).await },
    );
    let mut ticks: Vec<Tick> = Vec::new();
    let mut elapsed = Duration::ZERO;
    while cluster.metadata(walker).tablets.contains_key(&b) {
        ticks.retain(|t| t.lock().expect("tick slot").is_none());
        if ticks.is_empty() {
            for n in 0..3u64 {
                ticks.push(spawn_driver_ticks(&cluster, n));
            }
        }
        assert!(
            walk.lock().expect("walk slot").is_none(),
            "seed={seed} {shape:?}: the walk resolved while A was unreachable and B still live: \
             the window did not open ({:?})",
            walk.lock().unwrap()
        );
        assert!(
            elapsed < Duration::from_secs(60),
            "seed={seed} {shape:?}: B was not retired within 60s (split_b={:?})",
            split_b.lock().unwrap()
        );
        cluster.run_for(STEP);
        elapsed += STEP;
    }
    // B is retired from the walker's view while the walk is still on A.
    assert!(
        walk.lock().expect("walk slot").is_none(),
        "seed={seed} {shape:?}: the walk finished before B was retired"
    );
    cluster.heal_all();
    let mut since_heal = Duration::ZERO;
    while walk.lock().expect("walk slot").is_none() {
        ticks.retain(|t| t.lock().expect("tick slot").is_none());
        if ticks.is_empty() {
            for n in 0..3u64 {
                ticks.push(spawn_driver_ticks(&cluster, n));
            }
        }
        assert!(
            since_heal < WALK_BOUND,
            "seed={seed} {shape:?}: the walk took more than {WALK_BOUND:?} of virtual time after \
             the partition healed (B retired {elapsed:?} in); the retired tablet is being \
             chased instead of skipped"
        );
        cluster.run_for(STEP);
        since_heal += STEP;
    }
    eprintln!(
        "seed={seed} {shape:?}: walker={walker} la={la} lb={lb} b_retired_at={elapsed:?} \
         walk_after_heal={since_heal:?}"
    );
    let results = walk
        .lock()
        .unwrap()
        .clone()
        .expect("walk slot")
        .unwrap_or_else(|e| panic!("seed={seed} {shape:?}: grow_stream refused: {e}"));

    for (tablet, response) in &results {
        match response {
            ClientResponse::PutOk => {}
            ClientResponse::Error(e)
                if e == crate::STREAM_GROW_MID_SPLIT || e == crate::STREAM_GROW_NO_SPLIT_POINT => {}
            other => panic!(
                "seed={seed} {shape:?}: {tablet:?} came back as a non-skip result {other:?}; \
                 all results={results:?}"
            ),
        }
    }
    let b_entry = results.iter().find(|(t, _)| *t == b);
    assert!(
        matches!(b_entry, Some((_, ClientResponse::Error(e))) if e == crate::STREAM_GROW_MID_SPLIT),
        "seed={seed} {shape:?}: the retired tablet must be reported as a skip: {results:?}"
    );

    // Let every split converge.
    drive_until(
        &mut cluster,
        Duration::from_secs(180),
        "every tablet Active and every node agrees",
        |c| {
            (0..3u64).all(|n| {
                let t = live_tablets(c, n);
                !t.is_empty() && t.iter().all(|(_, s)| *s == TabletState::Active)
            }) && (1..3u64).all(|n| live_tablets(c, n) == live_tablets(c, 0))
        },
    );
    cluster.run_for(Duration::from_secs(3));

    // Lineage: every live tablet descends from the root via split_lineage.
    let meta = cluster.metadata(0);
    let live: Vec<TabletId> = live_tablets(&cluster, 0).iter().map(|(t, _)| *t).collect();
    assert!(
        !live.contains(&b) && live.len() >= 3,
        "seed={seed} {shape:?}: B must be retired and replaced by its children: {live:?}"
    );
    for t in &live {
        let mut cur = *t;
        let mut hops = 0;
        while cur != root {
            let l = meta.split_lineage.get(&cur).unwrap_or_else(|| {
                panic!("seed={seed} {shape:?}: {cur:?} (from {t:?}) has no split_lineage row")
            });
            cur = l.parent;
            hops += 1;
            assert!(hops < 16, "seed={seed} {shape:?}: lineage cycle at {t:?}");
        }
    }

    // Writes after the walk land in the stream: every key shows up.
    let keys: Vec<String> = (0..24).map(|i| format!("post{i:03}")).collect();
    for k in &keys {
        put(&mut cluster, k);
    }
    cluster.run_for(Duration::from_secs(2));
    let seen = stream_keys(&mut cluster, seed);
    for k in &keys {
        assert!(
            seen.contains(k),
            "seed={seed} {shape:?}: write {k} after the walk is missing from the stream \
             (saw {} keys)",
            seen.len()
        );
    }
}

/// Every `pk` the stream (all shards, TRIM_HORIZON, read to exhaustion)
/// carries.
fn stream_keys(cluster: &mut SimCluster, seed: u64) -> BTreeSet<String> {
    let label = cluster
        .metadata(0)
        .table_stream(TABLE)
        .map(|s| s.label.clone())
        .unwrap_or_else(|| panic!("seed={seed}: stream spec vanished"));
    let arn = format!("arn:aws:dynamodb:animus:0:table/{TABLE}/stream/{label}");
    let call = |cluster: &mut SimCluster, target: &str, body: String| {
        let (s, r) = cluster.dynamo_streams(0, target, body.as_bytes());
        assert_eq!(s, 200, "seed={seed}: {target}: {r}");
        serde_json::from_str::<serde_json::Value>(&r).expect("json")
    };
    let d = call(
        cluster,
        "DynamoDBStreams_20120810.DescribeStream",
        format!(r#"{{"StreamArn":"{arn}"}}"#),
    );
    let shards: Vec<String> = d["StreamDescription"]["Shards"]
        .as_array()
        .expect("shards")
        .iter()
        .map(|s| s["ShardId"].as_str().expect("id").to_owned())
        .collect();
    let mut out = BTreeSet::new();
    for shard in shards {
        let it = call(
            cluster,
            "DynamoDBStreams_20120810.GetShardIterator",
            format!(
                r#"{{"StreamArn":"{arn}","ShardId":"{shard}","ShardIteratorType":"TRIM_HORIZON"}}"#
            ),
        );
        let mut token = it["ShardIterator"].as_str().map(str::to_owned);
        for _ in 0..64 {
            let Some(t) = token.take() else { break };
            let r = call(
                cluster,
                "DynamoDBStreams_20120810.GetRecords",
                format!(r#"{{"ShardIterator":"{t}"}}"#),
            );
            let recs = r["Records"].as_array().cloned().unwrap_or_default();
            for rec in &recs {
                if let Some(pk) = rec["dynamodb"]["Keys"]["pk"]["S"].as_str() {
                    out.insert(pk.to_owned());
                }
            }
            if recs.is_empty() {
                break;
            }
            token = r["NextShardIterator"].as_str().map(str::to_owned);
        }
    }
    out
}

#[test]
fn a_tablet_retired_mid_walk_is_skipped_on_the_victims_old_leader() {
    run(env_seed(0x1345_0001), Shape::Local);
}

#[test]
fn a_tablet_retired_mid_walk_is_skipped_from_a_non_leader_walker() {
    run(env_seed(0x1345_0002), Shape::Remote);
}

#[test]
fn a_tablet_retired_mid_walk_is_skipped_over_seeds() {
    if std::env::var("ANIMUS_SEED").is_ok() {
        return;
    }
    for i in 0..4 {
        run(0x1345_0100 + i, Shape::Local);
        run(0x1345_0200 + i, Shape::Remote);
    }
}
