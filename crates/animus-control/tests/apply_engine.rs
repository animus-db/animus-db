//! ADR 0038 PR3's differential oracle: the apply task's published `Metadata`
//! cache always agrees with an independent scan-and-rebuild of the same
//! node's system-keyspace engine (`mirror::rebuild_metadata_from_engine`) —
//! the property that makes the cutover safe (the engine, not any in-core
//! field, is now the durable source of truth). The successor of PR2's
//! shadow-mode `mirror_engine.rs`: there is no more "shadow" side to diff
//! against a separate "real" side — `cache` *is* the real side, and this
//! file asserts it never diverges from its own engine, including across a
//! genuine crash + restart (proving the restart-recovery contract: rebuild
//! from the engine's `_applied_index` watermark, replay only the log tail).

use std::collections::BTreeMap;
use std::time::Duration;

use animus_control::{MetaCommand, NodeAddrs, NodeStatus, RaftNode, mirror};
use animus_env::nid;
use animus_placement::PlacementPolicy;
use animus_sim::{SimEnv, Simulator};
use animus_storage::MemoryEngine;
use animus_tablet::{Epoch, KeyRange, TabletId, TabletState};

const NODES: [u64; 3] = [0, 1, 2];

fn upsert(node: u64) -> MetaCommand {
    MetaCommand::UpsertMember {
        node: nid(node),
        labels: BTreeMap::new(),
        status: NodeStatus::Active,
    }
}

/// The current epoch of a tablet, read fresh off `metadata()` — the same
/// just-in-time read a real proposer's confirm loop does (see
/// `animusd::ClientCtx::trigger_split`) before issuing an epoch-CAS'd
/// command, rather than assuming a value computed earlier in the scenario
/// is still current (placement repair/rebalance can bump a tablet's epoch
/// out from under a hardcoded expectation — see the issue #539 note below).
fn current_epoch(nodes: &[RaftNode<SimEnv>], leader: usize, tablet: TabletId) -> Epoch {
    nodes[leader]
        .metadata()
        .tablets
        .get(&tablet)
        .unwrap_or_else(|| panic!("tablet {tablet:?} should exist"))
        .epoch
}

fn unique_leader(nodes: &[RaftNode<SimEnv>], seed: u64) -> usize {
    let ls: Vec<usize> = (0..nodes.len()).filter(|&i| nodes[i].is_leader()).collect();
    assert_eq!(ls.len(), 1, "expected one leader, got {ls:?} (seed={seed})");
    ls[0]
}

/// Assert every node's published cache agrees with its own engine's rebuild
/// — the core invariant this whole PR is built on.
async fn assert_cache_matches_engine(
    nodes: &[RaftNode<SimEnv>],
    engines: &[MemoryEngine],
    seed: u64,
    label: &str,
) {
    for (i, (node, engine)) in nodes.iter().zip(engines).enumerate() {
        let rebuilt = mirror::rebuild_metadata_from_engine(engine)
            .await
            .expect("engine scan");
        assert_eq!(
            rebuilt,
            node.metadata(),
            "seed={seed} node {i} ({label}): cache diverged from its own engine's rebuild"
        );
    }
}

/// A seed-swept mixed scenario — membership, tablet create/split/merge,
/// schema DDL, node-id-allocation — asserting the cache/engine
/// agreement holds after every step, then a crash-and-restart of one node,
/// asserting it holds again once the restarted node reconverges.
#[test]
fn cache_matches_engine_through_a_mixed_scenario_and_a_restart() {
    for seed in [0x5CE0_0001u64, 0x5CE0_0002, 0x5CE0_0003] {
        run_scenario(seed);
    }
}

fn run_scenario(seed: u64) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    rt.block_on(async move {
        let mut sim = Simulator::new(seed);
        let engines: Vec<MemoryEngine> = NODES.iter().map(|_| MemoryEngine::new()).collect();
        let mut nodes: Vec<RaftNode<SimEnv>> = NODES
            .iter()
            .map(|&id| {
                RaftNode::start(
                    sim.env(nid(id)),
                    NODES.iter().copied().map(nid).collect(),
                    engines[id as usize].clone(),
                )
            })
            .collect();
        sim.run_for(Duration::from_secs(2));
        let leader = unique_leader(&nodes, seed);

        // Membership + a table's tablet lifecycle + schema + a
        // cluster-allocated node id, one after another.
        nodes[leader].propose(upsert(10));
        nodes[leader].propose(upsert(11));
        nodes[leader].propose(MetaCommand::CreateTableSchema {
            table: "orders".to_string(),
            schema: animus_control::TableSchema::simple("id", animus_control::ColumnType::String),
        });
        nodes[leader].propose(MetaCommand::CreateTablet {
            tablet: TabletId(1),
            table: Some("orders".to_string()),
            range: KeyRange::whole(),
            replicas: vec![nid(210), nid(211)],
        });
        nodes[leader].propose(MetaCommand::SetTabletPolicy {
            tablet: TabletId(1),
            policy: Some(PlacementPolicy::simple("p", 2)),
        });
        nodes[leader].propose(MetaCommand::RegisterNode {
            node: nid(900),
            addrs: NodeAddrs {
                internal: "127.0.0.1:9900".to_string(),
                client: "127.0.0.1:9000".to_string(),
                admin: "127.0.0.1:9500".to_string(),
                intra: "127.0.0.1:9600".to_string(),
                role: "combined".to_string(),
            },
            labels: BTreeMap::new(),
        });
        sim.run_for(Duration::from_secs(2));
        assert_cache_matches_engine(&nodes, &engines, seed, "after initial commands").await;

        // ADR 0042/0043: enable a stream and seal its first shard —
        // proves `SealStreamShard`'s catalog row (and its own mirror arm)
        // replicates and durably survives on every node's engine, like any
        // other command in this scenario.
        nodes[leader].propose(MetaCommand::SetTableStream {
            table: "orders".to_string(),
            spec: Some(animus_control::StreamSpec {
                view_type: animus_control::StreamViewType::NewAndOldImages,
                label: "seed-scenario-L1".to_string(),
            }),
        });
        nodes[leader].propose(MetaCommand::SealStreamShard {
            table: "orders".to_string(),
            label: "seed-scenario-L1".to_string(),
            tablet: TabletId(1),
            epoch: 0,
            view_type: animus_control::StreamViewType::NewAndOldImages,
            hlc_range: (0, 100),
            count: 1,
            seal_wall_ms: 1_700_000_000_000,
            replicas: vec![nid(210), nid(211)],
            object_id: "orders/seed-scenario-L1/1/0/test".to_owned(),
        });
        sim.run_for(Duration::from_secs(1));
        assert_cache_matches_engine(&nodes, &engines, seed, "after SealStreamShard").await;
        // Positive assertion: the seal actually landed a catalog row, not
        // just left cache/engine agreeing on an unchanged empty map (a
        // rejected/no-op `SealStreamShard` — e.g. an epoch-chain gap or a
        // stale label — would pass the differential check just as
        // trivially as a genuine apply would).
        assert!(
            nodes[leader]
                .metadata()
                .stream_shards
                .contains_key(&(TabletId(1), 0)),
            "seed={seed}: first SealStreamShard should have landed a (tablet 1, epoch 0) row"
        );

        // In-place split (ADR 0058 Train 2 rung 3): `BeginSplitInPlace`, then
        // cutover — the child pair partitions the range, the parent
        // retires, lineage freezes.
        //
        // issue #539 (was: NOTE, copy-split deletion stack, layer 1): a
        // hardcoded `expected_epoch: Epoch::INITIAL` here used to be stale
        // by the time it landed — `SetTabletPolicy`'s RF=2 policy treats
        // tablet 1's replicas (`nid(210)`/`nid(211)`, chosen purely as
        // stream-shard-row filler above and never `RegisterNode`d/`UpsertMember`-activated) as
        // policy-violating, since only `nid(10)`/`nid(11)` are `Active`
        // members; the leader's own placement-repair reconcile loop
        // (`Metadata::reconcile`) swaps tablet 1's replicas onto those two
        // `Active` members within the very first `run_for` above, bumping
        // its epoch from `INITIAL` to `INITIAL.next().next()` well before
        // this call ever proposes anything. Every `BeginSplitInPlace`/
        // `CutoverSplit` call in this scenario now reads the tablet's
        // CURRENT epoch off `metadata()` immediately before proposing —
        // the same just-in-time pattern a real proposer's confirm loop
        // uses (`animusd::ClientCtx::trigger_split`) — instead of assuming
        // a value computed earlier in the scenario (or a bare
        // `Epoch::INITIAL`) is still current. See
        // `docs/engineering-lessons.md`'s entry on this for the full
        // incident: every downstream command in this round used to
        // silently no-op right along with the first rejection, and the
        // differential oracle below never noticed because a rejected
        // command changes neither side of the cache/engine comparison.
        // F11 (ADR 0042 §14): a streamed table's split key must be exactly
        // `TOKEN_BYTES` (8) long — `orders` already has a stream enabled
        // (`SetTableStream` above), so the pre-fix single-byte `vec![128u8]`
        // was ALSO rejected on this seatbelt, a second, independent
        // no-op cause the epoch fix alone doesn't uncover until the epoch
        // check stops shadowing it (the epoch-CAS is evaluated first in
        // `BeginSplitInPlace`'s apply arm, so the original stale-epoch
        // proposal never even reached this check).
        let split_key = vec![0x80, 0, 0, 0, 0, 0, 0, 0];
        let tablet1_epoch = current_epoch(&nodes, leader, TabletId(1));
        nodes[leader].propose(MetaCommand::BeginSplitInPlace {
            parent: TabletId(1),
            expected_epoch: tablet1_epoch,
            split_key,
            children: [
                (TabletId(2), vec![nid(210), nid(211)]),
                (TabletId(3), vec![nid(210), nid(211)]),
            ],
        });
        sim.run_for(Duration::from_secs(1));
        assert_cache_matches_engine(&nodes, &engines, seed, "after begin-split-in-place").await;
        // Positive assertion: `BeginSplitInPlace` actually applied — the
        // parent flipped to `Splitting`, its epoch advanced by one, and it
        // carries an intent naming exactly the two proposed children (never
        // a silently-rejected epoch-CAS no-op).
        {
            let meta = nodes[leader].metadata();
            let parent = meta.tablets.get(&TabletId(1)).unwrap_or_else(|| {
                panic!("seed={seed}: parent tablet 1 should still be present while Splitting")
            });
            assert_eq!(
                parent.state,
                TabletState::Splitting,
                "seed={seed}: BeginSplitInPlace should have flipped tablet 1 to Splitting"
            );
            assert_eq!(
                parent.epoch,
                tablet1_epoch.next(),
                "seed={seed}: BeginSplitInPlace should have bumped tablet 1's epoch"
            );
            let intent = parent.inplace_split.as_ref().unwrap_or_else(|| {
                panic!("seed={seed}: BeginSplitInPlace should have recorded a split intent")
            });
            assert_eq!(intent.children[0].id, TabletId(2));
            assert_eq!(intent.children[1].id, TabletId(3));
        }

        // The retired-to-be parent seals its next epoch (its final shard),
        // then cutover retires it; the child's own epoch-0 seal follows.
        nodes[leader].propose(MetaCommand::SealStreamShard {
            table: "orders".to_string(),
            label: "seed-scenario-L1".to_string(),
            tablet: TabletId(1),
            epoch: 1,
            view_type: animus_control::StreamViewType::NewAndOldImages,
            hlc_range: (100, 200),
            count: 1,
            seal_wall_ms: 1_700_000_000_001,
            replicas: vec![nid(210), nid(211)],
            object_id: "orders/seed-scenario-L1/1/1/test".to_owned(),
        });
        let tablet1_splitting_epoch = current_epoch(&nodes, leader, TabletId(1));
        nodes[leader].propose(MetaCommand::CutoverSplit {
            parent: TabletId(1),
            expected_epoch: tablet1_splitting_epoch,
            cutover_wall_ms: 1_700_000_000_010,
        });
        nodes[leader].propose(MetaCommand::SealStreamShard {
            table: "orders".to_string(),
            label: "seed-scenario-L1".to_string(),
            tablet: TabletId(2),
            epoch: 0,
            view_type: animus_control::StreamViewType::NewAndOldImages,
            hlc_range: (200, 300),
            count: 1,
            seal_wall_ms: 1_700_000_000_002,
            replicas: vec![nid(210), nid(211)],
            object_id: "orders/seed-scenario-L1/2/0/test".to_owned(),
        });
        sim.run_for(Duration::from_secs(1));
        assert_cache_matches_engine(&nodes, &engines, seed, "after split-child seal").await;
        // Positive assertion: `CutoverSplit` actually applied — the parent
        // is gone, both children are `Active` with `split_lineage` rows
        // naming it, and the child's own seal landed too.
        {
            let meta = nodes[leader].metadata();
            assert!(
                !meta.tablets.contains_key(&TabletId(1)),
                "seed={seed}: CutoverSplit should have retired parent tablet 1"
            );
            for child in [TabletId(2), TabletId(3)] {
                let t = meta.tablets.get(&child).unwrap_or_else(|| {
                    panic!("seed={seed}: child {child:?} should exist after CutoverSplit")
                });
                assert_eq!(t.state, TabletState::Active);
                let lineage = meta.split_lineage.get(&child).unwrap_or_else(|| {
                    panic!("seed={seed}: split_lineage entry missing for {child:?}")
                });
                assert_eq!(lineage.parent, TabletId(1));
            }
            assert!(
                meta.stream_shards.contains_key(&(TabletId(2), 0)),
                "seed={seed}: child tablet 2's own seal should have landed a row"
            );
        }

        // A full in-place split round on the split child — proves
        // `BeginSplitInPlace`'s mirror arm (parent state flip + recorded
        // intent + advanced allocator counter, no `Building` rows) and
        // `CutoverSplit`'s in-place branch (children activated straight
        // from the intent, parent tablet+policy DELETED, lineage rows
        // written) replicate and durably survive like every other command.
        //
        // Tablet 2 is the LEFT child of the first split (range `[start,
        // 0x80..)`, `intent.children[0]` above) — its own split key must
        // fall strictly inside THAT range, not the original whole-ring
        // range, so `0x40..` (not the pre-fix `0xC0..`, which lies past
        // tablet 2's own end and was silently swallowed by the same
        // epoch-CAS no-op along with everything else in this round).
        let tablet2_epoch = current_epoch(&nodes, leader, TabletId(2));
        nodes[leader].propose(MetaCommand::BeginSplitInPlace {
            parent: TabletId(2),
            expected_epoch: tablet2_epoch,
            split_key: vec![0x40, 0, 0, 0, 0, 0, 0, 0],
            children: [
                (TabletId(4), vec![nid(210), nid(211)]),
                (TabletId(5), vec![nid(210), nid(211)]),
            ],
        });
        sim.run_for(Duration::from_secs(1));
        assert_cache_matches_engine(&nodes, &engines, seed, "after BeginSplitInPlace").await;
        {
            let meta = nodes[leader].metadata();
            let parent = meta.tablets.get(&TabletId(2)).unwrap_or_else(|| {
                panic!("seed={seed}: parent tablet 2 should still be present while Splitting")
            });
            assert_eq!(parent.state, TabletState::Splitting);
            assert_eq!(parent.epoch, tablet2_epoch.next());
            let intent = parent.inplace_split.as_ref().unwrap_or_else(|| {
                panic!("seed={seed}: second-round BeginSplitInPlace should have recorded an intent")
            });
            assert_eq!(intent.children[0].id, TabletId(4));
            assert_eq!(intent.children[1].id, TabletId(5));
        }

        let tablet2_splitting_epoch = current_epoch(&nodes, leader, TabletId(2));
        nodes[leader].propose(MetaCommand::CutoverSplit {
            parent: TabletId(2),
            expected_epoch: tablet2_splitting_epoch,
            cutover_wall_ms: 1_700_000_000_003,
        });
        sim.run_for(Duration::from_secs(1));
        assert_cache_matches_engine(&nodes, &engines, seed, "after CutoverSplit").await;
        {
            let meta = nodes[leader].metadata();
            assert!(
                !meta.tablets.contains_key(&TabletId(2)),
                "seed={seed}: second-round CutoverSplit should have retired tablet 2"
            );
            for child in [TabletId(4), TabletId(5)] {
                let t = meta.tablets.get(&child).unwrap_or_else(|| {
                    panic!("seed={seed}: child {child:?} should exist after CutoverSplit")
                });
                assert_eq!(t.state, TabletState::Active);
                let lineage = meta.split_lineage.get(&child).unwrap_or_else(|| {
                    panic!("seed={seed}: split_lineage entry missing for {child:?}")
                });
                assert_eq!(lineage.parent, TabletId(2));
            }
        }

        // The janitor's two-phase reclaim (mark, then remove) — proves
        // `ExpireStreamShards`'s own mirror arm derives a `Put` (the marked
        // row) and then a `Delete` (the removed row), both durable.
        nodes[leader].propose(MetaCommand::ExpireStreamShards {
            rows: vec![(TabletId(1), 0)],
            remove: false,
        });
        sim.run_for(Duration::from_millis(500));
        assert_cache_matches_engine(&nodes, &engines, seed, "after ExpireStreamShards mark").await;
        assert!(
            nodes[leader]
                .metadata()
                .stream_shards
                .get(&(TabletId(1), 0))
                .unwrap_or_else(|| panic!(
                    "seed={seed}: (tablet 1, epoch 0) row should still exist to be marked"
                ))
                .expired,
            "seed={seed}: ExpireStreamShards mark should have set expired=true"
        );

        nodes[leader].propose(MetaCommand::ExpireStreamShards {
            rows: vec![(TabletId(1), 0)],
            remove: true,
        });
        sim.run_for(Duration::from_millis(500));
        assert_cache_matches_engine(&nodes, &engines, seed, "after ExpireStreamShards remove")
            .await;
        assert!(
            !nodes[leader]
                .metadata()
                .stream_shards
                .contains_key(&(TabletId(1), 0)),
            "seed={seed}: ExpireStreamShards remove should have deleted the (tablet 1, epoch 0) row"
        );

        nodes[leader].propose(MetaCommand::DropTableTablets {
            table: "orders".to_string(),
        });
        sim.run_for(Duration::from_secs(1));
        assert_cache_matches_engine(&nodes, &engines, seed, "after drop-table").await;
        assert_eq!(
            nodes[leader].metadata().tablets_for_table("orders").count(),
            0,
            "seed={seed}: DropTableTablets should have removed every remaining \"orders\" tablet"
        );

        // Crash-and-restart a follower on the same disk *and* the same
        // (durable) engine — recovery rebuilds the cache from the engine's
        // watermark and replays only the surviving log tail.
        let follower = (0..3).find(|&i| i != leader).unwrap();
        sim.stop(nid(follower as u64));
        sim.run_for(Duration::from_millis(200));
        nodes[follower] = RaftNode::start(
            sim.env(nid(follower as u64)),
            NODES.iter().copied().map(nid).collect(),
            engines[follower].clone(),
        );
        sim.run_for(Duration::from_secs(3));

        assert_cache_matches_engine(&nodes, &engines, seed, "after restart").await;
        // And every node still agrees with every other node.
        let leader = unique_leader(&nodes, seed);
        let reference = nodes[leader].metadata();
        for (i, n) in nodes.iter().enumerate() {
            assert_eq!(
                n.metadata(),
                reference,
                "seed={seed} node {i}: diverged from the leader after restart"
            );
        }
    });
}

/// ADR 0062 rung 2: the directed-Placing catalog (`Metadata::split_placing`)
/// mirrors correctly through the same cache/engine oracle as every other
/// per-entity collection — proven over an in-place split whose cutover-time
/// placement decision genuinely differs from the parent's fork-inherited
/// replicas (so a real `Some(target)` entry is written, not just the
/// "already satisfying" no-op case), followed by `MarkSplitPlacingDone`.
#[test]
fn cache_matches_engine_through_directed_placing() {
    for seed in [0x5CE0_0101u64, 0x5CE0_0102] {
        run_directed_placing_scenario(seed);
    }
}

fn run_directed_placing_scenario(seed: u64) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    rt.block_on(async move {
        let mut sim = Simulator::new(seed);
        let engines: Vec<MemoryEngine> = NODES.iter().map(|_| MemoryEngine::new()).collect();
        let nodes: Vec<RaftNode<SimEnv>> = NODES
            .iter()
            .map(|&id| {
                RaftNode::start(
                    sim.env(nid(id)),
                    NODES.iter().copied().map(nid).collect(),
                    engines[id as usize].clone(),
                )
            })
            .collect();
        sim.run_for(Duration::from_secs(2));
        let leader = unique_leader(&nodes, seed);

        // Three active placement candidates, disjoint from the control
        // cluster's own node ids (0/1/2, `NODES` above) so an id never does
        // double duty as both a control-Raft peer and a placement
        // candidate. "n21" sorts ahead of "n210"/"n211" in plain string
        // order (a shorter string is a prefix of, hence less than, any
        // longer string it prefixes), so a fresh `select_replicas(RF=2)`
        // prefers [n21, n210] over the parent's own fork-inherited
        // [n210, n211] — a genuine, non-trivial Placing target.
        //
        // **Timing note**: none of these three ids ever heartbeats for
        // real, so the leader's own failure detector's phantom-member
        // hardening (`node.rs`, ADR 0030) gives each a synthetic
        // observation the first tick it's seen `Active`-but-untracked, then
        // marks it `Down` once `DETECT_TIMEOUT` (500ms) passes with no
        // further heartbeat — this crate's own "do not drive load with
        // `UpsertMember` for node ids that will never heartbeat" test-design
        // gotcha (`animus-control/CLAUDE.md`'s Tests section). Every
        // proposal through `CutoverSplit` is therefore issued back-to-back,
        // in one batch, so the whole sequence commits and applies well
        // inside that 500ms window — the three candidates are still `Active`
        // at the one instant that matters (`CutoverSplit`'s own apply).
        nodes[leader].propose(upsert(21));
        nodes[leader].propose(upsert(210));
        nodes[leader].propose(upsert(211));
        nodes[leader].propose(MetaCommand::CreateTableSchema {
            table: "widgets".to_string(),
            schema: animus_control::TableSchema::simple("id", animus_control::ColumnType::String),
        });
        nodes[leader].propose(MetaCommand::CreateTablet {
            tablet: TabletId(100),
            table: Some("widgets".to_string()),
            range: KeyRange::whole(),
            replicas: vec![nid(210), nid(211)],
        });
        nodes[leader].propose(MetaCommand::SetTabletPolicy {
            tablet: TabletId(100),
            policy: Some(PlacementPolicy::simple("p", 2)),
        });
        nodes[leader].propose(MetaCommand::BeginSplitInPlace {
            parent: TabletId(100),
            expected_epoch: Epoch::INITIAL,
            split_key: vec![0x80],
            children: [
                (TabletId(101), vec![nid(210), nid(211)]),
                (TabletId(102), vec![nid(210), nid(211)]),
            ],
        });
        nodes[leader].propose(MetaCommand::CutoverSplit {
            parent: TabletId(100),
            expected_epoch: Epoch::INITIAL.next(),
            cutover_wall_ms: 1_700_000_000_100,
        });
        sim.run_for(Duration::from_millis(300));
        assert_cache_matches_engine(&nodes, &engines, seed, "after in-place CutoverSplit").await;

        // Both children forked onto the parent's own replicas, but a fresh
        // policy-satisfying target prefers a different set — a real,
        // written `split_placing` obligation for each child.
        let meta = nodes[leader].metadata();
        for child in [TabletId(101), TabletId(102)] {
            let entry = meta.split_placing.get(&child).unwrap_or_else(|| {
                panic!("seed={seed}: expected a split_placing entry for {child:?}")
            });
            assert_eq!(entry.target, Some(vec![nid(21), nid(210)]));
            assert!(!entry.done);
        }

        nodes[leader].propose(MetaCommand::MarkSplitPlacingDone {
            tablet: TabletId(101),
            expected_epoch: Epoch::INITIAL.next(),
        });
        sim.run_for(Duration::from_secs(1));
        assert_cache_matches_engine(&nodes, &engines, seed, "after MarkSplitPlacingDone").await;

        let meta = nodes[leader].metadata();
        assert!(meta.split_placing[&TabletId(101)].done);
        assert!(!meta.split_placing[&TabletId(102)].done);
    });
}
