//! C-17 Tier 1 (`docs/roadmap.md`): `SimEnv`/pure **scale and density
//! measurement** — metadata growth, reconciler/rebalance cost, control
//! `InstallSnapshot` catch-up size, and per-node group density under
//! quiescence. A **curve, not a threshold test**: every measurement test
//! prints machine-readable `C17 metric=<name> tablets=<n> nodes=<n>
//! value=<v>` lines (run with `--nocapture`) and asserts only structural /
//! correctness properties (a size grows ~linearly, a single-tablet delta
//! stays O(1), rebalance converges, a quiesced idle window does ~no work,
//! every acked write survives a storm). Nothing here is a wall-clock or
//! performance bound: `SimEnv` virtual time proves **operation counts and
//! message volume**, never CPU or RSS (that is C-17 Tier 2, `ProdEnv`).
//! Values are counts: bytes, messages, proposals, timer fires, moves.
//!
//! # Part A — pure metadata scale curve
//!
//! No `SimCluster`: a [`Metadata`] of N single-tablet tables (RF 3 policy
//! each) over M nodes is built directly ([`build_meta`] — `CreateTablet`'s
//! apply is itself O(tablets) per call, so a 50k-tablet build through the
//! real command path would be O(N^2); the direct builder is proven
//! byte-identical to the real `apply_and_derive_mirror` path at a small N
//! by `sim_cluster_scale_builder_matches_real_apply_path`) and then run
//! through the *real* pure functions: `encode_syskv_image_bytes` (the
//! control `InstallSnapshot` payload), `apply_and_derive_mirror` (the
//! long-poll delta), `host::plan` (the per-node reconciler decision),
//! `Metadata::rebalance`/`reconcile` (placement) and a bare `RaftCore`
//! chunk pump with the real image blob.
//!
//! Sizes: `tablets` in {100, 1_000, 10_000, 50_000} capped by
//! `ANIMUS_SCALE_MAX_TABLETS` (default [`DEFAULT_MAX_TABLETS`], small enough
//! for the per-push gate; the nightly `corpus-deep.yml` sets 50000), over
//! `nodes` in {3, 9, 30}.
//!
//! # Part B — real `SimCluster` groups (seed-reproducible corpus)
//!
//! Cells (`ANIMUS_SCALE_SEEDS=K` seeds per cell, `corpus::seed_expand`;
//! replay one with `ANIMUS_SEED=<seed>`): G real tablet groups with
//! quiescence ON, an idle virtual window measured for executor work
//! (`SimStats::task_polls`/`timer_fires`, the "nonzero steady CPU" proxy)
//! and message volume by stream, then k groups woken by writes; and a
//! split-storm cell with a node restart and a control-leader transfer
//! mid-storm.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Display;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_control::mirror::{self, KeyWrite};
use animus_control::node::encode_syskv_image_bytes;
use animus_control::raft::SNAPSHOT_CHUNK_BYTES;
use animus_control::{
    ApplyOutcome, ColumnType, DeltaRing, MetaCommand, Metadata, NodeStatus, PlacementPolicy,
    RaftCore, RaftMsg, TableSchema, syskv,
};
use animus_cp_data::host::{self, HostAction, LocalState, MetadataView, TabletFacts};
use animus_env::{Clock, EnvExt, Nanos, NodeId, nid};
use animus_sim::{SimStats, TraceEvent};
use animus_tablet::{KeyRange, Tablet, TabletId, TabletState};
use animus_test::corpus::{self, SeedVariant};
use serde::{Deserialize, Serialize};

use super::AutoSplitThresholds;
use super::sim_cluster::SimCluster;

/// Default `ANIMUS_SCALE_MAX_TABLETS`: the per-push gate runs the 100 and
/// 1_000 rows only; the nightly sets 50_000.
const DEFAULT_MAX_TABLETS: usize = 1_000;
/// Every size the curve can include (filtered by the knob).
const ALL_SIZES: [usize; 4] = [100, 1_000, 10_000, 50_000];
/// Node counts of the curve.
const NODE_COUNTS: [usize; 3] = [3, 9, 30];
/// The replication factor of every table in the pure curve.
const RF: usize = 3;
/// The rebalance-convergence sweep is O(moves x tablets) per transition, so
/// it is capped below the largest curve size.
const REBALANCE_SIZE_CAP: usize = 10_000;

/// `ANIMUS_SCALE_MAX_TABLETS` (default [`DEFAULT_MAX_TABLETS`]).
fn max_tablets() -> usize {
    std::env::var("ANIMUS_SCALE_MAX_TABLETS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(DEFAULT_MAX_TABLETS)
}

/// The curve's tablet counts under the current knob (always at least the
/// smallest size, so the structural asserts have something to compare).
fn sizes() -> Vec<usize> {
    let cap = max_tablets();
    let mut out: Vec<usize> = ALL_SIZES.iter().copied().filter(|&s| s <= cap).collect();
    if out.is_empty() {
        out.push(ALL_SIZES[0]);
    }
    out
}

/// Print one machine-readable measurement line.
fn emit(metric: &str, tablets: usize, nodes: usize, value: impl Display) {
    println!("C17 metric={metric} tablets={tablets} nodes={nodes} value={value}");
}

// ---------------------------------------------------------------------------
// Pure metadata builder.
// ---------------------------------------------------------------------------

type ImageEntry = (Vec<u8>, Option<Vec<u8>>, u64);

/// Tablet `i`'s (0-based) replica set: `RF` consecutive nodes, round-robin
/// — an even initial placement, like a freshly bootstrapped cluster.
fn replicas_for(i: usize, nodes: usize) -> Vec<NodeId> {
    let rf = RF.min(nodes);
    (0..rf).map(|k| nid(((i + k) % nodes) as u64)).collect()
}

fn add_members(meta: &mut Metadata, from: usize, to: usize) {
    for n in from..to {
        let mut labels = BTreeMap::new();
        labels.insert("zone".to_owned(), format!("z{}", n % 3));
        let outcome = meta.apply(&MetaCommand::UpsertMember {
            node: nid(n as u64),
            labels,
            status: NodeStatus::Active,
        });
        assert_eq!(outcome, ApplyOutcome::Applied);
    }
}

fn table_schema() -> TableSchema {
    TableSchema::composite("pk", ColumnType::String, "sk", ColumnType::String)
}

/// `tablets` single-tablet tables over `nodes` members, each with an RF
/// policy. Tablet ids are `1..=tablets`. `CreateTablet` is inserted
/// directly (see the module doc); members, schemas and policies go through
/// the real `Metadata::apply`.
fn build_meta(tablets: usize, nodes: usize) -> Metadata {
    let mut meta = Metadata::default();
    add_members(&mut meta, 0, nodes);
    let policy = PlacementPolicy::simple("cp-rf", RF.min(nodes));
    for i in 0..tablets {
        let id = TabletId(i as u64 + 1);
        let table = format!("t{i}");
        assert_eq!(
            meta.apply(&MetaCommand::CreateTableSchema {
                table: table.clone(),
                schema: table_schema(),
            }),
            ApplyOutcome::Applied
        );
        meta.tablets.insert(
            id,
            Tablet::with_table(id, Some(table), KeyRange::whole(), replicas_for(i, nodes)),
        );
        assert_eq!(
            meta.apply(&MetaCommand::SetTabletPolicy {
                tablet: id,
                policy: Some(policy.clone()),
            }),
            ApplyOutcome::Applied
        );
    }
    meta.next_tablet_id = tablets as u64 + 1;
    meta
}

fn put_json<T: serde::Serialize>(key: Vec<u8>, value: &T) -> (Vec<u8>, Vec<u8>) {
    (key, serde_json::to_vec(value).expect("serializes"))
}

/// The system-keyspace image the control `InstallSnapshot` would ship for
/// `meta`: one row per entity, derived from the final state with the same
/// `syskv` key helpers and `serde_json` values the mirror writes.
fn image_rows(meta: &Metadata) -> BTreeMap<Vec<u8>, Vec<u8>> {
    let mut rows = BTreeMap::new();
    for (id, m) in &meta.members {
        let (k, v) = put_json(syskv::member_key(id), m);
        rows.insert(k, v);
    }
    for (id, t) in &meta.tablets {
        let (k, v) = put_json(syskv::tablet_key(*id), t);
        rows.insert(k, v);
    }
    for (id, p) in &meta.policies {
        let (k, v) = put_json(syskv::policy_key(*id), p);
        rows.insert(k, v);
    }
    for (table, s) in meta.schemas.iter() {
        let (k, v) = put_json(syskv::schema_key(table), s);
        rows.insert(k, v);
    }
    rows.insert(
        syskv::counter_key(mirror::NEXT_TABLET_ID_COUNTER),
        meta.next_tablet_id.to_be_bytes().to_vec(),
    );
    rows
}

fn image_entries(meta: &Metadata) -> Vec<ImageEntry> {
    image_rows(meta)
        .into_iter()
        .map(|(k, v)| (k, Some(v), 1))
        .collect()
}

fn write_bytes(writes: &[KeyWrite]) -> usize {
    writes
        .iter()
        .map(|w| match w {
            KeyWrite::Put(k, v) => k.len() + v.len(),
            KeyWrite::Delete(k) => k.len(),
        })
        .sum()
}

/// Replicas hosted per member.
fn load_per_node(meta: &Metadata) -> BTreeMap<NodeId, usize> {
    let mut load: BTreeMap<NodeId, usize> = meta.members.keys().map(|n| (n.clone(), 0)).collect();
    for t in meta.tablets.values() {
        for r in &t.replicas {
            *load.entry(r.clone()).or_default() += 1;
        }
    }
    load
}

fn spread(load: &BTreeMap<NodeId, usize>) -> usize {
    let max = load.values().copied().max().unwrap_or(0);
    let min = load.values().copied().min().unwrap_or(0);
    max - min
}

// ---------------------------------------------------------------------------
// Part A tests.
// ---------------------------------------------------------------------------

/// The direct builder + state-derived image must equal the real command
/// path (`apply_and_derive_mirror` per command, accumulated last-write-wins)
/// — otherwise every size in the curve below measures a fiction.
#[test]
fn sim_cluster_scale_builder_matches_real_apply_path() {
    let (tablets, nodes) = (40usize, 5usize);
    let direct = build_meta(tablets, nodes);

    let mut real = Metadata::default();
    let mut acc: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut run = |meta: &mut Metadata, cmd: MetaCommand| {
        let (outcome, writes) = mirror::apply_and_derive_mirror(meta, &cmd);
        assert_eq!(outcome, ApplyOutcome::Applied, "{cmd:?}");
        for w in writes {
            match w {
                KeyWrite::Put(k, v) => {
                    acc.insert(k, v);
                }
                KeyWrite::Delete(k) => {
                    acc.remove(&k);
                }
            }
        }
    };
    for n in 0..nodes {
        let mut labels = BTreeMap::new();
        labels.insert("zone".to_owned(), format!("z{}", n % 3));
        run(
            &mut real,
            MetaCommand::UpsertMember {
                node: nid(n as u64),
                labels,
                status: NodeStatus::Active,
            },
        );
    }
    let policy = PlacementPolicy::simple("cp-rf", RF.min(nodes));
    for i in 0..tablets {
        let id = TabletId(i as u64 + 1);
        let table = format!("t{i}");
        run(
            &mut real,
            MetaCommand::CreateTableSchema {
                table: table.clone(),
                schema: table_schema(),
            },
        );
        run(
            &mut real,
            MetaCommand::CreateTablet {
                tablet: id,
                table: Some(table),
                range: KeyRange::whole(),
                replicas: replicas_for(i, nodes),
            },
        );
        run(
            &mut real,
            MetaCommand::SetTabletPolicy {
                tablet: id,
                policy: Some(policy.clone()),
            },
        );
    }
    assert_eq!(
        serde_json::to_vec(&direct).unwrap(),
        serde_json::to_vec(&real).unwrap(),
        "direct builder diverged from the real apply path"
    );
    assert_eq!(
        image_rows(&direct),
        acc,
        "state-derived image diverged from the mirror's accumulated writes"
    );
}

/// A1 + A2 + A3 of the curve over every (tablets, nodes) cell.
#[test]
fn sim_cluster_scale_metadata_curve() {
    let mut image_by_cell: BTreeMap<(usize, usize), usize> = BTreeMap::new();
    let mut delta_by_cell: BTreeMap<(usize, usize), usize> = BTreeMap::new();
    let mut split_delta_by_cell: BTreeMap<(usize, usize), usize> = BTreeMap::new();
    for &tablets in &sizes() {
        for &nodes in &NODE_COUNTS {
            let mut meta = build_meta(tablets, nodes);

            // --- A1: snapshot image size.
            let image = encode_syskv_image_bytes(&image_entries(&meta));
            let chunks = image.len().div_ceil(SNAPSHOT_CHUNK_BYTES);
            let status_json = serde_json::to_vec(&meta).expect("meta serializes").len();
            emit("syskv_image_bytes", tablets, nodes, image.len());
            emit("syskv_image_chunks_64k", tablets, nodes, chunks);
            emit(
                "syskv_image_bytes_per_tablet",
                tablets,
                nodes,
                image.len() / tablets,
            );
            emit("status_json_bytes", tablets, nodes, status_json);
            emit(
                "status_json_bytes_per_tablet",
                tablets,
                nodes,
                status_json / tablets,
            );
            image_by_cell.insert((tablets, nodes), image.len());

            // --- A2: mirror delta for one tablet's replica move.
            let victim = TabletId(1);
            let t = &meta.tablets[&victim];
            let mut new_replicas: Vec<NodeId> = t.replicas.clone();
            if let Some(extra) = (0..nodes as u64)
                .map(nid)
                .find(|n| !new_replicas.contains(n))
            {
                new_replicas.remove(0);
                new_replicas.push(extra);
            } else {
                new_replicas.pop();
            }
            let cas = MetaCommand::CasTabletReplicas {
                tablet: victim,
                expected_epoch: t.epoch,
                replicas: new_replicas,
            };
            let (outcome, writes) = mirror::apply_and_derive_mirror(&mut meta, &cas);
            assert_eq!(outcome, ApplyOutcome::Applied);
            let raw = write_bytes(&writes);
            let wire = serde_json::to_vec(&writes).expect("writes serialize").len();
            emit("mirror_delta_cas_writes", tablets, nodes, writes.len());
            emit("mirror_delta_cas_key_value_bytes", tablets, nodes, raw);
            emit("mirror_delta_cas_wire_json_bytes", tablets, nodes, wire);
            delta_by_cell.insert((tablets, nodes), wire);

            // ... and for one in-place split (Begin + Cutover).
            let t2 = meta.tablets[&TabletId(2)].clone();
            let next = meta.next_free_tablet_id().0;
            let (left, right) = (TabletId(next), TabletId(next + 1));
            let (o1, w1) = mirror::apply_and_derive_mirror(
                &mut meta,
                &MetaCommand::BeginSplitInPlace {
                    parent: t2.id,
                    expected_epoch: t2.epoch,
                    split_key: vec![0x80; 8],
                    children: [(left, t2.replicas.clone()), (right, t2.replicas.clone())],
                },
            );
            assert_eq!(o1, ApplyOutcome::Applied);
            let epoch = meta.tablets[&t2.id].epoch;
            let (o2, w2) = mirror::apply_and_derive_mirror(
                &mut meta,
                &MetaCommand::CutoverSplit {
                    parent: t2.id,
                    expected_epoch: epoch,
                    cutover_wall_ms: 0,
                },
            );
            assert_eq!(o2, ApplyOutcome::Applied);
            let split_wire = serde_json::to_vec(&[w1.clone(), w2.clone()]).unwrap().len();
            emit(
                "mirror_delta_split_writes",
                tablets,
                nodes,
                w1.len() + w2.len(),
            );
            emit(
                "mirror_delta_split_wire_json_bytes",
                tablets,
                nodes,
                split_wire,
            );
            split_delta_by_cell.insert((tablets, nodes), split_wire);

            // DeltaRing: how many such single-tablet commands fit before the
            // ring evicts (entry cap 1024 vs the 4 MiB byte cap), and the
            // bulk-change cliff: a node drain rewrites every tablet naming
            // that node, one command each — more than the ring holds means a
            // mirror that fell behind one drain falls back to a full Status.
            let mut ring = DeltaRing::default();
            let per = raw.max(2);
            let mut fits = 0u64;
            for i in 1..=100_000u64 {
                ring.push(
                    i,
                    vec![KeyWrite::Put(vec![0; per / 2], vec![0; per - per / 2])],
                );
                // `writes_since(0, i)` is None once entry 1 was evicted.
                if ring.writes_since(0, i).is_none() {
                    break;
                }
                fits = i;
            }
            emit("delta_ring_capacity_commands", tablets, nodes, fits);
            let drain_commands = meta.tablets_referencing(&nid(0));
            emit(
                "delta_ring_node_drain_commands",
                tablets,
                nodes,
                drain_commands,
            );
            emit(
                "delta_ring_drain_overflows_ring",
                tablets,
                nodes,
                drain_commands as u64 > fits,
            );

            // --- A3: host::plan cost on the (post-change) map, node 0's view.
            let me = nid(0);
            let view = MetadataView {
                tablets: meta.tablets.clone().into(),
                down: BTreeSet::new(),
                regions: BTreeMap::new(),
                preferred_leader: BTreeMap::new(),
            };
            let local: BTreeSet<TabletId> = meta
                .tablets
                .values()
                .filter(|t| t.replicas.contains(&me))
                .map(|t| t.id)
                .collect();
            let mut facts: BTreeMap<TabletId, TabletFacts> = BTreeMap::new();
            // Boot tick: nothing hosted yet -> one Host per local tablet.
            let (boot, state) =
                host::plan(&view, &facts, &local, &LocalState::default(), me.clone());
            let hosts = boot
                .iter()
                .filter(|a| matches!(a, HostAction::Host { .. }))
                .count();
            // Steady: everything hosted; leadership spread evenly (tablet id
            // picks which of its replicas leads).
            let mut led = 0usize;
            for id in &local {
                let t = &view.tablets[id];
                let is_leader = t.replicas.get(id.0 as usize % t.replicas.len()) == Some(&me);
                led += usize::from(is_leader);
                facts.insert(
                    *id,
                    TabletFacts {
                        hosted: true,
                        is_leader,
                        scope_range: Some(t.range.clone()),
                        ..TabletFacts::default()
                    },
                );
            }
            let (steady, state) = host::plan(&view, &facts, &local, &state, me.clone());
            let reconfigures = steady
                .iter()
                .filter(|a| matches!(a, HostAction::Reconfigure { .. }))
                .count();
            let others = steady.len() - reconfigures;
            emit("plan_tablets_in_view", tablets, nodes, view.tablets.len());
            emit("plan_local_tablets", tablets, nodes, local.len());
            emit("plan_boot_host_actions", tablets, nodes, hosts);
            emit("plan_steady_actions_total", tablets, nodes, steady.len());
            emit(
                "plan_steady_reconfigure_actions",
                tablets,
                nodes,
                reconfigures,
            );
            emit(
                "plan_steady_non_reconfigure_actions",
                tablets,
                nodes,
                others,
            );
            assert_eq!(hosts, local.len(), "boot tick must host every local tablet");
            assert_eq!(
                reconfigures, led,
                "steady tick: one Reconfigure per led tablet"
            );
            assert_eq!(
                others, 0,
                "tablets={tablets} nodes={nodes}: steady state must plan no Host/Reclaim/Release"
            );

            // One-tablet change: tablet 3 loses a replica; nothing else moves.
            let mut view2 = view.clone();
            let t3 = view2.tablets.get_mut(&TabletId(3)).unwrap();
            t3.replicas.pop();
            t3.epoch = t3.epoch.next();
            let (changed, _) = host::plan(&view2, &facts, &local, &state, me.clone());
            let changed_non_reconf = changed
                .iter()
                .filter(|a| !matches!(a, HostAction::Reconfigure { .. }))
                .count();
            emit(
                "plan_one_change_actions_total",
                tablets,
                nodes,
                changed.len(),
            );
            emit(
                "plan_one_change_non_reconfigure_actions",
                tablets,
                nodes,
                changed_non_reconf,
            );
            assert!(
                changed_non_reconf <= 2,
                "tablets={tablets} nodes={nodes}: a one-tablet change planned \
                 {changed_non_reconf} non-Reconfigure actions"
            );

            // The reconciler loops do `ctx.effective_metadata()` (a full
            // `Metadata` clone) then move `meta.tablets` into the view, on
            // EVERY metadata-watch wake of EVERY node: O(tablets) per wake
            // per node, and a single-tablet change wakes all `nodes` of them.
            let cloned = meta.clone();
            let replica_ids: usize = cloned.tablets.values().map(|t| t.replicas.len()).sum();
            let entities = cloned.tablets.len()
                + cloned.policies.len()
                + cloned.schemas.len()
                + cloned.members.len()
                + cloned.split_lineage.len()
                + cloned.split_placing.len();
            emit(
                "wake_clone_tablets_per_node_wake",
                tablets,
                nodes,
                cloned.tablets.len(),
            );
            emit(
                "wake_clone_entities_per_node_wake",
                tablets,
                nodes,
                entities,
            );
            emit(
                "wake_clone_replica_ids_per_node_wake",
                tablets,
                nodes,
                replica_ids,
            );
            emit(
                "wake_clone_tablets_cluster_wide_per_change",
                tablets,
                nodes,
                cloned.tablets.len() * nodes,
            );
            assert_eq!(cloned.tablets.len(), meta.tablets.len());
        }
    }

    // Structural: sizes grow ~linearly, never super-linearly; a one-tablet
    // delta is O(1) in tablet count.
    let sizes = sizes();
    let (small, large) = (sizes[0], *sizes.last().unwrap());
    for &nodes in &NODE_COUNTS {
        let per_small = image_by_cell[&(small, nodes)] as f64 / small as f64;
        let per_large = image_by_cell[&(large, nodes)] as f64 / large as f64;
        assert!(
            per_large <= per_small * 2.0,
            "nodes={nodes}: syskv image bytes/tablet grew {per_small:.0} -> {per_large:.0} \
             between {small} and {large} tablets (super-linear)"
        );
        if large > small {
            assert!(
                image_by_cell[&(large, nodes)] > image_by_cell[&(small, nodes)],
                "image must grow with tablet count"
            );
        }
        for table in [&delta_by_cell, &split_delta_by_cell] {
            let (d_small, d_large) = (table[&(small, nodes)], table[&(large, nodes)]);
            assert!(
                d_large <= d_small * 2 + 64,
                "nodes={nodes}: one-tablet mirror delta {d_small} -> {d_large} bytes between \
                 {small} and {large} tablets (not O(1))"
            );
        }
    }
}

/// A4: rebalance convergence after growing the cluster 3 -> 9 -> 30.
#[test]
fn sim_cluster_scale_rebalance_converges() {
    let none_done = BTreeSet::new();
    let none_down = BTreeSet::new();
    for &tablets in sizes().iter().filter(|&&s| s <= REBALANCE_SIZE_CAP) {
        let mut meta = build_meta(tablets, 3);
        for (from, to) in [(3usize, 9usize), (9, 30)] {
            add_members(&mut meta, from, to);
            let before = load_per_node(&meta);
            let mut moves = 0u64;
            let mut invocations = 0u64;
            // Termination bound: each move strictly improves balance; a
            // generous O(replicas) cap catches a livelock.
            let bound = (tablets * RF * 2) as u64 + 16;
            loop {
                invocations += 1;
                assert!(
                    invocations <= bound,
                    "tablets={tablets} {from}->{to}: rebalance did not terminate in {bound} calls"
                );
                match meta.rebalance(&none_done, &none_down) {
                    None => break,
                    Some(cmd) => {
                        assert_eq!(meta.apply(&cmd), ApplyOutcome::Applied);
                        moves += 1;
                    }
                }
            }
            let after = load_per_node(&meta);
            // The fewest moves that could possibly fill the newly added
            // members to their final load.
            let min_moves: usize = after
                .iter()
                .filter(|(n, _)| !before.contains_key(*n) || before[*n] == 0)
                .map(|(_, &l)| l)
                .sum();
            emit(
                &format!("rebalance_moves_{from}_to_{to}"),
                tablets,
                to,
                moves,
            );
            emit(
                &format!("rebalance_min_moves_{from}_to_{to}"),
                tablets,
                to,
                min_moves,
            );
            emit(
                &format!("rebalance_step_invocations_{from}_to_{to}"),
                tablets,
                to,
                invocations,
            );
            emit(
                &format!("rebalance_work_units_{from}_to_{to}"),
                tablets,
                to,
                invocations * tablets as u64,
            );
            emit(
                &format!("rebalance_final_max_minus_min_{from}_to_{to}"),
                tablets,
                to,
                spread(&after),
            );
            assert!(
                spread(&after) <= 1,
                "tablets={tablets} {from}->{to}: converged with max-min {} > 1: {after:?}",
                spread(&after)
            );
            // Idempotence: a balanced, healthy cluster proposes no repair.
            assert!(
                meta.reconcile(&none_done, &none_down).is_empty(),
                "tablets={tablets} {from}->{to}: repair pass proposed work on a balanced cluster"
            );
            assert_eq!(
                meta.tablets.len(),
                tablets,
                "rebalance never changes the tablet count"
            );
            assert!(
                meta.tablets.values().all(|t| t.replicas.len() == RF),
                "rebalance must preserve every tablet's RF"
            );
        }
    }
}

/// A5: the control `InstallSnapshot` that catches a lagging voter up, with
/// the REAL encoded image of the largest curve size, driven through the
/// bare `RaftCore` chunk pump (the `animus-control` install_snapshot test's
/// own pattern). Not a live `RaftNode` cluster over `SimEnv`: the
/// transfer's chunk/message/byte counts are fully determined by the image
/// length, the 64 KiB chunk size and the stop-and-wait ack protocol, all of
/// which the pump exercises exactly; a `RaftNode` run would add only the
/// engine/WAL I/O around it while re-materializing a 50k-tablet engine per
/// node. The stop-and-wait round count is the quantity that, multiplied by
/// the link RTT, is ADR 0039's "catch-up takes seconds-to-minutes" revisit
/// criterion — reported here as a count (RTT-independent).
#[test]
fn sim_cluster_scale_install_snapshot_catchup() {
    let tablets = *sizes().last().unwrap();
    let nodes = 3usize;
    let meta = build_meta(tablets, nodes);
    let image = encode_syskv_image_bytes(&image_entries(&meta));

    let pair: [NodeId; 2] = [nid(0), nid(1)];
    let now = Nanos(1_000_000_000);
    let mut leader: RaftCore = RaftCore::new(nid(0), &pair, Nanos(0), 7);
    let _ = leader.tick(now, 7);
    let _ = leader.handle(
        nid(1),
        RaftMsg::PreVoteResp {
            term: leader.term() + 1,
            granted: true,
        },
        now,
        7,
    );
    let _ = leader.handle(
        nid(1),
        RaftMsg::RequestVoteResp {
            term: leader.term(),
            granted: true,
        },
        now,
        7,
    );
    assert!(leader.is_leader());
    for i in 0..130u64 {
        let cmd = MetaCommand::UpsertMember {
            node: nid(i),
            labels: BTreeMap::new(),
            status: NodeStatus::Active,
        };
        if let animus_control::ProposeResult::Accepted { index, .. } = leader.propose(cmd) {
            let _ = leader.handle(
                nid(1),
                RaftMsg::AppendEntriesResp {
                    term: leader.term(),
                    success: true,
                    match_index: index,
                    needs_snapshot: false,
                    check_pending: false,
                },
                now,
                7,
            );
        }
    }
    leader.mark_durable_through(leader.last_log_index());
    leader.snapshot();
    leader.set_snapshot_blob(image.clone());

    let mut follower: RaftCore = RaftCore::new(nid(1), &pair, Nanos(0), 7);
    let hb = Nanos(now.0 + 1_000_000_000);
    let mut pending: Vec<(NodeId, RaftMsg)> = leader.tick(hb, 7);
    let (mut chunks, mut chunk_bytes, mut replies, mut rounds) = (0u64, 0u64, 0u64, 0u64);
    while !pending.is_empty() {
        rounds += 1;
        assert!(rounds < 1_000_000, "chunk exchange did not terminate");
        let mut next = Vec::new();
        for (to, msg) in pending {
            if to == nid(1) {
                if let RaftMsg::InstallSnapshot { data, .. } = &msg {
                    chunks += 1;
                    chunk_bytes += data.len() as u64;
                }
                next.extend(follower.handle(nid(0), msg, now, 7));
            } else {
                if matches!(msg, RaftMsg::InstallSnapshotResp { .. }) {
                    replies += 1;
                }
                next.extend(leader.handle(nid(1), msg, now, 7));
            }
        }
        pending = next;
    }
    emit("install_snapshot_image_bytes", tablets, nodes, image.len());
    emit("install_snapshot_chunks", tablets, nodes, chunks);
    emit(
        "install_snapshot_chunk_payload_bytes",
        tablets,
        nodes,
        chunk_bytes,
    );
    emit("install_snapshot_resp_messages", tablets, nodes, replies);
    emit(
        "install_snapshot_stop_and_wait_rounds",
        tablets,
        nodes,
        rounds,
    );
    assert_eq!(follower.snapshot_index(), leader.snapshot_index());
    assert_eq!(
        chunk_bytes,
        image.len() as u64,
        "every image byte ships exactly once on a clean link"
    );
    assert_eq!(
        chunks,
        image.len().div_ceil(SNAPSHOT_CHUNK_BYTES) as u64,
        "chunk count is ceil(image / SNAPSHOT_CHUNK_BYTES)"
    );
    assert_eq!(replies, chunks, "stop-and-wait: one ack per chunk");
}

// ===========================================================================
// Part B — real SimCluster groups.
// ===========================================================================

/// Idle measurement window (virtual).
const IDLE_WINDOW: Duration = Duration::from_secs(60);
/// `--quiesce-after` the clusters run with (production default).
const QUIESCE_AFTER: Duration = Duration::from_secs(5);
/// Settle poll step / budget for "every group quiesces".
const SETTLE_STEP: Duration = Duration::from_secs(5);
const SETTLE_BUDGET: Duration = Duration::from_secs(120);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum CellKind {
    /// G single-tablet tables, quiesce, idle window, wake k.
    Density,
    /// Concurrent in-place splits under a node restart and a control-leader
    /// transfer.
    SplitStorm,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Scenario {
    name: String,
    seed: u64,
    kind: CellKind,
    nodes: usize,
    /// Density: tablet groups. Storm: tables that each split.
    groups: usize,
    /// Density: groups woken by a write.
    wake: usize,
}

impl SeedVariant for Scenario {
    fn scenario_name(&self) -> &str {
        &self.name
    }
    fn reseeded(&self, name: String, seed: u64) -> Self {
        Scenario {
            name,
            seed,
            ..self.clone()
        }
    }
}

fn cell(name: &str, kind: CellKind, nodes: usize, groups: usize, wake: usize) -> Scenario {
    Scenario {
        name: name.to_owned(),
        seed: corpus::name_seed(name),
        kind,
        nodes,
        groups,
        wake,
    }
}

/// Largest density group count the current knob admits on a `nodes`-node
/// cluster. A 9-node cell is ~10x costlier per group than a 3-node one (the
/// groups are created on the first RF nodes, so the placement reconciler
/// then rebalances ~2/3 of every group's replicas across the wider cluster,
/// waking each moved group), hence the different divisors: 3 nodes admit
/// `max_tablets / 20` groups (default 1_000 -> 50), 9 nodes `max_tablets /
/// 100` (default -> 10; the nightly's 50_000 admits the whole {10, 50, 100}
/// list on both).
fn max_groups(nodes: usize) -> usize {
    let div = if nodes <= 3 { 20 } else { 100 };
    (max_tablets() / div).max(10)
}

/// The density cells (frozen names/seeds), capped by [`max_groups`].
fn group_cells() -> Vec<Scenario> {
    let mut out = Vec::new();
    for nodes in [3usize, 9] {
        out.push(cell(
            &format!("scale_density_n{nodes}_g0"),
            CellKind::Density,
            nodes,
            0,
            0,
        ));
        for g in [10usize, 50, 100] {
            if g <= max_groups(nodes) {
                out.push(cell(
                    &format!("scale_density_n{nodes}_g{g}"),
                    CellKind::Density,
                    nodes,
                    g,
                    g.min(5),
                ));
            }
        }
    }
    out
}

/// The split-storm cells.
fn storm_cells() -> Vec<Scenario> {
    let mut out = vec![cell("scale_storm_n3_t4", CellKind::SplitStorm, 3, 4, 0)];
    if max_tablets() >= 10_000 {
        out.push(cell("scale_storm_n5_t10", CellKind::SplitStorm, 5, 10, 0));
    }
    out
}

fn corpus_cells() -> Vec<Scenario> {
    let mut v = group_cells();
    v.extend(storm_cells());
    v
}

/// The corpus: cells seed-expanded by `ANIMUS_SCALE_SEEDS`, or — when
/// `ANIMUS_SEED` is set — only the (expanded) scenario carrying that seed.
fn corpus_scenarios() -> Vec<Scenario> {
    let k = corpus::seeds_from_env("ANIMUS_SCALE_SEEDS");
    let all = corpus::seed_expand(corpus_cells(), k);
    match std::env::var("ANIMUS_SEED")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
    {
        Some(seed) => {
            let one: Vec<Scenario> = all.into_iter().filter(|s| s.seed == seed).collect();
            assert!(
                !one.is_empty(),
                "ANIMUS_SEED={seed} names no scale scenario"
            );
            one
        }
        None => all,
    }
}

fn stats_delta(a: SimStats, b: SimStats) -> (u64, u64) {
    (b.task_polls - a.task_polls, b.timer_fires - a.timer_fires)
}

/// Messages / bytes sent in a trace slice, by stream class.
#[derive(Default, Clone, Debug)]
struct Traffic {
    control_msgs: u64,
    control_bytes: u64,
    tablet_msgs: u64,
    tablet_bytes: u64,
    other_msgs: u64,
    other_bytes: u64,
}

fn traffic(events: &[TraceEvent]) -> Traffic {
    let mut t = Traffic::default();
    for e in events {
        if let TraceEvent::Send { stream, len, .. } = e {
            let len = *len as u64;
            if *stream == 0 {
                t.control_msgs += 1;
                t.control_bytes += len;
            } else if *stream >= u64::MAX - 3 {
                t.other_msgs += 1;
                t.other_bytes += len;
            } else {
                t.tablet_msgs += 1;
                t.tablet_bytes += len;
            }
        }
    }
    t
}

fn emit_traffic(prefix: &str, groups: usize, nodes: usize, t: &Traffic, secs: u64) {
    let secs = secs.max(1);
    emit(
        &format!("{prefix}_control_msgs"),
        groups,
        nodes,
        t.control_msgs,
    );
    emit(
        &format!("{prefix}_control_bytes"),
        groups,
        nodes,
        t.control_bytes,
    );
    emit(
        &format!("{prefix}_tablet_msgs"),
        groups,
        nodes,
        t.tablet_msgs,
    );
    emit(
        &format!("{prefix}_tablet_bytes"),
        groups,
        nodes,
        t.tablet_bytes,
    );
    emit(&format!("{prefix}_other_msgs"), groups, nodes, t.other_msgs);
    emit(
        &format!("{prefix}_control_msgs_per_s"),
        groups,
        nodes,
        t.control_msgs / secs,
    );
    emit(
        &format!("{prefix}_tablet_msgs_per_s"),
        groups,
        nodes,
        t.tablet_msgs / secs,
    );
}

/// Sum of `(hosted, quiesced)` group replicas over every node.
fn cluster_quiesced(cluster: &SimCluster) -> (usize, usize) {
    (0..cluster.node_count() as u64)
        .map(|n| cluster.quiesced_counts(n))
        .fold((0, 0), |(h, q), (nh, nq)| (h + nh, q + nq))
}

/// Run until every group is hosted on exactly `expect_replicas` nodes in
/// total (initial placement + any rebalance finished) and every one is
/// quiesced — a converged-or-timeout poll (an eventual property; the budget
/// scales with the group count because creating G tables on the first RF
/// nodes of a wider cluster triggers ~G rebalance moves, each waking the
/// moved group). Returns virtual seconds waited; panics naming the seed on
/// timeout.
fn settle_all_quiesced(cluster: &mut SimCluster, expect_replicas: usize, what: &str) -> u64 {
    let budget = SETTLE_BUDGET + Duration::from_secs(15 * expect_replicas as u64);
    let mut waited = Duration::ZERO;
    loop {
        let (hosted, quiesced) = cluster_quiesced(cluster);
        if hosted == expect_replicas && hosted == quiesced && waited >= SETTLE_STEP {
            return waited.as_secs();
        }
        assert!(
            waited < budget,
            "{what}: groups never all quiesced within {budget:?} \
             (hosted={hosted} expected={expect_replicas} quiesced={quiesced}, seed={})",
            cluster.seed()
        );
        cluster.run_for(SETTLE_STEP);
        waited += SETTLE_STEP;
    }
}

#[derive(Default, Clone, Debug)]
struct DensityResult {
    idle_polls_per_s: u64,
    idle_timer_fires_per_s: u64,
}

fn run_density(s: &Scenario) -> DensityResult {
    let rf = 3.min(s.nodes);
    let mut cluster = SimCluster::new_with_cp_quiescence(s.seed, s.nodes, rf, Some(QUIESCE_AFTER));
    let sim = cluster.simulator();
    let (g, n) = (s.groups, s.nodes);

    let create0 = cluster.sim_stats();
    let tables: Vec<String> = (0..g).map(|i| format!("t{i}")).collect();
    for t in &tables {
        cluster.create_table_with_replication(t, rf);
    }
    let (cp, ct) = stats_delta(create0, cluster.sim_stats());
    emit("create_task_polls", g, n, cp);
    emit("create_timer_fires", g, n, ct);

    let waited = settle_all_quiesced(&mut cluster, g * rf, &s.name);
    emit("quiesce_settle_virtual_s", g, n, waited);
    let (hosted, quiesced) = cluster_quiesced(&cluster);
    emit("group_replicas_hosted", g, n, hosted);
    emit("group_replicas_quiesced_at_idle_start", g, n, quiesced);
    assert_eq!(hosted, g * rf, "{}: every group hosted on RF nodes", s.name);
    assert_eq!(quiesced, hosted, "{}: all quiesced at idle start", s.name);

    // --- Idle window.
    let (c0, t0) = (cluster.sim_stats(), sim.trace().len());
    let (ctl0, _) = cluster.control_raft_indices(0);
    cluster.run_for(IDLE_WINDOW);
    let (ctl1, _) = cluster.control_raft_indices(0);
    let (c1, trace) = (cluster.sim_stats(), sim.trace());
    let secs = IDLE_WINDOW.as_secs();
    let (polls, fires) = stats_delta(c0, c1);
    let idle = traffic(&trace[t0..]);
    emit("idle_task_polls_per_s", g, n, polls / secs);
    emit("idle_timer_fires_per_s", g, n, fires / secs);
    emit_traffic("idle", g, n, &idle, secs);
    // The control leader's steady-state proposal rate: with a populated
    // Metadata and nothing changing, the reconcile / failure-detect /
    // heartbeat loops must not propose (a commit-index delta counts every
    // committed entry, i.e. every proposal, over the window).
    emit("idle_control_proposals", g, n, ctl1 - ctl0);
    assert_eq!(
        ctl1,
        ctl0,
        "{}: the control group committed {} entries in an idle window (seed={})",
        s.name,
        ctl1 - ctl0,
        s.seed
    );
    let (hosted_end, quiesced_end) = cluster_quiesced(&cluster);
    emit("idle_quiesced_group_replicas_at_end", g, n, quiesced_end);
    assert_eq!(
        (hosted_end, quiesced_end),
        (hosted, hosted),
        "{}: a quiesced group woke during the idle window (seed={})",
        s.name,
        s.seed
    );
    // Structural: a quiesced group sends nothing. Timer wakeups are NOT
    // asserted zero — `apply_loop`'s 250ms `APPLY_SAFETY_POLL` keeps firing
    // on every replica (reported above as idle_timer_fires_per_s and in the
    // marginal per-group metric).
    assert_eq!(
        idle.tablet_msgs, 0,
        "{}: quiesced groups sent {} tablet-stream messages in an idle window (seed={})",
        s.name, idle.tablet_msgs, s.seed
    );

    // --- Wake k groups with writes, concurrently from client envs.
    let k = s.wake.min(g);
    if k > 0 {
        let handle = cluster.handle();
        let acked: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(Vec::new()));
        for (i, table) in tables.iter().take(k).enumerate() {
            let env = cluster.client_env(i as u64);
            let handle = handle.clone();
            let acked = Arc::clone(&acked);
            let table = table.clone();
            let node = (i % s.nodes) as u64;
            env.clone().spawn_task(async move {
                let value = format!("v-{i}");
                for _ in 0..20 {
                    if handle
                        .put(node, &table, "pk", "sk", value.as_bytes())
                        .await
                        .is_ok()
                    {
                        acked.lock().expect("acked").push((table.clone(), value));
                        return;
                    }
                    env.sleep(Duration::from_millis(100)).await;
                }
            });
        }
        let wake_window = Duration::from_secs(2);
        let (w0, tw0) = (cluster.sim_stats(), sim.trace().len());
        cluster.run_for(wake_window);
        let (w1, trace) = (cluster.sim_stats(), sim.trace());
        let (wp, wf) = stats_delta(w0, w1);
        let wt = traffic(&trace[tw0..]);
        let (h, q) = cluster_quiesced(&cluster);
        let awake = h - q;
        emit("wake_k", g, n, k);
        emit("wake_awake_group_replicas", g, n, awake);
        emit("wake_task_polls_per_s", g, n, wp / 2);
        emit("wake_timer_fires_per_s", g, n, wf / 2);
        emit_traffic("wake", g, n, &wt, 2);
        assert!(awake > 0, "{}: writes woke no group", s.name);
        assert!(
            awake <= k * rf,
            "{}: more replicas awake ({awake}) than the {k} written groups own ({})",
            s.name,
            k * rf
        );

        let resettle = settle_all_quiesced(&mut cluster, g * rf, &s.name);
        emit("wake_requiesce_virtual_s", g, n, resettle);
        let acked = acked.lock().expect("acked").clone();
        assert_eq!(
            acked.len(),
            k,
            "{}: every wake write acks (seed={})",
            s.name,
            s.seed
        );
        for (table, value) in acked {
            let got = cluster.get((s.nodes - 1) as u64, &table, "pk", "sk", true);
            assert_eq!(
                got,
                Ok(Some(value.into_bytes())),
                "{}: acked write to {table} lost (seed={})",
                s.name,
                s.seed
            );
        }
    }

    DensityResult {
        idle_polls_per_s: polls / secs,
        idle_timer_fires_per_s: fires / secs,
    }
}

/// Every table's live tablets tile its whole ring: sorted by range start,
/// first starts at the beginning, each ends where the next starts, the last
/// is open-ended, and none is left mid-split.
fn assert_tablet_map_consistent(meta: &Metadata, tables: &[String], what: &str) {
    for table in tables {
        let mut ts: Vec<&Tablet> = meta.tablets_for_table(table).map(|(_, t)| t).collect();
        ts.sort_by(|a, b| a.range.start.cmp(&b.range.start));
        assert!(!ts.is_empty(), "{what}: {table} has no tablets");
        assert!(
            ts[0].range.start.is_empty(),
            "{what}: {table} ring does not start at 0"
        );
        for w in ts.windows(2) {
            assert_eq!(
                w[0].range.end.as_deref(),
                Some(w[1].range.start.as_slice()),
                "{what}: {table} ring has a gap/overlap"
            );
        }
        assert!(
            ts.last().unwrap().range.end.is_none(),
            "{what}: {table} ring not closed"
        );
        for t in ts {
            assert_eq!(
                t.state,
                TabletState::Active,
                "{what}: {table} tablet {:?} not Active",
                t.id
            );
            assert!(
                t.inplace_split.is_none(),
                "{what}: {table} tablet {:?} mid-split",
                t.id
            );
        }
    }
}

/// `(table, sort key, value)` of every acknowledged storm write.
type AckedWrites = Arc<Mutex<Vec<(String, String, Vec<u8>)>>>;

fn run_split_storm(s: &Scenario) {
    let rf = 3.min(s.nodes);
    let mut cluster = SimCluster::new_with_cp_quiescence(s.seed, s.nodes, rf, Some(QUIESCE_AFTER));
    let sim = cluster.simulator();
    let (g, n) = (s.groups, s.nodes);
    let tables: Vec<String> = (0..g).map(|i| format!("t{i}")).collect();
    for t in &tables {
        cluster.create_table_with_replication(t, rf);
    }
    let node_ids: Vec<u64> = (0..n as u64).collect();
    let tablets_before = cluster.metadata(0).tablets.len();

    cluster.set_auto_split_thresholds(AutoSplitThresholds {
        bytes: Some(2_000),
        change_rate: None,
        ops_rate: None,
        tablet_capacity_ceilings: Default::default(),
    });

    let (c0, t0) = (cluster.sim_stats(), sim.trace().len());
    let storm_start = cluster.sim_now();
    let (ctl_commit0, _) = cluster.control_raft_indices(0);

    // One writer task per table: 16 keys x ~300 bytes, well past the 2_000
    // byte threshold, retrying through split-freeze/restart transients.
    let handle = cluster.handle();
    let acked: AckedWrites = Arc::new(Mutex::new(Vec::new()));
    for (i, table) in tables.iter().enumerate() {
        let env = cluster.client_env(i as u64);
        let handle = handle.clone();
        let acked = Arc::clone(&acked);
        let table = table.clone();
        let n = n as u64;
        env.clone().spawn_task(async move {
            for key in 0..16u64 {
                let sk = format!("k{key}");
                let value = format!("{table}-{key}-{}", "x".repeat(300)).into_bytes();
                for attempt in 0..200u64 {
                    // Rotate the issuing node so a restarted node is routed around.
                    let node = (i as u64 + attempt) % n;
                    if handle.put(node, &table, "pk", &sk, &value).await.is_ok() {
                        acked
                            .lock()
                            .expect("acked")
                            .push((table.clone(), sk.clone(), value));
                        break;
                    }
                    env.sleep(Duration::from_millis(100)).await;
                }
            }
        });
    }

    // Drive the storm: cutover drivers on every node each round, with a node
    // restart and a control-leader transfer mid-storm.
    let victim = (n - 1) as u64;
    let mut rounds = 0u64;
    let mut did_restart = false;
    let mut did_transfer = false;
    let expect_done = |c: &SimCluster| -> bool {
        let meta = c.metadata(0);
        meta.tablets
            .values()
            .all(|t| t.state == TabletState::Active && t.inplace_split.is_none())
            && meta.tablets.len() > tablets_before
    };
    loop {
        rounds += 1;
        assert!(
            rounds <= 120,
            "{}: storm never converged (seed={})",
            s.name,
            s.seed
        );
        cluster.run_for(Duration::from_millis(500));
        for &node in &node_ids {
            cluster.drive_inplace_split_cutover(node);
        }
        if rounds == 3 && !did_restart {
            cluster.restart(victim);
            did_restart = true;
        }
        if rounds == 5 && !did_transfer {
            let leader = cluster.control_leader_index() as u64;
            let target = node_ids
                .iter()
                .copied()
                .find(|&x| {
                    x != cluster.control_node_id(leader as usize)
                        && cluster.control_index_of(x).is_some()
                })
                .expect("a transfer target");
            cluster.transfer_control_leadership_to(target);
            did_transfer = true;
        }
        let writers_done = acked.lock().expect("acked").len() == g * 16;
        if did_restart && did_transfer && writers_done && expect_done(&cluster) {
            break;
        }
    }

    // Settle: every node's map converges to the same consistent tiling.
    // The restarted node (`victim`) is compared like every other node:
    // `SimCluster::restart` keeps its control syskv engine next to the
    // retained WAL, so a restarted node's `Metadata` must converge to the
    // same content (not merely the same indices) as the rest.
    let compared: Vec<u64> = node_ids.clone();
    let mut waited = 0u64;
    loop {
        let ref_meta = cluster.metadata(0);
        let same = compared
            .iter()
            .all(|&nd| cluster.metadata(nd).tablets == ref_meta.tablets);
        if same {
            break;
        }
        waited += 1;
        if waited >= 60 {
            let shapes: Vec<(u64, usize, usize)> = node_ids
                .iter()
                .map(|&nd| {
                    let m = cluster.metadata(nd);
                    (
                        nd,
                        m.tablets.len(),
                        m.tablets
                            .values()
                            .filter(|t| t.inplace_split.is_some())
                            .count(),
                    )
                })
                .collect();
            let idx: Vec<(u64, (u64, u64))> = node_ids
                .iter()
                .map(|&nd| (nd, cluster.control_raft_indices(nd)))
                .collect();
            panic!(
                "{}: tablet maps never converged (seed={}): (node, tablets, mid-split) = {shapes:?}; \
                 control (commit, applied) = {idx:?}",
                s.name, s.seed
            );
        }
        cluster.run_for(Duration::from_secs(1));
    }
    let meta = cluster.metadata(0);
    assert_tablet_map_consistent(&meta, &tables, &s.name);

    emit(
        "storm_reference_node_metadata_tablets",
        g,
        n,
        meta.tablets.len(),
    );
    let (polls, fires) = stats_delta(c0, cluster.sim_stats());
    let trace = sim.trace();
    let tr = traffic(&trace[t0..]);
    let (ctl_commit1, _) = cluster.control_raft_indices(0);
    let splits = meta.tablets.len() - tablets_before;
    emit("storm_splits_completed", g, n, splits);
    emit("storm_final_tablets", g, n, meta.tablets.len());
    emit(
        "storm_control_proposals",
        g,
        n,
        ctl_commit1.saturating_sub(ctl_commit0),
    );
    emit("storm_rounds", g, n, rounds);
    emit("storm_task_polls", g, n, polls);
    emit("storm_timer_fires", g, n, fires);
    let storm_secs = (cluster.sim_now().0 - storm_start.0) / 1_000_000_000;
    emit("storm_virtual_s", g, n, storm_secs);
    emit_traffic("storm", g, n, &tr, storm_secs);
    assert!(
        splits >= g,
        "{}: only {splits} splits for {g} over-threshold tables",
        s.name
    );

    // Every acked write survives: converged-or-timeout read-back.
    let acked = acked.lock().expect("acked").clone();
    assert_eq!(
        acked.len(),
        g * 16,
        "{}: writers did not finish (seed={})",
        s.name,
        s.seed
    );
    for (table, sk, value) in acked {
        let mut ok = false;
        for _ in 0..40 {
            if cluster.get(0, &table, "pk", &sk, true) == Ok(Some(value.clone())) {
                ok = true;
                break;
            }
            cluster.drive_inplace_split_cutover(0);
            cluster.run_for(Duration::from_millis(200));
        }
        assert!(
            ok,
            "{}: acked write {table}/{sk} lost (seed={})",
            s.name, s.seed
        );
    }
}

/// Density corpus: G quiesced real tablet groups (idle window, wake k).
#[test]
fn sim_cluster_scale_density_corpus() {
    let mut baseline: BTreeMap<usize, DensityResult> = BTreeMap::new();
    for s in corpus_scenarios()
        .into_iter()
        .filter(|s| s.kind == CellKind::Density)
    {
        eprintln!("scenario={} seed={}", s.name, s.seed);
        let r = run_density(&s);
        if s.groups == 0 {
            baseline.entry(s.nodes).or_insert(r);
        } else if let Some(b) = baseline.get(&s.nodes) {
            let g = s.groups as f64;
            emit(
                "marginal_idle_timer_fires_per_s_per_group",
                s.groups,
                s.nodes,
                format!(
                    "{:.3}",
                    (r.idle_timer_fires_per_s as f64 - b.idle_timer_fires_per_s as f64) / g
                ),
            );
            emit(
                "marginal_idle_task_polls_per_s_per_group",
                s.groups,
                s.nodes,
                format!(
                    "{:.3}",
                    (r.idle_polls_per_s as f64 - b.idle_polls_per_s as f64) / g
                ),
            );
        }
    }
}

/// Split-storm corpus: concurrent in-place splits under a node restart and
/// a control-leader transfer; every acked write must survive.
#[test]
fn sim_cluster_scale_split_storm_corpus() {
    for s in corpus_scenarios()
        .into_iter()
        .filter(|s| s.kind == CellKind::SplitStorm)
    {
        eprintln!("scenario={} seed={}", s.name, s.seed);
        run_split_storm(&s);
    }
}

/// Regression: restarting a combined control node after well over the
/// 64-entry snapshot threshold of control entries must leave its
/// `Metadata` content-equal to the leader's (the restarted node keeps its
/// control syskv engine next to its retained WAL).
#[test]
fn sim_cluster_scale_restart_past_compaction_matches_leader() {
    let seed = 0xC17_5EED;
    let mut cluster = SimCluster::new(seed, 5, 3);
    for i in 0..40 {
        cluster.create_table_with_replication(&format!("t{i}"), 3);
    }
    cluster.run_for(Duration::from_secs(5));
    let victim = 4u64;
    cluster.restart(victim);
    let mut waited = 0;
    loop {
        let reference = cluster.metadata(0);
        if cluster.metadata(victim).tablets == reference.tablets && !reference.tablets.is_empty() {
            assert!(reference.tablets.len() >= 40, "seed={seed}");
            break;
        }
        waited += 1;
        assert!(
            waited < 60,
            "restarted node never converged (seed={seed}): victim {} vs leader {} tablets",
            cluster.metadata(victim).tablets.len(),
            reference.tablets.len()
        );
        cluster.run_for(Duration::from_secs(1));
    }
}

/// Coverage guard: the default cell set must keep every dimension (a
/// zero-group control-plane baseline, two group sizes, both node counts,
/// a wake count, a split storm) — structural only, nothing is run.
#[test]
fn sim_cluster_scale_covers_every_cell_shape() {
    let cells = corpus_cells();
    for nodes in [3usize, 9] {
        assert!(
            cells
                .iter()
                .any(|c| c.kind == CellKind::Density && c.nodes == nodes && c.groups == 0),
            "no g0 baseline for {nodes} nodes"
        );
        let sizes: BTreeSet<usize> = cells
            .iter()
            .filter(|c| c.kind == CellKind::Density && c.nodes == nodes && c.groups > 0)
            .map(|c| c.groups)
            .collect();
        // The 3-node row keeps >= 2 sizes even at the per-push default; the
        // costlier 9-node row keeps at least one (more with a bigger knob).
        let need = if nodes <= 3 { 2 } else { 1 };
        assert!(
            sizes.len() >= need,
            "need >= {need} density group sizes for {nodes} nodes: {sizes:?}"
        );
    }
    assert!(
        cells
            .iter()
            .any(|c| c.kind == CellKind::Density && c.wake > 0),
        "no wake cell"
    );
    assert!(
        cells.iter().any(|c| c.kind == CellKind::SplitStorm),
        "no split-storm cell"
    );
    let names: BTreeSet<&str> = cells.iter().map(|c| c.name.as_str()).collect();
    assert_eq!(names.len(), cells.len(), "duplicate cell names");
}
