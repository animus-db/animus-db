//! `SimCluster` (ADR 0061 rung D1, C-04 D1 step 2): a multi-node
//! `ClientCtx<SimEnv, SimRelayClient<SimEnv>>` fixture, generalizing
//! [`super::two_node_relay_tests`]'s hand-built two-node smoke to N nodes
//! with a real fault surface (crash/restart/partition/heal). One
//! `Simulator`, `nodes` node ids, a real **multi-voter** control
//! `RaftNode<SimEnv>` quorum (every node id is a voter — the
//! `animus-control/tests/control_raft.rs::cluster` shape, not the
//! single-voter/shared-`Arc` stand-in [`super::two_node_relay_tests`] used
//! for its own two-node smoke), a [`SimRelayClient<SimEnv>`] per node with
//! its relay server installed, and a `ClientCtx<SimEnv, SimRelayClient
//! <SimEnv>>` per node whose `client_route`/`intra_route` map **every**
//! node id to `NodeId::to_string()` up front (ADR 0061 rung C3d's address
//! convention) — a real cluster's routing tables grow as nodes join
//! (ADR 0030/0032), but this fixture's whole node set is known at
//! construction, so pre-populating once is both simpler and sufficient for
//! every scenario D1 needs.
//!
//! In-crate `#[cfg(test)] mod`, exactly like its two siblings above (`lib.rs`'s
//! own top-of-file doc on `ClientCtx`'s private fields explains why: no
//! external `tests/*.rs` file can construct one without widening
//! visibility this rung does not need to widen).
//!
//! # Design choices (read before extending)
//!
//! - **Tablets are hosted by a real per-node `animus_cp_data::host::
//!   Reconciler` (ADR 0061 rung D4 PR 1, closing issue #715 — see "Updated
//!   since D3" below for the full account of what this replaced).**
//!   [`SimCluster::new`] builds one `Reconciler<SimEnv, MemoryEngine>` per
//!   node (its own `MemoryTabletEngines` registry, `on_host`/`on_teardown`
//!   hooks mirroring hosting changes into that node's `ClusterEdgeState` —
//!   the identical read-only-mirror discipline `animusd`'s own production
//!   node assembly uses) and spawns a driving loop
//!   ([`spawn_reconciler_loop`]) that ticks it whenever `Metadata` changes.
//!   [`SimCluster::create_table_with_replication`] (the hand-hosted bypass)
//!   and a wire-issued `CreateTable` (`dynamo::dispatch_table_op` →
//!   `ClientCtx::provision_tablet`) both provision a tablet the identical
//!   way now — `CreateTableSchema`/`CreateTablet` plus a `SetTabletPolicy`
//!   — and are discovered/hosted/reconfigured/released by the SAME
//!   reconciler loop, purely off `Tablet.replicas.contains(&base_id)`
//!   (`host.rs`'s own hosting predicate). This is deliberately the real
//!   production mechanism (ADR 0031), not a stand-in — see
//!   `crates/animus-cp-data/tests/reconciler_corpus.rs`'s `Cluster`/
//!   `ClusterNode` for the harness shape this file's own per-node wiring
//!   mirrors.
//! - **DDL is a control-plane-Raft bypass, not `ClientCtx::propose_schema`.**
//!   Every table this fixture creates is seeded by proposing directly on
//!   whichever control `RaftNode` [`SimCluster::control_leader_index`]
//!   currently finds leading — never through a `ClientCtx` method — for the
//!   identical reason `simenv_client_ctx_tests`/`two_node_relay_tests`
//!   above bypass it. A genuine multi-voter control quorum reaching
//!   agreement on a `ProposeSchema` **relayed** through `ClientCtx` (rather
//!   than proposed directly on a `RaftNode` handle this test module holds)
//!   is still unexercised by this fixture — see the "What commit 3 needs"
//!   note in this crate's `CLAUDE.md` SimCluster section.
//! - **Restart is a true process restart, on `MemoryEngine` — but no
//!   longer a wipe.** [`SimCluster::restart`] mirrors `raftkv_
//!   linearizable.rs`'s own `StopRestart` nemesis: `Simulator::stop` (drops
//!   every task the node owns — its control `RaftNode` driver, every
//!   hosted `RaftKvNode` driver, its reconciler-loop task, its relay
//!   receive loop — durable disk aside) followed by fresh `RaftNode::
//!   start` and a fresh `Reconciler` on the same node id. **Since ADR
//!   0061 rung D4 PR 1, the fresh reconciler reuses the SAME
//!   `MemoryTabletEngines` handle this node was built with** — mirroring
//!   `reconciler_corpus.rs::Cluster::crash_restart`'s own "a durable
//!   engine (`LsmEngine` in production) survives a process crash" modeling
//!   — so a restarted node's own tablet data is **not** wiped any more (a
//!   deliberate behavior change from this fixture's pre-D4 restart, which
//!   always built a brand-new `MemoryEngine::new()`; see the ADR's
//!   matching amendment). Recovery either way is via ordinary peer
//!   catch-up / chunked `InstallSnapshot`, never local WAL replay — a
//!   caught-up engine just needs less of it. A durable (`LsmEngine`)
//!   `SimCluster` tier is a natural follow-on (mirroring `raftkv_
//!   linearizable.rs`'s own two-tier design) but is not built here.
//! - **What is still `ProdEnv`-only.** `SegmentStoreHandle`'s own `Cluster`
//!   variant (this fixture still uses an `Fs` placeholder there — nothing
//!   this fixture drives reads `ctx.segment_store`), and quiescence/
//!   heartbeat-batching/shared-WAL (the reconciler's own `enable_
//!   quiescence`/`enable_heartbeat_batching`/`enable_shared_wal` opt-ins
//!   are never called here, exactly as before this rung). **`ctx.
//!   backup_store` is NO LONGER a placeholder (ADR 0061 rung D4 PR 5)** —
//!   every node's own `BackupStoreHandle::S3` wraps a clone of one real,
//!   shared `SimSegmentStore` (see [`SimCluster::new`]'s own construction
//!   comment), and `animus_node::backup_janitor::backup_janitor_loop`
//!   runs on every node for real — see `sim_cluster_backup_janitor.rs`'s
//!   own module doc.
//!
//! # Updated since D1 (read this before trusting the bullets above at face
//! value)
//!
//! **ADR 0061 rung D3 PR 2a** closed the gap the "DDL is a control-plane-
//! Raft bypass" bullet above describes for `create_table_with_replication`
//! specifically (that bypass is still exactly how *this fixture's own*
//! hand-hosted tables are seeded — the bullet is accurate for that one
//! method) but is no longer true of `ClientCtx::propose_schema` in
//! general: [`ClusterEdgeState::control`] widened from a fixed
//! `Arc<Mutex<Vec<RaftNode<ProdEnv>>>>` to `Arc<Mutex<Vec<RaftNode<E>>>>`,
//! and [`SimCluster::new`] now calls `register_control` for every node —
//! `propose_schema`'s real leader-local fast path (and its non-leader
//! relay branch, now genuinely exercised for the first time rather than a
//! leader relaying `ProposeSchema` to itself and recursing to timeout)
//! both work under `SimEnv`. Reached today via `dynamo::create_table`/
//! `delete_table` (through `dynamo::dispatch_table_op`, `SimClusterHandle::
//! dynamo`'s own new DDL arm) — see `dynamo.rs`'s own doc.
//!
//! That same rung needed [`SimCluster::seed_members`] (populates
//! `Metadata::members` — `SimCluster::new` used to leave it permanently
//! empty) and a per-node `animus_control::node::heartbeat_loop` spawned in
//! both [`SimCluster::new`] and [`SimCluster::restart`] (a real,
//! previously-unreachable bug: a member seeded `Active` flipped back to
//! `Down` within `DETECT_TIMEOUT` with no heartbeat loop running — see
//! `docs/engineering-lessons.md`) to make wire DDL work at all, and a
//! minimal per-node watcher (`spawn_policy_tablet_host_loop`, since
//! deleted — see below) to host a wire-provisioned tablet, since this
//! fixture ran no reconciler at all at the time. That watcher's own
//! investigation found a real, deterministic gap (ADR 0061 rung D3 PR 2a's
//! own "reconciler-hazard" finding): it only ever *added* a replica newly
//! named in a tablet's `Metadata.tablets[t].replicas`, never tearing down
//! one a rebalance dropped — reachable with a plain `CreateTable` whenever
//! `node_count > MAX_REPLICATION_FACTOR` (3), genuinely split-brain-shaped.
//! `create_table_with_replication`'s own tablet-id minting also changed at
//! that rung — it used to keep its own fixture-local counter
//! (`next_tablet_id`, deleted), which could and did collide with
//! `Metadata::next_free_tablet_id()` (the wire path's own live allocator)
//! the moment a test used both paths in one cluster; it now reads that same
//! live allocator instead.
//!
//! **ADR 0061 rung D4 PR 1 (closing issue #715) deletes
//! `spawn_policy_tablet_host_loop` outright** and replaces it with the real
//! `animus_cp_data::host::Reconciler` (see the "Design choices" bullet
//! above) — `create_table_with_replication` no longer constructs a
//! `RaftKvNode` directly either; it provisions through `CreateTableSchema`/
//! `CreateTablet`/`SetTabletPolicy` and waits for the reconciler to host it,
//! exactly like a wire-created table. This closes the reconciler-hazard gap
//! structurally: the SAME mechanism that tears down a dropped replica in
//! production now runs under this fixture too, so `node_count >
//! MAX_REPLICATION_FACTOR` is no longer a hazard a `CreateTable` caller has
//! to avoid — see `sim_cluster_dynamo_table_ops.rs::
//! every_node_hosts_exactly_its_replica_set_after_rebalance` (the former
//! `reconciler_hazard_fires_deterministically_when_node_count_exceeds_
//! replication`, flipped from a characterization of the hazard to a
//! convergence proof it's closed). See `crates/animusd/CLAUDE.md`'s own D4
//! PR 1 entry and ADR 0061's matching 2026-09-07 amendment for the full
//! account of all of the above.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_cp_data::hlc::HlcTimestamp;
use animus_cp_data::{KIND_BASE, ResolveOutcome, StageOutcome, TxnId, TxnOutcome};
use animus_dynamo::AttributeValue;
use animus_env::{EnvExt, nid};
use animus_node::SimRelayClient;
// ADR 0061 rung D4 PR 4: the generic `RemoteControlClient<R>` — distinct
// from this crate's own `RemoteControlClient` (a `control_handle.rs` alias
// bound to `R = AnimusdRelayClient`, brought in by `super::*` and useless
// here) — is what [`SimCluster::grow`] installs on a growth node, bound to
// `R = SimRelayClient<SimEnv>` instead.
use animus_node::control_handle::RemoteControlClient as GenericRemoteControlClient;
use animus_sim::{NetConfig, SimEnv, SimSegmentStore, Simulator};

use super::*;

/// How long a single client op ([`SimCluster::put`]/`get`/`delete`/`scan`)
/// is driven before it's recorded as timed out. Generous: `CLIENT_TIMEOUT`
/// itself is 10s (the overall budget `forward_to_tablet_leader`'s own
/// hint-chasing loop and `cp_read`'s retry loop are bounded by), so this
/// covers one full such budget plus headroom for the surrounding
/// `spawn_task`/poll overhead — see `simenv_client_ctx_tests::
/// spawn_and_capture`'s own doc for why a generous, fixed budget (rather
/// than a tighter one tuned per call site) is the right shape here: a
/// multi-node fault scenario (a crashed leader, a partitioned minority)
/// can legitimately need the whole `CLIENT_TIMEOUT` window to either
/// converge or fail cleanly.
const OP_BUDGET: Duration = Duration::from_secs(12);

/// The fallback poll interval each node's own [`spawn_reconciler_loop`]
/// falls back to when no `metadata_watch()` wake fires — mirrors
/// `animusd::RECONCILE_FALLBACK_INTERVAL`'s event-driven-with-fallback
/// shape (that constant is 500ms in production), shorter here since
/// `SimEnv` virtual time costs nothing and a caller's own convergence
/// polls (`SimCluster::poll_until`, 50-100ms steps) still want the
/// reconciler to react promptly on the rare tick a real commit's own wake
/// is somehow missed. **Deliberately not as short as `poll_until`'s own
/// step** (an earlier draft used 50ms): every real hosting/reconfigure
/// decision is driven by `metadata_watch()`'s own wake, which resolves in
/// near-zero virtual time on an actual commit regardless of this value —
/// this constant only bounds the missed-wake safety net, and a long-
/// running scenario (the corpus's own multi-second `SETTLE`/fault-window/
/// `DRAIN`) pays for every fallback tick whether or not anything changed.
/// A first draft at 50ms measured ~3s/scenario in `sim_cluster_corpus.rs`
/// at `ANIMUS_SIMCLUSTER_SEEDS=3` (vs. ~1.75s/scenario pre-D4-PR-1);
/// 200ms cut that back down with no change in which scenario passes —
/// still 2.5x more responsive than production's own fallback.
const RECONCILER_FALLBACK: Duration = Duration::from_millis(200);

/// Build a fresh per-node `Reconciler<SimEnv, MemoryEngine>` (ADR 0061 rung
/// D4 PR 1) — mirrors `animusd`'s own production node assembly exactly
/// (`BoundNode::start_with`'s `on_host`/`on_teardown` closures): a fresh
/// hosting/re-registered-after-a-timed-out-teardown mirrors into `edge`'s
/// own routing registry, and a teardown unregisters from it — `Reconciler`
/// stays the single writer of "does this node host tablet T," `edge`'s own
/// `raftkv` map a read-only mirror of it, exactly like production.
fn build_reconciler(
    env: SimEnv,
    engines: MemoryTabletEngines,
    node_id: NodeId,
    edge: ClusterEdgeState<SimEnv>,
) -> Reconciler<SimEnv, MemoryEngine> {
    let host_edge = edge.clone();
    let teardown_edge = edge;
    let base_id = node_id.clone();
    Reconciler::new(
        env,
        engines,
        node_id,
        move |tablet, node: &RaftKvNode<SimEnv, MemoryEngine>| {
            host_edge.register_raftkv(tablet, CpGroup::Mem(node.clone()));
        },
        move |tablet| {
            teardown_edge.unregister_raftkv(tablet, base_id.clone());
        },
    )
}

/// Drive `reconciler`'s per-tick lifecycle on `ctx`'s own node — this
/// fixture's ONE tablet-hosting path since ADR 0061 rung D4 PR 1 (closing
/// issue #715, see the module doc's own "Updated since D3" section),
/// mirroring `animusd::tablet_host_reconciler_loop`'s own event-driven-
/// with-fallback shape: race `ctx.control.metadata_watch().changed(..)`
/// against a fixed [`RECONCILER_FALLBACK`] sleep, coalesce to the freshest
/// observed index regardless of which arm woke the loop, then tick once.
///
/// Spawned as its own task, **never `block_on`'d** — see this crate's own
/// `CLAUDE.md` (and `animus-cp-data/CLAUDE.md`'s Tests section): a tick
/// whose planned action tears a group down internally polls `env.sleep()`
/// while waiting for the driver to stop, which only resolves while
/// `Simulator::run_for` is advancing virtual time from OUTSIDE this task —
/// exactly the shape every caller of this fixture already drives through
/// (`SimCluster::poll_until`/`run_for`/`spawn_and_capture`).
///
/// **No `fork_wake()` arm and no pre-recovery `last_applied() == 0`
/// guard**, unlike `animusd::tablet_host_reconciler_loop`: this fixture
/// never splits a tablet (no `SplitTablet` fork this arm would ever need
/// to notice sooner — see the module doc's own restart bullet), and
/// [`SimCluster::new`] already drives the control group's first election
/// to completion before any caller can reach this loop, so
/// `effective_metadata()` never reads as the pre-recovery empty default a
/// genuinely cold node's `ctx.control.last_applied() == 0` window guards
/// against in production.
fn spawn_reconciler_loop(ctx: SimNodeCtx, mut reconciler: Reconciler<SimEnv, MemoryEngine>) {
    ctx.env.clone().spawn_task(async move {
        let watch = ctx.control.metadata_watch();
        let mut last_seen = watch.latest();
        loop {
            let env = ctx.env.clone();
            let _ =
                futures::future::select(watch.changed(last_seen), env.sleep(RECONCILER_FALLBACK))
                    .await;
            // Coalesce: take the freshest observed index regardless of
            // which arm woke the loop, mirroring `tablet_host_reconciler_
            // loop`'s own identical comment.
            last_seen = watch.latest();

            let meta = ctx.effective_metadata();
            let down: BTreeSet<NodeId> = meta
                .members
                .iter()
                .filter(|(_, m)| m.status == NodeStatus::Down)
                .map(|(id, _)| id.clone())
                .collect();
            let view = MetadataView {
                tablets: meta.tablets,
                down,
            };
            reconciler.tick(&view).await;
        }
    });
}

/// A `SimEnv`-native re-implementation of `animusd`'s own
/// `remote_metadata_watch_loop`/`remote_metadata_sync_loop` (ADR 0061 rung
/// D4 PR 4) — those functions are structurally unreachable under `SimEnv`:
/// `ClientCtx`'s bare name defaults to `E = ProdEnv`/`R = AnimusdRelayClient`
/// (so a `ClientCtx<SimEnv, SimRelayClient<SimEnv>>` doesn't even type-check
/// as their parameter), and `remote_metadata_watch_loop`'s own retry backoff
/// is a bare `tokio::time::sleep` — the `Env`-seam violation this fixture's
/// whole reason for existing (`SimEnv`-driven determinism) can't route
/// around. This is a **new, parallel implementation of the same long-poll
/// protocol**, not a generalization of the production function (which
/// stays untouched) — it drives the exact same wire round trip
/// (`ClientRequest::WatchMetadata`/`Status`, `RemoteControlClient::observe`/
/// `observe_delta`) against the exact same [`GenericRemoteControlClient`]
/// type production's own `ControlHandle::Remote` wraps, so what's actually
/// exercised — the mirror's real observe/delta/leader-hint logic — is
/// identical; only the executor and the sleep primitive differ. This is
/// the one genuinely new mechanism [`SimCluster::grow`] adds: a
/// `ControlHandle::Remote` node's mirror-sync path had never run under
/// `SimEnv` before this rung.
///
/// Spawned once per growth node, mirroring [`spawn_reconciler_loop`]'s own
/// "spawn as its own task, never `block_on`'d" discipline — every `.await`
/// inside only resolves while a caller elsewhere is driving
/// `Simulator::run_for`.
fn spawn_remote_mirror_sync_loop(
    ctx: SimNodeCtx,
    remote: GenericRemoteControlClient<SimRelayClient<SimEnv>>,
    seeds: Vec<String>,
) {
    ctx.env.clone().spawn_task(async move {
        loop {
            let last_seen = remote.metadata_watch().latest();
            let mut candidates = Vec::with_capacity(seeds.len() + 1);
            if let Some(addr) = remote.intra_leader_addr_hint() {
                candidates.push(addr);
            }
            candidates.extend(seeds.iter().cloned());

            let mut synced = false;
            for addr in candidates {
                match remote
                    .relay()
                    .relay(
                        addr,
                        &ClientRequest::WatchMetadata { last_seen },
                        WATCH_METADATA_CLIENT_TIMEOUT,
                    )
                    .await
                {
                    ClientResponse::Status {
                        metadata,
                        leader_hint,
                        intra_leader_hint,
                        watermark,
                        control_voters,
                    } => {
                        remote.observe(
                            metadata,
                            leader_hint,
                            intra_leader_hint,
                            watermark,
                            control_voters,
                        );
                        synced = true;
                        break;
                    }
                    ClientResponse::MetadataDelta {
                        writes,
                        watermark,
                        leader_hint,
                        intra_leader_hint,
                        control_voters,
                    } => {
                        remote.observe_delta(
                            last_seen,
                            &writes,
                            leader_hint,
                            intra_leader_hint,
                            watermark,
                            control_voters,
                        );
                        synced = true;
                        break;
                    }
                    _ => {}
                }
            }
            if synced {
                continue;
            }
            for addr in &seeds {
                if let ClientResponse::Status {
                    metadata,
                    leader_hint,
                    intra_leader_hint,
                    watermark,
                    control_voters,
                } = remote
                    .relay()
                    .relay(addr.clone(), &ClientRequest::Status, CLIENT_TIMEOUT)
                    .await
                {
                    remote.observe(
                        metadata,
                        leader_hint,
                        intra_leader_hint,
                        watermark,
                        control_voters,
                    );
                    break;
                }
            }
            ctx.env.sleep(REMOTE_WATCH_RETRY_BACKOFF).await;
        }
    });
}

/// This fixture's fixed key encoding: every table's items are addressed by
/// a `(pk, sk)` pair of DynamoDB `S` (string) attributes, run through the
/// real `dynamo::item_key` (ADR 0022/0023 token + escape) — the identical
/// wire-key shape `cp_kind_write_raw`/`cp_get`'s production callers build,
/// not a simplified stand-in. A table need not actually declare a sort key
/// in its own `TableSchema` for this to work (the KV write/read path below
/// `dynamo.rs` never validates a key against the schema), but every
/// `SimCluster` table this fixture creates does declare one (`composite_schema`)
/// so a real composite-key table is what's actually being exercised.
fn item_key(pk: &str, sk: &str) -> Vec<u8> {
    dynamo::item_key(
        &AttributeValue::S(pk.to_owned()),
        Some(&AttributeValue::S(sk.to_owned())),
    )
}

/// A placeholder socket address for `AdminInfo`'s fields — never dialed
/// (this fixture never binds a listener), the same stand-in
/// `simenv_client_ctx_tests`/`two_node_relay_tests` use.
fn placeholder_addr() -> SocketAddr {
    "127.0.0.1:1".parse().expect("valid placeholder addr")
}

/// [`SimCluster::scan`]'s own row shape — named, mirroring `lib.rs`'s own
/// `StageConditions` alias, to keep clippy's `type_complexity` lint happy
/// (the same "name it instead of nesting it inline" convention that
/// alias's own doc states).
type ScanRows = Vec<(Vec<u8>, Vec<u8>)>;

/// One node's `ClientCtx` handle, named for the same clippy `type_complexity`
/// reason as [`ScanRows`] — [`SimClusterHandle`]'s own `ctxs` field is a
/// `Vec` of these behind an `Arc<Mutex<..>>`.
type SimNodeCtx = ClientCtx<SimEnv, SimRelayClient<SimEnv>>;

/// What this fixture knows about one tablet it has provisioned: its table
/// name (diagnostics only), and the node ids named as its INITIAL replica
/// set (in the order [`SimCluster::create_table`] chose them —
/// `replicas[0]` has no special status, it's simply this fixture's own
/// bookkeeping order, not a leader hint). **A static snapshot, not a live
/// mirror** — since ADR 0061 rung D4 PR 1, the tablet's real `Metadata`
/// replica set can move under the control plane's own ordinary rebalance
/// (`cluster.metadata(node).tablets[..].replicas` is the live source; see
/// [`SimClusterHandle::hosted_tablets`]).
///
/// `Clone` (ADR 0061 rung D1 step 3, the [`SimClusterHandle`] refactor
/// below): a corpus's own concurrently-spawned client tasks read a
/// **snapshot** of this map (`SimClusterHandle::replicas_of`) rather than
/// holding the shared lock across an `.await`.
#[derive(Clone)]
struct TabletInfo {
    table: String,
    replicas: Vec<u64>,
}

/// A cheap, `Clone`-able handle onto this cluster's per-node `ClientCtx`s
/// and provisioned-tablet bookkeeping (ADR 0061 rung D1 step 3, "What
/// commit 3 needs" item (b)) — every field the *driver* (`SimCluster`,
/// below) mutates, behind a `Mutex` so a corpus's own concurrently
/// `env.spawn_task`-ed client-op tasks can share one cluster: each op
/// method below clones the target node's own `ClientCtx` out from under a
/// **brief** lock (never held across an `.await` — `ClientCtx::clone` is
/// cheap, every field is either `Copy`, an `Arc`, or a small handle) and
/// then awaits on that owned clone, so many concurrent ops on different
/// (or the same) node never contend on the lock for longer than a clone.
///
/// **Why a handle at all, instead of just `Arc<Mutex<SimCluster>>`**: the
/// driver's own fault-injection methods (`crash`/`restart`/`partition`/
/// `heal_all`/`run_for`) also need `&mut self.sim` (`Simulator` is not
/// `Sync`-shareable the way a plain data map is) — see the module's own
/// "Design decisions" doc, below, for why those stay `&mut self` methods
/// on the outer [`SimCluster`] rather than moving onto this handle too.
/// `SimClusterHandle` carries only the two fields client-issued ops
/// actually read/write (`ctxs`, `tablets`); `sim`/`controls`/`crashed` stay
/// exclusively on the driver.
///
/// Every op method here is self-bounded: `ClientCtx::cp_kind_write_raw`/
/// `cp_get`/`cp_scan` each carry their own internal `CLIENT_TIMEOUT`-bounded
/// retry loop (`cp_route`'s own deadline, `forward_to_tablet_leader`'s own
/// hint-chasing deadline), so a call here always resolves — `Ok` or a
/// timeout-shaped `Err` — well inside the corpus's own per-op poll window,
/// with **no wrapper `spawn_and_capture`/`OP_BUDGET` needed for a handle
/// method awaited directly inside an already-spawned task** (unlike
/// [`SimCluster::put`]/`get`/`delete`/`scan` below, which still need that
/// wrapper because they're driven synchronously from a test's own `&mut
/// self` call, not from inside a task the corpus itself spawned).
#[derive(Clone)]
pub(crate) struct SimClusterHandle {
    ctxs: Arc<Mutex<Vec<SimNodeCtx>>>,
    tablets: Arc<Mutex<BTreeMap<TabletId, TabletInfo>>>,
}

impl SimClusterHandle {
    fn new(ctxs: Vec<SimNodeCtx>) -> Self {
        SimClusterHandle {
            ctxs: Arc::new(Mutex::new(ctxs)),
            tablets: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    /// A brief-lock clone of `node`'s own `ClientCtx` — every op method
    /// below builds on this rather than holding the lock across an
    /// `.await`. `ClientCtx::clone` is cheap (every field is `Copy`, an
    /// `Arc`, or a small handle) — see [`SimClusterHandle`]'s own doc.
    fn ctx(&self, node: u64) -> SimNodeCtx {
        self.ctxs.lock().expect("ctxs poisoned")[node as usize].clone()
    }

    fn set_ctx(&self, node: u64, ctx: SimNodeCtx) {
        self.ctxs.lock().expect("ctxs poisoned")[node as usize] = ctx;
    }

    /// Append a freshly built node's own `ClientCtx` (ADR 0061 rung D4 PR 4,
    /// [`SimCluster::grow`]) — unlike [`set_ctx`](Self::set_ctx), which
    /// replaces an existing index, this grows the vec by one. The pushed
    /// ctx's own index becomes its node id, so the caller must push at
    /// exactly `ctxs.len()` (i.e. `SimCluster::node_count()`, read BEFORE
    /// this call) — [`SimCluster::grow`] is this fixture's one caller and
    /// upholds that by construction.
    fn push_ctx(&self, ctx: SimNodeCtx) {
        self.ctxs.lock().expect("ctxs poisoned").push(ctx);
    }

    fn insert_tablet(&self, tablet: TabletId, info: TabletInfo) {
        self.tablets
            .lock()
            .expect("tablets poisoned")
            .insert(tablet, info);
    }

    fn all_have_table_tablet(&self, table: &str) -> bool {
        self.ctxs
            .lock()
            .expect("ctxs poisoned")
            .iter()
            .all(|ctx| ctx.effective_metadata().has_table_tablet(table))
    }

    /// ADR 0061 rung D3 PR 2a: every node's own view of `Metadata::members`
    /// shows every node id (`0..node_count`, derived from `ctxs.len()`
    /// rather than taking a parameter — this handle already knows the
    /// cluster's whole node set) `Active` — the convergence check
    /// [`SimCluster::seed_members`] polls on, mirroring
    /// [`all_have_table_tablet`]'s own shape.
    fn all_members_active(&self) -> bool {
        let ctxs = self.ctxs.lock().expect("ctxs poisoned");
        let count = ctxs.len() as u64;
        ctxs.iter().all(|ctx| {
            let meta = ctx.effective_metadata();
            (0..count).all(|n| {
                meta.members
                    .get(&nid(n))
                    .is_some_and(|m| m.status == NodeStatus::Active)
            })
        })
    }

    /// ADR 0065 §5(b): every node's own view of `table`'s per-table
    /// `throughput` matches `spec` — the `SetTableThroughput` convergence
    /// check [`SimCluster::set_table_throughput`] polls on, mirroring
    /// [`all_have_table_tablet`]'s own shape.
    fn all_have_table_throughput(
        &self,
        table: &str,
        spec: Option<&animus_control::ProvisionedThroughput>,
    ) -> bool {
        self.ctxs
            .lock()
            .expect("ctxs poisoned")
            .iter()
            .all(|ctx| ctx.effective_metadata().table_throughput(table) == spec)
    }

    /// ADR 0065 §5(b): recompute every node's own `ClientCtx::
    /// any_table_throughput` flag from that node's own just-converged
    /// `effective_metadata()` — this fixture never spawns
    /// `index_drain::change_consumer_loop` (the real cluster's own
    /// metadata-watch recompute site), and its DDL calls
    /// (`SimCluster::create_table_with_replication`/`set_table_throughput`)
    /// propose directly on the control leader rather than through
    /// `dynamo::create_table`/`update_table_throughput` (the real
    /// cluster's own synchronous recompute site), so nothing in this
    /// harness would otherwise ever flip the flag at all. Called after
    /// [`SimCluster::set_table_throughput`]'s own convergence poll, once
    /// every node's metadata already agrees.
    fn recompute_any_table_throughput_all(&self) {
        for ctx in self.ctxs.lock().expect("ctxs poisoned").iter() {
            let meta = ctx.effective_metadata();
            ctx.recompute_any_table_throughput(&meta);
        }
    }

    fn is_leader_local(&self, node: u64, tablet: TabletId) -> bool {
        self.ctxs.lock().expect("ctxs poisoned")[node as usize]
            .edge
            .local_cp(tablet)
            .is_some_and(|g| g.is_leader())
    }

    /// ADR 0065's test-reachable hook (`ClientCtx::set_throttle_defaults`)
    /// on one node — mutates through the node's own `Arc<ThrottleDefaults>`,
    /// so it's visible to every clone of that node's `ClientCtx`, including
    /// the one closed over by its own relay-serving handler (installed
    /// before this handle's `ctxs` vec was ever built — both clones share
    /// the identical `Arc`). Plain sync: `set_throttle_defaults` itself
    /// does no I/O and needs no simulator drive.
    fn set_throttle_defaults(&self, node: u64, read_units: Option<u64>, write_units: Option<u64>) {
        self.ctxs.lock().expect("ctxs poisoned")[node as usize]
            .set_throttle_defaults(read_units, write_units);
    }

    /// `node`'s own internal `SimEnv` — the env a corpus spawns a
    /// node-issued op's own driving task onto, mirroring [`SimCluster::
    /// spawn_and_capture`]'s identical read. Cheap: `SimEnv` is itself a
    /// small `Clone`-able handle.
    pub(crate) fn env(&self, node: u64) -> SimEnv {
        self.ctx(node).env.clone()
    }

    /// The tablet id [`SimCluster::create_table`] minted for `table`, if
    /// this fixture created one.
    pub(crate) fn tablet_of(&self, table: &str) -> Option<TabletId> {
        self.tablets
            .lock()
            .expect("tablets poisoned")
            .iter()
            .find(|(_, info)| info.table == table)
            .map(|(&tablet, _)| tablet)
    }

    /// `tablet`'s own provisioned replica set, in [`SimCluster::
    /// create_table`]'s own bookkeeping order — empty if this fixture never
    /// provisioned `tablet` (should not happen for a tablet id this handle
    /// itself minted, but a corpus's own bug should read "no replicas"
    /// rather than panic).
    pub(crate) fn replicas_of(&self, tablet: TabletId) -> Vec<u64> {
        self.tablets
            .lock()
            .expect("tablets poisoned")
            .get(&tablet)
            .map(|info| info.replicas.clone())
            .unwrap_or_default()
    }

    /// The node id currently hosting `tablet`'s own leader replica, if any
    /// one of its known replicas believes it leads — [`SimCluster::
    /// leader_index_of`]'s handle-callable twin.
    ///
    /// **Scans every node id, not just [`Self::replicas_of`]'s own
    /// bookkeeping** (ADR 0061 rung D3 PR 3b) — `replicas_of` only ever
    /// knows about a table `SimCluster::create_table_with_replication`
    /// hand-hosted (the one call site that populates `self.tablets`); a
    /// **wire**-provisioned table (`ClientCtx::provision_tablet`, reached
    /// through `dynamo::dispatch_table_op`'s own `CreateTable` arm) never
    /// gets an entry there at all, so the old `replicas_of`-scoped scan
    /// always came up empty for one — found live converting `SimCluster::
    /// drain_gsi`'s own callers, every one of which creates its table
    /// through the real wire (`cluster.dynamo(.., "..CreateTable", ..)`),
    /// not `create_table_with_replication`. Scanning every node is strictly
    /// more general and no less correct for a hand-hosted table either:
    /// `is_leader_local` already answers `false` for any node hosting no
    /// replica of `tablet` at all (`ClusterEdgeState::local_cp` returns
    /// `None` there), so a non-replica node was always going to be skipped
    /// regardless of which set this loop iterates.
    pub(crate) fn leader_index_of(&self, tablet: TabletId) -> Option<u64> {
        let count = self.ctxs.lock().expect("ctxs poisoned").len() as u64;
        (0..count).find(|&n| self.is_leader_local(n, tablet))
    }

    /// `node`'s own view of the replicated control-plane `Metadata`.
    pub(crate) fn metadata(&self, node: u64) -> Metadata {
        self.ctx(node).effective_metadata()
    }

    /// Every tablet id `node`'s own `ClusterEdgeState` currently holds a
    /// live CP group handle for (ADR 0061 rung D4 PR 1) — regardless of a
    /// tablet's origin (hand-hosted via [`SimCluster::
    /// create_table_with_replication`] or wire-provisioned), since both are
    /// discovered and hosted by the same real `host::Reconciler` now. The
    /// "no zombie groups" invariant (a stale handle for a replica a
    /// rebalance moved off this node) compares this directly against
    /// `Metadata::tablets`' own current replica sets — see
    /// `sim_cluster_dynamo_table_ops.rs::
    /// every_node_hosts_exactly_its_replica_set_after_rebalance`.
    pub(crate) fn hosted_tablets(&self, node: u64) -> BTreeSet<TabletId> {
        self.ctx(node)
            .edge
            .hosted_groups()
            .into_iter()
            .map(|(tablet, _group)| tablet)
            .collect()
    }

    /// Write `value` at `(pk, sk)` in `table`, issued from `node`'s own
    /// `ClientCtx` — awaited directly (no wrapper): see this type's own doc
    /// for why every op here is already self-bounded.
    pub(crate) async fn put(
        &self,
        node: u64,
        table: &str,
        pk: &str,
        sk: &str,
        value: &[u8],
    ) -> Result<(), String> {
        let ctx = self.ctx(node);
        let key = item_key(pk, sk);
        ctx.cp_kind_write_raw(
            table,
            vec![(KIND_BASE, key, Some(value.to_vec()))],
            Vec::new(),
        )
        .await
    }

    /// Delete the item at `(pk, sk)` in `table`, issued from `node`'s own
    /// `ClientCtx` — [`SimClusterHandle::put`]'s sibling.
    pub(crate) async fn delete(
        &self,
        node: u64,
        table: &str,
        pk: &str,
        sk: &str,
    ) -> Result<(), String> {
        let ctx = self.ctx(node);
        let key = item_key(pk, sk);
        ctx.cp_kind_write_raw(table, vec![(KIND_BASE, key, None)], Vec::new())
            .await
    }

    /// Read `(pk, sk)` in `table`, issued from `node`'s own `ClientCtx` —
    /// see [`SimCluster::get`]'s own doc for the `consistent` contract.
    pub(crate) async fn get(
        &self,
        node: u64,
        table: &str,
        pk: &str,
        sk: &str,
        consistent: bool,
    ) -> Result<Option<Vec<u8>>, String> {
        let ctx = self.ctx(node);
        let key = item_key(pk, sk);
        match ctx.cp_get(table, key, !consistent).await {
            ClientResponse::Value(v) => Ok(v),
            ClientResponse::Error(e) => Err(e),
            other => Err(format!("unexpected get response: {other:?}")),
        }
    }

    /// Whole-table scan, issued from `node`'s own `ClientCtx`.
    pub(crate) async fn scan(
        &self,
        node: u64,
        table: &str,
        consistent: bool,
    ) -> Result<ScanRows, String> {
        let ctx = self.ctx(node);
        let consistency = ReadConsistency::from_consistent_read(consistent);
        ctx.cp_scan(table, Vec::new(), None, None, false, consistency)
            .await
    }

    /// A **raw, unrouted** local-engine read of `(pk, sk)` on `node`'s own
    /// replica of `tablet` — `None` if `node` hosts no replica of `tablet`
    /// or the key is absent there. Mirrors `raftkv_linearizable.rs`'s own
    /// `final_state`'s use of `local_get` (never `cp_get`, which always
    /// routes to *a* leader and so can never distinguish two replicas'
    /// own raw state) — the primitive a corpus's own cross-replica
    /// durability/convergence check needs.
    pub(crate) async fn local_value(
        &self,
        node: u64,
        tablet: TabletId,
        pk: &str,
        sk: &str,
    ) -> Option<Vec<u8>> {
        let ctx = self.ctx(node);
        let group = ctx.edge.local_cp(tablet)?;
        group.local_get(&item_key(pk, sk)).await
    }

    /// Run a decoded DynamoDB wire request (`X-Amz-Target` + JSON body)
    /// against `node`'s own `ClientCtx`, through the exact same
    /// `dynamo::execute_item_op_as` production's TCP listener calls
    /// (ADR 0061 rung D2 PR 1) — never a bespoke test-only reimplementation.
    /// An unrestricted [`crate::authz::Principal`], mirroring `admin.rs::
    /// action_data_dynamo`'s own unauthenticated proxy: this fixture has no
    /// SigV4 listener in front of it to resolve a scoped one from (see the
    /// module doc's own "still `ProdEnv`-only" bullet — `ctx.dynamo_auth` is
    /// `None` on every node here, exactly like every sibling `SimEnv`
    /// harness). Only the eight operations `dispatch_item_op` covers today
    /// succeed; everything else decodes fine and comes back a clean
    /// `InternalServerError` — see that function's own doc for the list.
    /// Self-bounded like every other op method here (`ClientCtx::
    /// cp_kind_write_item`/`cp_read`/`cp_scan`'s own internal
    /// `CLIENT_TIMEOUT` budgets), so callable directly inside an
    /// `env.spawn_task`-ed future with no wrapper needed.
    pub(crate) async fn dynamo(&self, node: u64, target: &str, body: &[u8]) -> (u16, String) {
        let ctx = self.ctx(node);
        crate::dynamo::execute_item_op_as(
            &ctx,
            &crate::authz::Principal::unrestricted(),
            target,
            body,
        )
        .await
    }

    /// Stage a **fresh anchor** transaction writing `value` at `key` on
    /// `table`, retrying through a decided-but-still-unresolved blocker via
    /// `push_resolution_if_decided` exactly like production — issue #734's
    /// `ClientCtx::txn_prepare_pushing`, issued from `node`'s own
    /// `ClientCtx`. This fixture spawns no `txn_resolver_loop`
    /// (`SimCluster`'s own module doc — no background loops at all), so
    /// unlike the real-thread `issue_298_conflict_tests` regression this is
    /// immune to that loop's real-time race by construction: nothing but a
    /// scenario's own explicit calls can ever touch an intent here.
    pub(crate) async fn txn_prepare_pushing(
        &self,
        node: u64,
        table: &str,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
    ) -> Result<(TxnId, Vec<u8>, String, HlcTimestamp), TxnAbortReason> {
        let ctx = self.ctx(node);
        ctx.txn_prepare_pushing(
            table,
            None,
            vec![TxnWrite::plain(key, value)],
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .await
    }

    /// A single, direct anchor stage attempt for `value` at `key` on
    /// `table` — never [`txn_prepare_pushing`](Self::txn_prepare_pushing)'s
    /// own retry loop, so a caller can observe exactly what ONE stage
    /// attempt reports (`StageOutcome::IntentBlocked`/`Staged`/...),
    /// issued from `node`'s own `ClientCtx`.
    pub(crate) async fn txn_prepare_once(
        &self,
        node: u64,
        table: &str,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
    ) -> Result<(TxnId, Vec<u8>, String, HlcTimestamp, StageOutcome), TxnAbortReason> {
        let ctx = self.ctx(node);
        ctx.txn_prepare(
            table,
            None,
            vec![TxnWrite::plain(key, value)],
            Vec::new(),
            Vec::new(),
            Vec::new(),
        )
        .await
    }

    /// Decide `txn_id`'s anchor record `commit`/abort — `ClientCtx::
    /// txn_decide_anchor`, issued from `node`'s own `ClientCtx`. Never
    /// resolves the transaction's own intents (matching production: decide
    /// and resolve are always two separate calls) — see
    /// [`push_resolution_if_decided`](Self::push_resolution_if_decided)/
    /// [`txn_resolve_participant`](Self::txn_resolve_participant) for that.
    pub(crate) async fn txn_decide_anchor(
        &self,
        node: u64,
        table: &str,
        txn_id: TxnId,
        record_key: Vec<u8>,
        commit: bool,
        min_commit_ts: HlcTimestamp,
    ) -> Result<TxnOutcome, String> {
        let ctx = self.ctx(node);
        ctx.txn_decide_anchor(table, txn_id, record_key, commit, min_commit_ts, None)
            .await
    }

    /// `ClientCtx::push_resolution_if_decided` (issue #298 residual fix,
    /// this fixture's regression for the mechanism issue #734's `Node::
    /// abort_background_tasks_for_test` had to isolate on the real-thread
    /// side) — if `blocker`'s own record already decided, pushes its
    /// resolution before returning; a no-op otherwise. Issued from `node`'s
    /// own `ClientCtx`.
    pub(crate) async fn push_resolution_if_decided(
        &self,
        node: u64,
        table: &str,
        blocked_key: Vec<u8>,
        blocker: TxnId,
        blocker_record_table: String,
        blocker_record_key: Vec<u8>,
    ) {
        let ctx = self.ctx(node);
        ctx.push_resolution_if_decided(
            table,
            &blocked_key,
            blocker,
            blocker_record_table,
            blocker_record_key,
            0,
        )
        .await
    }

    /// `ClientCtx::txn_resolve_participant` — a single, one-shot resolve of
    /// `keys` per `outcome`, issued from `node`'s own `ClientCtx`. See that
    /// method's own doc for why `Ok(ResolveOutcome::Fenced)` means "retry
    /// with fresh routing," never "done."
    pub(crate) async fn txn_resolve_participant(
        &self,
        node: u64,
        table: &str,
        txn_id: TxnId,
        record_key: Vec<u8>,
        keys: Vec<Vec<u8>>,
        outcome: TxnOutcome,
    ) -> Result<ResolveOutcome, String> {
        let ctx = self.ctx(node);
        ctx.txn_resolve_participant(table, txn_id, record_key, keys, outcome)
            .await
    }
}

/// See the module doc for the full design. Every node id is `0..nodes`; a
/// [`SimCluster`] method addresses a node by that plain `u64` index (never
/// a wrapped [`NodeId`] — `nid(index)` is the one, infallible conversion
/// this whole fixture uses, mirroring `raftkv_linearizable.rs`'s own
/// `GROUP_IDS: [u64; N]` convention), except [`SimCluster::leader_of`],
/// which hands back a real `NodeId` since that's what a caller comparing
/// against `Metadata`/wire-level identities actually wants.
pub(crate) struct SimCluster {
    sim: Simulator,
    /// Total node count — every id in `0..nodes` is a control-plane voter.
    nodes: usize,
    /// The default replication factor [`SimCluster::create_table`] hosts a
    /// fresh table's tablet on (nodes `0..replication`) — a caller wanting
    /// a different factor for one table uses
    /// [`SimCluster::create_table_with_replication`] instead.
    replication: usize,
    /// One control `RaftNode<SimEnv>` per node id, index == node id — the
    /// real multi-voter quorum (see the module doc's DDL-bypass bullet for
    /// why this fixture proposes on these handles directly rather than
    /// through any node's own `ClientCtx`).
    controls: Vec<RaftNode<SimEnv>>,
    /// The `Clone`-able, `Mutex`-backed handle onto every node's own
    /// `ClientCtx<SimEnv, SimRelayClient<SimEnv>>` and the provisioned-
    /// tablet bookkeeping (ADR 0061 rung D1 step 3) — [`SimCluster::
    /// handle`] hands a cheap clone of this to a corpus's own concurrently
    /// spawned client-op tasks; every driver method below that used to read
    /// `self.ctxs`/`self.tablets` directly now goes through it too, so
    /// there is exactly one copy of this bookkeeping, shared identically by
    /// the driver and by any handle a corpus holds.
    shared: SimClusterHandle,
    /// One [`MemoryTabletEngines`] registry per node, index == node id (ADR
    /// 0061 rung D4 PR 1) — the per-node engine seam each node's own
    /// `Reconciler` opens through. Kept here (not just inside the
    /// reconciler each node's driving task owns) so [`SimCluster::restart`]
    /// can hand the SAME registry to a freshly built `Reconciler`, modeling
    /// a durable engine surviving a process crash — see the module doc's
    /// own restart bullet.
    engines: Vec<MemoryTabletEngines>,
    /// Node ids currently [`SimCluster::crash`]ed (muted, tasks still
    /// alive) — tracked so [`SimCluster::heal_all`] knows which ones need
    /// `Simulator::restart` (the un-mute call, unrelated to this struct's
    /// own [`SimCluster::restart`] method despite the shared name — see
    /// that method's own doc).
    crashed: BTreeSet<u64>,
    /// ADR 0061 rung D4 PR 2: the auto-split trigger thresholds this
    /// cluster is opted into, if any — `None` (the default every existing
    /// scenario gets) means `auto_split_loop` is never spawned at all.
    /// Stored so [`SimCluster::restart`] can respawn the loop with the
    /// SAME configuration a restarted node's `Simulator::stop` just
    /// dropped, mirroring how it already respawns `heartbeat_loop`/the
    /// reconciler. Set via [`SimCluster::set_auto_split_thresholds`].
    auto_split: Option<AutoSplitThresholds>,
    /// ADR 0061 rung D4 PR 5: the ONE `SimSegmentStore` every node's own
    /// `ClientCtx::backup_store` (`BackupStoreHandle::S3`) wraps a clone
    /// of — see [`SimCluster::new`]'s own construction comment for why one
    /// shared store, not a per-node local directory. Kept here (not just
    /// inside each `ClientCtx`) so [`SimCluster::backup_store`] can hand a
    /// test a handle for direct assertions/seeding, mirroring `engines`'
    /// own "kept at the driver level for outside access" role above.
    backup_store: SimSegmentStore,
}

impl SimCluster {
    /// Build a fresh `nodes`-node cluster: one `Simulator::new(seed)`, a
    /// real `nodes`-voter control `RaftNode<SimEnv>` quorum, a
    /// `SimRelayClient<SimEnv>` per node with its relay server already
    /// installed, and a `ClientCtx<SimEnv, SimRelayClient<SimEnv>>` per
    /// node whose `client_route`/`intra_route` already name every node
    /// (see the module doc). `replication` is this fixture's own default
    /// replication factor for [`SimCluster::create_table`] — it does not
    /// itself constrain `nodes` beyond the obvious `1..=nodes`.
    ///
    /// Settles the control group (drives past its first election) before
    /// returning, so a caller's very first [`SimCluster::create_table`]
    /// call finds a leader immediately rather than needing its own
    /// warm-up wait.
    pub(crate) fn new(seed: u64, nodes: usize, replication: usize) -> Self {
        assert!(nodes >= 1, "a cluster needs at least one node");
        assert!(
            (1..=nodes).contains(&replication),
            "replication must be between 1 and the node count"
        );
        let sim = Simulator::new(seed);
        let ids: Vec<NodeId> = (0..nodes as u64).map(nid).collect();

        // ADR 0061 rung C3d's own convention: a `SimRelayClient` address
        // IS `NodeId::to_string()`. Every node's whole address book is
        // known up front (this fixture never grows), so both routing
        // tables are simply "every node, by its own address" from the
        // start — no `route_sync_loop`/`intra_route_sync_loop` equivalent
        // is needed here.
        let route: BTreeMap<NodeId, String> =
            ids.iter().map(|id| (id.clone(), id.to_string())).collect();

        let controls: Vec<RaftNode<SimEnv>> = ids
            .iter()
            .map(|id| RaftNode::start(sim.env(id.clone()), ids.clone(), MemoryEngine::new()))
            .collect();

        // ADR 0061 rung D3 PR 2a — real finding, not part of the original
        // design pass: every node heartbeats every control voter
        // (`animus_control::node::heartbeat_loop`, the identical loop a
        // real deployment's `BoundNode::start_with` spawns per member),
        // load-bearing for `Metadata::members` to stay `Active` at all.
        // Without this, `RaftNode`'s own `detect_loop` — spawned
        // unconditionally by `RaftNode::start` itself, not by this fixture
        // — flips every seeded member back to `Down` within `DETECT_TIMEOUT`
        // (500ms) of `SimCluster::new` returning: `detect_loop`'s "phantom-
        // member hardening" (ADR 0030, `node.rs`) gives an `Active`-but-
        // untracked member exactly one **synthetic** observation the first
        // tick it sees one, and with no further *real* heartbeat ever
        // arriving, that synthetic timestamp ages out like any other and
        // the member is judged dead — the opposite of the design pass's own
        // claim that "a directly-activated member cannot flip back to
        // `Down`" (that claim covers only the *orphan sweep*, a different
        // mechanism gated on `has_activated`, not the liveness detector
        // gated on `FailureDetector::tracks`). Confirmed empirically before
        // this fix landed: a `provision_tablet` call issued more than
        // ~500ms of virtual time after `SimCluster::new` (i.e. essentially
        // every real one, since a single `SimCluster::dynamo` call alone
        // burns `OP_BUDGET` = 12s) found every member `Down` and could never
        // pick a non-empty replica set, hanging until its own commit-wait
        // deadline. See `docs/engineering-lessons.md`'s matching entry.
        for id in &ids {
            let env = sim.env(id.clone());
            let control_ids = ids.clone();
            env.spawn_task(animus_control::node::heartbeat_loop(
                env.clone(),
                control_ids,
            ));
        }

        let relays: Vec<SimRelayClient<SimEnv>> = ids
            .iter()
            .map(|id| SimRelayClient::new(sim.env(id.clone())))
            .collect();

        // ADR 0061 rung D4 PR 5: ONE shared `SimSegmentStore`, not a
        // per-node placeholder — see the module doc's own "backup store
        // choice" note for why `BackupStoreHandle::S3` (a single shared
        // object store, no per-node local directory) is the right variant
        // to wrap it in, unlike `Fs`/`Cluster`'s own per-node-local-
        // directory shape every other `sim_cluster_*` fixture's placeholder
        // uses. Every node's own `BackupStoreHandle::S3` below wraps its
        // own `Arc<dyn SegmentStore>` around the SAME underlying
        // `SimSegmentStore` (cheap to clone — its own state lives behind an
        // `Arc<Mutex<..>>`), so every node's `list_local`/`delete_local`
        // sees every other node's own writes, exactly like a real S3
        // bucket would.
        let backup_store = SimSegmentStore::new(sim.env(ids[0].clone()));

        let mut ctxs: Vec<SimNodeCtx> = Vec::with_capacity(nodes);
        for (i, id) in ids.iter().enumerate() {
            let admin = Arc::new(AdminInfo {
                auto_split_ops_rate_threshold: None,
                throttle_read_units: None,
                throttle_write_units: None,
                node_id: Some(id.clone()),
                internal_addr: Some(placeholder_addr()),
                client_addr: placeholder_addr(),
                dynamo_addr: None,
                admin_addr: placeholder_addr(),
                role: "combined",
                control_ids: ids.clone(),
                peers: BTreeMap::new(),
                admin_addrs: vec![placeholder_addr()],
                auto_split_bytes_threshold: None,
                // No `DataRole` on any node in this fixture (`data: None`
                // below) — see the module doc's "still `ProdEnv`-only" bullet.
                backup_store: None,
                segment_store: None,
                quiesce_after_ms: None,
                auth_enabled: None,
                auth_access_key_ids: None,
                otlp_endpoint: None,
            });
            let ctx: SimNodeCtx = ClientCtx {
                control: GenericControlHandle::Local(controls[i].clone()),
                edge: ClusterEdgeState::<SimEnv>::new(),
                env: sim.env(id.clone()),
                // ADR 0061 rung D2 PR 1: a real `DataRole`, not `None` — the
                // generic `dynamo::dispatch_item_op`/`write_path::
                // kind_write_item_at_leader` paths this rung wires up call
                // `ctx.data()` (a panic on `None`, ADR 0035 PR3's own
                // control-only-node guard) on their hot paths
                // (`raftkv_metrics.incr`/`request_rates.observe`). Every
                // `DataRole` field is a plain, `Env`-free `Default`-able
                // handle (`MetricsHandle`/`StreamSealKnobs`/
                // `ChangeRateTracker`/`RequestRateTracker`, none of them
                // touch `E` at all) — see that struct's own doc — so
                // constructing a real one costs nothing and needs no
                // `ProdEnv`. `base_id` is this node's own id, `DataRole`'s
                // real-cluster meaning (ADR 0023's replica-set identity).
                data: Some(DataRole {
                    raftkv_metrics: MetricsHandle::noop(),
                    base_id: id.clone(),
                    stream_seal_knobs: StreamSealKnobs::default(),
                    change_rates: ChangeRateTracker::default(),
                    request_rates: RequestRateTracker::default(),
                }),
                // `cp_kind_write_raw`/`cp_get`/`cp_scan` never read this
                // one — see the module doc's own "still `ProdEnv`-only"
                // bullet, and `simenv_client_ctx_tests::single_node_ctx`'s
                // own doc for why the `Fs` placeholder needs no real
                // filesystem or `ProdEnv` to satisfy the field.
                segment_store: SegmentStoreHandle::Fs(FsSegmentStore::new(format!(
                    "unused-segment-store-{i}"
                ))),
                // ADR 0061 rung D4 PR 5: every node's own handle wraps the
                // SAME shared `backup_store` built above — see that
                // binding's own comment for why `S3` (a genuinely shared
                // object store) is the right variant here, unlike
                // `segment_store`'s still-placeholder `Fs` above (nothing
                // this fixture drives reads it).
                backup_store: BackupStoreHandle::S3(Arc::new(backup_store.clone())),
                export_store_factory: Arc::new(Mutex::new(default_export_store_factory(None))),
                backup_janitor_progress: Arc::new(Mutex::new(
                    animus_node::backup_janitor::JanitorProgress::default(),
                )),
                ttl_reaper_progress: Arc::new(Mutex::new(
                    animus_node::ttl_reaper::TtlReaperProgress::default(),
                )),
                segment_janitor_progress: Arc::new(Mutex::new(
                    segment_janitor::SegmentJanitorProgress::default(),
                )),
                client_route: Arc::new(Mutex::new(route.clone())),
                intra_route: Arc::new(Mutex::new(route.clone())),
                admin,
                metrics_history: Arc::new(Mutex::new(VecDeque::new())),
                remote_metadata: Arc::new(Mutex::new(None)),
                control_storage: None,
                dynamo_auth: None,
                tls: None,
                relay: relays[i].clone(),
                throttle: ThrottleTracker::new(),
                throttle_defaults: Arc::new(ThrottleDefaults::default()),
                any_table_throughput: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            };
            ctxs.push(ctx);
        }

        // Install the generic relayed-request dispatcher (ADR 0061 rung
        // C3d Deliverable A) on every node, closed over that node's own
        // (now fully built) `ClientCtx` — the same wiring
        // `two_node_relay_tests::two_node_ctx` does for its one relaying
        // node, done here for all of them.
        for (i, relay) in relays.iter().enumerate() {
            let ctx = ctxs[i].clone();
            relay.serve(move |req| {
                let ctx = ctx.clone();
                async move { forwarding::handle_relayed_request(&ctx, req).await }
            });
        }

        // ADR 0061 rung D3 PR 2a: register each node's own control handle
        // onto its own edge — restores `ClientCtx::propose_schema`'s real
        // leader-local fast path under `SimEnv` (see `ClusterEdgeState::
        // control`'s own doc for the full account). Before this,
        // `leader_handle()` always answered `None` regardless of which node
        // actually led the control group, so every schema proposal —
        // including one issued on the leader itself — took the relay
        // branch, which under `SimEnv` meant relaying to **itself**;
        // `forwarding::handle_relayed_request`'s own `ProposeSchema` arm
        // re-resolves the leader the identical way and re-relays, recursing
        // until the caller's own timeout. `SimCluster::new` is the one
        // place this fixture builds its `RaftNode<SimEnv>` handles, so this
        // is the natural home for the registration — mirrors `BoundNode::
        // start_with`'s own `edge.register_control(raft.clone())` call in
        // production.
        for (i, control) in controls.iter().enumerate() {
            ctxs[i].edge.register_control(control.clone());
        }

        // ADR 0061 rung D4 PR 1: one real `animus_cp_data::host::
        // Reconciler` per node — see `build_reconciler`/
        // `spawn_reconciler_loop`'s own docs. This is now this fixture's
        // ONLY tablet-hosting path (closing issue #715 — see the module
        // doc's own "Updated since D3" section): both
        // `create_table_with_replication`'s hand-hosted tables and a
        // wire-provisioned `CreateTable`'s policy-carrying tablet are
        // discovered and hosted/reconfigured/released identically, through
        // the SAME reconciler loop.
        let engines: Vec<MemoryTabletEngines> =
            (0..nodes).map(|_| MemoryTabletEngines::new()).collect();
        for (i, id) in ids.iter().enumerate() {
            let reconciler = build_reconciler(
                ctxs[i].env.clone(),
                engines[i].clone(),
                id.clone(),
                ctxs[i].edge.clone(),
            );
            spawn_reconciler_loop(ctxs[i].clone(), reconciler);
        }

        // ADR 0061 rung D4 PR 5: one `animus_node::backup_janitor::
        // backup_janitor_loop` per node, unconditionally — mirrors
        // `heartbeat_loop`'s own always-on spawn above (D4 PR 1), not
        // `auto_split_loop`'s own opt-in shape (D4 PR 2): this loop's own
        // leader gate (`ControlLeaderHost::control_leader`) already makes
        // it a cheap no-op idle sleep on every non-leader node, so there is
        // no reason to gate spawning it at all.
        for ctx in &ctxs {
            let env = ctx.env.clone();
            env.spawn_task(backup_janitor::backup_janitor_loop(ctx.clone()));
        }

        let mut cluster = SimCluster {
            sim,
            nodes,
            replication,
            controls,
            shared: SimClusterHandle::new(ctxs),
            engines,
            crashed: BTreeSet::new(),
            auto_split: None,
            backup_store,
        };
        // Let the control group elect before any caller touches it —
        // generous for up to a handful of voters under `SimEnv`'s
        // near-instant elections.
        cluster.sim.run_for(Duration::from_secs(2));
        // ADR 0061 rung D3 PR 2a: populate `Metadata::members` — see
        // `SimCluster::seed_members`'s own doc.
        cluster.seed_members();
        cluster
    }

    /// Populate `Metadata::members` for every node in this cluster (ADR
    /// 0061 rung D3 PR 2a). `SimCluster` used to leave `members`
    /// permanently empty — harmless to every hand-hosted DDL scenario this
    /// module's own tests drive (see the confirmation below), but it blocks
    /// the real, wire-driven `dynamo::create_table` path this rung adds:
    /// `ClientCtx::provision_tablet` reads `meta.members` to size a fresh
    /// table's initial replica set, and would otherwise always come up
    /// empty, so a wire `CreateTable` issued against this fixture could
    /// never actually provision a tablet.
    ///
    /// For every node: `RegisterNode` with `role: "combined"` — **not**
    /// `"control"`, since `RegisterNode`'s own `claims_membership` gate
    /// (`animus_control::meta`) never inserts into `members` at all for a
    /// control-only registration (a control-only node can never host a
    /// tablet, so appearing in `members` would make it a placement
    /// candidate and silently corrupt placement the moment it's picked) —
    /// then `UpsertMember { status: Active }`. `RegisterNode` alone inserts
    /// the member `Down`; this fixture runs no heartbeat loop
    /// (`heartbeat_loop_live`/the control leader's `detect_loop`) to ever
    /// promote it the way a real cluster's failure detector would, so a
    /// direct `UpsertMember{Active}` is the only way this fixture's members
    /// ever become `Active` at all. It also sets `Member::has_activated`
    /// (`UpsertMember`'s apply arm, sticky once true), so the orphan-member
    /// sweep — not that this fixture runs one either — could never reclaim
    /// the id even if it did. Converged-or-timeout polled, the same shape
    /// `create_table_with_replication`'s own tail uses.
    ///
    /// **Confirmed harmless to every hand-hosted scenario** (verified
    /// against `animus-control`'s own source, not assumed): `reconcile_
    /// placement`/`rebalance_placement` iterate `Metadata::policies`, never
    /// `members` directly, and this fixture never proposes
    /// `SetTabletPolicy` for a hand-hosted table (only `ClientCtx::
    /// provision_tablet`'s own real wire path does) — so populating
    /// `members` here cannot make the reconciler (which this fixture never
    /// runs in the first place — DDL is a direct `RaftNode::propose` bypass,
    /// see the module doc) touch a hand-hosted table's tablet. `cargo test
    /// -p animusd --lib` keeps its identical pass count before and after
    /// this change (240 both before and after, at the time this rung
    /// landed).
    fn seed_members(&mut self) {
        let leader = self.control_leader_index();
        for n in 0..self.nodes as u64 {
            let id = nid(n);
            let addr = id.to_string();
            let addrs = NodeAddrs {
                internal: addr.clone(),
                client: addr.clone(),
                intra: addr.clone(),
                admin: addr,
                role: "combined".to_owned(),
            };
            assert!(
                matches!(
                    self.controls[leader].propose(MetaCommand::RegisterNode {
                        node: id.clone(),
                        addrs,
                        labels: BTreeMap::new(),
                    }),
                    ProposeResult::Accepted { .. }
                ),
                "RegisterNode must be accepted by the current control leader (node={id})"
            );
            assert!(
                matches!(
                    self.controls[leader].propose(MetaCommand::UpsertMember {
                        node: id.clone(),
                        labels: BTreeMap::new(),
                        status: NodeStatus::Active,
                    }),
                    ProposeResult::Accepted { .. }
                ),
                "UpsertMember must be accepted by the current control leader (node={id})"
            );
        }
        self.poll_until(Duration::from_secs(5), |c| c.shared.all_members_active());
    }

    /// The number of nodes this cluster was built with.
    pub(crate) fn node_count(&self) -> usize {
        self.nodes
    }

    /// A cheap `Clone` of this cluster's [`SimClusterHandle`] — the one
    /// thing a corpus hands into its own concurrently `env.spawn_task`-ed
    /// client-op tasks (ADR 0061 rung D1 step 3). The driver itself
    /// (`crash`/`restart`/`partition`/`heal_all`/`run_for`, plus DDL) stays
    /// on `&mut self` here — see [`SimClusterHandle`]'s own doc for why
    /// those don't move onto the handle too.
    pub(crate) fn handle(&self) -> SimClusterHandle {
        self.shared.clone()
    }

    /// A `SimEnv` for a **client-only** id, disjoint from every node id
    /// this cluster's own `0..nodes` range uses (mirroring `animus_test`'s
    /// `raftkv_linearizable.rs::CLIENT_IDS` convention) — never targeted by
    /// [`SimCluster::crash`]/`restart`/`partition`, so a corpus's own
    /// client-driver task spawned on this env always keeps making progress
    /// (retrying/rotating nodes) regardless of which node it currently
    /// targets. `idx` is the corpus's own client index (`0..clients`);
    /// distinct `idx`s never collide with each other or with a real node id
    /// for any `nodes` this fixture is ever built with (well under this
    /// offset).
    pub(crate) fn client_env(&self, idx: u64) -> SimEnv {
        const CLIENT_ID_BASE: u64 = 10_000;
        self.sim.env(nid(CLIENT_ID_BASE + idx))
    }

    /// Index (in `0..node_count()`) of whichever control `RaftNode`
    /// currently believes it leads, waiting out an election if none does
    /// yet. Panics if no leader emerges within a generous bound — every
    /// scenario this fixture drives keeps the control group itself
    /// healthy (it is never a fault target), so a failure here means the
    /// cluster is broken, not that the caller should retry.
    ///
    /// `pub(crate)` since ADR 0061 rung D3 PR 2a: `sim_cluster_dynamo_
    /// table_ops.rs`'s own control-leader-vs-follower `CreateTable`/
    /// `DeleteTable` regressions need to pick a leader-issued and a
    /// follower-issued target node — the identical "widen only what a
    /// sibling `#[cfg(test)] mod` genuinely needs, nothing else" discipline
    /// this crate's own visibility lesson (`docs/engineering-lessons.md`)
    /// documents.
    pub(crate) fn control_leader_index(&mut self) -> usize {
        for _ in 0..40 {
            if let Some(i) = self.controls.iter().position(RaftNode::is_leader) {
                return i;
            }
            self.sim.run_for(Duration::from_millis(50));
        }
        panic!("control group never elected a leader");
    }

    /// Drive the simulator forward in `step`-sized increments, calling
    /// `done(self)` after each, until it returns `true` or `budget` is
    /// exhausted — the converged-or-timeout idiom root `CLAUDE.md`'s
    /// Testing rule requires for every eventual property in this repo,
    /// generalized into one shared helper so `create_table`'s own setup
    /// waits and every test's convergence assertions share one
    /// implementation.
    fn poll_until(&mut self, budget: Duration, mut done: impl FnMut(&Self) -> bool) {
        const STEP: Duration = Duration::from_millis(50);
        let mut elapsed = Duration::ZERO;
        loop {
            if done(self) {
                return;
            }
            assert!(
                elapsed < budget,
                "condition did not converge within {budget:?} (seed={})",
                self.sim.seed()
            );
            self.sim.run_for(STEP);
            elapsed += STEP;
        }
    }

    /// [`SimCluster::create_table_with_replication`] at this cluster's own
    /// default replication factor (the `replication` passed to
    /// [`SimCluster::new`]).
    pub(crate) fn create_table(&mut self, table: &str) -> TabletId {
        self.create_table_with_replication(table, self.replication)
    }

    /// Create `table` (a composite `(pk, sk)` schema, both `S`/string
    /// attributes — see `item_key`'s own doc), targeting the first
    /// `replication` node ids (`0..replication`) as its tablet's initial
    /// replica set. DDL is seeded by proposing directly on the control
    /// group's current leader; this call drives the simulator itself
    /// (never requires a caller-side `run_for`) and returns only once
    /// every node's own `Metadata` shows the table **and** the tablet's
    /// freshly hosted group has elected a leader — so a caller's very next
    /// `put`/`get` can rely on both being true immediately.
    ///
    /// **Since ADR 0061 rung D4 PR 1, this no longer constructs a
    /// `RaftKvNode` by hand** — it provisions the tablet the same way a
    /// wire-issued `CreateTable` does (`CreateTableSchema`/`CreateTablet`
    /// plus a `MetaCommand::SetTabletPolicy`, mirroring `ClientCtx::
    /// provision_tablet`'s own shape) and waits for every node's own real
    /// `host::Reconciler` (see the module doc) to discover and host it.
    /// The policy's own recorded replication factor is `replication`
    /// itself, not the wire path's fixed `MAX_REPLICATION_FACTOR` — this
    /// preserves the pre-existing "exactly `replication` replicas, on
    /// nodes `0..replication`" contract for a caller wanting a specific
    /// factor different from what a real `CreateTable` would pick.
    pub(crate) fn create_table_with_replication(
        &mut self,
        table: &str,
        replication: usize,
    ) -> TabletId {
        assert!(
            (1..=self.nodes).contains(&replication),
            "replication must be between 1 and the node count"
        );
        let replicas: Vec<u64> = (0..replication as u64).collect();
        let replica_ids: Vec<NodeId> = replicas.iter().copied().map(nid).collect();

        let leader = self.control_leader_index();
        // ADR 0061 rung D3 PR 2a fix: derive the fresh id from this leader's
        // own **live** `Metadata` (`next_free_tablet_id`), never a
        // fixture-local counter — a real finding from this rung's own work,
        // not a pre-existing concern: before this rung, every `SimCluster`
        // test used *either* this hand-hosted path *or* the wire-provisioned
        // path (`ClientCtx::provision_tablet`, which already reads
        // `Metadata::next_free_tablet_id()` fresh), never both in the same
        // cluster. This module's own `list_tables_sorts_paginates_and_
        // excludes_gsi_hidden_tables` test is the first to mix them (three
        // wire-created tables, then one hand-hosted one) and is what
        // surfaced the bug: a fixture-local counter starting at 1 and
        // incrementing only on THIS method's own calls collided with
        // `TabletId(1)` already minted by the first wire-created table,
        // silently wedging the hand-hosted `CreateTablet` propose behind a
        // "tablet already exists" rejection until this method's own
        // `poll_until` timed out. See `docs/engineering-lessons.md`'s
        // matching entry.
        let tablet = self.controls[leader].metadata().next_free_tablet_id();
        let schema = TableSchema::composite("pk", ColumnType::String, "sk", ColumnType::String);
        assert!(
            matches!(
                self.controls[leader].propose(MetaCommand::CreateTableSchema {
                    table: table.to_owned(),
                    schema,
                }),
                ProposeResult::Accepted { .. }
            ),
            "CreateTableSchema must be accepted by the current control leader (table={table})"
        );
        assert!(
            matches!(
                self.controls[leader].propose(MetaCommand::CreateTablet {
                    tablet,
                    table: Some(table.to_owned()),
                    range: KeyRange::whole(),
                    replicas: replica_ids.clone(),
                }),
                ProposeResult::Accepted { .. }
            ),
            "CreateTablet must be accepted by the current control leader (table={table})"
        );
        // ADR 0061 rung D4 PR 1: attach a placement policy — the signal
        // every node's own `host::Reconciler` hosts a tablet off of
        // (`Tablet.replicas.contains(&base_id)` alone isn't enough; see
        // `host.rs`'s own `plan` doc: only a *policy-carrying* tablet is
        // this fixture's live territory at all, matching the real
        // `reconcile_placement`/`rebalance_placement` gate). Proposed
        // against a freshly re-resolved leader (the earlier one may have
        // changed while the two proposals above committed).
        let leader = self.control_leader_index();
        assert!(
            matches!(
                self.controls[leader].propose(MetaCommand::SetTabletPolicy {
                    tablet,
                    policy: Some(PlacementPolicy::simple("cp-rf", replication)),
                }),
                ProposeResult::Accepted { .. }
            ),
            "SetTabletPolicy must be accepted by the current control leader (table={table})"
        );

        // Converged-or-timeout: every node's own control read must show
        // the freshly seeded schema/tablet before this fixture waits on
        // any node's own reconciler to have hosted it.
        self.poll_until(Duration::from_secs(5), |c| {
            c.shared.all_have_table_tablet(table)
        });

        self.shared.insert_tablet(
            tablet,
            TabletInfo {
                table: table.to_owned(),
                replicas: replicas.clone(),
            },
        );

        // Let every node's own reconciler discover and host the fresh
        // policy-carrying tablet, then let the freshly formed group elect
        // a leader — same converged-or-timeout shape as before, never a
        // fixed sleep, just no longer built by this method's own hand.
        self.poll_until(Duration::from_secs(5), move |c| {
            replicas
                .iter()
                .any(|&n| c.shared.is_leader_local(n, tablet))
        });

        tablet
    }

    /// The tablet id [`SimCluster::create_table`] minted for `table`, if
    /// this fixture created one (this fixture never splits a table, so the
    /// mapping is always 1:1 and stable for the table's whole lifetime).
    pub(crate) fn tablet_of(&self, table: &str) -> Option<TabletId> {
        self.shared.tablet_of(table)
    }

    /// The node id currently hosting `tablet`'s own leader replica, if
    /// any one of its known replicas believes it leads (there is at most
    /// one true leader at a time; a stale double-belief during an
    /// election is possible but transient — callers wanting a stable
    /// answer should poll via [`SimCluster::run_for`] first).
    pub(crate) fn leader_of(&self, tablet: TabletId) -> Option<NodeId> {
        self.leader_index_of(tablet).map(nid)
    }

    /// [`SimCluster::leader_of`]'s own plain-`u64` sibling — every other
    /// method on this fixture (`put`/`get`/`crash`/`restart`/…) addresses a
    /// node by this same index (see the struct's own doc for why: `nid`'s
    /// concrete string encoding, `"n{n}"`, is `animus-env`'s own
    /// implementation detail, not something a caller of this fixture
    /// should ever need to parse back out of a `NodeId` to get a usable
    /// index again).
    pub(crate) fn leader_index_of(&self, tablet: TabletId) -> Option<u64> {
        self.shared.leader_index_of(tablet)
    }

    /// ADR 0061 rung D4 PR 2: whether `node`'s own local `CpGroup` handle
    /// for `tablet` currently believes IT leads — the exact fact
    /// `auto_split_loop`'s own `ctx.edge.cp_leader(tablet)` gate is built
    /// on (a `None` there is this same predicate answering `false`). A thin
    /// public wrapper over `SimClusterHandle::is_leader_local`, which
    /// [`SimCluster::leader_index_of`] already uses internally.
    pub(crate) fn is_leader_local(&self, node: u64, tablet: TabletId) -> bool {
        self.shared.is_leader_local(node, tablet)
    }

    /// `node`'s own view of the replicated control-plane `Metadata` —
    /// `ClientCtx::effective_metadata`'s exact read, so a caller can
    /// assert on tablet placement / schema visibility per node.
    pub(crate) fn metadata(&self, node: u64) -> Metadata {
        self.shared.metadata(node)
    }

    /// [`SimClusterHandle::hosted_tablets`]'s own driver-callable twin.
    pub(crate) fn hosted_tablets(&self, node: u64) -> BTreeSet<TabletId> {
        self.shared.hosted_tablets(node)
    }

    /// ADR 0061 rung D4 PR 5: the ONE shared `SimSegmentStore` every node's
    /// own `ClientCtx::backup_store` wraps a clone of — a cheap `Clone`
    /// (its own state lives behind an `Arc<Mutex<..>>`), so a caller can
    /// inspect it directly (`SimSegmentStore::stored_ids`/`get`, both
    /// plain sync/async methods needing no simulator drive for a genuine
    /// read of already-landed state) without going through any one node's
    /// own `ClientCtx`.
    pub(crate) fn backup_store(&self) -> SimSegmentStore {
        self.backup_store.clone()
    }

    /// Durably `put` a backup object directly into this cluster's shared
    /// `SimSegmentStore` (ADR 0061 rung D4 PR 5) — the corpus's own way to
    /// place a real manifest/data object under a backup id before proposing
    /// its catalog transitions on the control leader, mirroring
    /// `backup_capture.rs`/`backup_completion.rs`'s own production `put`
    /// call. Driven via [`SimCluster::spawn_and_capture`] on node 0's own
    /// env — which node spawns it on is arbitrary (`SimSegmentStore` draws
    /// off the `Simulator`'s one shared RNG stream regardless of which
    /// node's handle is used, and `put` sends no network message), the
    /// same "any env will do" reasoning [`SimCluster::client_env`] states
    /// for a client-only id.
    pub(crate) fn seed_backup_object(&mut self, id: &str, bytes: &[u8]) {
        let store = self.backup_store.clone();
        let (id, bytes) = (id.to_owned(), bytes.to_vec());
        self.spawn_and_capture(0, async move {
            use animus_env::SegmentStore;
            store.put(&id, &bytes).await.expect("seed backup object")
        });
    }

    /// `node`'s own live `animus_node::backup_janitor::JanitorProgress`
    /// snapshot (ADR 0061 rung D4 PR 5, roadmap U-07) — the identical
    /// `GET /admin/backup-store` reads back in production, a plain
    /// synchronous lock/clone/drop (never held across an `.await`,
    /// `client_ctx_host.rs`'s own `BackupJanitorProgressHost` impl).
    pub(crate) fn backup_janitor_progress(
        &self,
        node: u64,
    ) -> animus_node::backup_janitor::JanitorProgress {
        self.shared
            .ctx(node)
            .backup_janitor_progress
            .lock()
            .expect("backup janitor progress poisoned")
            .clone()
    }

    /// `tablet`'s own private engine on node `node` (ADR 0050 rung 1) —
    /// get-or-create through the SAME [`MemoryTabletEngines`] registry the
    /// node's own `Reconciler` opens from (ADR 0061 rung D4 PR 1, C-04 D4
    /// PR 3), mirroring `animus-cp-data/tests/reconciler_corpus.rs::
    /// Cluster::storage`'s own convention exactly: a reclaimed tablet's
    /// engine reads back **empty** (a fresh, just-recreated `MemoryEngine`),
    /// which is what a drop-table GC assertion actually wants to see — the
    /// real proof that `HostAction::Reclaim`'s teardown deleted the
    /// tablet's own data, not merely a `Metadata`/hosted-set bookkeeping
    /// check. `node` may host no replica of `tablet` at all (or never has)
    /// — this still returns a valid, empty engine rather than panicking,
    /// same as the production registry.
    pub(crate) fn storage(&self, node: u64, tablet: TabletId) -> MemoryEngine {
        self.engines[node as usize].engine(tablet)
    }

    /// ADR 0061 rung D4 PR 2: opt this cluster into the auto-split trigger
    /// — spawns [`auto_split_loop`] on EVERY node, right now (mirroring
    /// [`SimCluster::new`]'s own `heartbeat_loop` spawn, D4 PR 1, exactly
    /// — a loop spawned this way starts sleeping for `AUTO_SPLIT_INTERVAL`
    /// immediately, so calling this before or after any writes is
    /// equally fine). **OFF by default** — every scenario that never
    /// calls this pays nothing extra: no loop is spawned, so there is
    /// nothing to poll and nothing to skip. `thresholds` is stored (this
    /// struct's own `auto_split` field) so a later [`SimCluster::restart`]
    /// respawns the identical configuration on the restarted node — its
    /// `Simulator::stop` drops every task the node owned, including this
    /// one, exactly like `heartbeat_loop`/the reconciler loop.
    pub(crate) fn set_auto_split_thresholds(&mut self, thresholds: AutoSplitThresholds) {
        self.auto_split = Some(thresholds);
        for node in 0..self.nodes as u64 {
            self.spawn_auto_split(node, thresholds);
        }
    }

    /// Spawn one [`auto_split_loop`] task on `node`'s own env, closed over
    /// a fresh clone of `node`'s own current `ClientCtx` — the identical
    /// "clone the ctx, spawn on its own env" shape [`SimCluster::restart`]
    /// already uses for `heartbeat_loop`.
    fn spawn_auto_split(&self, node: u64, thresholds: AutoSplitThresholds) {
        let ctx = self.shared.ctx(node);
        let env = ctx.env.clone();
        env.spawn_task(auto_split_loop(ctx, thresholds));
    }

    /// ADR 0065 §5(b): `node`'s own current `ClientCtx::
    /// any_table_throughput` flag — a relaxed load, matching the flag's
    /// own real-request read. Test-only accessor proving the flag's
    /// recompute points (`SimCluster::set_table_throughput`'s tail call to
    /// [`SimClusterHandle::recompute_any_table_throughput_all`]) actually
    /// flip it, both directions.
    pub(crate) fn any_table_throughput(&self, node: u64) -> bool {
        self.shared
            .ctx(node)
            .any_table_throughput
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// ADR 0065's test-reachable hook: set `node`'s cluster-wide default
    /// throttle limits — `None`/`None` (every node's own default) means
    /// `PAY_PER_REQUEST`, unthrottled. See `ClientCtx::set_throttle_
    /// defaults`'s own doc for why this exists at all (step 4's real
    /// config surface doesn't yet).
    pub(crate) fn set_throttle_defaults(
        &self,
        node: u64,
        read_units: Option<u64>,
        write_units: Option<u64>,
    ) {
        self.shared
            .set_throttle_defaults(node, read_units, write_units);
    }

    /// [`SimCluster::set_throttle_defaults`] on every node at once — the
    /// shape a real cluster-wide config knob would have, once step 4 adds
    /// one.
    pub(crate) fn set_throttle_defaults_all(
        &self,
        read_units: Option<u64>,
        write_units: Option<u64>,
    ) {
        for node in 0..self.nodes as u64 {
            self.set_throttle_defaults(node, read_units, write_units);
        }
    }

    /// ADR 0065 §5(b), W-08 step 4: set (or clear, `spec: None`) `table`'s
    /// own **per-table** provisioned throughput — `MetaCommand::
    /// SetTableThroughput`, proposed on the control group's current leader
    /// and converged-or-timeout polled across every node, the identical
    /// shape [`SimCluster::create_table_with_replication`]'s own tail uses.
    /// Overrides [`SimCluster::set_throttle_defaults_all`]'s cluster-wide
    /// default for this table (ADR 0065 Decision 5(b)) — `ClientCtx::
    /// throttle_limits_for` reads this back before falling through to it.
    pub(crate) fn set_table_throughput(
        &mut self,
        table: &str,
        spec: Option<animus_control::ProvisionedThroughput>,
    ) {
        let leader = self.control_leader_index();
        let outcome = self.controls[leader].propose(MetaCommand::SetTableThroughput {
            table: table.to_owned(),
            spec,
        });
        assert!(
            matches!(outcome, ProposeResult::Accepted { .. }),
            "SetTableThroughput must be accepted by the current control leader (table={table})"
        );
        self.poll_until(Duration::from_secs(5), |c| {
            c.shared.all_have_table_throughput(table, spec.as_ref())
        });
        self.shared.recompute_any_table_throughput_all();
    }

    /// Propose `command` directly on this cluster's CURRENT control-plane
    /// leader (ADR 0061 rung D4 PR 5) — the same `self.controls[leader]
    /// .propose(..)` bypass every DDL helper above already uses (see the
    /// module doc's own "DDL is a control-plane-Raft bypass" bullet),
    /// generalized to any `MetaCommand` rather than the handful this
    /// file's own methods build by hand. The backup-janitor corpus uses
    /// this to drive the backup catalog's own commands
    /// (`BeginBackup`/`RecordBackupTabletComplete`/`CompleteBackup`/
    /// `FailBackup`/`MarkBackupDeleted`/`DeleteBackup`) the identical way
    /// `animus-control/tests/backup_catalog.rs`'s own `propose_accepted`
    /// helper does against a bare `RaftNode` handle — this crate's own
    /// analogue, just resolving the leader through this fixture instead of
    /// a caller-supplied index. Returns the raw `ProposeResult` rather than
    /// asserting `Accepted` itself, since a corpus scenario legitimately
    /// wants to assert a REJECTION too (e.g. proposing against an unknown
    /// backup id).
    pub(crate) fn propose_meta(&mut self, command: MetaCommand) -> ProposeResult {
        let leader = self.control_leader_index();
        self.controls[leader].propose(command)
    }

    /// Stage (prepare) ONE write of a raw plain 2PC transaction against
    /// `table`, issued from `node`'s own coordinator — and, deliberately,
    /// never decides or resolves it (ADR 0061 rung F, C-06 PR 3). This is
    /// this fixture's own way of expressing "the coordinator crashed right
    /// after prepare," mirroring `cp_txn.rs`'s own `prepare_via_any_node`
    /// idiom (see that test file's own doc for why driving the internal
    /// prepare step directly and simply never sending decide/resolve is the
    /// cleanest way to express a vanished coordinator over a real cluster)
    /// — but via a direct in-process `ClientCtx::txn_prepare` call, which
    /// already resolves/forwards to the correct tablet leader internally,
    /// rather than a hand-rolled per-node wire retry loop.
    ///
    /// `anchor` is `None` for the very first write of a transaction (which
    /// mints the anchor record) and `Some((txn_id, record_key,
    /// record_table))` — this call's own returned triple — for every
    /// subsequent participant. `participant_spans` is meaningful only for
    /// the anchor call (`anchor: None`) — every OTHER participant's own
    /// `(table, span)` pair, exactly what `ClientCtx::cp_txn`'s own anchor
    /// stage builds (see that method's doc): omitting it (an empty `Vec`)
    /// leaves the anchor's own record unaware of any participant, so
    /// in-doubt recovery's `all_staged` check can only ever verify the
    /// anchor's own keys — a caller staging more than one participant must
    /// pass every later participant's own `(table, key)` here, one entry
    /// per key, each covering exactly that key
    /// (`KeyRange::new(key.clone(), Some(key-with-a-trailing-0-byte)`,
    /// `cp_txn`'s own per-key span shape). Ignored (and safe to leave
    /// empty) for a participant call (`anchor: Some(..)`), which never
    /// creates a record to populate. Panics if the stage did not fully
    /// apply (`StageOutcome::Staged`, the only outcome any of this rung's
    /// own scenarios should ever see — none of these calls carry a
    /// condition to fail) or did not complete within [`OP_BUDGET`], both
    /// fixture-setup bugs, not a legitimate scenario outcome.
    pub(crate) fn txn_prepare_only(
        &mut self,
        node: u64,
        table: &str,
        anchor: Option<(animus_cp_data::TxnId, Vec<u8>, String)>,
        participant_spans: Vec<(String, KeyRange)>,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
    ) -> (animus_cp_data::TxnId, Vec<u8>, String) {
        let ctx = self.shared.ctx(node);
        let table = table.to_owned();
        let outcome = self.spawn_and_capture(node, async move {
            ctx.txn_prepare(
                &table,
                anchor,
                vec![animus_cp_data::TxnWrite::plain(key, value)],
                Vec::new(),
                participant_spans,
                Vec::new(),
            )
            .await
        });
        match outcome {
            Some(Ok((
                txn_id,
                record_key,
                record_table,
                _ts,
                animus_cp_data::StageOutcome::Staged,
            ))) => (txn_id, record_key, record_table),
            Some(Ok((.., other))) => {
                panic!("txn_prepare_only: stage did not fully apply: {other:?}")
            }
            Some(Err(e)) => panic!("txn_prepare_only: stage failed: {e:?}"),
            None => panic!("txn_prepare_only on node {node} did not complete within {OP_BUDGET:?}"),
        }
    }

    /// A **raw, routed** read of `key` (arbitrary physical bytes) in
    /// `table`, issued from `node`'s own `ClientCtx` — [`SimClusterHandle::
    /// get`]'s sibling for a caller (like [`SimCluster::txn_prepare_only`])
    /// that already has the exact physical key bytes in hand, skipping
    /// `item_key`'s pk/sk `AttributeValue` encoding entirely — the raw
    /// plain-KV read `cp_txn.rs`'s own `ClientRequest::Get` uses.
    pub(crate) fn raw_get(
        &mut self,
        node: u64,
        table: &str,
        key: Vec<u8>,
        consistent: bool,
    ) -> Result<Option<Vec<u8>>, String> {
        let ctx = self.shared.ctx(node);
        let table = table.to_owned();
        self.spawn_and_capture(node, async move {
            match ctx.cp_get(&table, key, !consistent).await {
                ClientResponse::Value(v) => Ok(v),
                ClientResponse::Error(e) => Err(e),
                other => Err(format!("unexpected get response: {other:?}")),
            }
        })
        .unwrap_or_else(|| {
            Err(format!(
                "raw_get on node {node} did not complete within {OP_BUDGET:?}"
            ))
        })
    }

    /// Move control-plane leadership to `target` (ADR 0061 rung D4 PR 5) —
    /// `RaftCore::transfer_leadership`'s own real handoff (ADR 0029/0037),
    /// not a `crash`/`restart`-driven forced re-election: the current
    /// leader freezes new proposes and hands off cleanly once `target`'s
    /// own log has caught up. A no-op if `target` already leads. Retries
    /// the arm attempt (bounded) since a single attempt only succeeds if
    /// `target`'s replicated log has caught up to the leader's current
    /// commit index at that precise instant — the identical one-shot-arm
    /// caveat `animusd::CLAUDE.md`'s own issue #405 entry documents for
    /// `admin_remove_control_member`'s self-removal transfer.
    pub(crate) fn transfer_control_leadership_to(&mut self, target: u64) {
        let mut leader = self.control_leader_index();
        if leader == target as usize {
            return;
        }
        for _ in 0..20 {
            if self.controls[leader].transfer_leadership(nid(target)) {
                break;
            }
            self.sim.run_for(Duration::from_millis(200));
            leader = self.control_leader_index();
            if leader == target as usize {
                return;
            }
        }
        self.sim.run_for(Duration::from_secs(2));
        let new_leader = self.control_leader_index();
        assert_eq!(
            new_leader, target as usize,
            "control leadership must move to node {target} within budget (still on {new_leader})"
        );
    }

    /// Spawn `fut` onto `node`'s own env and drive the simulator for
    /// [`OP_BUDGET`], returning its result — `None` means `fut` never
    /// resolved in that window. Mirrors `simenv_client_ctx_tests`/
    /// `two_node_relay_tests`'s own `spawn_and_capture` exactly (see
    /// either's doc for the `futures::executor::block_on`-hangs-under-
    /// `SimEnv` gotcha this avoids), generalized to pick the spawning
    /// env by node index instead of always using one fixed `ClientCtx`.
    fn spawn_and_capture<T, F>(&mut self, node: u64, fut: F) -> Option<T>
    where
        T: Send + 'static,
        F: std::future::Future<Output = T> + Send + 'static,
    {
        let env = self.shared.env(node);
        let slot: Arc<Mutex<Option<T>>> = Arc::new(Mutex::new(None));
        let out = slot.clone();
        env.spawn_task(async move {
            let result = fut.await;
            *out.lock().expect("result slot poisoned") = Some(result);
        });
        self.sim.run_for(OP_BUDGET);
        slot.lock().expect("result slot poisoned").take()
    }

    /// Write `value` at `(pk, sk)` in `table`, issued from `node`'s own
    /// `ClientCtx` — the real `cp_kind_write_raw` route → propose →
    /// confirm loop, forwarded over the real `SimRelayClient` wire when
    /// `node` doesn't host the tablet's leader (or hosts no replica of it
    /// at all). `Err` on a timeout or a routing/propose failure — this
    /// fixture never panics on a failed op, since several scenarios
    /// (a partitioned minority) expect one.
    pub(crate) fn put(
        &mut self,
        node: u64,
        table: &str,
        pk: &str,
        sk: &str,
        value: &[u8],
    ) -> Result<(), String> {
        let handle = self.shared.clone();
        let (table, pk, sk, value) = (
            table.to_owned(),
            pk.to_owned(),
            sk.to_owned(),
            value.to_vec(),
        );
        self.spawn_and_capture(node, async move {
            handle.put(node, &table, &pk, &sk, &value).await
        })
        .unwrap_or_else(|| {
            Err(format!(
                "put on node {node} did not complete within {OP_BUDGET:?}"
            ))
        })
    }

    /// Delete the item at `(pk, sk)` in `table`, issued from `node`'s own
    /// `ClientCtx` — the [`SimCluster::put`] sibling.
    pub(crate) fn delete(
        &mut self,
        node: u64,
        table: &str,
        pk: &str,
        sk: &str,
    ) -> Result<(), String> {
        let handle = self.shared.clone();
        let (table, pk, sk) = (table.to_owned(), pk.to_owned(), sk.to_owned());
        self.spawn_and_capture(
            node,
            async move { handle.delete(node, &table, &pk, &sk).await },
        )
        .unwrap_or_else(|| {
            Err(format!(
                "delete on node {node} did not complete within {OP_BUDGET:?}"
            ))
        })
    }

    /// Read `(pk, sk)` in `table`, issued from `node`'s own `ClientCtx`.
    /// `consistent == true` is the real `ConsistentRead: true` path
    /// (ADR 0055's `Strong`/ReadIndex, forwarded to the tablet's actual
    /// leader when `node` doesn't host it); `false` is the cheap
    /// replica-local eventual path, which can only ever be served by a
    /// node hosting a replica of the tablet. `Ok(None)` is a genuine
    /// absent read (never an error, matching the production wire's own
    /// contract) — see `cp_get`'s own doc.
    pub(crate) fn get(
        &mut self,
        node: u64,
        table: &str,
        pk: &str,
        sk: &str,
        consistent: bool,
    ) -> Result<Option<Vec<u8>>, String> {
        let handle = self.shared.clone();
        let (table, pk, sk) = (table.to_owned(), pk.to_owned(), sk.to_owned());
        self.spawn_and_capture(node, async move {
            handle.get(node, &table, &pk, &sk, consistent).await
        })
        .unwrap_or_else(|| {
            Err(format!(
                "get on node {node} did not complete within {OP_BUDGET:?}"
            ))
        })
    }

    /// Whole-table scan, issued from `node`'s own `ClientCtx` — the cheap
    /// extra read shape beyond point `get`, at the same `consistent`
    /// granularity as [`SimCluster::get`].
    pub(crate) fn scan(
        &mut self,
        node: u64,
        table: &str,
        consistent: bool,
    ) -> Result<ScanRows, String> {
        let handle = self.shared.clone();
        let table = table.to_owned();
        self.spawn_and_capture(
            node,
            async move { handle.scan(node, &table, consistent).await },
        )
        .unwrap_or_else(|| {
            Err(format!(
                "scan on node {node} did not complete within {OP_BUDGET:?}"
            ))
        })
    }

    /// Run a decoded DynamoDB wire request against `node`'s own `ClientCtx`
    /// (ADR 0061 rung D2 PR 1) — [`SimClusterHandle::dynamo`]'s synchronous
    /// sibling, driven from a test's own `&mut self` call exactly like
    /// [`SimCluster::put`]/`get`/`scan` above (never panics on a timeout;
    /// returns a synthetic `500`/timeout-message body instead, so a caller
    /// asserting on the returned status code sees a real, if unlikely,
    /// failure mode rather than a panic).
    pub(crate) fn dynamo(&mut self, node: u64, target: &str, body: &[u8]) -> (u16, String) {
        let handle = self.shared.clone();
        let (target, body) = (target.to_owned(), body.to_vec());
        self.spawn_and_capture(
            node,
            async move { handle.dynamo(node, &target, &body).await },
        )
        .unwrap_or_else(|| {
            (
                500,
                format!("dynamo request on node {node} did not complete within {OP_BUDGET:?}"),
            )
        })
    }

    /// [`SimClusterHandle::txn_prepare_pushing`], driven from a test's own
    /// `&mut self` call exactly like [`SimCluster::put`] above.
    pub(crate) fn txn_prepare_pushing(
        &mut self,
        node: u64,
        table: &str,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
    ) -> Result<(TxnId, Vec<u8>, String, HlcTimestamp), TxnAbortReason> {
        let handle = self.shared.clone();
        let table = table.to_owned();
        self.spawn_and_capture(node, async move {
            handle.txn_prepare_pushing(node, &table, key, value).await
        })
        .unwrap_or_else(|| {
            Err(TxnAbortReason::Other(format!(
                "txn_prepare_pushing on node {node} did not complete within {OP_BUDGET:?}"
            )))
        })
    }

    /// [`SimClusterHandle::txn_prepare_once`], driven from a test's own
    /// `&mut self` call exactly like [`SimCluster::put`] above.
    pub(crate) fn txn_prepare_once(
        &mut self,
        node: u64,
        table: &str,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
    ) -> Result<(TxnId, Vec<u8>, String, HlcTimestamp, StageOutcome), TxnAbortReason> {
        let handle = self.shared.clone();
        let table = table.to_owned();
        self.spawn_and_capture(node, async move {
            handle.txn_prepare_once(node, &table, key, value).await
        })
        .unwrap_or_else(|| {
            Err(TxnAbortReason::Other(format!(
                "txn_prepare_once on node {node} did not complete within {OP_BUDGET:?}"
            )))
        })
    }

    /// [`SimClusterHandle::txn_decide_anchor`], driven from a test's own
    /// `&mut self` call exactly like [`SimCluster::put`] above.
    pub(crate) fn txn_decide_anchor(
        &mut self,
        node: u64,
        table: &str,
        txn_id: TxnId,
        record_key: Vec<u8>,
        commit: bool,
        min_commit_ts: HlcTimestamp,
    ) -> Result<TxnOutcome, String> {
        let handle = self.shared.clone();
        let table = table.to_owned();
        self.spawn_and_capture(node, async move {
            handle
                .txn_decide_anchor(node, &table, txn_id, record_key, commit, min_commit_ts)
                .await
        })
        .unwrap_or_else(|| {
            Err(format!(
                "txn_decide_anchor on node {node} did not complete within {OP_BUDGET:?}"
            ))
        })
    }

    /// [`SimClusterHandle::push_resolution_if_decided`], driven from a
    /// test's own `&mut self` call exactly like [`SimCluster::put`] above.
    pub(crate) fn push_resolution_if_decided(
        &mut self,
        node: u64,
        table: &str,
        blocked_key: Vec<u8>,
        blocker: TxnId,
        blocker_record_table: String,
        blocker_record_key: Vec<u8>,
    ) {
        let handle = self.shared.clone();
        let table = table.to_owned();
        self.spawn_and_capture(node, async move {
            handle
                .push_resolution_if_decided(
                    node,
                    &table,
                    blocked_key,
                    blocker,
                    blocker_record_table,
                    blocker_record_key,
                )
                .await
        });
    }

    /// [`SimClusterHandle::txn_resolve_participant`], driven from a test's
    /// own `&mut self` call exactly like [`SimCluster::put`] above.
    pub(crate) fn txn_resolve_participant(
        &mut self,
        node: u64,
        table: &str,
        txn_id: TxnId,
        record_key: Vec<u8>,
        keys: Vec<Vec<u8>>,
        outcome: TxnOutcome,
    ) -> Result<ResolveOutcome, String> {
        let handle = self.shared.clone();
        let table = table.to_owned();
        self.spawn_and_capture(node, async move {
            handle
                .txn_resolve_participant(node, &table, txn_id, record_key, keys, outcome)
                .await
        })
        .unwrap_or_else(|| {
            Err(format!(
                "txn_resolve_participant on node {node} did not complete within {OP_BUDGET:?}"
            ))
        })
    }

    /// Run several DynamoDB wire requests **concurrently** (ADR 0061 rung
    /// D3 PR 1 — the shared fixture helper every converted `ProdEnv`
    /// `tokio::spawn`-raced-writers test now uses), each `(node, target,
    /// body)` triple spawned onto its own node's env via `env.spawn_task`
    /// exactly like [`SimCluster::dynamo`]'s single-request form, but all
    /// spawned *before* the one shared `Simulator::run_for(OP_BUDGET)` call
    /// that drives every one of them at once — so two requests genuinely
    /// race the same key/tablet the way the original real-thread
    /// `tokio::spawn` pair did, rather than resolving one at a time the way
    /// calling [`SimCluster::dynamo`] in a loop would. Results come back in
    /// the same order as `requests`; a request that does not complete
    /// within [`OP_BUDGET`] reports the same synthetic `500`/timeout body
    /// `dynamo`'s own single-request form does, rather than panicking.
    pub(crate) fn dynamo_concurrent(
        &mut self,
        requests: &[(u64, &str, &[u8])],
    ) -> Vec<(u16, String)> {
        type Slot = Arc<Mutex<Option<(u16, String)>>>;
        let slots: Vec<Slot> = requests
            .iter()
            .map(|_| Arc::new(Mutex::new(None)))
            .collect();
        for ((node, target, body), slot) in requests.iter().zip(slots.iter()) {
            let handle = self.shared.clone();
            let env = self.shared.env(*node);
            let (node, target, body) = (*node, (*target).to_owned(), body.to_vec());
            let out = slot.clone();
            env.spawn_task(async move {
                let result = handle.dynamo(node, &target, &body).await;
                *out.lock().expect("result slot poisoned") = Some(result);
            });
        }
        self.sim.run_for(OP_BUDGET);
        slots
            .into_iter()
            .zip(requests.iter())
            .map(|(slot, (node, _, _))| {
                slot.lock().expect("result slot poisoned").take().unwrap_or_else(|| {
                    (
                        500,
                        format!("dynamo request on node {node} did not complete within {OP_BUDGET:?}"),
                    )
                })
            })
            .collect()
    }

    /// Drain pending GSI writes for `table`'s tablets that `node` leads
    /// (ADR 0061 rung D3 PR 3b) — a test-only stand-in for `index_drain::
    /// change_consumer_loop`'s GSI-drain arm, which this fixture never
    /// spawns at all (see the module doc's own "hand-hosted, not
    /// reconciler-hosted" bullet). Closes the gap named in
    /// `sim_cluster_dynamo_table_ops.rs`'s former `gsi_query_reads_empty_
    /// under_the_fixture_until_the_drain_generalizes` (now a positive
    /// assertion — see that module's own doc): before this method existed,
    /// a GSI's own hidden `<base>$<index>` table was never materialized
    /// under `SimCluster` at all, so a GSI `Query`/`Scan` always read back
    /// `Count: 0`.
    ///
    /// Replicates `change_consumer_loop`'s own per-led-tablet guard
    /// sequence **by hand**, not by calling into the loop itself (this
    /// fixture drives one tick's worth of work synchronously rather than
    /// spawning the real background loop, which polls forever and has
    /// nothing else — quiescence, the seal/trim arms, the backfill seeder —
    /// this method has any use for):
    ///
    /// - **Leader check**: only a tablet `node`'s own edge both hosts *and*
    ///   currently leads is drained (`group.is_leader()`) — an unled or
    ///   unhosted tablet of `table` is silently skipped, mirroring the
    ///   production loop's own `if !group.is_leader() { continue }`.
    /// - **Hidden-table skip**: refuses outright (`debug_assert!`-free,
    ///   just a no-op return) if `table` is itself a GSI's own hidden
    ///   `<base>$<index>` table (`is_index_table_name`), mirroring the
    ///   production loop's identical guard — "a hidden index table holds
    ///   index rows; it has no indexes of its own, and must never recurse
    ///   into maintaining any." No caller of this method is expected to
    ///   ever pass one, but the guard costs nothing to keep for parity.
    /// - **`is_quiesced()`/`Building`-child skips are NOT replicated here —
    ///   they are unreachable under this fixture.** `SimCluster` never
    ///   calls `RaftKvNode::enable_quiescence` (quiescence stays
    ///   permanently off for every group this fixture hosts — see
    ///   `CpGroup::is_quiesced`'s own doc: it answers `false` until
    ///   quiescence is explicitly enabled), and this fixture never splits a
    ///   tablet (no `TabletState::Building` row is ever minted here — see
    ///   the module doc's own "hand-hosted, not reconciler-hosted" bullet:
    ///   every tablet this fixture ever hosts is minted straight to
    ///   `Active`). Both of the production loop's own skips are therefore
    ///   dead code in this harness; a future rung that gives `SimCluster`
    ///   real quiescence or splitting would need to add them back here too.
    ///
    /// `gsis` is computed exactly as `change_consumer_loop` does:
    /// `meta.table_indexes(table)` filtered to `IndexKind::Global` with
    /// status `Creating` or `Active` (a `Deleting` index is excluded,
    /// matching production — this loop must stop touching an index being
    /// torn down).
    ///
    /// Calls [`index_drain::drain_tablet`] once per matching tablet, then
    /// drives the simulator up to [`OP_BUDGET`] so every resulting
    /// `cp_kind_write_raw` call (the GSI row writes/deletes and the
    /// trailing cursor write) actually commits before this call returns —
    /// `drain_tablet` itself already awaits each of those to completion
    /// internally, so this is [`SimCluster::spawn_and_capture`]'s own
    /// bounded-wait shape, not a fixed sleep.
    ///
    /// **A table's tablets can have different leaders.** This fixture never
    /// splits a table into more than one tablet today (a table minted by
    /// [`SimCluster::create_table_with_replication`] always has exactly
    /// one), so there is only ever one tablet to drain in practice — but
    /// this method does not assume that: it only touches tablets `node`
    /// itself leads, so a caller with a multi-tablet table should call this
    /// once per node that leads one of that table's tablets (find each via
    /// [`SimCluster::leader_index_of`]), not expect one call to reach every
    /// tablet regardless of who leads it.
    ///
    /// Panics on a genuine drain failure (a `String` error out of
    /// `drain_tablet`) or on not completing within `OP_BUDGET` — both are
    /// fixture-setup bugs a converted test should never see, unlike
    /// `put`/`get`/`scan`/`dynamo`'s own `Result`/status-code returns,
    /// which exist because *those* are the very outcomes several scenarios
    /// deliberately provoke.
    pub(crate) fn drain_gsi(&mut self, node: u64, table: &str) {
        let table_owned = table.to_owned();
        let handle = self.shared.clone();
        let outcome: Option<Result<(), String>> = self.spawn_and_capture(node, async move {
            let ctx = handle.ctx(node);
            let meta = ctx.effective_metadata();
            if animus_dynamo::is_index_table_name(&table_owned) {
                return Ok(());
            }
            let gsis: Vec<animus_control::IndexDef> = meta
                .table_indexes(&table_owned)
                .iter()
                .filter(|i| {
                    i.kind == animus_control::IndexKind::Global
                        && matches!(i.status, IndexStatus::Creating | IndexStatus::Active)
                })
                .cloned()
                .collect();
            for (tablet, group) in ctx.edge.hosted_groups() {
                let led_here = meta
                    .tablets
                    .get(&tablet)
                    .is_some_and(|t| t.table.as_deref() == Some(table_owned.as_str()));
                if !led_here || !group.is_leader() {
                    continue;
                }
                index_drain::drain_tablet(&ctx, &meta, &table_owned, &group, &gsis).await?;
            }
            Ok(())
        });
        match outcome {
            Some(Ok(())) => {}
            Some(Err(e)) => panic!("drain_gsi(node={node}, table={table}) failed: {e}"),
            None => panic!(
                "drain_gsi(node={node}, table={table}) did not complete within {OP_BUDGET:?}"
            ),
        }
    }

    /// ADR 0061 rung D4 PR 2: drive one pass of `index_drain::
    /// inplace_split_driver_tick` — the propose of `MetaCommand::
    /// CutoverSplit` — for every currently-`Splitting` tablet `node`
    /// leads. This fixture never spawns `index_drain::change_consumer_loop`
    /// itself (see [`SimClusterHandle::recompute_any_table_throughput_all`]'s
    /// own doc for the identical reason — no production loop this fixture
    /// runs would otherwise ever propose the cutover, so the fork made by
    /// the real `host::Reconciler` (already running here since D4 PR 1)
    /// would stall forever in `Splitting`), so a caller polls this
    /// alongside [`SimCluster::run_for`] until convergence (two `Active`
    /// children, no `Splitting` parent) — mirroring [`SimCluster::
    /// drain_gsi`]'s own manual-drive shape exactly, one node/pass at a
    /// time. Widened `index_drain::{inplace_split_driver_tick,
    /// gsi_caught_up}` to `<E: Env, R: RelayClient>`/`<E: Env>`
    /// (previously concrete `ProdEnv` only) to make this possible — every
    /// callee they use was already generic (rung C5 step 3b). A no-op
    /// (`Ok(())` immediately) on a node that leads no `Splitting` tablet,
    /// so calling this on every node every poll tick is cheap. Panics on a
    /// genuine driver error or a timeout, exactly like `drain_gsi`.
    pub(crate) fn drive_inplace_split_cutover(&mut self, node: u64) {
        let handle = self.shared.clone();
        let outcome: Option<Result<(), String>> = self.spawn_and_capture(node, async move {
            let ctx = handle.ctx(node);
            let meta = ctx.effective_metadata();
            for (tablet, group) in ctx.edge.hosted_groups() {
                if !group.is_leader() {
                    continue;
                }
                let splitting = meta
                    .tablets
                    .get(&tablet)
                    .is_some_and(|t| t.state == TabletState::Splitting);
                if !splitting {
                    continue;
                }
                index_drain::inplace_split_driver_tick(&ctx, &meta, tablet, &group).await?;
            }
            Ok(())
        });
        match outcome {
            Some(Ok(())) => {}
            Some(Err(e)) => panic!("drive_inplace_split_cutover(node={node}) failed: {e}"),
            None => panic!(
                "drive_inplace_split_cutover(node={node}) did not complete within {OP_BUDGET:?}"
            ),
        }
    }

    /// Crash `node`: its tasks stay alive but muted (no sends land, its
    /// inbox is cleared) — `Simulator::crash`'s own contract. Use
    /// [`SimCluster::restart`] instead for a true process restart (a
    /// fresh `RaftNode`/`RaftKvNode` on the same id).
    pub(crate) fn crash(&mut self, node: u64) {
        self.sim.crash(nid(node));
        self.crashed.insert(node);
    }

    /// A true process restart of `node`: every task it owns is dropped
    /// (`Simulator::stop` — its control `RaftNode` driver, every hosted
    /// `RaftKvNode` driver, its reconciler-loop task, its relay receive
    /// loop), then a fresh control `RaftNode` and a fresh `Reconciler` are
    /// built on the same id. If `node` was `crash`ed (not merely alive),
    /// it is first un-muted (`Simulator::restart`, animus-sim's own
    /// required un-crash-before-stop sequencing — see that method's
    /// crate's own `CLAUDE.md` gotcha) so the following `stop` actually
    /// removes live tasks rather than muted ones.
    ///
    /// **Since ADR 0061 rung D4 PR 1, the fresh `Reconciler` reuses the
    /// SAME `MemoryTabletEngines` handle this node was built with**
    /// (`self.engines[node]`) — see the module doc's own restart bullet
    /// for why this node's tablet data is no longer wiped by a restart.
    ///
    /// **`node` must be one of the original `0..nodes` control-voter ids
    /// this cluster was constructed with (ADR 0061 rung D4 PR 4)** — this
    /// method indexes `self.controls[node]`, a control-voter-only `Vec`
    /// that a [`SimCluster::grow`]n data-only node was never pushed onto
    /// (it has no local control `RaftNode` at all — see that method's own
    /// doc). Restarting a grown node is out of this rung's scope (no
    /// scenario needs it); calling this with a grown node's index panics on
    /// the `Vec` index, not gracefully.
    pub(crate) fn restart(&mut self, node: u64) {
        let id = nid(node);
        if self.crashed.remove(&node) {
            self.sim.restart(id.clone());
        }
        self.sim.stop(id.clone());

        let all_ids: Vec<NodeId> = (0..self.nodes as u64).map(nid).collect();
        let fresh_control: RaftNode<SimEnv> = RaftNode::start(
            self.sim.env(id.clone()),
            all_ids.clone(),
            MemoryEngine::new(),
        );
        let fresh_relay: SimRelayClient<SimEnv> = SimRelayClient::new(self.sim.env(id.clone()));

        // A restarted node's own `Simulator::stop` dropped its previous
        // `heartbeat_loop` task along with everything else it owned — see
        // `SimCluster::new`'s own comment on why this is load-bearing, not
        // optional.
        let heartbeat_env = self.sim.env(id.clone());
        heartbeat_env.spawn_task(animus_control::node::heartbeat_loop(
            heartbeat_env.clone(),
            all_ids,
        ));

        let mut ctx = self.shared.ctx(node);
        ctx.control = GenericControlHandle::Local(fresh_control.clone());
        ctx.relay = fresh_relay.clone();

        // C-06 PR 4 (2026-09-08, issue found investigating
        // `dynamowire_stop_restart`): `SimCluster::new` registers every
        // node's own control handle onto `ClusterEdgeState::control` via
        // `edge.register_control(control.clone())` (see this file's own
        // module-doc pointer above), because `ClientCtx::propose_schema`'s
        // LOCAL-propose fast path reads `ctx.edge`'s registry, not
        // `ctx.control` — the two are different fields with different
        // lifecycles (`animus-node/CLAUDE.md`'s `ControlHandle` entry).
        // This `restart` rebuilds `ctx.control` above but, before this fix,
        // never told the edge about the fresh handle at all — so a
        // restarted node's `ClusterEdgeState::control` entry kept pointing
        // at the OLD, `Simulator::stop`ped (dead) `RaftNode` forever after.
        // Reads still worked (they go through `ctx.control` directly), but
        // any NEW schema proposal issued through this node's own fast path
        // (e.g. `ensure_txn_idempotency_table`'s `CreateTableSchema`, which
        // `TransactWriteItems`'s `ClientRequestToken` bootstrap needs) spun
        // until `SCHEMA_COMMIT_TIMEOUT`, even with a real, reachable,
        // healthy leader elsewhere — reproduced deterministically via
        // `dynamowire_stop_restart_s02` (an ordinary, non-transactional
        // `CreateTable` from the restarted node reproduced the identical
        // failure, proving this was general control-plane-restart
        // infrastructure, not anything transact-specific).
        //
        // **`register_control` itself is the wrong call here — it only
        // APPENDS** (its own doc: "called once per node," true in
        // production, where a restart always gets a brand-new
        // `ClusterEdgeState`; this fixture instead reuses the SAME `Arc<
        // ClusterEdgeState>` across a restart). A first fix that called
        // `register_control` here left `dynamowire_stop_restart_s02`
        // failing identically — the edge's `control` vec now held BOTH the
        // stale, stopped handle and the fresh one, and `leader_handle()`'s
        // `find` could return the stale one first (frozen at whatever
        // leadership belief it held the instant it was stopped), silently
        // reintroducing the exact bug the append was meant to fix. Use
        // [`ClusterEdgeState::replace_control`] instead — it clears the
        // vec before pushing, so this restarted node's edge holds exactly
        // one control handle at all times, the same invariant
        // `SimCluster::new` establishes and every other node in the
        // cluster maintains for its own entire lifetime.
        ctx.edge.replace_control(fresh_control.clone());

        // ADR 0061 rung D4 PR 1: every `RaftKvNode` driver task this node
        // owned — for ANY tablet, hand-hosted or wire-provisioned — was
        // just dropped by `Simulator::stop` above, so every one of this
        // node's own edge registrations is now stale (a live handle for a
        // driver task that no longer exists). Purge them all — scanning
        // `ctx.edge.hosted_groups()` directly rather than this fixture's
        // own `tablets_snapshot()` bookkeeping, which (like `TabletInfo`
        // generally) only ever knows about a table `create_table_with_
        // replication` itself provisioned, never a wire-created one — the
        // fresh reconciler built below re-hosts every one of them fresh
        // from `Metadata`.
        for (tablet, _group) in ctx.edge.hosted_groups() {
            ctx.edge.unregister_raftkv(tablet, id.clone());
        }

        let reconciler = build_reconciler(
            ctx.env.clone(),
            self.engines[node as usize].clone(),
            id.clone(),
            ctx.edge.clone(),
        );
        spawn_reconciler_loop(ctx.clone(), reconciler);

        // ADR 0061 rung D4 PR 5: `Simulator::stop` above dropped this
        // node's own `backup_janitor_loop` task along with everything else
        // it owned — respawn it unconditionally, exactly like
        // `heartbeat_loop`/the reconciler loop above (this loop is never
        // gated behind an opt-in the way `auto_split_loop` below is).
        // `ctx.backup_store` is untouched by this restart (it was never
        // reassigned above), so the respawned loop still shares the SAME
        // underlying `SimSegmentStore` every other node writes to.
        let janitor_env = ctx.env.clone();
        janitor_env.spawn_task(backup_janitor::backup_janitor_loop(ctx.clone()));

        let ctx_for_server = ctx.clone();
        fresh_relay.serve(move |req| {
            let ctx = ctx_for_server.clone();
            async move { forwarding::handle_relayed_request(&ctx, req).await }
        });

        self.controls[node as usize] = fresh_control;
        self.shared.set_ctx(node, ctx);

        // ADR 0061 rung D4 PR 2: `Simulator::stop` above dropped this
        // node's own `auto_split_loop` task along with everything else it
        // owned, exactly like `heartbeat_loop` — respawn it with the SAME
        // configuration if this cluster ever opted in (`self.auto_split`
        // is `None` for every scenario that never called
        // `set_auto_split_thresholds`, so this is a no-op there).
        if let Some(thresholds) = self.auto_split {
            self.spawn_auto_split(node, thresholds);
        }
    }

    /// Symmetrically partition `a` and `b` (`Simulator::partition_pair`).
    pub(crate) fn partition(&mut self, a: u64, b: u64) {
        self.sim.partition_pair(nid(a), nid(b));
    }

    /// Heal every partition this fixture has created and `Simulator::
    /// restart` every `crash`ed node (un-muting it — this does not touch
    /// a node that was `restart`ed via [`SimCluster::restart`], which was
    /// never `crash`ed in the first place). Resets `NetConfig` to default
    /// too, mirroring `raftkv_linearizable.rs`'s own `Group::heal_all` —
    /// a fired ambient network fault must not outlive its intended window.
    pub(crate) fn heal_all(&mut self) {
        for i in 0..self.nodes as u64 {
            for j in (i + 1)..self.nodes as u64 {
                self.sim.heal(nid(i), nid(j));
            }
        }
        let crashed: Vec<u64> = self.crashed.iter().copied().collect();
        for n in crashed {
            self.sim.restart(nid(n));
        }
        self.crashed.clear();
        self.sim.set_net_config(NetConfig::default());
    }

    /// Advance virtual time by `dur` with nothing else scheduled — for a
    /// caller that wants to hold a fault open, wait out an election
    /// window, or drain a background effect between two assertions.
    pub(crate) fn run_for(&mut self, dur: Duration) {
        self.sim.run_for(dur);
    }

    /// The seed this cluster was built from — for an assertion message
    /// naming a replayable run (`ANIMUS_SEED=<seed>`, root `CLAUDE.md`'s
    /// convention).
    pub(crate) fn seed(&self) -> u64 {
        self.sim.seed()
    }

    /// **ADR 0061 rung D4 PR 4: add a node after construction** — the one
    /// piece of ADR 0030/0032 growth/decommission machinery [`SimCluster::
    /// new`] structurally cannot exercise (the module doc's own "the whole
    /// node set is known at construction" note). `role` must be `"data"`
    /// today — a `"combined"` growth node (a new control-plane voter, via
    /// `change_membership`/`admin_add_control_member`) was scoped for this
    /// rung and deferred: it needs a genuinely new `RaftNode<SimEnv>` joining
    /// the **live** control quorum (`self.controls` growing, not just
    /// `self.nodes`), which is a materially different — and separately
    /// budgeted — piece of machinery than a data-only node's `ControlHandle::
    /// Remote` mirror. Returns the new node's own `u64` index (always
    /// `self.node_count()` as observed just before this call — indices are
    /// **never reused**, even across a later [`SimCluster::remove`] of a
    /// different node, since this fixture only ever appends).
    ///
    /// Mirrors `animusd::BoundDataNode::start_data_with_growth`'s real
    /// construction (see that method's own doc) as closely as a `SimEnv`
    /// fixture can — same `ControlHandle::Remote(RemoteControlClient::new(
    /// ..))`, same per-node reconciler/heartbeat/backup-janitor/auto-split
    /// spawns [`SimCluster::new`] already gives every original node — with
    /// two deliberate departures, both documented at their own call site
    /// below: **self-registration is the fixture's own `RegisterNode`+
    /// `UpsertMember{Active}` control-plane bypass** (`SimCluster::
    /// seed_members`'s idiom, not `ClientCtx::admin_add_member`'s real
    /// relay+failure-detector-promotion dance), and **the mirror-sync loop
    /// is [`spawn_remote_mirror_sync_loop`]**, a `SimEnv`-native
    /// reimplementation of `remote_metadata_sync_loop` (see that function's
    /// own doc for why the production one can't be called directly).
    ///
    /// **Route tables are patched, not synced** — every existing node's own
    /// `client_route`/`intra_route` gains this node's entry via one direct
    /// mutation of each `Arc<Mutex<..>>` map, and the new node's own routes
    /// are seeded from the (now-patched) union. This is deliberately NOT a
    /// `route_sync_loop`/`intra_route_sync_loop` equivalent — those loops
    /// exist in production because a real node only ever learns of another
    /// one incrementally, over time, from `Metadata`; this fixture already
    /// holds every `ClientCtx` in one process, so a one-shot patch at the
    /// instant of growth is both simpler and sufficient for every scenario
    /// this rung's own module needs. A future rung wanting to prove the real
    /// sync loops themselves would need to build them fresh here, not widen
    /// this one.
    ///
    /// Returns once every node's own view of `Metadata::members` shows the
    /// new node `Active` (converged-or-timeout polled, the same discipline
    /// every other DDL-shaped method on this fixture uses) — so a caller's
    /// very next op issued from the new node, or targeting it, can rely on
    /// that being true.
    pub(crate) fn grow(&mut self, role: &str) -> u64 {
        assert_eq!(
            role, "data",
            "SimCluster::grow supports role=\"data\" (data-only growth) \
             only today — a \"combined\" (new control-plane voter) growth \
             node is deferred, see this method's own doc"
        );
        let new_n = self.nodes as u64;
        let id = nid(new_n);
        let addr = id.to_string();

        // The pre-growth control quorum's own addresses — this fixture's
        // `SimRelayClient` addressing convention (`NodeId::to_string()`) is
        // identical to `client_route`/`intra_route`'s own entries built in
        // `SimCluster::new`. Fixed for the life of this rung (no `"combined"`
        // growth yet, so `self.controls` never grows).
        let control_ids: Vec<NodeId> = (0..self.controls.len() as u64).map(nid).collect();
        let seeds: Vec<String> = control_ids.iter().map(NodeId::to_string).collect();

        // Patch every EXISTING node's own route tables with the new node's
        // entry, then read back the union from node 0's own (now-patched)
        // map as the new node's own initial route tables — see this
        // method's own doc for why a one-shot patch, not a sync loop, is
        // the right shape here.
        for n in 0..self.nodes as u64 {
            let existing = self.shared.ctx(n);
            existing
                .client_route
                .lock()
                .expect("client route poisoned")
                .insert(id.clone(), addr.clone());
            existing
                .intra_route
                .lock()
                .expect("intra route poisoned")
                .insert(id.clone(), addr.clone());
        }
        let route: BTreeMap<NodeId, String> = self
            .shared
            .ctx(0)
            .client_route
            .lock()
            .expect("client route poisoned")
            .clone();

        let env = self.sim.env(id.clone());
        let relay: SimRelayClient<SimEnv> = SimRelayClient::new(env.clone());
        let remote = GenericRemoteControlClient::new(seeds.clone(), relay.clone(), CLIENT_TIMEOUT);
        let control = GenericControlHandle::Remote(remote.clone());
        let edge = ClusterEdgeState::<SimEnv>::new();

        let admin = Arc::new(AdminInfo {
            auto_split_ops_rate_threshold: None,
            throttle_read_units: None,
            throttle_write_units: None,
            node_id: Some(id.clone()),
            internal_addr: Some(placeholder_addr()),
            client_addr: placeholder_addr(),
            dynamo_addr: None,
            admin_addr: placeholder_addr(),
            role: "data",
            control_ids: control_ids.clone(),
            peers: BTreeMap::new(),
            admin_addrs: vec![placeholder_addr()],
            auto_split_bytes_threshold: None,
            backup_store: None,
            segment_store: None,
            quiesce_after_ms: None,
            auth_enabled: None,
            auth_access_key_ids: None,
            otlp_endpoint: None,
        });

        let ctx: SimNodeCtx = ClientCtx {
            control,
            edge: edge.clone(),
            env: env.clone(),
            data: Some(DataRole {
                raftkv_metrics: MetricsHandle::noop(),
                base_id: id.clone(),
                stream_seal_knobs: StreamSealKnobs::default(),
                change_rates: ChangeRateTracker::default(),
                request_rates: RequestRateTracker::default(),
            }),
            segment_store: SegmentStoreHandle::Fs(FsSegmentStore::new(format!(
                "unused-segment-store-{new_n}"
            ))),
            backup_store: BackupStoreHandle::S3(Arc::new(self.backup_store.clone())),
            export_store_factory: Arc::new(Mutex::new(default_export_store_factory(None))),
            backup_janitor_progress: Arc::new(Mutex::new(
                animus_node::backup_janitor::JanitorProgress::default(),
            )),
            ttl_reaper_progress: Arc::new(Mutex::new(
                animus_node::ttl_reaper::TtlReaperProgress::default(),
            )),
            segment_janitor_progress: Arc::new(Mutex::new(
                segment_janitor::SegmentJanitorProgress::default(),
            )),
            client_route: Arc::new(Mutex::new(route.clone())),
            intra_route: Arc::new(Mutex::new(route)),
            admin,
            metrics_history: Arc::new(Mutex::new(VecDeque::new())),
            remote_metadata: Arc::new(Mutex::new(None)),
            control_storage: None,
            dynamo_auth: None,
            tls: None,
            relay: relay.clone(),
            throttle: ThrottleTracker::new(),
            throttle_defaults: Arc::new(ThrottleDefaults::default()),
            any_table_throughput: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        };

        // Install the relay server, exactly like `SimCluster::new`/
        // `restart` do for every other node.
        let ctx_for_server = ctx.clone();
        relay.serve(move |req| {
            let ctx = ctx_for_server.clone();
            async move { forwarding::handle_relayed_request(&ctx, req).await }
        });

        self.shared.push_ctx(ctx.clone());
        self.engines.push(MemoryTabletEngines::new());
        self.nodes += 1;

        // Self-registration: `RegisterNode` + `UpsertMember{Active}`,
        // proposed directly on the current control leader — the identical
        // control-plane bypass idiom `SimCluster::seed_members` uses for the
        // initial node set, deliberately not `ClientCtx::admin_add_member`'s
        // real relay + `Down`-then-failure-detector-promotion dance (see
        // this method's own doc).
        let leader = self.control_leader_index();
        let addrs = NodeAddrs {
            internal: addr.clone(),
            client: addr.clone(),
            intra: addr.clone(),
            admin: addr,
            role: "data".to_owned(),
        };
        assert!(
            matches!(
                self.controls[leader].propose(MetaCommand::RegisterNode {
                    node: id.clone(),
                    addrs,
                    labels: BTreeMap::new(),
                }),
                ProposeResult::Accepted { .. }
            ),
            "RegisterNode must be accepted by the current control leader (grow node={new_n})"
        );
        assert!(
            matches!(
                self.controls[leader].propose(MetaCommand::UpsertMember {
                    node: id.clone(),
                    labels: BTreeMap::new(),
                    status: NodeStatus::Active,
                }),
                ProposeResult::Accepted { .. }
            ),
            "UpsertMember must be accepted by the current control leader (grow node={new_n})"
        );

        // Heartbeat the (pre-growth) control voter set — load-bearing, the
        // identical `SimCluster::new`/`restart` reasoning: a member with no
        // heartbeat loop running flips back to `Down` within
        // `DETECT_TIMEOUT` of the control group's own failure detector.
        let hb_env = env.clone();
        hb_env.spawn_task(animus_control::node::heartbeat_loop(
            hb_env.clone(),
            control_ids,
        ));

        // The real per-node tablet-host reconciler — identical construction
        // to `SimCluster::new`/`restart`.
        let reconciler = build_reconciler(
            env.clone(),
            self.engines[new_n as usize].clone(),
            id.clone(),
            edge,
        );
        spawn_reconciler_loop(ctx.clone(), reconciler);

        // The backup janitor — unconditional spawn, identical to
        // `SimCluster::new`/`restart`.
        let janitor_env = env.clone();
        janitor_env.spawn_task(backup_janitor::backup_janitor_loop(ctx.clone()));

        // Auto-split, if this cluster opted in — identical to
        // `SimCluster::restart`'s own respawn.
        if let Some(thresholds) = self.auto_split {
            self.spawn_auto_split(new_n, thresholds);
        }

        // The one genuinely new mechanism this rung adds: a `SimEnv`-native
        // mirror-sync loop driving `ControlHandle::Remote`'s real
        // observe/observe_delta/leader-hint logic — see that function's own
        // doc for why this couldn't just call `animusd`'s own
        // `remote_metadata_sync_loop`.
        spawn_remote_mirror_sync_loop(ctx, remote, seeds);

        // Converge: every node's own view of `Metadata::members` shows the
        // new node `Active` — including the new node's own `Remote` mirror,
        // which only becomes true once `spawn_remote_mirror_sync_loop`'s
        // first round trip lands.
        let target = id;
        let total = self.nodes as u64;
        self.poll_until(Duration::from_secs(10), move |c| {
            (0..total).all(|n| {
                c.metadata(n)
                    .members
                    .get(&target)
                    .is_some_and(|m| m.status == NodeStatus::Active)
            })
        });

        new_n
    }

    /// **ADR 0061 rung D4 PR 4: drive the ADR 0032 decommission sequence's
    /// drain half** on `node` — `ClientCtx::admin_drain`, proposed on the
    /// CURRENT control leader's own ctx (mirroring production's own
    /// local-leader-only, not-relayed discipline for this admin action; see
    /// that method's own doc). Marks `node` `Leaving` so the control-plane
    /// leader's own `reconcile_loop` (spawned unconditionally by
    /// `RaftNode::start`, already running on every control voter this
    /// fixture builds — no extra driving needed) excludes it from
    /// `active_candidates` and repairs every tablet policy that named it,
    /// same real production mechanism, not a stand-in.
    ///
    /// Converged-or-timeout polled on [`SimCluster::hosted_tablets`]`(node)`
    /// going empty — the real per-node reconciler's own teardown, not a
    /// bookkeeping flip. A node hosting nothing to begin with converges
    /// immediately (this is a valid, if less interesting, call).
    pub(crate) fn drain(&mut self, node: u64) {
        let leader = self.control_leader_index();
        let ctx = self.shared.ctx(leader as u64);
        ctx.admin_drain(nid(node)).unwrap_or_else(|e| {
            panic!("admin_drain(node={node}) must be accepted by the control leader: {e}")
        });
        self.poll_until(Duration::from_secs(20), |c| {
            c.shared.hosted_tablets(node).is_empty()
        });
    }

    /// **ADR 0061 rung D4 PR 4: drive the ADR 0032 decommission sequence's
    /// finishing half** on `node` — `ClientCtx::admin_remove_member`,
    /// proposed on the current control leader's own ctx (same not-relayed
    /// discipline as [`SimCluster::drain`]). Panics if the control leader
    /// rejects it — the common cause is calling this before [`SimCluster::
    /// drain`] has converged (the member is still `Active`/`Joining`, or
    /// still referenced by a tablet); the panic message says so.
    ///
    /// Converged-or-timeout polled on every node's own view of
    /// `Metadata::members` no longer naming `node` at all. **`node`'s own
    /// index is never reused by a later [`SimCluster::grow`]** — this
    /// fixture only ever appends (`grow`'s own `new_n = self.node_count()`),
    /// so a removed node's id simply becomes permanently inert bookkeeping,
    /// mirroring production's own "ids are never reused" tablet/node
    /// convention (root `CLAUDE.md`).
    pub(crate) fn remove(&mut self, node: u64) {
        let leader = self.control_leader_index();
        let ctx = self.shared.ctx(leader as u64);
        ctx.admin_remove_member(nid(node)).unwrap_or_else(|e| {
            panic!(
                "admin_remove_member(node={node}) must be accepted by the control \
                 leader — did SimCluster::drain(node) converge first? ({e})"
            )
        });
        let target = nid(node);
        let total = self.nodes as u64;
        self.poll_until(Duration::from_secs(10), move |c| {
            (0..total).all(|n| !c.metadata(n).members.contains_key(&target))
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Scenario 1: 3 nodes, RF 3. A write through the leader is readable,
    /// eventually, from every node — **including a real linearizable
    /// (`ConsistentRead: true`-equivalent) read from a non-leader**, which
    /// must forward to the actual leader over the real `SimRelayClient`
    /// wire and observe the write.
    #[test]
    fn write_on_leader_reads_back_consistent_from_every_node() {
        run_write_on_leader_reads_back_consistent_from_every_node(0x51C1_0001);
    }

    #[test]
    fn write_on_leader_reads_back_consistent_from_every_node_seed2() {
        run_write_on_leader_reads_back_consistent_from_every_node(0x51C1_0002);
    }

    #[test]
    fn write_on_leader_reads_back_consistent_from_every_node_seed3() {
        run_write_on_leader_reads_back_consistent_from_every_node(0x51C1_0003);
    }

    /// Replay proof (repo convention): `ANIMUS_SEED=<seed> cargo test -p
    /// animusd --lib replays_scenario_1_from_an_explicit_env_seed`.
    #[test]
    fn replays_scenario_1_from_an_explicit_env_seed() {
        let seed = std::env::var("ANIMUS_SEED")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0x51C1_0004);
        run_write_on_leader_reads_back_consistent_from_every_node(seed);
    }

    fn run_write_on_leader_reads_back_consistent_from_every_node(seed: u64) {
        let mut cluster = SimCluster::new(seed, 3, 3);
        cluster.create_table("orders");
        let tablet = cluster.tablet_of("orders").expect("just created");
        let leader = cluster
            .leader_index_of(tablet)
            .expect("the fresh group elected a leader");

        cluster
            .put(leader, "orders", "cust-1", "order-1", b"first order")
            .unwrap_or_else(|e| panic!("write on the leader must succeed (seed={seed}): {e}"));

        for node in 0..cluster.node_count() as u64 {
            cluster.poll_until_get_eq(
                node,
                "orders",
                "cust-1",
                "order-1",
                true,
                Some(b"first order".to_vec()),
                Duration::from_secs(5),
            );
        }

        // `scan` (the cheap extra read shape beyond point `get`) sees the
        // same write, from the leader itself.
        let scanned = cluster
            .scan(leader, "orders", true)
            .unwrap_or_else(|e| panic!("scan on the leader must succeed (seed={seed}): {e}"));
        assert!(
            scanned.iter().any(|(_, v)| v == b"first order"),
            "a whole-table scan must see the write it just made (seed={seed}): {scanned:?}"
        );

        // `delete` removes it — proven by a subsequent consistent `get`
        // observing a clean absent, never an error (mirroring `cp_get`'s
        // own "unprovisioned/emptied table reads as absent" contract).
        cluster
            .delete(leader, "orders", "cust-1", "order-1")
            .unwrap_or_else(|e| panic!("delete on the leader must succeed (seed={seed}): {e}"));
        cluster.poll_until_get_eq(
            leader,
            "orders",
            "cust-1",
            "order-1",
            true,
            None,
            Duration::from_secs(5),
        );
    }

    /// Scenario 2: RF 2 of 3 nodes. A write issued from the non-hosting
    /// third node succeeds through the relay (a genuine "no local
    /// replica at all" forward, not merely "hosts a non-leader replica")
    /// and is readable everywhere, including that same non-hosting node.
    #[test]
    fn write_from_a_non_hosting_node_forwards_and_is_readable_everywhere() {
        run_write_from_a_non_hosting_node(0x51C2_0001);
    }

    #[test]
    fn write_from_a_non_hosting_node_forwards_and_is_readable_everywhere_seed2() {
        run_write_from_a_non_hosting_node(0x51C2_0002);
    }

    fn run_write_from_a_non_hosting_node(seed: u64) {
        let mut cluster = SimCluster::new(seed, 3, 2);
        cluster.create_table("orders");
        let tablet = cluster.tablet_of("orders").expect("just created");
        assert!(
            cluster.leader_of(tablet).is_some(),
            "the RF-2 group must have elected a leader (seed={seed})"
        );
        // Node 2 hosts no replica at all (replication == 2 hosts nodes 0,1).
        let non_hosting = 2u64;

        cluster
            .put(
                non_hosting,
                "orders",
                "cust-2",
                "order-1",
                b"from the non-hosting node",
            )
            .unwrap_or_else(|e| {
                panic!("write from a non-hosting node must forward and succeed (seed={seed}): {e}")
            });

        for node in 0..cluster.node_count() as u64 {
            cluster.poll_until_get_eq(
                node,
                "orders",
                "cust-2",
                "order-1",
                true,
                Some(b"from the non-hosting node".to_vec()),
                Duration::from_secs(5),
            );
        }
    }

    /// Scenario 3: crash the tablet leader, wait out an election window,
    /// write through a surviving node, restart the crashed node, and
    /// confirm the whole group converges on the write — a
    /// converged-or-timeout poll throughout, never a one-shot assert.
    #[test]
    fn crash_leader_write_through_survivor_then_restart_converges() {
        run_crash_leader_write_through_survivor_then_restart(0x51C3_0001);
    }

    #[test]
    fn crash_leader_write_through_survivor_then_restart_converges_seed2() {
        run_crash_leader_write_through_survivor_then_restart(0x51C3_0002);
    }

    fn run_crash_leader_write_through_survivor_then_restart(seed: u64) {
        let mut cluster = SimCluster::new(seed, 3, 3);
        cluster.create_table("orders");
        let tablet = cluster.tablet_of("orders").expect("just created");
        let leader = cluster.leader_index_of(tablet).expect("elected");

        cluster.crash(leader);
        cluster.run_for(Duration::from_millis(1500)); // election window

        let survivor = (0..cluster.node_count() as u64)
            .find(|&n| n != leader)
            .expect("a 3-node cluster has a survivor");
        cluster
            .put(survivor, "orders", "cust-3", "order-1", b"through a survivor")
            .unwrap_or_else(|e| {
                panic!("a write through a surviving node must succeed after the leader crashes (seed={seed}): {e}")
            });

        cluster.restart(leader);
        cluster.run_for(Duration::from_secs(2));

        for node in 0..cluster.node_count() as u64 {
            cluster.poll_until_get_eq(
                node,
                "orders",
                "cust-3",
                "order-1",
                true,
                Some(b"through a survivor".to_vec()),
                Duration::from_secs(8),
            );
        }
    }

    /// Scenario 4: a partitioned minority node cannot ack a write (its
    /// own attempt must fail — it cannot reach the majority side at
    /// all), the write succeeds when issued on the majority side, and
    /// the minority node catches up once healed.
    #[test]
    fn partitioned_minority_cannot_ack_majority_succeeds_and_heals() {
        run_partitioned_minority(0x51C4_0001);
    }

    #[test]
    fn partitioned_minority_cannot_ack_majority_succeeds_and_heals_seed2() {
        run_partitioned_minority(0x51C4_0002);
    }

    fn run_partitioned_minority(seed: u64) {
        let mut cluster = SimCluster::new(seed, 3, 3);
        cluster.create_table("orders");
        let tablet = cluster.tablet_of("orders").expect("just created");
        let leader = cluster.leader_index_of(tablet).expect("elected");
        // Isolate a non-leader replica as a 1-node minority — the leader
        // and the remaining replica still form a majority of 3.
        let minority = (0..cluster.node_count() as u64)
            .find(|&n| n != leader)
            .expect("a 3-node cluster has a non-leader replica");

        for n in 0..cluster.node_count() as u64 {
            if n != minority {
                cluster.partition(minority, n);
            }
        }
        cluster.run_for(Duration::from_millis(500));

        let minority_result = cluster.put(
            minority,
            "orders",
            "cust-4",
            "order-1",
            b"attempted from the minority",
        );
        assert!(
            minority_result.is_err(),
            "an isolated minority node must not be able to ack a write (seed={seed}): {minority_result:?}"
        );

        cluster
            .put(
                leader,
                "orders",
                "cust-4",
                "order-1",
                b"from the majority side",
            )
            .unwrap_or_else(|e| {
                panic!("a write on the majority side must still succeed (seed={seed}): {e}")
            });

        cluster.heal_all();
        cluster.run_for(Duration::from_secs(1));

        for node in 0..cluster.node_count() as u64 {
            cluster.poll_until_get_eq(
                node,
                "orders",
                "cust-4",
                "order-1",
                true,
                Some(b"from the majority side".to_vec()),
                Duration::from_secs(8),
            );
        }
    }

    /// Scenario 5: a second `create_table` works after the first — DDL
    /// proposed twice against the same live control quorum, with both
    /// tables' schema/tablet visible on every node.
    #[test]
    fn a_second_create_table_works_after_the_first() {
        let seed = 0x51C5_0001;
        let mut cluster = SimCluster::new(seed, 3, 3);
        cluster.create_table("orders");
        cluster.create_table("customers");

        for node in 0..cluster.node_count() as u64 {
            let meta = cluster.metadata(node);
            assert!(
                meta.has_table_tablet("orders"),
                "node {node} must see the first table (seed={seed})"
            );
            assert!(
                meta.has_table_tablet("customers"),
                "node {node} must see the second table (seed={seed})"
            );
        }

        // Both tables are independently writable/readable — proof the two
        // tablets' distinct `stream = tablet.0` Raft addressing (ADR 0026
        // Stage B) never cross-talks, the exact hazard
        // `animus-test/CLAUDE.md`'s stream-corpus entry documents for
        // `RaftKvNode::start_scoped` (this fixture always uses
        // `start_hosted` with the tablet id as the stream for precisely
        // this reason).
        cluster
            .put(0, "orders", "cust-5", "order-1", b"orders row")
            .expect("orders write succeeds");
        cluster
            .put(0, "customers", "cust-5", "profile", b"customers row")
            .expect("customers write succeeds");
        for node in 0..cluster.node_count() as u64 {
            cluster.poll_until_get_eq(
                node,
                "orders",
                "cust-5",
                "order-1",
                true,
                Some(b"orders row".to_vec()),
                Duration::from_secs(5),
            );
            cluster.poll_until_get_eq(
                node,
                "customers",
                "cust-5",
                "profile",
                true,
                Some(b"customers row".to_vec()),
                Duration::from_secs(5),
            );
        }
    }

    impl SimCluster {
        /// Converged-or-timeout `get` assertion, shared by every scenario
        /// above (root `CLAUDE.md`'s Testing rule: an eventual property
        /// gets a converged-or-timeout poll, never a fixed-deadline
        /// one-shot assert): retries `node`'s own `get(table, pk, sk,
        /// consistent)` — each call already carries its own internal
        /// route/confirm retry budget (`OP_BUDGET`) — up to a small fixed
        /// number of independent attempts, advancing virtual time by
        /// `settle` between them, until it equals `expected` or the
        /// attempts are exhausted. Bounded by attempt count rather than a
        /// wall/virtual-time deadline directly, since each attempt is
        /// already a full fresh `CLIENT_TIMEOUT`-bounded client call, not
        /// a cheap poll — a caller wanting a specific total ceiling can
        /// pass a `settle` sized so `attempts * (settle + CLIENT_TIMEOUT)`
        /// stays inside it.
        #[allow(clippy::too_many_arguments)] // a plain (node, table, pk, sk, consistent, expected, settle) test-helper parameter list — splitting it into a struct would just move the same seven pieces of information one level of indirection away
        fn poll_until_get_eq(
            &mut self,
            node: u64,
            table: &str,
            pk: &str,
            sk: &str,
            consistent: bool,
            expected: Option<Vec<u8>>,
            settle: Duration,
        ) {
            const ATTEMPTS: usize = 3;
            let seed = self.seed();
            let mut last: Result<Option<Vec<u8>>, String> = Err("never attempted".to_owned());
            for _ in 0..ATTEMPTS {
                last = self.get(node, table, pk, sk, consistent);
                if last.as_ref().ok() == Some(&expected) {
                    return;
                }
                self.run_for(settle);
            }
            panic!(
                "node {node}'s own {} read of {table}/{pk}/{sk} never converged to \
                 {expected:?} within {ATTEMPTS} attempts (last={last:?}, seed={seed})",
                if consistent { "consistent" } else { "eventual" }
            );
        }
    }
}
