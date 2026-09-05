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
//! - **Tablets are hand-hosted, not reconciler-hosted.** [`SimCluster::
//!   create_table`] proposes `CreateTableSchema`/`CreateTablet` directly on
//!   the control group's current leader (the bypass every `SimEnv`
//!   `ClientCtx` fixture in this crate uses — `ClientCtx::propose_schema`'s
//!   local-propose fast path is `ClusterEdgeState::control:
//!   Arc<Mutex<Vec<RaftNode<ProdEnv>>>>`-typed regardless of the enclosing
//!   `ClientCtx<E, R>`'s own `E`, per the eighth 2026-08-28 ADR 0061
//!   amendment), then constructs a `RaftKvNode<SimEnv, MemoryEngine>`
//!   directly on each replica node and registers it into that node's own
//!   `ClusterEdgeState` — mirroring `animus-test::raftkv_linearizable`'s
//!   `Group::start`, not `animus-cp-data::host::Reconciler`. `Metadata`'s
//!   tablet row (`replicas: Vec<NodeId>`) and each hosting node's edge
//!   registration are built from the exact same replica list in the same
//!   call, so they can never disagree. This is deliberately **not** the
//!   real production path (`animus-cp-data::host::Reconciler` discovers
//!   hosting from replicated `Metadata` event-driven, ADR 0031) — a
//!   reconciler-hosted `SimCluster` is a legitimate future rung (it would
//!   additionally prove the reconciler's own event loop, not just the
//!   client paths this rung targets) but is more machinery than D1's own
//!   brief asks for; see `crates/animus-cp-data/tests/reconciler_corpus.rs`'s
//!   `Cluster`/`ClusterNode` for the shape a future rung could lift.
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
//! - **Restart is a true process restart, on `MemoryEngine`.** [`SimCluster::
//!   restart`] mirrors `raftkv_linearizable.rs`'s own `StopRestart` nemesis:
//!   `Simulator::stop` (drops every task the node owns — its control
//!   `RaftNode` driver, every hosted `RaftKvNode` driver, its relay receive
//!   loop — durable disk aside) followed by fresh `RaftNode::start`/
//!   `RaftKvNode::start_hosted` calls on the same node id. Since this
//!   fixture only ever uses `MemoryEngine` (matching every other `SimEnv`
//!   `ClientCtx` harness in this crate), a "restart" is a **wipe-and-rejoin**
//!   for both the restarted control voter and every tablet group it
//!   hosted — recovery is via ordinary peer catch-up / chunked
//!   `InstallSnapshot`, never local WAL replay. A durable (`LsmEngine`)
//!   `SimCluster` tier is a natural follow-on (mirroring
//!   `raftkv_linearizable.rs`'s own two-tier design) but is not built here.
//! - **What is still `ProdEnv`-only.** `ClientCtx::propose_schema`'s
//!   local-propose fast path (as above); `SegmentStoreHandle`/
//!   `BackupStoreHandle`'s `Cluster` variant (this fixture only ever uses
//!   the `Fs` placeholder, like every sibling harness — nothing this
//!   fixture drives reads `ctx.segment_store`/`ctx.backup_store`); and a
//!   `DataRole` (`data: None` on every node — no DynamoDB wire edge, no TTL
//!   reaper, no stream/backup loops; this fixture drives the plain
//!   `cp_kind_write_raw`/`cp_get`/`cp_scan` client-protocol methods only).

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_cp_data::{KIND_BASE, StorageScope};
use animus_dynamo::AttributeValue;
use animus_env::{EnvExt, nid};
use animus_node::SimRelayClient;
use animus_sim::{NetConfig, SimEnv, Simulator};

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
/// name (diagnostics only), its declared range (always
/// [`KeyRange::whole`] — this fixture never splits a table), and the node
/// ids currently hosting a replica (in the order [`SimCluster::
/// create_table`] chose them — `replicas[0]` has no special status, it's
/// simply this fixture's own bookkeeping order, not a leader hint).
///
/// `Clone` (ADR 0061 rung D1 step 3, the [`SimClusterHandle`] refactor
/// below): a corpus's own concurrently-spawned client tasks read a
/// **snapshot** of this map (`SimClusterHandle::replicas_of`/
/// `tablets_snapshot`) rather than holding the shared lock across an
/// `.await`.
#[derive(Clone)]
struct TabletInfo {
    table: String,
    range: KeyRange,
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

    fn register_raftkv(&self, node: u64, tablet: TabletId, group: CpGroup<SimEnv>) {
        self.ctxs.lock().expect("ctxs poisoned")[node as usize]
            .edge
            .register_raftkv(tablet, group);
    }

    fn insert_tablet(&self, tablet: TabletId, info: TabletInfo) {
        self.tablets
            .lock()
            .expect("tablets poisoned")
            .insert(tablet, info);
    }

    /// A snapshot clone of the whole tablet map — used by [`SimCluster::
    /// restart`], which must iterate every provisioned tablet while also
    /// driving the simulator (an `.await`-free loop over a held lock would
    /// be sound too, but a snapshot keeps this method's shape identical to
    /// every other reader here, which all snapshot-then-release).
    fn tablets_snapshot(&self) -> BTreeMap<TabletId, TabletInfo> {
        self.tablets.lock().expect("tablets poisoned").clone()
    }

    fn all_have_table_tablet(&self, table: &str) -> bool {
        self.ctxs
            .lock()
            .expect("ctxs poisoned")
            .iter()
            .all(|ctx| ctx.effective_metadata().has_table_tablet(table))
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
    pub(crate) fn leader_index_of(&self, tablet: TabletId) -> Option<u64> {
        self.replicas_of(tablet)
            .into_iter()
            .find(|&n| self.is_leader_local(n, tablet))
    }

    /// `node`'s own view of the replicated control-plane `Metadata`.
    pub(crate) fn metadata(&self, node: u64) -> Metadata {
        self.ctx(node).effective_metadata()
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
    next_tablet_id: u64,
    /// Node ids currently [`SimCluster::crash`]ed (muted, tasks still
    /// alive) — tracked so [`SimCluster::heal_all`] knows which ones need
    /// `Simulator::restart` (the un-mute call, unrelated to this struct's
    /// own [`SimCluster::restart`] method despite the shared name — see
    /// that method's own doc).
    crashed: BTreeSet<u64>,
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

        let relays: Vec<SimRelayClient<SimEnv>> = ids
            .iter()
            .map(|id| SimRelayClient::new(sim.env(id.clone())))
            .collect();

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
                data: None,
                // Neither `cp_kind_write_raw`/`cp_get`/`cp_scan` (the only
                // methods this fixture drives) ever reads these — see the
                // module doc's "still `ProdEnv`-only" bullet, and
                // `simenv_client_ctx_tests::single_node_ctx`'s own doc for
                // why the `Fs` placeholder needs no real filesystem or
                // `ProdEnv` to satisfy the field.
                segment_store: SegmentStoreHandle::Fs(FsSegmentStore::new(format!(
                    "unused-segment-store-{i}"
                ))),
                backup_store: BackupStoreHandle::Fs(FsSegmentStore::new(format!(
                    "unused-backup-store-{i}"
                ))),
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

        let mut cluster = SimCluster {
            sim,
            nodes,
            replication,
            controls,
            shared: SimClusterHandle::new(ctxs),
            next_tablet_id: 1,
            crashed: BTreeSet::new(),
        };
        // Let the control group elect before any caller touches it —
        // generous for up to a handful of voters under `SimEnv`'s
        // near-instant elections.
        cluster.sim.run_for(Duration::from_secs(2));
        cluster
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
    fn control_leader_index(&mut self) -> usize {
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
    /// attributes — see `item_key`'s own doc) and host its single tablet
    /// on the first `replication` node ids (`0..replication`) — see the
    /// module doc's hand-hosting bullet for why. DDL is seeded by
    /// proposing directly on the control group's current leader; this
    /// call drives the simulator itself (never requires a caller-side
    /// `run_for`) and returns only once every node's own `Metadata`
    /// shows the table **and** the tablet's freshly hosted group has
    /// elected a leader — so a caller's very next `put`/`get` can rely on
    /// both being true immediately.
    pub(crate) fn create_table_with_replication(
        &mut self,
        table: &str,
        replication: usize,
    ) -> TabletId {
        assert!(
            (1..=self.nodes).contains(&replication),
            "replication must be between 1 and the node count"
        );
        let tablet = TabletId(self.next_tablet_id);
        self.next_tablet_id += 1;
        let replicas: Vec<u64> = (0..replication as u64).collect();
        let replica_ids: Vec<NodeId> = replicas.iter().copied().map(nid).collect();

        let leader = self.control_leader_index();
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

        // Converged-or-timeout: every node's own control read must show
        // the freshly seeded schema/tablet before this fixture hosts
        // anything against it.
        self.poll_until(Duration::from_secs(5), |c| {
            c.shared.all_have_table_tablet(table)
        });

        self.shared.insert_tablet(
            tablet,
            TabletInfo {
                table: table.to_owned(),
                range: KeyRange::whole(),
                replicas: replicas.clone(),
            },
        );

        for &n in &replicas {
            let kv: RaftKvNode<SimEnv, MemoryEngine> = RaftKvNode::start_hosted(
                self.sim.env(nid(n)),
                replica_ids.clone(),
                MemoryEngine::new(),
                StorageScope::new(KeyRange::whole()),
                tablet.0,
            );
            self.shared.register_raftkv(n, tablet, CpGroup::Mem(kv));
        }

        // Let the fresh group elect a leader before returning — same
        // converged-or-timeout shape, never a fixed sleep.
        self.poll_until(Duration::from_secs(3), move |c| {
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

    /// `node`'s own view of the replicated control-plane `Metadata` —
    /// `ClientCtx::effective_metadata`'s exact read, so a caller can
    /// assert on tablet placement / schema visibility per node.
    pub(crate) fn metadata(&self, node: u64) -> Metadata {
        self.shared.metadata(node)
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

    /// Crash `node`: its tasks stay alive but muted (no sends land, its
    /// inbox is cleared) — `Simulator::crash`'s own contract. Use
    /// [`SimCluster::restart`] instead for a true process restart (a
    /// fresh `RaftNode`/`RaftKvNode` on the same id).
    pub(crate) fn crash(&mut self, node: u64) {
        self.sim.crash(nid(node));
        self.crashed.insert(node);
    }

    /// A true process restart of `node`: every task it owns is dropped
    /// (`Simulator::stop`), then a fresh control `RaftNode` and a fresh
    /// `RaftKvNode` for every tablet this fixture recorded `node` as
    /// hosting are started on the same id, each on a brand-new
    /// `MemoryEngine` — see the module doc's own restart bullet for why
    /// this is a wipe-and-rejoin on this fixture's engine tier, not a WAL
    /// recovery. If `node` was `crash`ed (not merely alive), it is first
    /// un-muted (`Simulator::restart`, animus-sim's own required
    /// un-crash-before-stop sequencing — see that method's crate's own
    /// `CLAUDE.md` gotcha) so the following `stop` actually removes live
    /// tasks rather than muted ones.
    pub(crate) fn restart(&mut self, node: u64) {
        let id = nid(node);
        if self.crashed.remove(&node) {
            self.sim.restart(id.clone());
        }
        self.sim.stop(id.clone());

        let all_ids: Vec<NodeId> = (0..self.nodes as u64).map(nid).collect();
        let fresh_control: RaftNode<SimEnv> =
            RaftNode::start(self.sim.env(id.clone()), all_ids, MemoryEngine::new());
        let fresh_relay: SimRelayClient<SimEnv> = SimRelayClient::new(self.sim.env(id.clone()));

        let mut ctx = self.shared.ctx(node);
        ctx.control = GenericControlHandle::Local(fresh_control.clone());
        ctx.relay = fresh_relay.clone();

        for (&tablet, info) in &self.shared.tablets_snapshot() {
            if !info.replicas.contains(&node) {
                continue;
            }
            ctx.edge.unregister_raftkv(tablet, id.clone());
            let replica_ids: Vec<NodeId> = info.replicas.iter().copied().map(nid).collect();
            let fresh_kv: RaftKvNode<SimEnv, MemoryEngine> = RaftKvNode::start_hosted(
                self.sim.env(id.clone()),
                replica_ids,
                MemoryEngine::new(),
                StorageScope::new(info.range.clone()),
                tablet.0,
            );
            ctx.edge.register_raftkv(tablet, CpGroup::Mem(fresh_kv));
        }

        let ctx_for_server = ctx.clone();
        fresh_relay.serve(move |req| {
            let ctx = ctx_for_server.clone();
            async move { forwarding::handle_relayed_request(&ctx, req).await }
        });

        self.controls[node as usize] = fresh_control;
        self.shared.set_ctx(node, ctx);
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
