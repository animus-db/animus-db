//! `ClusterSegmentStore` (ADR 0043 §A7b, decision F5): the **default**
//! [`SegmentStore`] — K-way replicated across nodes' own local segment
//! directories, over the `Env` network/disk seams (ADR 0003), so it is fully
//! `SimEnv`-fault-injectable like every other distributed mechanism in this
//! codebase, not a production-only integration surface tested by hand.
//!
//! **Why the default is this heavy, not a single directory.** A stream's
//! sealed records are exactly as much "the database's own data" as anything
//! else it holds — a default store any less durable than the rest of this
//! system (Raft-replicated, fsynced-before-ack) would be a durability
//! *regression* for a user who enables Streams. So `put_replicated` only
//! returns `Ok` once **K** nodes have each independently written and fsynced
//! the object through their own [`SegmentStore`] (in production,
//! [`FsSegmentStore`](animus_env::FsSegmentStore); in a sim test,
//! `animus-sim`'s `SimSegmentStore`) — this crate treats that inner store as
//! an opaque per-node building block and adds only the K-way fan-out,
//! placement, and repair-friendly bookkeeping on top.
//!
//! **Composition, not a rewrite of `SegmentStore`'s own durability
//! contract.** `ClusterSegmentStore<E, S>` wraps (a) an inner local `S:
//! SegmentStore` — this node's own segment directory — and (b) `E: Env`'s
//! `Network` for peer transfer. It does not touch disk itself; every byte it
//! ever writes goes through `S`, on this node or a peer's own copy of `S`
//! reached over the wire.
//!
//! **Wire shape (ADR 0026).** Each node that constructs a
//! [`ClusterSegmentStore`] and calls [`ClusterSegmentStore::start`] runs
//! exactly one serving task, listening (and replying) on the single
//! reserved [`SEGMENT_STREAM`] — the same "one dedicated stream, one
//! single-consumer serving task" shape `RaftKvNode`'s own per-tablet driver
//! loop uses on its `stream` (`lib.rs`'s `drive`), generalized to a
//! cluster-wide (not per-tablet) responsibility. [`SegmentWire`] is this
//! module's own enum, `serde_json`'d over the `Network`'s `Vec<u8>`
//! payloads — the same "define and (de)serialize your own message type"
//! convention the wire edges (`animus-dynamo`/`animus-cql`) already use, not
//! this crate's own compact binary `codec` (that optimization exists for the
//! *hot* per-write Raft path; segment operations are one per seal-epoch —
//! by default every 4 MiB or 4 hours — so `serde_json`'s cost is immaterial
//! here).
//!
//! **Placement.** Which K nodes hold a given id's replicas comes from
//! [`PlacementView`] — a small seam (not a `Metadata` dependency: this crate
//! stays decoupled from `animus-control`'s replicated state, mirroring
//! `host.rs`'s own `MetadataView` projection) that hands back the current
//! candidate node set. `choose_targets` feeds that set straight into
//! `animus_placement::select_replicas` with a plain `PlacementPolicy::simple`
//! (no residency/spread constraints yet — label-aware refinement is a named
//! follow-up for whichever later PR backs [`PlacementView`] with the real
//! `Metadata` mirror and has node labels to hand); this reuses the *existing*
//! policy engine (ADR 0005) rather than a second, bespoke one, and inherits
//! its determinism (same candidates in ⇒ same K chosen, on every node).
//! **Degraded mode is deliberate, not a bug**: `K = min(default_k,
//! candidates.len())`, so a single-node dev cluster still works (`K = 1`,
//! tested below) instead of `put` refusing to ever succeed.
//!
//! **Timeouts are bounded and `env.sleep`-based**, mirroring
//! `RaftKvNode::linearizable_get`'s own read-barrier poll shape
//! (`READ_TIMEOUT`/`READ_POLL` in `lib.rs`): a partitioned/dead target makes
//! a `put`/`get`/`delete` fail cleanly within its own timeout, never hang.
//!
//! **Partial-K puts leave harmless orphans (as-built amendment).** If
//! `put_replicated` errors after successfully writing to some (but not all)
//! of its K targets, those written copies are never cataloged (the caller —
//! the sealer, `animusd::index_drain::seal_now` — only commits
//! `MetaCommand::SealStreamShard` after `put_replicated` itself returns
//! `Ok`). **This used to say a retried `put_replicated` "simply overwrites
//! those stray copies" — that was true only because every attempt shared one
//! deterministic id, which is exactly the design this crate no longer uses**
//! (see `animus_cp_data::segment`'s own module doc for the data-loss bug the
//! shared-id scheme caused). Every real caller now writes each attempt at
//! its own unique id (`segment::segment_object_id`), so a genuine *retry* of
//! the *same* attempt reuses that *same* id (`SegmentStore::put`'s
//! write-once contract treats a byte-identical re-put as a safe no-op —
//! never a real hazard), while a **different** attempt (a fresh `seal_now`
//! call, e.g. after a lost ack) writes to a **fresh** id and leaves the
//! partial-K copies at the *old* id as permanent, unreferenced orphans — a
//! bounded amount of wasted disk on nodes that happened to ack an attempt
//! nothing ever pointed at, reclaimed by the segment janitor's own orphan
//! sweep (`animusd::segment_janitor`) rather than by a future overwrite.
//! Never "fix" a partial failure by trying to roll back the targets that
//! already succeeded — that would add a second distributed failure mode
//! (the rollback itself can partially fail) to clean up a case that is
//! already safe to leave alone.

use std::collections::BTreeMap;
use std::io;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_env::{Env, EnvExt, NodeId, SegmentStore};
use animus_placement::{Candidate, PlacementPolicy, select_replicas};
use serde::{Deserialize, Serialize};

/// The one reserved `(node, stream)` address (ADR 0026) every
/// `ClusterSegmentStore` serving task listens and replies on. Chosen at the
/// far end of the `u64` space, deliberately outside any range a `TabletId`
/// (`animus_tablet::TabletId`, `u64`, monotonic from 1, never reused) could
/// plausibly reach — a reserved, well-known stream for a cluster-wide infra
/// responsibility, the same "reserved constant" shape [`PRIMARY_STREAM`]
/// itself uses for the control plane.
///
/// [`PRIMARY_STREAM`]: animus_env::PRIMARY_STREAM
pub const SEGMENT_STREAM: u64 = u64::MAX;

/// Default replication factor (ADR 0043 §A7b, decision F5): this database's
/// own `RF`. A cluster with fewer eligible candidates degrades to `K =
/// candidates.len()` (see the module doc) rather than refusing to serve.
pub const DEFAULT_K: usize = 3;

/// How long [`ClusterSegmentStore::put_replicated`] waits for every chosen
/// replica to ack before giving up.
const PUT_TIMEOUT: Duration = Duration::from_secs(10);
/// Poll granularity while a put waits for its K acks.
const PUT_POLL: Duration = Duration::from_millis(20);
/// How long [`ClusterSegmentStore::delete_from`] waits for every recorded
/// replica to ack before giving up.
const DELETE_TIMEOUT: Duration = Duration::from_secs(10);
/// Poll granularity while a delete waits for its acks.
const DELETE_POLL: Duration = Duration::from_millis(20);
/// How long a single fetch attempt to one peer waits before that attempt is
/// abandoned (and, if attempts remain, retried).
const FETCH_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(3);
/// Poll granularity while a fetch attempt waits for its reply.
const FETCH_POLL: Duration = Duration::from_millis(20);
/// Bounded per-replica retry count for [`ClusterSegmentStore::get_from`]'s
/// network fetch path (ADR 0043 §A7b: "per-attempt bounded retry").
const FETCH_ATTEMPTS: u32 = 3;

/// The current candidate node set a [`ClusterSegmentStore`] may replicate
/// onto, plus this store's own node id — a small seam so the *choice* of
/// candidates varies independently of the store itself (mirroring
/// `host.rs`'s `MetadataView` projection, which keeps this crate decoupled
/// from `animus-control`'s replicated state). A `SimEnv` test backs this
/// with [`StaticPlacementView`]; `animusd`'s wiring (a later PR) backs it
/// with a live view over the replicated `Metadata` mirror.
///
/// Deliberately not label-aware yet: `choose_targets` builds a plain
/// [`animus_placement::Candidate`] with no labels from every id this trait
/// returns, so today's selection is failure-domain-*blind*, deterministic
/// spread (see the module doc). A future `PlacementView` that also exposes
/// each candidate's topology labels can upgrade `choose_targets` to a
/// `spread_across`-policy without changing this trait's shape.
pub trait PlacementView: Send + Sync {
    /// This store's own node id.
    fn self_id(&self) -> NodeId;

    /// The current candidate nodes eligible to hold a replica, ordinarily
    /// including [`self_id`](PlacementView::self_id) — a node is almost
    /// always its own candidate. Order carries no meaning; every caller
    /// that needs a deterministic order sorts.
    fn candidates(&self) -> Vec<NodeId>;
}

/// A fixed candidate list: the shape a `SimEnv` test (and, until a live
/// `Metadata`-backed view lands, a fixed dev config) both want.
#[derive(Clone, Debug)]
pub struct StaticPlacementView {
    self_id: NodeId,
    candidates: Vec<NodeId>,
}

impl StaticPlacementView {
    /// Build a view whose candidate set never changes for the lifetime of
    /// the store built over it.
    #[must_use]
    pub fn new(self_id: NodeId, candidates: Vec<NodeId>) -> Self {
        StaticPlacementView {
            self_id,
            candidates,
        }
    }
}

impl PlacementView for StaticPlacementView {
    fn self_id(&self) -> NodeId {
        self.self_id.clone()
    }

    fn candidates(&self) -> Vec<NodeId> {
        self.candidates.clone()
    }
}

/// This module's own wire enum (ADR 0026/ADR 0043 §A7b): `serde_json`'d over
/// the `Network`'s `Vec<u8>` payloads, carried on [`SEGMENT_STREAM`]. Every
/// request variant is answered by its matching reply variant, correlated by
/// `req_id` (a per-store, monotonically increasing counter — see
/// `Pending::next_req_id`) — the same shape `RaftKvNode`'s own
/// `ReadProbe`/`ReadProbeAck` pair uses for a non-consensus request/reply
/// riding the same stream as everything else addressed to a node.
#[derive(Clone, Debug, Serialize, Deserialize)]
enum SegmentWire {
    /// Write `bytes` at `id` on the receiving node's own inner store.
    Store {
        req_id: u64,
        id: String,
        bytes: Vec<u8>,
    },
    /// Reply to [`Store`](SegmentWire::Store).
    StoreAck { req_id: u64, result: WireResult },
    /// Fetch `id` from the receiving node's own inner store.
    Fetch { req_id: u64, id: String },
    /// Reply to [`Fetch`](SegmentWire::Fetch).
    FetchReply { req_id: u64, result: WireOptResult },
    /// Delete `id` from the receiving node's own inner store.
    Delete { req_id: u64, id: String },
    /// Reply to [`Delete`](SegmentWire::Delete).
    DeleteAck { req_id: u64, result: WireResult },
}

/// The outcome of a `put`/`delete` at one replica.
#[derive(Clone, Debug, Serialize, Deserialize)]
enum WireResult {
    Ok,
    Err(String),
}

/// The outcome of a `get` at one replica: found, definitively absent
/// (never written, or deleted — [`SegmentStore::get`]'s own defined `None`
/// outcome), or a local error at that replica.
#[derive(Clone, Debug, Serialize, Deserialize)]
enum WireOptResult {
    Found(Vec<u8>),
    NotFound,
    Err(String),
}

fn encode(msg: &SegmentWire) -> Vec<u8> {
    serde_json::to_vec(msg).expect("SegmentWire always serializes")
}

/// One pending request/reply correlation slot: `None` until the matching
/// reply lands, mirroring `RaftKvNode`'s own `reads.pending` shape (a
/// `BTreeMap` a poll loop repeatedly checks under a `Mutex`, never a
/// tokio-only channel/oneshot — this crate has `SimEnv` callers with no
/// tokio runtime present, so every wait here is `env.sleep`-based polling,
/// not an executor-specific primitive).
#[derive(Clone, Debug)]
enum PendingReply {
    Store(WireResult),
    Fetch(WireOptResult),
    Delete(WireResult),
}

struct Pending {
    next_req_id: u64,
    slots: BTreeMap<u64, Option<PendingReply>>,
}

fn next_req_id(pending: &Mutex<Pending>) -> u64 {
    let mut p = pending.lock().expect("segment store pending poisoned");
    let id = p.next_req_id;
    p.next_req_id += 1;
    id
}

fn register_pending(pending: &Mutex<Pending>, req_id: u64) {
    pending
        .lock()
        .expect("segment store pending poisoned")
        .slots
        .insert(req_id, None);
}

/// Stash a reply into its slot. A reply for a `req_id` whose slot was
/// already cleaned up (the caller gave up and moved on, or this reply is a
/// stray duplicate) is silently dropped — safe, since every caller of
/// [`register_pending`] either consumes its slot or times out and discards
/// it; a late arrival changes nothing observable either way.
fn stash_reply(pending: &Mutex<Pending>, req_id: u64, reply: PendingReply) {
    let mut p = pending.lock().expect("segment store pending poisoned");
    if let Some(slot) = p.slots.get_mut(&req_id) {
        *slot = Some(reply);
    }
}

fn peek_reply(pending: &Mutex<Pending>, req_id: u64) -> Option<PendingReply> {
    pending
        .lock()
        .expect("segment store pending poisoned")
        .slots
        .get(&req_id)
        .cloned()
        .flatten()
}

fn drop_pending(pending: &Mutex<Pending>, req_id: u64) {
    pending
        .lock()
        .expect("segment store pending poisoned")
        .slots
        .remove(&req_id);
}

fn drop_pending_many(pending: &Mutex<Pending>, req_ids: &[(NodeId, u64)]) {
    let mut p = pending.lock().expect("segment store pending poisoned");
    for (_, id) in req_ids {
        p.slots.remove(id);
    }
}

/// The **default** [`SegmentStore`] (ADR 0043 §A7b, decision F5): K-way
/// replication of an immutable segment across `K` nodes' own local
/// `S`-backed directories, chosen via [`PlacementView`]/`animus-placement`,
/// over `E`'s `Network` seam. See the module doc for the full design.
///
/// Cheap to clone: every field is either `Clone` cheaply (`E`, a shared
/// `Arc`) or itself designed to be (`S`, matching `SimSegmentStore`'s and
/// `FsSegmentStore`'s own "cheap to clone, shares state" contract) — clones
/// share the same pending-request table and the same inner local store.
pub struct ClusterSegmentStore<E: Env, S: SegmentStore + Clone + Send + Sync + 'static> {
    env: E,
    local: S,
    placement: Arc<dyn PlacementView>,
    default_k: usize,
    pending: Arc<Mutex<Pending>>,
}

impl<E: Env, S: SegmentStore + Clone + Send + Sync + 'static> Clone for ClusterSegmentStore<E, S> {
    fn clone(&self) -> Self {
        ClusterSegmentStore {
            env: self.env.clone(),
            local: self.local.clone(),
            placement: Arc::clone(&self.placement),
            default_k: self.default_k,
            pending: Arc::clone(&self.pending),
        }
    }
}

impl<E: Env, S: SegmentStore + Clone + Send + Sync + 'static> ClusterSegmentStore<E, S> {
    /// Build the store at [`DEFAULT_K`], **without** starting its serving
    /// task — see [`start`](Self::start), which does both. Splitting
    /// construction from starting mirrors `RaftKvNode`'s own
    /// "construction never implicitly spawns anything you didn't ask for"
    /// discipline, and lets a test hold a handle to a store on a node whose
    /// serving task it wants to start (or crash/restart) on its own
    /// schedule.
    #[must_use]
    pub fn new(env: E, local: S, placement: Arc<dyn PlacementView>) -> Self {
        Self::with_k(env, local, placement, DEFAULT_K)
    }

    /// Like [`new`](Self::new), with an explicit replication factor instead
    /// of [`DEFAULT_K`] — mainly for a test exercising the `K <` default
    /// degraded path deliberately rather than relying on a small candidate
    /// pool to trigger it incidentally.
    #[must_use]
    pub fn with_k(env: E, local: S, placement: Arc<dyn PlacementView>, default_k: usize) -> Self {
        ClusterSegmentStore {
            env,
            local,
            placement,
            default_k,
            pending: Arc::new(Mutex::new(Pending {
                next_req_id: 0,
                slots: BTreeMap::new(),
            })),
        }
    }

    /// Build the store at [`DEFAULT_K`] and spawn its serving task
    /// (`env.spawn_task`, ADR 0026: listens/replies on [`SEGMENT_STREAM`]).
    /// Exactly one `ClusterSegmentStore` per node may ever call `start`
    /// (or [`start_with_k`](Self::start_with_k)) — `(node, stream)` is
    /// single-consumer (ADR 0026), and this is the one task that consumes
    /// this node's [`SEGMENT_STREAM`] inbox.
    #[must_use]
    pub fn start(env: E, local: S, placement: Arc<dyn PlacementView>) -> Self {
        Self::start_with_k(env, local, placement, DEFAULT_K)
    }

    /// Like [`start`](Self::start), with an explicit replication factor —
    /// see [`with_k`](Self::with_k).
    #[must_use]
    pub fn start_with_k(
        env: E,
        local: S,
        placement: Arc<dyn PlacementView>,
        default_k: usize,
    ) -> Self {
        let store = Self::with_k(env, local, placement, default_k);
        store.spawn_serving_task();
        store
    }

    fn spawn_serving_task(&self) {
        let env = self.env.clone();
        let local = self.local.clone();
        let pending = Arc::clone(&self.pending);
        self.env.spawn_task(serve_loop(env, local, pending));
    }

    /// This store's own inner local segment store — for test introspection
    /// (e.g. confirming an object actually landed at a given replica's own
    /// copy), mirroring `RaftKvNode::storage()`'s identical purpose.
    #[must_use]
    pub fn local(&self) -> &S {
        &self.local
    }

    /// Choose this put's `K` target nodes: `K = min(default_k,
    /// candidates.len())` (the degraded-mode rule, module doc), then
    /// `animus_placement::select_replicas` over a label-blind
    /// `PlacementPolicy::simple` — deterministic, sorted, for the current
    /// candidate set. Errors only if there are zero candidates at all (a
    /// misconfigured/empty view — `select_replicas` itself cannot fail once
    /// `K <= candidates.len()` and no residency/spread constraint is set).
    fn choose_targets(&self) -> io::Result<Vec<NodeId>> {
        let candidates = self.placement.candidates();
        if candidates.is_empty() {
            return Err(io::Error::other(
                "segment store: placement view has no candidates",
            ));
        }
        let k = self.default_k.min(candidates.len()).max(1);
        let pool: Vec<Candidate> = candidates
            .into_iter()
            .map(|n| Candidate::new(n, BTreeMap::new()))
            .collect();
        let policy = PlacementPolicy::simple("segment-store", k);
        select_replicas(&pool, &policy).map_err(|e| {
            io::Error::other(format!("segment store: placement selection failed: {e}"))
        })
    }

    /// **The load-bearing write path** (ADR 0043 §A7b, F5): push `bytes` at
    /// `id` to this put's chosen `K` target nodes (in parallel — every send
    /// goes out before this waits on any reply) and return `Ok` with the
    /// **sorted** replica set **only once every one of them has durably
    /// written it** (each target's own `SegmentStore::put` returned `Ok`,
    /// which for the production `FsSegmentStore` building block means
    /// fsynced). All-or-error: a single target's explicit failure, or the
    /// whole attempt timing out with any target still unheard-from, fails
    /// the **entire** call — never a partial success. See the module doc
    /// ("Partial-K puts leave harmless orphans") for why a caller's retry
    /// after such a failure is always safe.
    ///
    /// The trait's own [`SegmentStore::put`] delegates here and discards the
    /// replica set; call this directly when the caller needs to know *which*
    /// nodes to record (the sealer's `SealStreamShard.replicas` field, ADR
    /// 0043 §A3 step 3 — a later PR).
    pub async fn put_replicated(&self, id: &str, bytes: &[u8]) -> io::Result<Vec<NodeId>> {
        let targets = self.choose_targets()?;
        self.put_to_targets(&targets, id, bytes).await?;
        let mut replicas = targets;
        replicas.sort();
        Ok(replicas)
    }

    /// [`put_replicated`](Self::put_replicated)'s own fan-out/wait body,
    /// generalized to an explicit `targets` list instead of always calling
    /// [`choose_targets`](Self::choose_targets) itself — the primitive the
    /// segment janitor's own replica-repair sweep (ADR 0043 §A9, a later
    /// PR) needs to push a re-replicated copy at exactly the fresh
    /// target(s) it chose (excluding whichever replicas already survive),
    /// rather than re-running the *whole* K-selection and potentially
    /// re-writing replicas that already hold the object. `put_replicated`
    /// is now a thin wrapper over this with `targets = choose_targets()?`.
    pub async fn put_to_targets(
        &self,
        targets: &[NodeId],
        id: &str,
        bytes: &[u8],
    ) -> io::Result<()> {
        let self_id = self.env.node_id();

        let mut req_ids: Vec<(NodeId, u64)> = Vec::with_capacity(targets.len());
        for t in targets {
            let req_id = next_req_id(&self.pending);
            register_pending(&self.pending, req_id);
            req_ids.push((t.clone(), req_id));
        }

        // Fan out: a local target writes directly (no network round trip for
        // our own replica); every other target gets a `Store` message. None
        // of these sends block on a reply, so this loop completes without
        // waiting on any single target — "parallel" in the sense that no
        // target's ack gates starting the next target's request.
        for (t, req_id) in &req_ids {
            if *t == self_id {
                let result = match self.local.put(id, bytes).await {
                    Ok(()) => WireResult::Ok,
                    Err(e) => WireResult::Err(e.to_string()),
                };
                stash_reply(&self.pending, *req_id, PendingReply::Store(result));
            } else {
                let payload = encode(&SegmentWire::Store {
                    req_id: *req_id,
                    id: id.to_string(),
                    bytes: bytes.to_vec(),
                });
                self.env
                    .send_stream(t.clone(), SEGMENT_STREAM, payload)
                    .await;
            }
        }

        let deadline = self.env.now().saturating_add(PUT_TIMEOUT);
        loop {
            let mut all_ok = true;
            for (t, req_id) in &req_ids {
                match peek_reply(&self.pending, *req_id) {
                    Some(PendingReply::Store(WireResult::Ok)) => {}
                    Some(PendingReply::Store(WireResult::Err(e))) => {
                        drop_pending_many(&self.pending, &req_ids);
                        return Err(io::Error::other(format!(
                            "segment store: replica {t} failed to store {id:?}: {e}"
                        )));
                    }
                    _ => all_ok = false,
                }
            }
            if all_ok {
                drop_pending_many(&self.pending, &req_ids);
                return Ok(());
            }
            if self.env.now() >= deadline {
                drop_pending_many(&self.pending, &req_ids);
                return Err(io::Error::other(format!(
                    "segment store: put of {id:?} timed out waiting for {} replicas",
                    req_ids.len()
                )));
            }
            self.env.sleep(PUT_POLL).await;
        }
    }

    /// **Replica repair** (ADR 0043 §A9, the segment janitor's own
    /// re-replication step, round-3 PR7): given `id`'s currently
    /// **surviving** replica set and a live copy's `bytes` (the caller
    /// already fetched them, typically via
    /// [`get_from`](Self::get_from)`(surviving, id)`), push that copy to
    /// enough freshly-chosen candidates — excluding every id already in
    /// `surviving` — to reach `target_k`, and return the resulting replica
    /// set (`surviving` plus whichever fresh targets were actually
    /// written), sorted. Degrades to fewer than `target_k` if fewer
    /// candidates exist beyond `surviving` (the identical degraded-mode
    /// philosophy [`choose_targets`](Self::choose_targets) already uses for
    /// a fresh put) — the janitor simply re-attempts on a later tick once
    /// more candidates return. `target_k <= surviving.len()` (nothing to
    /// repair) is a cheap no-op returning `surviving` sorted, with no
    /// network I/O at all.
    pub async fn repair(
        &self,
        id: &str,
        bytes: &[u8],
        surviving: &[NodeId],
        target_k: usize,
    ) -> io::Result<Vec<NodeId>> {
        let needed = target_k.saturating_sub(surviving.len());
        let mut result: Vec<NodeId> = surviving.to_vec();
        if needed == 0 {
            result.sort();
            return Ok(result);
        }
        let mut candidates = self.placement.candidates();
        candidates.sort();
        let fresh: Vec<NodeId> = candidates
            .into_iter()
            .filter(|c| !surviving.contains(c))
            .take(needed)
            .collect();
        if !fresh.is_empty() {
            self.put_to_targets(&fresh, id, bytes).await?;
            result.extend(fresh);
        }
        result.sort();
        Ok(result)
    }

    /// **The load-bearing read path** (ADR 0043 §A7b): fetch `id` from
    /// `replicas` — a catalog row's own recorded replica set, ADR 0043
    /// §A3's `SealStreamShard.replicas` — trying [`self`](Env::node_id)
    /// locally first if it is one of them, then the rest **in the order
    /// given**, each attempt bounded and retried up to [`FETCH_ATTEMPTS`]
    /// times. Returns the first replica's answer that this node could
    /// actually reach — `Some`/`None` alike, since any live recorded
    /// replica's answer is authoritative for an immutable, already-cataloged
    /// segment (no read-repair/quorum needed, unlike the mutable CP data
    /// plane). Errors only if **no** replica in the list could be reached at
    /// all — a genuine outage, distinct from [`SegmentStore::get`]'s
    /// contract-defined `None` (a deleted/never-written id), which this
    /// method only ever returns on a replica's own definitive answer.
    ///
    /// The trait's own [`SegmentStore::get`] is a weaker, contract/testing-
    /// only path — it has no catalog row to consult, so it falls back to
    /// this store's *current* placement-view candidates instead of a
    /// specific recorded set. Use `get_from` for the real stream read path.
    pub async fn get_from(&self, replicas: &[NodeId], id: &str) -> io::Result<Option<Vec<u8>>> {
        let self_id = self.env.node_id();
        let mut ordered: Vec<NodeId> = Vec::with_capacity(replicas.len());
        if replicas.contains(&self_id) {
            ordered.push(self_id.clone());
        }
        for r in replicas {
            if *r != self_id {
                ordered.push(r.clone());
            }
        }

        let mut last_err: Option<io::Error> = None;
        for target in &ordered {
            let outcome = if *target == self_id {
                self.local.get(id).await
            } else {
                self.fetch_from_peer(target, id, FETCH_ATTEMPTS).await
            };
            match outcome {
                Ok(v) => return Ok(v),
                Err(e) => last_err = Some(e),
            }
        }
        Err(last_err.unwrap_or_else(|| {
            io::Error::other(format!(
                "segment store: no replica given for {id:?} (empty replica list)"
            ))
        }))
    }

    /// Fetch `id` from `target` over the network, retrying up to `attempts`
    /// times (each bounded by [`FETCH_ATTEMPT_TIMEOUT`]) before giving up on
    /// this one target. A definitive reply — found, or the replica's own
    /// local error — is trusted on the first attempt that receives one; only
    /// an unreachable/timed-out attempt is retried.
    async fn fetch_from_peer(
        &self,
        target: &NodeId,
        id: &str,
        attempts: u32,
    ) -> io::Result<Option<Vec<u8>>> {
        let mut last_err: Option<io::Error> = None;
        for _ in 0..attempts.max(1) {
            let req_id = next_req_id(&self.pending);
            register_pending(&self.pending, req_id);
            let payload = encode(&SegmentWire::Fetch {
                req_id,
                id: id.to_string(),
            });
            self.env
                .send_stream(target.clone(), SEGMENT_STREAM, payload)
                .await;

            let deadline = self.env.now().saturating_add(FETCH_ATTEMPT_TIMEOUT);
            loop {
                match peek_reply(&self.pending, req_id) {
                    Some(PendingReply::Fetch(WireOptResult::Found(bytes))) => {
                        drop_pending(&self.pending, req_id);
                        return Ok(Some(bytes));
                    }
                    Some(PendingReply::Fetch(WireOptResult::NotFound)) => {
                        drop_pending(&self.pending, req_id);
                        return Ok(None);
                    }
                    Some(PendingReply::Fetch(WireOptResult::Err(e))) => {
                        drop_pending(&self.pending, req_id);
                        last_err = Some(io::Error::other(format!(
                            "segment store: peer {target} fetch error for {id:?}: {e}"
                        )));
                        break;
                    }
                    _ => {}
                }
                if self.env.now() >= deadline {
                    drop_pending(&self.pending, req_id);
                    last_err = Some(io::Error::other(format!(
                        "segment store: peer {target} unreachable fetching {id:?}"
                    )));
                    break;
                }
                self.env.sleep(FETCH_POLL).await;
            }
        }
        Err(last_err.unwrap_or_else(|| {
            io::Error::other(format!(
                "segment store: peer {target} unreachable for {id:?}"
            ))
        }))
    }

    /// **Idempotent delete at every recorded replica** (ADR 0043 §A9: the
    /// segment janitor's own reclaim step uses this once a catalog row is
    /// past retention). All-or-error like [`put_replicated`] — but "error"
    /// here only ever means "a replica was unreachable within the timeout,"
    /// never "the id didn't exist there" (per-replica delete is itself
    /// idempotent, `SegmentStore::delete`'s own contract). A caller (the
    /// janitor) retries on `Err`; retrying is always safe.
    pub async fn delete_from(&self, replicas: &[NodeId], id: &str) -> io::Result<()> {
        let self_id = self.env.node_id();
        let mut req_ids: Vec<(NodeId, u64)> = Vec::with_capacity(replicas.len());
        for r in replicas {
            let req_id = next_req_id(&self.pending);
            register_pending(&self.pending, req_id);
            req_ids.push((r.clone(), req_id));
        }

        for (r, req_id) in &req_ids {
            if *r == self_id {
                let result = match self.local.delete(id).await {
                    Ok(()) => WireResult::Ok,
                    Err(e) => WireResult::Err(e.to_string()),
                };
                stash_reply(&self.pending, *req_id, PendingReply::Delete(result));
            } else {
                let payload = encode(&SegmentWire::Delete {
                    req_id: *req_id,
                    id: id.to_string(),
                });
                self.env
                    .send_stream(r.clone(), SEGMENT_STREAM, payload)
                    .await;
            }
        }

        let deadline = self.env.now().saturating_add(DELETE_TIMEOUT);
        loop {
            let mut all_done = true;
            for (r, req_id) in &req_ids {
                match peek_reply(&self.pending, *req_id) {
                    Some(PendingReply::Delete(WireResult::Ok)) => {}
                    Some(PendingReply::Delete(WireResult::Err(e))) => {
                        drop_pending_many(&self.pending, &req_ids);
                        return Err(io::Error::other(format!(
                            "segment store: replica {r} failed to delete {id:?}: {e}"
                        )));
                    }
                    _ => all_done = false,
                }
            }
            if all_done {
                drop_pending_many(&self.pending, &req_ids);
                return Ok(());
            }
            if self.env.now() >= deadline {
                drop_pending_many(&self.pending, &req_ids);
                return Err(io::Error::other(format!(
                    "segment store: delete of {id:?} timed out waiting for {} replicas",
                    req_ids.len()
                )));
            }
            self.env.sleep(DELETE_POLL).await;
        }
    }
}

#[async_trait::async_trait]
impl<E: Env, S: SegmentStore + Clone + Send + Sync + 'static> SegmentStore
    for ClusterSegmentStore<E, S>
{
    /// Delegates to [`put_replicated`](Self::put_replicated) and discards
    /// the replica set — see that method's doc for the actual durability
    /// contract this satisfies (K-fsynced, all-or-error).
    async fn put(&self, id: &str, bytes: &[u8]) -> io::Result<()> {
        self.put_replicated(id, bytes).await.map(|_| ())
    }

    /// **Not** the load-bearing read path (that's
    /// [`get_from`](Self::get_from), against a catalog row's own recorded
    /// replica set) — this is a contract/testing-only best-effort fallback
    /// with no catalog to consult: try the local copy, then this store's
    /// *current* placement-view candidates, one attempt each, returning the
    /// first reachable answer.
    async fn get(&self, id: &str) -> io::Result<Option<Vec<u8>>> {
        if let Ok(Some(bytes)) = self.local.get(id).await {
            return Ok(Some(bytes));
        }
        let self_id = self.env.node_id();
        let mut last_err: Option<io::Error> = None;
        for candidate in self.placement.candidates() {
            if candidate == self_id {
                continue;
            }
            match self.fetch_from_peer(&candidate, id, 1).await {
                Ok(v) => return Ok(v),
                Err(e) => last_err = Some(e),
            }
        }
        match last_err {
            Some(e) => Err(e),
            // Local already answered (absent, or a local error we're
            // choosing not to surface here since nothing else disagreed);
            // no other candidate to consult.
            None => Ok(None),
        }
    }

    /// Local delete plus a **best-effort**, un-awaited broadcast to this
    /// store's current placement-view candidates — not the load-bearing
    /// reclaim path (that's [`delete_from`](Self::delete_from), against a
    /// catalog row's own recorded replica set, retried by the janitor on
    /// failure). This trait method exists for contract/testing parity; its
    /// own success/failure is entirely the **local** delete's outcome.
    async fn delete(&self, id: &str) -> io::Result<()> {
        let local_result = self.local.delete(id).await;
        let self_id = self.env.node_id();
        for candidate in self.placement.candidates() {
            if candidate == self_id {
                continue;
            }
            let req_id = next_req_id(&self.pending);
            let payload = encode(&SegmentWire::Delete {
                req_id,
                id: id.to_string(),
            });
            self.env
                .send_stream(candidate, SEGMENT_STREAM, payload)
                .await;
        }
        local_result
    }

    /// **Local-only** (see the trait's own doc: `list` is debug/sweep-only,
    /// never load-bearing for a read). A cluster-wide listing would need a
    /// fan-out-and-merge this contract doesn't require paying for; an
    /// operator wanting one queries every node's own store.
    async fn list(&self, prefix: &str) -> io::Result<Vec<String>> {
        self.local.list(prefix).await
    }
}

/// The one serving task a [`ClusterSegmentStore::start`] spawns per node:
/// the single consumer of this node's [`SEGMENT_STREAM`] inbox (ADR 0026).
/// Dispatches on the decoded [`SegmentWire`] variant — a *request* variant
/// (`Store`/`Fetch`/`Delete`) is served against `local` and answered with
/// its matching reply, addressed back to `envelope.from` on the same
/// stream; a *reply* variant is stashed into `pending` for whichever
/// `ClusterSegmentStore` method is polling that `req_id`. Runs until the
/// env itself is torn down (this task is never explicitly stopped —
/// mirroring `RaftKvNode`'s consensus loop, which the same codebase's
/// shutdown story handles by tearing down the whole node, not by
/// signalling individual background tasks).
async fn serve_loop<E: Env, S: SegmentStore>(env: E, local: S, pending: Arc<Mutex<Pending>>) {
    loop {
        let envelope = env.recv_stream(SEGMENT_STREAM).await;
        let msg = match serde_json::from_slice::<SegmentWire>(&envelope.payload) {
            Ok(msg) => msg,
            Err(err) => {
                tracing::warn!(?err, "undecodable segment-store message dropped");
                continue;
            }
        };
        match msg {
            SegmentWire::Store { req_id, id, bytes } => {
                let result = match local.put(&id, &bytes).await {
                    Ok(()) => WireResult::Ok,
                    Err(e) => WireResult::Err(e.to_string()),
                };
                let reply = encode(&SegmentWire::StoreAck { req_id, result });
                env.send_stream(envelope.from, SEGMENT_STREAM, reply).await;
            }
            SegmentWire::Fetch { req_id, id } => {
                let result = match local.get(&id).await {
                    Ok(Some(bytes)) => WireOptResult::Found(bytes),
                    Ok(None) => WireOptResult::NotFound,
                    Err(e) => WireOptResult::Err(e.to_string()),
                };
                let reply = encode(&SegmentWire::FetchReply { req_id, result });
                env.send_stream(envelope.from, SEGMENT_STREAM, reply).await;
            }
            SegmentWire::Delete { req_id, id } => {
                let result = match local.delete(&id).await {
                    Ok(()) => WireResult::Ok,
                    Err(e) => WireResult::Err(e.to_string()),
                };
                let reply = encode(&SegmentWire::DeleteAck { req_id, result });
                env.send_stream(envelope.from, SEGMENT_STREAM, reply).await;
            }
            SegmentWire::StoreAck { req_id, result } => {
                stash_reply(&pending, req_id, PendingReply::Store(result));
            }
            SegmentWire::FetchReply { req_id, result } => {
                stash_reply(&pending, req_id, PendingReply::Fetch(result));
            }
            SegmentWire::DeleteAck { req_id, result } => {
                stash_reply(&pending, req_id, PendingReply::Delete(result));
            }
        }
    }
}
