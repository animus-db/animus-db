//! Production [`Env`] implementation.
//!
//! Real wall-derived monotonic clock, OS randomness, `tokio` task spawning,
//! length-prefixed TCP messaging, and `tokio::fs` with real `fsync`. This is the
//! non-deterministic side of the seam: it is **not** exercised by the
//! simulation tests, which run against `animus-sim`'s `SimEnv`. Keep production
//! behavior here so the rest of the codebase stays environment-agnostic.

// ADR 0003 / ADR 0061 Decision 4 (rung B5): every other crate is expected to
// reach real time/randomness/task-spawning ONLY through the `Clock`/`Rng`/
// `Spawner` methods this module implements — that's the whole point of the
// `Env` seam. This module IS that implementation, so it is the one place in
// the workspace where `Instant::now`/`SystemTime::now`/`tokio::spawn`/
// `tokio::time::{sleep,timeout}` are the correct, intended call — exempted
// here at the module level rather than at each of this file's ~30 call
// sites, which would just repeat the same one reason thirty times.
// `OsRng` (a unit struct, so it trips `disallowed_types` rather than this
// lint) is allowed individually at its own four call sites below instead —
// unlike the rest of this file's real-time/IO, it's few enough sites that
// scoping the allow tightly costs nothing and keeps `disallowed_types`
// (HashMap/HashSet, ADR 0003's other half) live for the rest of this file.
#![allow(
    clippy::disallowed_methods,
    reason = "this module is ProdEnv itself — the sanctioned real-time/IO/RNG implementation the Env seam exists to wrap (ADR 0003); see ADR 0061 Decision 4"
)]

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, mpsc};

use crate::handshake;
#[cfg(test)]
use crate::nid;
use crate::tls::server_name_for;
use crate::{
    Clock, Disk, Env, Envelope, InboxCap, MaybeTlsStream, Metric, MetricsHandle, Nanos, Network,
    NodeId, Rng, Spawner, TlsConfig, TlsMaterial, UnixMillis,
};

/// A production environment for a single node.
///
/// Cheap to clone (everything shared lives behind an `Arc`). Construct one per
/// node role with [`ProdEnv::bind`], then install the peer address book with
/// [`set_peers`](ProdEnv::set_peers) (deferred so a whole cluster can bind to
/// ephemeral ports first, then exchange addresses).
#[derive(Clone)]
pub struct ProdEnv {
    inner: Arc<Inner>,
}

/// Per-stream inbound queues + parked-receiver wakers for one env's inbox
/// (ADR 0026), demultiplexed from every accepted connection's frames by the
/// frame's `stream` field. `(node, stream)` stays single-consumer — this
/// generalizes the pre-multiplexing single inbox rather than changing that
/// invariant: two different streams' queues/wakers never contend with each
/// other's *data*, only briefly on the `StdMutex` guarding the map structure
/// itself (the same micro-contention every `BTreeMap`-behind-a-`Mutex` design
/// in this codebase already accepts) — nothing is held across an `.await`
/// while holding this lock, so one stream's consumer being asleep never blocks
/// another stream's delivery or consumer.
#[derive(Default)]
struct Demux {
    queues: BTreeMap<u64, VecDeque<Envelope>>,
    wakers: BTreeMap<u64, Waker>,
    /// Per-stream observability bookkeeping (issue: an unread stream's
    /// queue in `queues` above grows forever — this is the measurement
    /// this map exists to make possible, not a fix for it). Maintained
    /// incrementally alongside `queues`/`wakers` on the exact same push
    /// (`spawn_pump`) and pop (`RecvStream::poll`) sites — O(1) per
    /// frame, no scan of `queues` itself required to answer "how many
    /// bytes/frames are queued right now" (see [`ProdEnv::inbox_stats`]).
    /// Never pruned when a stream's queue empties — a still-open stream's
    /// bookkeeping outlives an empty queue by design (this is what makes
    /// `ever_polled`/`last_pop` meaningful); an explicitly [`close_stream`]d
    /// stream's entry IS pruned, see `closed` below.
    ///
    /// [`close_stream`]: ProdEnv::close_stream
    stream_meta: BTreeMap<u64, StreamMeta>,
    /// Streams explicitly retired via [`ProdEnv::close_stream`] and not yet
    /// reopened by a subsequent `recv_stream` call (ADR 0026, 2026-09-28
    /// amendment). Bounded by the number of tablets this node has ever
    /// *hosted and torn down* — never by traffic volume — since a tombstone
    /// is inserted only at an explicit close and removed the moment
    /// `recv_stream` is next called for that stream (or the node itself
    /// restarts, a fresh `ProdEnv` process starting with none). A stream
    /// never closed is simply absent here, which is exactly "not yet
    /// opened" staying indistinguishable from "ordinary, never-closed
    /// stream" — the distinction this set exists to make is solely
    /// "was `close_stream` ever called for this stream, since its last
    /// reopen."
    closed: BTreeSet<u64>,
    /// The per-stream backpressure cap enforced at push time (ADR 0026,
    /// 2026-09-28 inbox-cap amendment) — see [`crate::InboxCap`]'s own doc
    /// for the derivation and [`spawn_pump`]'s drop-oldest enforcement for
    /// the mechanism. Defaults to [`InboxCap::default`]; changeable at
    /// runtime via [`ProdEnv::set_inbox_cap`] (a test shrinking it to
    /// provoke overflow deterministically does not need to reconstruct the
    /// env).
    cap: InboxCap,
}

/// One stream's observability bookkeeping inside [`Demux`] — see that
/// type's own doc. `bytes` is the sum of `payload.len()` for every
/// envelope currently sitting in this stream's `Demux::queues` entry;
/// frame count is read directly off that `VecDeque`'s own length rather
/// than duplicated here, so the two can never drift apart.
#[derive(Default, Clone, Copy)]
struct StreamMeta {
    /// Sum of `payload.len()` for every envelope currently queued.
    bytes: usize,
    /// Whether [`RecvStream::poll`] has ever been polled for this stream
    /// at all (parked pending, or immediately ready) — distinguishes
    /// "nobody has ever tried to consume this stream" from "a consumer
    /// exists but is lagging."
    ever_polled: bool,
    /// Whether a receiver is *currently* parked on this stream (its most
    /// recent poll returned `Pending` and no frame has arrived/been
    /// popped since).
    waker_parked: bool,
    /// Wall-clock instant of the last successful pop from this stream's
    /// queue, or `None` if it has never been popped — "never consumed"
    /// vs. "lagging" is exactly this field being `None` vs. `Some` with
    /// a large `elapsed()`.
    last_pop: Option<Instant>,
}

/// A point-in-time snapshot of one stream's [`Demux`] bookkeeping
/// ([`ProdEnv::inbox_stats`]) — pure observability, never used to make a
/// routing/backpressure decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StreamInboxStats {
    /// The stream id (ADR 0026) — `0` is `PRIMARY_STREAM` (the control
    /// plane, or a non-split tablet's CP group on a combined node); a
    /// nonzero id is a tablet id (`stream = tablet_id`, ADR 0040 Decision
    /// A) — decoding it further (is this tablet still live, which table)
    /// needs the replicated `Metadata` this crate doesn't have, so that
    /// decoding is left to the caller (`animusd`'s admin debug route).
    pub stream: u64,
    /// Frames currently queued for this stream.
    pub frames: usize,
    /// Payload bytes currently queued for this stream (sum of every
    /// queued envelope's `payload.len()`).
    pub bytes: usize,
    /// Whether a receiver is *currently* parked on this stream.
    pub waker_parked: bool,
    /// Whether this stream has ever been polled by a receiver at all.
    pub ever_polled: bool,
    /// Milliseconds since this stream's last successful pop, or `None`
    /// if it has never been popped — "never consumed" vs. "lagging."
    pub since_last_pop_ms: Option<u64>,
}

/// A point-in-time snapshot of a [`ProdEnv`]'s whole multiplexed inbox
/// ([`ProdEnv::inbox_stats`]): aggregate totals across every stream (fed
/// into `Metric::DemuxQueuedFrames`/`Metric::DemuxQueuedBytes` by
/// [`Env::refresh_inbox_metrics`](crate::Env::refresh_inbox_metrics)) plus
/// the largest streams by queued bytes, descending.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InboxStats {
    /// Total frames queued across every stream this env has ever seen.
    pub total_frames: usize,
    /// Total payload bytes queued across every stream this env has ever
    /// seen — the number that grows unboundedly for a stream nobody
    /// reads.
    pub total_bytes: usize,
    /// The largest streams by `bytes` descending, capped at whatever
    /// `top_n` [`ProdEnv::inbox_stats`] was called with.
    pub top_streams: Vec<StreamInboxStats>,
}

/// This env's handshake `ext` policy (ADR 0073 Phase 2), shared by the
/// accept loop and every dial: its own `ext` bytes (default empty — the
/// pre-Phase-2 preamble, byte for byte) and whether a peer advertising no
/// `ext` (a Phase 1 binary) is refused. Both are read at handshake time, so
/// a change applies to **new** connections; additionally an already-open
/// accepted connection whose peer sent an empty `ext` is closed on its next
/// frame once `require_peer_ext` is on (see `read_frames`).
#[derive(Default)]
struct HandshakeCfg {
    own_ext: StdMutex<Arc<[u8]>>,
    require_peer_ext: AtomicBool,
}

impl HandshakeCfg {
    fn own_ext(&self) -> Arc<[u8]> {
        Arc::clone(&self.own_ext.lock().expect("handshake cfg poisoned"))
    }
    fn require(&self) -> bool {
        self.require_peer_ext.load(Ordering::Acquire)
    }
}

struct Inner {
    node_id: NodeId,
    /// Handshake `ext` policy; see [`HandshakeCfg`].
    hs: Arc<HandshakeCfg>,
    start: Instant,
    /// The peer address book: node id -> `host:port` (a hostname or a numeric
    /// address — `TcpStream::connect` resolves either). Kept as a string end
    /// to end (ADR: advertise/dial split) so a peer can be registered by a
    /// stable DNS name (e.g. a Kubernetes StatefulSet pod's own name) rather
    /// than the numeric address it happens to be bound to.
    peers: Arc<StdMutex<BTreeMap<NodeId, String>>>,
    /// This env's own listener address (always numeric — this is a bind
    /// address, never an advertised one).
    local_addr: SocketAddr,
    /// Cached outbound connections, one per destination address string, so
    /// `send`/`send_stream` do not pay a TCP handshake (or, for a hostname
    /// peer, a fresh DNS lookup) per message (Raft heartbeats/AppendEntries/
    /// votes are the hot path). Keyed by the address **string** exactly as
    /// registered in `peers`, not `NodeId` and not a resolved `SocketAddr`:
    /// the frame header carries `from` per message and the receiver demuxes
    /// per *listener*, so one connection per address string is correct even
    /// when several ids map to it, and a re-mapped peer id naturally picks up
    /// a fresh connection. Resolution (DNS or numeric parse) happens only on
    /// the connect path — an already-cached live stream is reused with no
    /// lookup at all; a write failure drops the stale entry and the
    /// reconnect-once re-resolves, which is what lets a moved pod (same
    /// hostname, new IP) recover on its very next send. The outer `StdMutex`
    /// only guards map lookup/insert (never held across `.await`); the
    /// per-address `tokio::sync::Mutex` serializes whole-frame writes so
    /// concurrent senders to one peer never interleave frames, without
    /// head-of-line blocking *across* peers.
    #[allow(clippy::type_complexity)]
    conns: Arc<StdMutex<BTreeMap<String, Arc<Mutex<Option<MaybeTlsStream>>>>>>,
    /// This node's intra-wire TLS material (ADR 0064), or `None` for plain
    /// TCP — the default, and the only mode this crate had before this ADR.
    /// Shared (not per-connection) since every accept/dial this env performs
    /// speaks the same mode: a cluster is either all-TLS or all-plain on the
    /// internal wire (config-validated one layer up, `animusd`, commit 2).
    tls: Option<TlsMaterial>,
    /// This node's `Disk` implementation — plain (byte-identical to
    /// pre-ADR-0069 behavior) unless an `--encryption-key` was configured at
    /// [`bind_with_tls`](ProdEnv::bind_with_tls), in which case every file
    /// this env reads/writes is sealed under it (ADR 0069). See
    /// [`DiskBackend`]'s own doc.
    disk: DiskBackend,
    /// This env's multiplexed inbox (ADR 0026): a background pump task (spawned
    /// alongside the accept loop, see `spawn_pump`) drains the accept loop's raw
    /// per-connection frames and files each into `demux.queues[frame.stream]`.
    demux: Arc<StdMutex<Demux>>,
    /// Abort handles for every task this env owns — the inbound-connection
    /// accept loop, its demux pump, and everything spawned through
    /// [`Spawner::spawn`] (the Raft driver, the replica serve loop, and, most
    /// importantly, one send task per outbound frame — `send_stream`'s
    /// issue #661 `SEND_TIMEOUT`-bounded connect+write task). [`shutdown`]
    /// (ProdEnv::shutdown) aborts every handle still in this vec, so the
    /// node can be torn down and its listener port freed.
    ///
    /// **Pruned, not append-only (the leak this field used to be).** A
    /// `tokio::task::AbortHandle` pins its task's `Cell` in the runtime for
    /// as long as the handle lives — a finished task's `Cell` is only
    /// actually freed once *every* handle to it (the runtime's own internal
    /// one included) is dropped. Before this, nothing ever removed a
    /// *finished* task's handle from this vec — only `shutdown`/
    /// `shutdown_and_wait` (`mem::take`ing the whole vec) ever shrank it —
    /// so on a long-lived process this vec, and the task `Cell`s it pinned,
    /// grew without bound with every send: one outbound frame, one spawned
    /// send task, one permanently-leaked `Cell`, forever (heaptrack of a
    /// live `animusd --cluster-control 3 --cluster-data 5` run under bulk
    /// load found ~94 MB of a 254 MB RSS peak in exactly these cells, all
    /// still "live" only because this vec still held their `AbortHandle`).
    /// **Completion-driven sweep (issue #1105).** The first fix (#1062) swept
    /// only inside `spawn`, once `tasks.len()` reached a high-water mark
    /// recomputed after each sweep as `max(FLOOR, 2 * live-at-that-sweep)`.
    /// That leaked in production: after a burst (say 50k in-flight sends at
    /// sweep time, threshold 100k) the threshold stayed at the *burst's* peak
    /// while a quiet node spawned too slowly ever to reach it again, so up to
    /// ~2x the peak live count of *finished* handles stayed pinned
    /// indefinitely — violating the "roughly 2x the live count" invariant.
    /// The fix is to drive pruning by task *completion* with a threshold
    /// derived from *current* state, never a remembered peak: every spawned
    /// task carries a `CompletionGuard` whose `Drop` (normal completion,
    /// panic unwind, or abort — all drop it) bumps `finished_unswept` and
    /// calls `maybe_sweep`, which sweeps once
    /// `finished_unswept >= max(TASK_PRUNE_FLOOR, tasks.len() - finished_unswept)`
    /// (finished >= max(FLOOR, approx live)). So finished-but-tracked handles
    /// are bounded at every point in time, with no dependence on any future
    /// spawn, and a sweep (O(live + finished)) runs only after at least
    /// `max(FLOOR, live)` completions: amortized O(1) per task.
    ///
    /// **Deadlock rule.** The guard takes this mutex from inside a task's
    /// drop, and `tokio::spawn` may drop the future *inline* (for example
    /// while the runtime is shutting down), running the guard synchronously
    /// on the caller. So `spawn` must NEVER hold this lock across
    /// `tokio::spawn`, and no code may drop an *unfinished* task's last
    /// `AbortHandle`/`JoinHandle` while holding it (the sweep only drops
    /// handles whose task `is_finished()`, and `shutdown`/`shutdown_and_wait`
    /// release the lock at the end of their `mem::take` statement before
    /// aborting anything).
    tasks: StdMutex<Vec<tokio::task::AbortHandle>>,
    /// Number of tracked tasks that have completed (finished, panicked or
    /// been aborted) since the last sweep — see `tasks`'s doc. The sweep's
    /// `store(0)` can undercount: a task whose guard already ran (counted)
    /// but which is not yet `is_finished()` when the sweep's `retain` looks
    /// survives that sweep yet is no longer counted afterwards. Only a
    /// handful of in-flight tasks can be in that window, so the undercount
    /// is bounded by a small constant, and harmless: it can only delay the
    /// next sweep by that many completions. Completions of tasks already
    /// `mem::take`n out by `shutdown` only *over*-count, which the next
    /// sweep resets.
    finished_unswept: AtomicUsize,
    /// Count of tasks spawned through [`Spawner::spawn`] that panicked
    /// (issue #939) — see that impl's own doc for why this exists and how
    /// it's counted. A cancelled (aborted) task never increments this: its
    /// future is dropped, not unwound, so it never reaches the counting
    /// path at all (verified by `spawn_aborted_task_never_counts_as_a_
    /// panic`).
    task_panics: AtomicU64,
    /// Subset of `task_panics` that were **consensus-loop** tasks (spawned
    /// via [`Spawner::spawn_critical`], issue #1220).
    critical_task_panics: AtomicU64,
    /// The first spawned-task panic's message, if any (issue #939) — kept
    /// so a teardown check can name what happened rather than just "N
    /// panics". Only the first is kept: a cascade of panics after the
    /// first is rarely more informative and this stays a single small
    /// allocation regardless of how many tasks eventually panic.
    first_task_panic: StdMutex<Option<String>>,
    /// This node's recording metrics sink (ADR 0015). A real recording handle
    /// (unlike the no-op an arbitrary `Env` returns by default), so the assembled
    /// production node accumulates control-plane counters; integration exposes a
    /// snapshot of it (see `metrics_text`). Cheap to clone; shared across this
    /// env's clones so every role-handle records into one sink.
    metrics: MetricsHandle,
}

impl ProdEnv {
    /// Bind this node's listener (start accepting peer connections) and create
    /// its data directory. Returns the environment and the actual bound address
    /// (useful when `listen` has port 0 for an OS-assigned port).
    ///
    /// The peer address book starts empty; install it with
    /// [`set_peers`](Self::set_peers) before sending.
    ///
    /// # Errors
    /// Returns an error if the listen address cannot be bound or the data
    /// directory cannot be created.
    pub async fn bind(
        node_id: NodeId,
        listen: SocketAddr,
        data_dir: impl Into<PathBuf>,
    ) -> std::io::Result<(Self, SocketAddr)> {
        Self::bind_with_tls(node_id, listen, data_dir, None).await
    }

    /// Like [`bind`](Self::bind), but with the intra-node wire's TLS mode
    /// explicit (ADR 0064, S-01 step 1): `None` is plain TCP — byte-for-byte
    /// the same transport [`bind`](Self::bind) has always used — `Some`
    /// loads the given [`TlsConfig`]'s PEM files once and speaks **mutual**
    /// TLS on every accept and dial this env performs (see the `tls` module
    /// doc for what that means and why it's the only mode built so far).
    ///
    /// This is `bind`'s general form specifically so every existing caller
    /// of `bind` — and every test — keeps compiling and behaving identically
    /// with no change; only a caller that actually wants TLS reaches for
    /// this constructor instead.
    ///
    /// # Errors
    /// Returns an error if the listen address cannot be bound, the data
    /// directory cannot be created, or (when `tls` is `Some`) its PEM files
    /// cannot be read or rustls rejects the resulting material.
    pub async fn bind_with_tls(
        node_id: NodeId,
        listen: SocketAddr,
        data_dir: impl Into<PathBuf>,
        tls: Option<TlsConfig>,
    ) -> std::io::Result<(Self, SocketAddr)> {
        Self::bind_with_tls_and_key(node_id, listen, data_dir, tls, None).await
    }

    /// Like [`bind_with_tls`](Self::bind_with_tls), but with encryption at
    /// rest explicit (ADR 0069, S-03 PR 1): `None` is plaintext — byte-for-
    /// byte the same on-disk behavior every existing caller of `bind`/
    /// `bind_with_tls` has always had — `Some` composes every `Disk`
    /// method this env serves with `EncryptedDisk`, sealing every file
    /// under the given key. Verifies/initializes the data directory's
    /// encryption marker (see `verify_or_init_marker`) **before** returning
    /// — a wrong or missing key against an already-encrypted data
    /// directory (or vice versa) is refused right here, before any
    /// WAL/engine file in it is opened for use.
    ///
    /// This is `bind_with_tls`'s general form for the identical reason
    /// `bind_with_tls` is `bind`'s: every existing caller of `bind`/
    /// `bind_with_tls` — and every test — keeps compiling and behaving
    /// identically with no change; only a caller that actually wants
    /// encryption at rest reaches for this constructor instead.
    ///
    /// # Errors
    /// Everything [`bind_with_tls`](Self::bind_with_tls) can return, plus a
    /// loud refusal (see `verify_or_init_marker`'s own doc for the exact
    /// text) on an `encryption_key`/data-directory mismatch.
    pub async fn bind_with_tls_and_key(
        node_id: NodeId,
        listen: SocketAddr,
        data_dir: impl Into<PathBuf>,
        tls: Option<TlsConfig>,
        encryption_key: Option<crate::EncryptionKey>,
    ) -> std::io::Result<(Self, SocketAddr)> {
        let tls = tls.map(|cfg| cfg.load()).transpose()?;
        let data_dir = data_dir.into();
        tokio::fs::create_dir_all(&data_dir).await?;
        let listener = TcpListener::bind(listen).await?;
        let local_addr = listener.local_addr()?;
        // Built before `spawn_accept` (rather than inside `Inner` only) so
        // the accept loop's own per-connection handshake (ADR 0073 Phase 0,
        // workstream D) can record `Metric::NetworkHandshakeRefused` into
        // the same sink this env's `metrics_text()`/`Env::metrics()` expose —
        // one recording handle for the whole env, not a second one.
        let metrics = MetricsHandle::recording();
        let hs = Arc::new(HandshakeCfg::default());
        let (raw_rx, accept_abort) =
            spawn_accept(listener, tls.clone(), metrics.clone(), Arc::clone(&hs));
        let demux = Arc::new(StdMutex::new(Demux::default()));
        let pump_abort = spawn_pump(raw_rx, Arc::clone(&demux), metrics.clone());

        let raw = RawFsDisk {
            data_dir,
            dir_synced: Arc::new(StdMutex::new(BTreeSet::new())),
        };
        let disk = match encryption_key {
            None => {
                crate::verify_or_init_marker(&raw, &DiskSaltRng, None).await?;
                DiskBackend::Plain(raw)
            }
            Some(key) => {
                crate::verify_or_init_marker(&raw, &DiskSaltRng, Some(&key)).await?;
                DiskBackend::Encrypted(crate::EncryptedDisk::new(raw, DiskSaltRng, key))
            }
        };

        let env = Self {
            inner: Arc::new(Inner {
                node_id,
                hs,
                start: Instant::now(),
                peers: Arc::new(StdMutex::new(BTreeMap::new())),
                local_addr,
                conns: Arc::new(StdMutex::new(BTreeMap::new())),
                tls,
                disk,
                demux,
                tasks: StdMutex::new(vec![accept_abort, pump_abort]),
                finished_unswept: AtomicUsize::new(0),
                task_panics: AtomicU64::new(0),
                critical_task_panics: AtomicU64::new(0),
                first_task_panic: StdMutex::new(None),
                metrics,
            }),
        };
        Ok((env, local_addr))
    }

    /// Set the handshake `ext` bytes (ADR 0073 Phase 2; see
    /// [`handshake::encode_ext`]) this env advertises in its preamble on
    /// both dial and accept. Default empty, which writes today's preamble
    /// bytes exactly. Applies to connections handshaken after the call;
    /// existing pooled connections keep what they advertised.
    pub fn set_own_ext(&self, ext: Vec<u8>) {
        *self
            .inner
            .hs
            .own_ext
            .lock()
            .expect("handshake cfg poisoned") = Arc::from(ext);
    }

    /// This env's own current handshake `ext` bytes.
    #[must_use]
    pub fn own_ext(&self) -> Vec<u8> {
        self.inner.hs.own_ext().to_vec()
    }

    /// This env's own listener address.
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.inner.local_addr
    }

    /// Install (or replace) the peer address book: a map from node id to
    /// `host:port` (a hostname or a numeric address) for every node this env
    /// may send to.
    pub fn set_peers(&self, peers: BTreeMap<NodeId, String>) {
        *self.inner.peers.lock().expect("peers poisoned") = peers;
    }

    /// Add or replace a **single** entry in the peer address book, leaving every
    /// other entry untouched — the incremental dual of [`set_peers`](Self::set_peers),
    /// which replaces the whole map (there is no `get_peers` to read-modify-write
    /// around, by design: a full periodic rebuild from a known-good source, as
    /// `animusd`'s `peer_sync_loop` does for the `raftkv` role, is the intended
    /// pattern for anything that needs to *converge*).
    ///
    /// This exists for a case that pattern doesn't cover: a **control**-role
    /// voter added at runtime via `RaftCore::change_membership` (ADR 0037) needs
    /// its address reachable *before* the leader can replicate anything to it,
    /// and unlike the `raftkv` role, the control role has no periodic peer-sync
    /// loop (the control group was static before ADR 0037, ADR 0030's scope
    /// decision). `animusd`'s control-membership admin action calls this on the
    /// **local leader's** own env immediately after registering the new voter,
    /// so its very next `AppendEntries`/`InstallSnapshot` has somewhere to go —
    /// see `ProdEnv::send`'s own doc for what happens to a message with no known
    /// peer address ("dropped... Raft retries once the address lands").
    ///
    /// **Known scope limit (documented, not fixed here):** this updates only
    /// *this* env's own peer book, i.e. whichever node happens to make the call
    /// (the leader at the time of the admin action). Another existing voter
    /// learns the new peer's address only once *it* independently sends to
    /// (or receives a message identifying) that id through some other path —
    /// today, only by itself later becoming leader and being handed the same
    /// admin call, or an operator restarting it with an updated static config.
    /// A generalized replicated-address + periodic-resync mechanism for the
    /// control role (mirroring `peer_sync_loop`) is deliberately deferred —
    /// see the ADR 0037 stack's engineering-lessons entry.
    pub fn merge_peer(&self, id: NodeId, addr: String) {
        self.inner
            .peers
            .lock()
            .expect("peers poisoned")
            .insert(id, addr);
    }

    /// Abort every task this env owns — its inbound-connection accept loop and
    /// everything spawned through [`Spawner::spawn`] — so the node can be torn
    /// down cleanly and its listener port freed for a restart. Idempotent; once
    /// called, this env should no longer be used to spawn or receive.
    ///
    /// **`abort()` only *requests* cancellation — it does not wait for the
    /// task to actually stop.** The accept loop's `TcpListener` (and thus the
    /// port) is only released once the aborted task is next polled and
    /// dropped by the runtime, which can lag arbitrarily behind this call
    /// returning under CPU contention. A caller that must rebind the same
    /// address afterward (a same-address restart) needs
    /// [`shutdown_and_wait`](Self::shutdown_and_wait) instead; this plain
    /// `shutdown` remains for callers that only need the task to stop
    /// eventually (most simulated-crash tests never rebind the killed node's
    /// address in the same process).
    pub fn shutdown(&self) {
        let handles = std::mem::take(&mut *self.inner.tasks.lock().expect("tasks poisoned"));
        for h in handles {
            h.abort();
        }
    }

    /// Like [`shutdown`](Self::shutdown), but also waits (bounded,
    /// best-effort) for every aborted task — including the accept loop that
    /// owns this env's listening `TcpListener` — to actually finish
    /// unwinding before returning, so the listener really is dropped and its
    /// port really is free by the time this call completes.
    ///
    /// This closes a real flake: `abort()` schedules cancellation but does not
    /// synchronously drop the task's future, so a bare [`shutdown`] followed
    /// immediately by a rebind on the same address can race this *same*
    /// process's own not-yet-unwound accept-loop task for the port. Under
    /// light load the runtime polls (and drops) the cancelled task within
    /// microseconds; under `cargo test --workspace`-level CPU contention that
    /// can lag for seconds — long enough to intermittently fail a
    /// same-address restart test even behind a generous rebind-retry bound
    /// (`AddrInUse`, indistinguishable from a genuinely-occupied port without
    /// this fix — see the port-TOCTOU entries in
    /// `docs/engineering-lessons.md`). The wait itself is capped at a few
    /// seconds so a task that is somehow never polled again can't hang a
    /// caller forever — a vanishingly unlikely case given accept loops are
    /// perpetually parked in `.accept().await` (an immediately-cancellable
    /// await point), included only as defense in depth.
    pub async fn shutdown_and_wait(&self) {
        let handles = std::mem::take(&mut *self.inner.tasks.lock().expect("tasks poisoned"));
        for h in &handles {
            h.abort();
        }
        wait_all_finished(&handles).await;
    }

    /// Count of tasks spawned through [`Spawner::spawn`]/
    /// [`EnvExt::spawn_task`](crate::EnvExt::spawn_task) on this env that
    /// panicked (issue #939) — see that impl's own doc for the mechanism. A
    /// test teardown check polls this (`animusd::Node` sums it across a
    /// node's role envs) to fail loudly instead of letting a zombie replica
    /// masquerade as a passing run.
    #[must_use]
    pub fn spawned_task_panics(&self) -> u64 {
        self.inner.task_panics.load(Ordering::SeqCst)
    }

    /// Count of **consensus-loop** tasks (spawned via
    /// [`Spawner::spawn_critical`]) that panicked on this env (issue #1220);
    /// a subset of [`spawned_task_panics`](Self::spawned_task_panics). Also
    /// exported as `Metric::ConsensusTaskPanics`.
    #[must_use]
    pub fn consensus_task_panics(&self) -> u64 {
        self.inner.critical_task_panics.load(Ordering::SeqCst)
    }

    /// The first spawned-task panic's message this env counted, if any
    /// (issue #939) — `None` until [`spawned_task_panics`](Self::
    /// spawned_task_panics) is nonzero.
    #[must_use]
    pub fn first_spawned_task_panic(&self) -> Option<String> {
        self.inner
            .first_task_panic
            .lock()
            .expect("first_task_panic poisoned")
            .clone()
    }

    /// The current number of `AbortHandle`s this env is tracking for
    /// [`shutdown`](Self::shutdown)/[`shutdown_and_wait`](Self::
    /// shutdown_and_wait) — the live length of `Inner::tasks`, after
    /// whatever pruning [`Spawner::spawn`] has already done. Test-only: the
    /// regression proof for the task-handle-leak fix polls this to assert
    /// the vec stays bounded rather than growing with every spawn; nothing
    /// in production code needs this count (the `Metric::
    /// SpawnedTaskHandlesTracked` level gauge is the production-facing
    /// equivalent).
    #[cfg(test)]
    fn tracked_task_handles(&self) -> usize {
        self.inner.tasks.lock().expect("tasks poisoned").len()
    }

    /// A point-in-time text export of this env's recorded metrics (ADR 0015):
    /// one `name value` line per counter plus the leadership gauge, in stable
    /// order. This is what an integration-level `/metrics` endpoint serves; the
    /// `Env` seam itself does no HTTP. A pure read of the atomic sink.
    ///
    /// Refreshes the demux inbox gauges ([`Env::refresh_inbox_metrics`])
    /// first, so this export always reflects the current queue state
    /// rather than whatever the last refresh happened to leave behind.
    #[must_use]
    pub fn metrics_text(&self) -> String {
        self.refresh_inbox_metrics_inner();
        self.inner.metrics.snapshot().to_text()
    }

    /// Replace this env's per-stream inbox backpressure cap (ADR 0026,
    /// 2026-09-28 inbox-cap amendment) — see [`crate::InboxCap`]'s own doc
    /// for what it bounds and the default's derivation. Takes effect on the
    /// very next frame pushed to any stream (`spawn_pump`'s own
    /// `enforce_inbox_cap` reads it fresh under the same lock each time);
    /// shrinking it can also evict an already-over-the-new-cap stream's
    /// oldest frames on that next push, not only newly-arriving ones. Used
    /// by a test that wants to provoke overflow deterministically without
    /// sending megabytes of real payload.
    pub fn set_inbox_cap(&self, cap: InboxCap) {
        self.inner.demux.lock().expect("demux poisoned").cap = cap;
    }

    /// This env's current per-stream inbox backpressure cap ([`set_inbox_cap`](Self::set_inbox_cap)).
    #[must_use]
    pub fn inbox_cap(&self) -> InboxCap {
        self.inner.demux.lock().expect("demux poisoned").cap
    }

    /// A point-in-time snapshot of this env's multiplexed inbox (ADR
    /// 0026): total queued frames/bytes across every stream this env has
    /// ever demultiplexed a frame for, plus the `top_n` largest streams by
    /// queued bytes, descending. A pure read under the `Demux` lock — no
    /// wall clock beyond a plain `Instant::now()` for the "time since last
    /// pop" field (this module's own sanctioned real-time boundary, see
    /// this file's module-level `disallowed_methods` allow), no I/O.
    ///
    /// `top_n = 0` skips building/sorting the per-stream list entirely —
    /// the cheap path [`Env::refresh_inbox_metrics`] uses, since it only
    /// wants the two totals.
    #[must_use]
    pub fn inbox_stats(&self, top_n: usize) -> InboxStats {
        let d = self.inner.demux.lock().expect("demux poisoned");
        let (total_frames, total_bytes) = inbox_totals_locked(&d);
        let mut top_streams = Vec::new();
        if top_n > 0 {
            let now = Instant::now();
            // Union of both maps' keys: a stream can have a `stream_meta`
            // entry with no `queues` entry (polled but never pushed to —
            // `RecvStream::poll`'s pending arm never creates a `queues`
            // entry) or vice versa (pushed to but never yet polled).
            let mut ids: BTreeSet<u64> = d.queues.keys().copied().collect();
            ids.extend(d.stream_meta.keys().copied());
            top_streams = ids
                .into_iter()
                .map(|stream| {
                    let frames = d.queues.get(&stream).map_or(0, VecDeque::len);
                    let meta = d.stream_meta.get(&stream).copied().unwrap_or_default();
                    StreamInboxStats {
                        stream,
                        frames,
                        bytes: meta.bytes,
                        waker_parked: meta.waker_parked,
                        ever_polled: meta.ever_polled,
                        since_last_pop_ms: meta.last_pop.map(|t| {
                            u64::try_from(now.duration_since(t).as_millis()).unwrap_or(u64::MAX)
                        }),
                    }
                })
                .collect();
            top_streams.sort_by(|a, b| b.bytes.cmp(&a.bytes).then(a.stream.cmp(&b.stream)));
            top_streams.truncate(top_n);
        }
        InboxStats {
            total_frames,
            total_bytes,
            top_streams,
        }
    }

    /// The actual body of [`Env::refresh_inbox_metrics`] — a free inherent
    /// method (not the trait method itself) so [`metrics_text`](Self::
    /// metrics_text) above can call it without going through the trait,
    /// and so the trait impl below can delegate to it the same way
    /// [`merge_peer`](Self::merge_peer)'s trait impl delegates to its own
    /// inherent method.
    fn refresh_inbox_metrics_inner(&self) {
        let (total_frames, total_bytes) = {
            let d = self.inner.demux.lock().expect("demux poisoned");
            inbox_totals_locked(&d)
        };
        self.inner
            .metrics
            .set(Metric::DemuxQueuedFrames, total_frames as u64);
        self.inner
            .metrics
            .set(Metric::DemuxQueuedBytes, total_bytes as u64);
    }
}

/// Sum every stream's queued frame count / byte count under an already-
/// locked [`Demux`] — `O(distinct streams ever seen)`, never
/// `O(total frames queued)`, since `stream_meta`'s `bytes` field is
/// maintained incrementally on push/pop (see that field's own doc)
/// rather than by summing every envelope's payload length here.
fn inbox_totals_locked(d: &Demux) -> (usize, usize) {
    let total_frames = d.queues.values().map(VecDeque::len).sum();
    let total_bytes = d.stream_meta.values().map(|m| m.bytes).sum();
    (total_frames, total_bytes)
}

/// Ensure the parent directory of `path` exists, so opening a file whose name
/// carries a subdirectory prefix (e.g. `"db/wal"`) creates the intervening
/// directories instead of silently failing on a missing parent.
///
/// Called only on the *miss* path (an open failed `NotFound`), not per-append:
/// the data dir is created at `bind`, so the common case pays no extra syscall.
async fn ensure_parent(path: &std::path::Path) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    Ok(())
}

/// `fsync` a directory. POSIX requires an explicit fsync of the containing
/// directory to persist a *namespace* change (file creation, rename): without
/// it, a just-created WAL segment or a completed manifest swap can vanish on
/// power loss even after the file's own `sync_all` returned. Opening a
/// directory read-only and `fsync`ing it is the standard Linux idiom
/// (`std::fs::File::open` on a directory works there; tokio wraps it).
async fn sync_dir(dir: &std::path::Path) -> std::io::Result<()> {
    let f = tokio::fs::File::open(dir).await?;
    f.sync_all().await
}

/// Open `path` for appending, creating the file if absent (but not its parent
/// directories — see [`ProdEnv`]'s `append` for the retry-on-`NotFound` dance).
async fn open_append(path: &std::path::Path) -> std::io::Result<tokio::fs::File> {
    tokio::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .await
}

/// Upper bound on a frame's declared sender-id length. A node id is a short
/// string, so 1 KiB is far above any real one.
const MAX_FROM_LEN: usize = 1024;

/// Upper bound on a frame's declared payload length: 64 MiB, the same value
/// as `animus_node::MAX_FRAME_LEN` (the client protocol's cap). Duplicated
/// here because `animus-node` depends on this crate, not the reverse.
const MAX_FRAME_PAYLOAD_LEN: usize = 64 << 20;

/// Reject a peer-declared length over `max` *before* anything is allocated
/// for it: the length is untrusted (this port is open unless mutual TLS is
/// configured), and an unchecked `vec![0; len]` of a `u32` is a ~4 GiB
/// allocation per frame.
fn check_frame_len(what: &str, len: usize, max: usize) -> std::io::Result<()> {
    if len > max {
        tracing::warn!(
            what,
            len,
            max,
            "refusing oversized frame; closing connection"
        );
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("declared {what} length {len} exceeds cap {max}"),
        ));
    }
    Ok(())
}

/// Read length-prefixed `[from_len: u32][from: utf8 bytes][stream: u64][len:
/// u32][payload]` frames until EOF (ADR 0040 PR3 changed `from` from a fixed
/// `u64` to a length-prefixed UTF-8 string, since node ids are strings now;
/// ADR 0026 added the `stream` field; the rest of the frame is unchanged).
/// These are the *raw*, not-yet-demultiplexed frames off one accepted
/// connection — `spawn_pump` fans them out by `stream` into an env's
/// [`Demux`].
///
/// **This frame format is unchanged by ADR 0073 Phase 0's workstream D.**
/// What changed is what precedes it: every connection this env accepts or
/// dials now opens with a one-time [`handshake`] preamble exchange
/// ([`perform_handshake`]) — both sides write their own
/// [`handshake::NETWORK_PROTOCOL`] preamble, then read and check the
/// peer's, before a single frame is written or read. `read_frames` itself
/// is only ever called on a connection that already passed that check
/// (`spawn_accept` calls [`perform_handshake`] first and simply drops the
/// connection on failure, never reaching this function) — so the frame
/// layout here, and the receive side in general, needs no change: the
/// handshake is a connection-setup step, not a per-frame one.
async fn read_frames<S: AsyncRead + Unpin>(
    mut stream: S,
    tx: mpsc::UnboundedSender<Envelope>,
    peer_ext: Arc<[u8]>,
    hs: Arc<HandshakeCfg>,
    metrics: MetricsHandle,
) -> std::io::Result<()> {
    loop {
        let from_len = match stream.read_u32().await {
            Ok(v) => v as usize,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e),
        };
        check_frame_len("sender id", from_len, MAX_FROM_LEN)?;
        let mut from_bytes = vec![0u8; from_len];
        stream.read_exact(&mut from_bytes).await?;
        let from_str = String::from_utf8(from_bytes)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e.utf8_error()))?;
        // The sending side only ever writes an id that already passed
        // `NodeId::propose` (or the wire-trusted `nid`/test-support path) at
        // its own intake boundary — re-validating here would just duplicate
        // that check for no benefit, so this uses the unchecked constructor.
        let from = NodeId::new_unchecked(from_str);
        let msg_stream = stream.read_u64().await?;
        let len = stream.read_u32().await? as usize;
        check_frame_len("payload", len, MAX_FRAME_PAYLOAD_LEN)?;
        let mut payload = vec![0u8; len];
        stream.read_exact(&mut payload).await?;
        // The era-on refusal reaches connections opened before the flag
        // flipped: a Phase 1 (empty-ext) peer's open connection is closed on
        // its next frame; it re-dials and is refused at the handshake.
        if peer_ext.is_empty() && hs.require() {
            metrics.incr(Metric::NetworkHandshakeRefused);
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                handshake::HandshakeError::Phase1Peer,
            ));
        }
        if tx
            .send(Envelope {
                from,
                stream: msg_stream,
                payload,
                peer_ext: Arc::clone(&peer_ext),
            })
            .is_err()
        {
            return Ok(()); // receiver gone; node shutting down
        }
    }
}

/// Drain `raw_rx` (one accept loop's raw, not-yet-demultiplexed frames) and file
/// each into `demux`, keyed by the frame's `stream` (ADR 0026). Waking a parked
/// `recv_stream` is done with the demux lock dropped first (never wake while
/// holding a lock another poll might need). Runs until the accept loop's sender
/// side is dropped (env shutdown).
///
/// A frame addressed to a [`Demux::closed`] stream (ADR 0026, 2026-09-28
/// amendment) is dropped here rather than queued — counted via
/// [`Metric::DemuxFramesDroppedClosed`] — since nothing will ever `recv` a
/// closed stream's frames until it is reopened, and queuing them anyway
/// would just reproduce the exact unbounded-growth defect closing the
/// stream exists to fix.
fn spawn_pump(
    mut raw_rx: mpsc::UnboundedReceiver<Envelope>,
    demux: Arc<StdMutex<Demux>>,
    metrics: MetricsHandle,
) -> tokio::task::AbortHandle {
    let handle = tokio::spawn(async move {
        while let Some(env) = raw_rx.recv().await {
            let stream = env.stream;
            let waker = {
                let mut d = demux.lock().expect("demux poisoned");
                if d.closed.contains(&stream) {
                    metrics.incr(Metric::DemuxFramesDroppedClosed);
                    None
                } else {
                    d.stream_meta.entry(stream).or_default().bytes += env.payload.len();
                    d.queues.entry(stream).or_default().push_back(env);
                    enforce_inbox_cap(&mut d, stream, &metrics);
                    d.wakers.remove(&stream)
                }
            };
            if let Some(w) = waker {
                w.wake();
            }
        }
    });
    handle.abort_handle()
}

/// Enforce `d.cap` on `stream` after a push (ADR 0026, 2026-09-28 inbox-cap
/// amendment): while the stream's queue is over either bound, drop its
/// **oldest** frame (the front of the `VecDeque` — the newest, just-pushed
/// frame at the back is never the one evicted) and count it via
/// [`Metric::DemuxFramesDroppedOverflow`]. A no-op for the overwhelming
/// majority of pushes (both bounds default far above ordinary traffic —
/// see [`crate::InboxCap`]'s own doc) and a plain loop rather than a single
/// `if`, since shrinking `cap` at runtime (`ProdEnv::set_inbox_cap`, what a
/// test does to provoke overflow at a small scale) can leave a stream more
/// than one frame over its new cap at once.
fn enforce_inbox_cap(d: &mut Demux, stream: u64, metrics: &MetricsHandle) {
    if crate::is_reserved_stream(stream) {
        // Reserved, per-node streams (PRIMARY_STREAM, and the small block
        // just below `u64::MAX` — see that function's own doc) never have
        // this cap's "consumer might never start" liveness gap, and some
        // legitimately carry a single frame far larger than an ordinary
        // tablet's own Raft entry — capping them regressed a real
        // production shape (a forward chase's retry budget exhausted
        // against spuriously-evicted relay replies) before this exemption
        // existed.
        return;
    }
    let cap = d.cap;
    while d
        .queues
        .get(&stream)
        .is_some_and(|q| q.len() > cap.max_frames)
        || d.stream_meta
            .get(&stream)
            .is_some_and(|m| m.bytes > cap.max_bytes)
    {
        let Some(dropped) = d.queues.get_mut(&stream).and_then(VecDeque::pop_front) else {
            break;
        };
        if let Some(meta) = d.stream_meta.get_mut(&stream) {
            meta.bytes = meta.bytes.saturating_sub(dropped.payload.len());
        }
        metrics.incr(Metric::DemuxFramesDroppedOverflow);
    }
}

/// Future that yields the next message addressed to a node on a given stream
/// (ADR 0026), mirroring `animus-sim`'s `Recv`.
struct RecvStream {
    demux: Arc<StdMutex<Demux>>,
    stream: u64,
}

impl Future for RecvStream {
    type Output = Envelope;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Envelope> {
        let mut d = self.demux.lock().expect("demux poisoned");
        if let Some(env) = d.queues.get_mut(&self.stream).and_then(VecDeque::pop_front) {
            let meta = d.stream_meta.entry(self.stream).or_default();
            meta.bytes = meta.bytes.saturating_sub(env.payload.len());
            meta.ever_polled = true;
            meta.waker_parked = false;
            meta.last_pop = Some(Instant::now());
            Poll::Ready(env)
        } else {
            let meta = d.stream_meta.entry(self.stream).or_default();
            meta.ever_polled = true;
            meta.waker_parked = true;
            d.wakers.insert(self.stream, cx.waker().clone());
            Poll::Pending
        }
    }
}

#[async_trait::async_trait]
impl Clock for ProdEnv {
    fn now(&self) -> Nanos {
        Nanos(
            self.inner
                .start
                .elapsed()
                .as_nanos()
                .min(u128::from(u64::MAX)) as u64,
        )
    }

    fn wall_now(&self) -> UnixMillis {
        // The host's real calendar clock, read fresh every call so an NTP
        // correction is picked up rather than baked in at bind time. A
        // pre-epoch system clock (only reachable if the host is grossly
        // misconfigured) reads as 0 rather than panicking; nothing here is
        // load-bearing for timing (see `Clock::wall_now`'s contract).
        UnixMillis(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.as_millis().min(u128::from(u64::MAX)) as u64),
        )
    }

    async fn sleep(&self, dur: Duration) {
        tokio::time::sleep(dur).await;
    }
}

#[allow(
    clippy::disallowed_types,
    reason = "OsRng is the sanctioned real-randomness source ProdEnv's Rng impl wraps (ADR 0003); see ADR 0061 Decision 4"
)]
impl Rng for ProdEnv {
    fn next_u64(&self) -> u64 {
        rand::RngCore::next_u64(&mut rand::rngs::OsRng)
    }

    fn fill_bytes(&self, dst: &mut [u8]) {
        rand::RngCore::fill_bytes(&mut rand::rngs::OsRng, dst);
    }
}

/// A minimal [`Rng`] usable at a CLI **pre-bind** boundary — before any
/// [`ProdEnv`] exists to draw from at all (a joining process mints/validates
/// its identity, over the network, before ever binding a listener). Real OS
/// randomness (`rand::rngs::OsRng`), byte-for-byte the same source
/// [`ProdEnv`]'s own [`Rng`] impl above draws from.
///
/// This is the ADR 0040 replacement for `generate_join_nonce`'s narrower,
/// bespoke OS-randomness exception (ADR 0036): rather than a one-off function
/// scoped to a single call site with its own hand-written justification,
/// pre-bind entropy now has one sanctioned, reusable home on the `Rng` trait
/// itself — any future pre-bind caller reaches for this instead of
/// reinventing the exception. Still the same narrow carve-out from the
/// `Env`-seam rule (ADR 0003): **only** for a genuine pre-bind CLI boundary
/// no `SimEnv` test ever drives (a joining process's own `NodeId::mint` call,
/// before `Node::bind`/`ProdEnv::bind` exist) — anything that runs in-process
/// on a live, already-bound node (e.g. `admin_add_control_member`'s minted-id
/// path) must keep drawing from its own bound env's `Rng` instead
/// (`leader.env().next_u64()`), never this type, so a `SimEnv` test can still
/// drive it deterministically.
#[derive(Debug, Default, Clone, Copy)]
pub struct PreBindRng;

#[allow(
    clippy::disallowed_types,
    reason = "OsRng is the sanctioned real-randomness source PreBindRng wraps at the pre-bind CLI boundary (ADR 0040 PR4); see ADR 0061 Decision 4"
)]
impl Rng for PreBindRng {
    fn next_u64(&self) -> u64 {
        rand::RngCore::next_u64(&mut rand::rngs::OsRng)
    }

    fn fill_bytes(&self, dst: &mut [u8]) {
        rand::RngCore::fill_bytes(&mut rand::rngs::OsRng, dst);
    }
}

/// A standalone real `Clock + Rng` with no node, listener or data directory —
/// for the few process-boundary constructors that need only time and
/// randomness (an `S3SegmentStore` built by a test harness or tool outside a
/// bound node). A bound node passes its own [`ProdEnv`] instead. Real wall
/// clock, real monotonic clock, `tokio` sleep and OS randomness, byte-for-byte
/// what [`ProdEnv`]'s own `Clock`/`Rng` impls use.
#[derive(Clone)]
pub struct ProdClockRng {
    start: std::time::Instant,
}

impl ProdClockRng {
    /// A fresh clock anchored at "now".
    #[must_use]
    pub fn new() -> Self {
        ProdClockRng {
            start: std::time::Instant::now(),
        }
    }
}

impl Default for ProdClockRng {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl Clock for ProdClockRng {
    fn now(&self) -> Nanos {
        Nanos(self.start.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64)
    }

    fn wall_now(&self) -> UnixMillis {
        UnixMillis(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.as_millis().min(u128::from(u64::MAX)) as u64),
        )
    }

    async fn sleep(&self, dur: Duration) {
        tokio::time::sleep(dur).await;
    }
}

#[allow(
    clippy::disallowed_types,
    reason = "OsRng is the sanctioned real-randomness source ProdClockRng wraps, like ProdEnv's and PreBindRng's own Rng impls (ADR 0061 Decision 4)"
)]
impl Rng for ProdClockRng {
    fn next_u64(&self) -> u64 {
        rand::RngCore::next_u64(&mut rand::rngs::OsRng)
    }

    fn fill_bytes(&self, dst: &mut [u8]) {
        rand::RngCore::fill_bytes(&mut rand::rngs::OsRng, dst);
    }
}

#[async_trait::async_trait]
impl Network for ProdEnv {
    async fn send_stream(&self, to: NodeId, stream: u64, payload: Vec<u8>) {
        let addr = {
            let peers = self.inner.peers.lock().expect("peers poisoned");
            match peers.get(&to) {
                Some(addr) => addr.clone(),
                None => {
                    // Fire-and-forget: an unknown peer is just another way the
                    // message is dropped (the caller gets no delivery result). It
                    // is an *expected transient* during membership changes — e.g. a
                    // tablet leader replicating to a freshly-minted CP split sibling
                    // before that member's address has propagated (control plane →
                    // per-node peer-sync); Raft retries on the next heartbeat once
                    // the address lands. A *genuinely* missing peer surfaces as the
                    // higher-level symptom (no leader / no progress) with its own
                    // logging, so this stays at debug to avoid alarming noise.
                    tracing::debug!(
                        to = %to,
                        "send to peer with no known address (dropped)"
                    );
                    return;
                }
            }
        };
        // Fire-and-forget semantics: a transport error is the network dropping
        // the message, not an error to the caller (see `Network::send`).
        let from = self.inner.node_id.clone();
        // Grab (or create) this address's connection slot. The map lock is a
        // `StdMutex` and must not be held across an `.await` — clone the
        // per-address `Arc` out and drop the guard before any I/O.
        let slot = {
            let mut conns = self.inner.conns.lock().expect("conns poisoned");
            Arc::clone(conns.entry(addr.clone()).or_default())
        };
        let tls = self.inner.tls.clone();
        let metrics = self.inner.metrics.clone();
        let hs = Arc::clone(&self.inner.hs);
        // Issue #661: run the actual connect+write on its own task, bounded by
        // `SEND_TIMEOUT`, instead of inline on this `.await` — see
        // `SEND_TIMEOUT`'s own doc for why a caller (most importantly a Raft
        // driver's own outbound-dispatch loop, which awaits one peer at a time)
        // must never be made to depend on this *particular* peer's own
        // reachability: a raw `TcpStream::connect`/write against a silently
        // unreachable address (no RST — e.g. a Kubernetes pod's collapsed
        // network endpoint after a hard restart) can otherwise ride the OS's
        // own multi-minute TCP retry timeout, during which every other peer
        // queued behind it in the same loop gets nothing at all.
        self.spawn(Box::pin(async move {
            match tokio::time::timeout(
                SEND_TIMEOUT,
                send_frame_pooled(
                    &slot,
                    &addr,
                    &from,
                    stream,
                    &payload,
                    DialCtx {
                        tls: tls.as_ref(),
                        metrics: &metrics,
                        hs: &hs,
                    },
                ),
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    tracing::debug!(?err, to = %to, %addr, "send failed (dropped)");
                }
                Err(_elapsed) => {
                    // Deliberately does NOT drop the cached connection here
                    // (issue #924 investigation): a `SEND_TIMEOUT` elapsing
                    // means only "this one send didn't finish in 2s" — under
                    // real host contention (a busy CI box, a CPU-starved
                    // node) that is routinely just scheduling latency on an
                    // otherwise perfectly healthy connection, not evidence
                    // the peer is unreachable. An earlier version of this
                    // fix cleared the slot unconditionally on timeout too,
                    // reasoning that a spurious reconnect "only costs one
                    // extra handshake" — that reasoning was wrong under
                    // *sustained* contention: discarding a warm connection
                    // forces the *next* chunk to pay a fresh connect, which
                    // can itself exceed `SEND_TIMEOUT` under the same load,
                    // repeating forever and turning transient slowness into
                    // total, permanent lack of progress (caught by this
                    // crate's own `large_metadata_catch_up_stays_live`
                    // sibling in `animus-control`, a ~1100-chunk streaming
                    // `InstallSnapshot` that went from "slow" to "zero bytes
                    // ever delivered" under exactly this change on a loaded
                    // box). The actually-dead-peer case this bound exists
                    // for is already covered by `POOLED_SOCKET_DEAD_PEER_
                    // TIMEOUT`'s keepalive/`TCP_USER_TIMEOUT`, which surface
                    // as a genuine write **error** (the `Ok(Err(err))` arm
                    // above, which does drop the slot) well within this
                    // 2s bound in the ordinary case — see that constant's
                    // own doc. See `docs/lessons/code-patterns/` for the
                    // general form of this lesson.
                    tracing::debug!(to = %to, %addr, "send timed out (dropped)");
                }
            }
        }));
    }

    async fn recv_stream(&self, stream: u64) -> Envelope {
        // Reopen (ADR 0026, 2026-09-28 amendment): clear this stream's
        // closed mark, if any, before ever awaiting — see this trait
        // method's own doc for why a stream that was never closed is
        // untouched by this (removing an absent key from a `BTreeSet` is a
        // no-op) and why this must happen unconditionally on every call,
        // not just the first, since a caller may `recv_stream` the same
        // stream many times over its lifetime.
        {
            let mut d = self.inner.demux.lock().expect("demux poisoned");
            d.closed.remove(&stream);
        }
        RecvStream {
            demux: Arc::clone(&self.inner.demux),
            stream,
        }
        .await
    }

    fn set_require_peer_ext(&self, on: bool) {
        self.inner.hs.require_peer_ext.store(on, Ordering::Release);
    }

    fn close_stream(&self, stream: u64) {
        debug_assert_ne!(
            stream,
            crate::PRIMARY_STREAM,
            "closing PRIMARY_STREAM is never correct — it is the node's own \
             control-plane/non-split-tablet stream, never a retired tablet's"
        );
        // Never wake a parked receiver here: the caller contract (see this
        // method's own trait doc) is that nothing is still polling this
        // stream by the time this is called, so any lingering waker belongs
        // to a task that is already gone — dropping it (not waking it) is
        // correct either way.
        let mut d = self.inner.demux.lock().expect("demux poisoned");
        d.queues.remove(&stream);
        d.stream_meta.remove(&stream);
        d.wakers.remove(&stream);
        d.closed.insert(stream);
    }
}

/// How long [`ProdEnv::shutdown_and_wait`] polls for every aborted task to
/// report finished before giving up. Generous — this only ever matters under
/// heavy host-level contention — but bounded so a caller can never hang
/// forever on a task that, for some unforeseen reason, is never polled again.
const SHUTDOWN_WAIT_TIMEOUT: Duration = Duration::from_secs(10);

/// Poll `AbortHandle::is_finished` on every handle until they've all reported
/// finished or [`SHUTDOWN_WAIT_TIMEOUT`] elapses, whichever comes first —
/// [`ProdEnv::shutdown_and_wait`]'s "actually wait for the abort to take
/// effect" step. Best-effort: a timeout here is silently swallowed (the
/// handles were already aborted; the caller proceeds regardless), matching
/// `shutdown`'s existing fire-and-forget failure mode for the pathological
/// case, while still turning the common case into a genuine guarantee.
async fn wait_all_finished(handles: &[tokio::task::AbortHandle]) {
    let poll = async {
        loop {
            if handles.iter().all(tokio::task::AbortHandle::is_finished) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    };
    let _ = tokio::time::timeout(SHUTDOWN_WAIT_TIMEOUT, poll).await;
}

/// How long [`spawn_accept`]'s loop backs off after a failed `accept()`
/// before retrying, so a *persistent* failure (e.g. the process pinned at
/// its file-descriptor ulimit) degrades to a bounded retry rate instead of
/// spinning the executor at 100% CPU re-entering `accept()` immediately.
/// Deliberately short — the common case is a single transient blip (see
/// this function's own doc) that should resume accepting within a fraction
/// of an election timeout, not linger backed off while peers time out
/// waiting to reach this node.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(10);

/// Bound on one outbound frame's whole connect+write ([`send_frame_pooled`],
/// including its one-shot reconnect) — issue #661.
///
/// `Network::send`/`send_stream` are documented fire-and-forget (see that
/// trait's own doc): the caller must never be made to depend on the
/// destination's own reachability. Before this bound existed,
/// `send_stream`'s connect+write ran inline on the caller's own `.await`
/// with no timeout at all, so a peer that was silently unreachable — no RST,
/// no ICMP, just dropped packets, exactly what a hard-killed Kubernetes
/// pod's collapsed network endpoint looks like to a sender that had a
/// connection cached to its old IP — rode the OS's own TCP SYN-retry /
/// retransmission timeout, commonly a minute or more. That is invisible on
/// any inline caller that dispatches to several peers **in the same task**,
/// most importantly a Raft driver's own outbound-message loop
/// (`animus-control`/`animus-cp-data`'s `drive`, which `.await`s
/// `env.send(..)` once per peer, sequentially): a single unreachable peer
/// stalled delivery to *every other* peer queued behind it in that same
/// dispatch round, starving the whole node's heartbeat/AppendEntries/vote
/// traffic — the network-path twin of issue #279's slow-fsync livelock, and
/// (unlike #279) invisible to `SimEnv`, which has no real sockets and no OS
/// TCP retry timers. `send_stream` now spawns the connect+write onto its own
/// task (see its own doc) *and* bounds it here, so both the caller and every
/// other peer's own dispatch are decoupled from this one peer's fate.
/// Generous relative to `heartbeat_interval`/`election_base` (a merely-slow-
/// but-live peer must never look like a timeout) yet far below the OS's own
/// multi-minute default — see `docs/engineering-lessons.md`'s matching
/// entry for the incident this closes.
const SEND_TIMEOUT: Duration = Duration::from_secs(2);

/// The minimum number of completed-but-unswept tasks before `maybe_sweep`
/// sweeps `Inner::tasks` (see that field's doc, issue #1105): the sweep
/// fires once `finished_unswept >= max(TASK_PRUNE_FLOOR, live)`. Chosen as a
/// round number comfortably above the steady-state handle count of a
/// small/medium cluster node so a quiet node essentially never sweeps, while
/// bounding finished-but-pinned handles to a small constant however few live
/// tasks there are.
const TASK_PRUNE_FLOOR: usize = 1024;

/// Bounds how long a pooled outbound or accepted socket can sit with data
/// genuinely unacknowledged (or, on platforms without [`TCP_USER_TIMEOUT`]
/// wired up, how long it can sit fully idle) before the kernel forces it
/// closed — issue #924.
///
/// [`SEND_TIMEOUT`]'s own doc closes the case of a peer that never *answers
/// the connect itself*. It does not close a distinct, worse case: a pooled
/// connection that was already established and cached, whose peer then
/// vanishes with **no FIN/RST at all** — exactly what a Kubernetes pod
/// recreated at a new IP looks like when its old network namespace's
/// teardown races (or loses to) the final FIN on the way out. Every write
/// on that connection still succeeds instantly (the bytes just land in this
/// host's own kernel send buffer; nothing about `write()` ever notices the
/// peer is gone), so `send_frame_pooled`'s "reconnect once on a write
/// error" path (issue #661) never triggers — the OS's own retransmission
/// timer is the only thing standing between this and detecting the vanish,
/// and that timer's default budget (`tcp_retries2`, Linux default ~15
/// exponential-backoff retries, commonly 13–15 **minutes** before the
/// kernel finally reports the write as failed) is why issue #924's
/// recreated voter sat `PreCandidate` with no leader contact for minutes: a
/// heartbeat "sent" every interval, none of them ever actually arriving,
/// none of them ever failing either.
///
/// [`TCP_KEEPALIVE_TIME`]/[`TCP_KEEPALIVE_INTERVAL`]/
/// [`TCP_KEEPALIVE_RETRIES`] plus, on Linux/Android/Fuchsia/Cygwin (the
/// only targets `socket2::Socket::set_tcp_user_timeout` supports),
/// `TCP_USER_TIMEOUT` set to this same bound, are applied to **every**
/// pooled outbound connection ([`connect_nodelay`]) and every accepted
/// inbound one ([`spawn_accept`]) — symmetric, and independent of whether
/// TLS is layered on top (both operate below the TLS record layer, on the
/// raw `TcpStream`). `TCP_USER_TIMEOUT` is the primary defense: per Linux's
/// `tcp(7)`, it bounds "the maximum amount of time transmitted data may
/// remain unacknowledged" *and* — this is the part that matters here —
/// "when used with the keepalive option, `TCP_USER_TIMEOUT` will override
/// keepalive to determine when to close the connection due to keepalive
/// failure," so it also bounds an idle connection's own keepalive-probe
/// failures, not just outstanding writes. Keepalive is kept alongside it
/// (not dropped once `TCP_USER_TIMEOUT` is set) for two reasons: it is the
/// **only** bound available on the handful of platforms without
/// `TCP_USER_TIMEOUT` (see [`harden_pooled_socket`]'s own doc for exactly
/// which), and on Linux itself an idle connection with no outstanding
/// writes needs keepalive's own probes to generate the traffic
/// `TCP_USER_TIMEOUT` bounds the acknowledgment of in the first place — a
/// connection that never writes and never probes never triggers either
/// timer.
///
/// **Detection bound**: on Linux, a vanished-with-no-FIN/RST peer surfaces
/// as a write/keepalive-probe error within this bound (5s) of its last
/// acknowledged traffic, at which point `send_frame_pooled`'s existing
/// reconnect-once path (issue #661) re-resolves the peer's address string —
/// picking up a moved pod's new IP exactly the way a live-but-restarted
/// peer already does — well within a Raft election timeout. Documented in
/// ADR 0003's ProdEnv notes and ADR 0060's rollout section (the operator's
/// own rollout wait budgets, `scripts/e2e-kind.sh`'s `wait_for_progress`
/// among them, already run to a 600s hard cap / 120s stall window, comfortably
/// above this). Chosen well under `heartbeat_interval`/`election_base` (so
/// this never itself looks like an election timeout to a merely-slow-but-
/// live peer) yet far below the OS's own multi-minute default.
///
/// [`TCP_USER_TIMEOUT`]: https://man7.org/linux/man-pages/man7/tcp.7.html
const POOLED_SOCKET_DEAD_PEER_TIMEOUT: Duration = Duration::from_secs(5);

/// How long a pooled/accepted connection may sit fully idle before the
/// first TCP keepalive probe is sent (`TCP_KEEPIDLE` on Linux/most Unix,
/// `TCP_KEEPALIVE` on macOS/iOS). Combined with
/// [`TCP_KEEPALIVE_INTERVAL`]/[`TCP_KEEPALIVE_RETRIES`] below, the worst-
/// case keepalive-only detection time (no `TCP_USER_TIMEOUT` support) is
/// `TIME + INTERVAL * RETRIES` = 2s + 1s*3 = 5s, matching
/// [`POOLED_SOCKET_DEAD_PEER_TIMEOUT`] — the two mechanisms are tuned to
/// the same bound rather than one silently being the stricter one.
const TCP_KEEPALIVE_TIME: Duration = Duration::from_secs(2);

/// Interval between successive keepalive probes once the idle period above
/// has elapsed and the first probe goes unanswered (`TCP_KEEPINTVL`).
const TCP_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(1);

/// Number of unanswered keepalive probes before the kernel gives up on the
/// connection (`TCP_KEEPCNT`).
const TCP_KEEPALIVE_RETRIES: u32 = 3;

/// Apply [`POOLED_SOCKET_DEAD_PEER_TIMEOUT`]'s keepalive + `TCP_USER_TIMEOUT`
/// hardening to a just-connected or just-accepted socket (issue #924) —
/// called from both [`connect_nodelay`] (dial side) and [`spawn_accept`]
/// (accept side) so a vanished peer is detected symmetrically regardless of
/// which side of the pooled connection this node was on.
///
/// `socket2::SockRef::from(stream)` borrows the live `tokio::net::TcpStream`
/// by its raw fd — no ownership transfer, no duplication, no interruption
/// of whatever is already `.await`ing on it — since `tokio::net::TcpStream`
/// itself exposes no keepalive/user-timeout setters at all.
///
/// **Best-effort, never connection-fatal**: a platform or sandbox that
/// rejects one of these `setsockopt` calls (a restrictive seccomp filter,
/// an exotic OS) degrades to "no early detection on this one socket" —
/// logged at `warn` — rather than failing the connect/accept outright. The
/// pre-#924 behavior (no hardening at all) is a strict subset of every
/// platform's outcome here, so this can never make a working connection
/// stop working.
///
/// **Platform coverage**: `TcpKeepalive::with_time`/`with_interval` are
/// supported on every Unix `socket2` targets plus Windows;
/// `with_retries` and `set_tcp_user_timeout` both additionally need the
/// `all` Cargo feature (enabled unconditionally on this dependency, see
/// `Cargo.toml`), and `set_tcp_user_timeout` itself only exists for
/// Linux/Android/Fuchsia/Cygwin — every other target (macOS/BSD/Windows/
/// dev sandboxes) falls back to keepalive alone, whose own worst case is
/// tuned to the identical bound (see [`TCP_KEEPALIVE_TIME`]'s doc) except
/// that it cannot distinguish "peer vanished" from "peer's kernel alive but
/// its application never reads" (a live kernel always acks a keepalive
/// probe, application-read state notwithstanding) — a real but narrower gap
/// than the one this fixes, tracked rather than closed for those platforms
/// since v1's actual deployment target (ADR 0060: Kubernetes, Linux nodes)
/// always has `TCP_USER_TIMEOUT` available.
fn harden_pooled_socket(stream: &TcpStream, peer_desc: &str) {
    let sock_ref = socket2::SockRef::from(stream);
    let keepalive = socket2::TcpKeepalive::new()
        .with_time(TCP_KEEPALIVE_TIME)
        .with_interval(TCP_KEEPALIVE_INTERVAL)
        .with_retries(TCP_KEEPALIVE_RETRIES);
    if let Err(err) = sock_ref.set_tcp_keepalive(&keepalive) {
        tracing::warn!(?err, peer = %peer_desc, "failed to set TCP keepalive (continuing without it)");
    }
    #[cfg(any(
        target_os = "linux",
        target_os = "android",
        target_os = "fuchsia",
        target_os = "cygwin"
    ))]
    if let Err(err) = sock_ref.set_tcp_user_timeout(Some(POOLED_SOCKET_DEAD_PEER_TIMEOUT)) {
        tracing::warn!(?err, peer = %peer_desc, "failed to set TCP_USER_TIMEOUT (continuing without it)");
    }
}

/// Bounds how long the per-connection handshake preamble exchange
/// ([`perform_handshake`], ADR 0073 Phase 0 workstream D) may take end to
/// end — this build's own write plus the peer's preamble arriving and
/// being read. Real time is fine here — this is `ProdEnv`, the
/// nondeterministic side of the seam (ADR 0003) — and this bound only ever
/// matters for a peer that is slow, wedged, or was never going to send a
/// preamble at all (a pre-baseline peer whose first bytes are a raw,
/// unversioned frame, or a peer that connects and then falls silent).
/// Generous relative to a same-datacenter round trip (this is a one-time
/// per-connection cost, not a per-frame one — see the `handshake` module's
/// own "why per-connection" doc) yet, on the **dial** side, mostly moot in
/// practice: `send_frame_pooled`'s own [`SEND_TIMEOUT`] (2s) already wraps
/// the whole connect-plus-handshake-plus-write, so this bound's real job is
/// the **accept** side, which has no other timeout guarding it at all —
/// without it, a peer that opens a connection and never sends anything
/// would park an accept-side task (and its `read_frames` never entered)
/// forever.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Every way a per-connection preamble exchange ([`exchange_preamble`]) can
/// fail, before a decoded [`handshake::Preamble`] would even reach
/// [`handshake::check_peer`] (a plain I/O failure — EOF, a reset, or this
/// build's own write erroring), the genuine protocol refusal `check_peer`
/// reports, or the whole exchange simply running out of time. Kept as one
/// enum, and `pub` (ADR 0073 Phase 0, workstream D, layer 3), so a caller
/// outside this crate — `animusd`'s own client/intra port, which needs the
/// identical exchange over its own [`Metric::ClientHandshakeRefused`] rather
/// than this module's [`Metric::NetworkHandshakeRefused`] — can log and
/// count each branch exactly like [`perform_handshake`] does below, without
/// this crate duplicating that logic for a second protocol.
#[derive(Debug)]
pub enum PreambleError {
    /// EOF, a reset, or any other I/O error while writing this build's own
    /// preamble or reading the peer's. **Never** counted as a refusal (see
    /// [`perform_handshake`]'s own doc for the counting decision) — this is
    /// what a merely slow or already-dead peer looks like, not a wire
    /// mismatch.
    Io(std::io::Error),
    /// The peer's preamble decoded but named the wrong magic/version, or
    /// declared an over-long extension — a genuine protocol mismatch.
    /// Counted via a caller's own handshake-refusal metric.
    Refused(handshake::HandshakeError),
    /// The whole exchange (this build's own write, plus the peer's preamble
    /// arriving and being read) did not complete within the caller-supplied
    /// timeout.
    TimedOut,
}

/// Reads one peer [`handshake::Preamble`] off `conn`: the fixed
/// [`handshake::HEADER_LEN`]-byte header first, then whatever extension
/// bytes it declares — mirroring [`handshake::decode`]'s own incremental-
/// read contract (see that function's own doc).
///
/// **The magic is checked against `expected` straight off the fixed
/// header, before `ext_len` is trusted at all**: a peer that is not
/// speaking this protocol (a pre-baseline peer's raw frame, or a client-
/// protocol dialer on the internal port) has arbitrary bytes where
/// `ext_len` sits, so trusting them first would either misname the
/// refusal as `ExtensionTooLong` or park this reader waiting for up to
/// [`handshake::MAX_EXTENSION_LEN`] bytes that never come — surfacing as
/// an uncounted timeout instead of a counted `BadMagic`. The version is
/// still [`handshake::check_peer`]'s job, once the whole `Preamble` is in
/// hand.
///
/// **Generic over `S: AsyncRead + Unpin`** (ADR 0073 Phase 0, workstream D,
/// layer 3) rather than named to [`MaybeTlsStream`] — the one implementation
/// [`exchange_preamble`] shares across every wire this crate's handshake
/// serves, on both this crate's own [`NETWORK_PROTOCOL`](handshake::
/// NETWORK_PROTOCOL) transport and `animusd`'s client/intra
/// [`CLIENT_PROTOCOL`](handshake::CLIENT_PROTOCOL) one.
pub async fn read_preamble<S: AsyncRead + Unpin>(
    conn: &mut S,
    expected: &handshake::ProtocolSpec,
) -> Result<handshake::Preamble, PreambleError> {
    let mut header = [0u8; handshake::HEADER_LEN];
    conn.read_exact(&mut header)
        .await
        .map_err(PreambleError::Io)?;
    if header[0..4] != expected.magic {
        let mut found = [0u8; 4];
        found.copy_from_slice(&header[0..4]);
        return Err(PreambleError::Refused(
            handshake::HandshakeError::BadMagic {
                protocol: expected.name,
                found,
            },
        ));
    }
    match handshake::decode(&header) {
        Ok((preamble, _)) => Ok(preamble),
        Err(handshake::HandshakeError::Incomplete) => {
            // The header alone decoded far enough to learn `ext_len` but
            // needs more bytes before a full `Preamble` comes out — read
            // exactly that many more and decode the whole thing.
            let ext_len = u16::from_le_bytes([header[5], header[6]]) as usize;
            let mut buf = header.to_vec();
            buf.resize(handshake::HEADER_LEN + ext_len, 0);
            conn.read_exact(&mut buf[handshake::HEADER_LEN..])
                .await
                .map_err(PreambleError::Io)?;
            let (preamble, _) = handshake::decode(&buf).map_err(PreambleError::Refused)?;
            Ok(preamble)
        }
        Err(other) => Err(PreambleError::Refused(other)),
    }
}

/// Writes this build's own preamble for `spec` to `conn` — the write half
/// of [`exchange_preamble`], split out so it composes with any `S:
/// AsyncWrite + Unpin`, exactly like [`read_preamble`]'s own read half.
pub async fn write_own_preamble<S: AsyncWrite + Unpin>(
    conn: &mut S,
    spec: &handshake::ProtocolSpec,
) -> Result<(), PreambleError> {
    write_own_preamble_with(conn, spec, &[]).await
}

/// [`write_own_preamble`] advertising `ext` (ADR 0073 Phase 2) in the
/// preamble's extension area. `ext` must fit [`handshake::MAX_EXTENSION_LEN`].
pub async fn write_own_preamble_with<S: AsyncWrite + Unpin>(
    conn: &mut S,
    spec: &handshake::ProtocolSpec,
    ext: &[u8],
) -> Result<(), PreambleError> {
    let mut preamble = handshake::Preamble::for_protocol(spec);
    preamble.extensions = ext.to_vec();
    let ours = handshake::encode(&preamble);
    conn.write_all(&ours).await.map_err(PreambleError::Io)?;
    conn.flush().await.map_err(PreambleError::Io)
}

/// **The one implementation of the per-connection preamble exchange** (ADR
/// 0073 Phase 0, workstream D): writes this build's own `spec` preamble
/// first — so a mismatched peer can name the mismatch too, even when it's
/// about to refuse this side — then reads and checks the peer's, the whole
/// exchange bounded by `timeout`. Generic over `S: AsyncRead + AsyncWrite +
/// Unpin`, so this same function serves both this module's own
/// [`MaybeTlsStream`]-typed [`NETWORK_PROTOCOL`](handshake::
/// NETWORK_PROTOCOL) exchange (via [`perform_handshake`], below) and
/// `animusd`'s client/intra [`CLIENT_PROTOCOL`](handshake::CLIENT_PROTOCOL)
/// one, on whatever stream type each caller already has in hand (a plain
/// `TcpStream`, or a [`MaybeTlsStream`]) — one codec, one exchange, two
/// independently-versioned protocols and two independently-counted metrics
/// layered on top by each caller's own thin wrapper (see
/// [`perform_handshake`]'s own doc for this crate's own wrapper; `animusd`'s
/// is the client/intra port's counterpart).
///
/// Symmetric and connection-shaped exactly as `handshake.rs`'s own module
/// doc describes: no separate "client" and "server" preamble shape, no
/// extra round trip to negotiate who writes first.
pub async fn exchange_preamble<S: AsyncRead + AsyncWrite + Unpin>(
    conn: &mut S,
    spec: &handshake::ProtocolSpec,
    timeout: Duration,
) -> Result<(), PreambleError> {
    exchange_preamble_with(conn, spec, timeout, &[], false)
        .await
        .map(|_| ())
}

/// [`exchange_preamble`] with the Phase 2 `ext` area (ADR 0073): advertises
/// `own_ext`, checks the peer with [`handshake::check_peer_ext`]
/// (`require_peer_ext` refuses an empty peer `ext`), and returns the peer's
/// preamble so the caller can keep its `ext`.
pub async fn exchange_preamble_with<S: AsyncRead + AsyncWrite + Unpin>(
    conn: &mut S,
    spec: &handshake::ProtocolSpec,
    timeout: Duration,
    own_ext: &[u8],
    require_peer_ext: bool,
) -> Result<handshake::Preamble, PreambleError> {
    match tokio::time::timeout(timeout, async {
        write_own_preamble_with(conn, spec, own_ext).await?;
        let peer = read_preamble(conn, spec).await?;
        handshake::check_peer_ext(spec, own_ext, &peer, require_peer_ext)
            .map_err(PreambleError::Refused)?;
        Ok(peer)
    })
    .await
    {
        Ok(result) => result,
        Err(_elapsed) => Err(PreambleError::TimedOut),
    }
}

/// Performs this build's half of the per-connection handshake (ADR 0073
/// Phase 0, workstream D — see `handshake.rs`'s own module doc for why a
/// per-connection preamble, not a per-message field, is the right shape)
/// on `conn`, run once right after TLS is established (or right after
/// connect/accept, for a plain connection), before a single frame is
/// written or read on it: **writes this build's own
/// [`handshake::NETWORK_PROTOCOL`] preamble first** — so a mismatched peer
/// can name the mismatch too, even when it's about to refuse this side —
/// then reads and checks the peer's, the whole exchange bounded by
/// [`HANDSHAKE_TIMEOUT`].
///
/// `role` is `"accept"` or `"dial"`, only for the log line; `peer_desc` is
/// the peer's address as this side knows it (the accepted socket's
/// `peer_addr`, or the dial target string).
///
/// **Every failure is logged at `warn`** — louder than `read_frames`'s own
/// `debug`-level "peer connection closed", deliberately: a handshake
/// refusal or timeout is worth an operator's attention in a way an
/// ordinary connection close is not.
///
/// **Counting decision**: only a genuine protocol refusal (bad magic,
/// unsupported version, or an oversized declared extension) increments
/// [`Metric::NetworkHandshakeRefused`]. A plain I/O failure (EOF, reset) or
/// a timeout is logged but **not** counted — both are what a merely slow
/// or already-dead peer looks like (already covered by this env's existing
/// dead-peer detection, `POOLED_SOCKET_DEAD_PEER_TIMEOUT`/`SEND_TIMEOUT`),
/// not evidence of a wire mismatch. Folding them into the same counter
/// would turn a targeted "a real peer spoke the wrong protocol" signal
/// into a vague "this connection had some problem" one.
///
/// **Log-volume decision (repeated mismatched dials)**: a persistently
/// mismatched peer re-dials (and re-fails this handshake) at whatever rate
/// its own caller sends at — `send_frame_pooled` caches nothing across a
/// failed connect, so there is no cached connection to reuse and no retry
/// loop *within* one call beyond the existing reconnect-once. This can mean
/// one `warn` per heartbeat interval for as long as the mismatch persists,
/// which this deliberately does **not** suppress: a real, persistent
/// version mismatch is not expected to occur at all at this phase (ADR
/// 0073 Phase 0 — no rolling upgrade exists yet, so it can only mean a
/// misconfigured/mismatched deploy), and an operator actively debugging one
/// wants every occurrence visible, not rate-limited away. The rate is
/// bounded by the caller's own send cadence, not unbounded — it cannot hot-
/// loop — and this mirrors `spawn_accept`'s own pre-existing "TLS handshake
/// failed" `warn` (below), which already logs on every failed attempt with
/// no suppression.
///
/// On any failure the connection is simply not returned — the caller closes
/// it (by dropping it) without ever entering the frame loop.
async fn perform_handshake(
    mut conn: MaybeTlsStream,
    role: &'static str,
    peer_desc: &str,
    metrics: &MetricsHandle,
    hs: &HandshakeCfg,
) -> std::io::Result<(MaybeTlsStream, Arc<[u8]>)> {
    let own_ext = hs.own_ext();
    match exchange_preamble_with(
        &mut conn,
        &handshake::NETWORK_PROTOCOL,
        HANDSHAKE_TIMEOUT,
        &own_ext,
        hs.require(),
    )
    .await
    {
        Ok(peer) => Ok((conn, Arc::from(peer.extensions))),
        Err(PreambleError::Io(err)) => {
            tracing::warn!(
                ?err,
                peer = %peer_desc,
                role,
                "network handshake failed (closing connection)"
            );
            Err(err)
        }
        Err(PreambleError::Refused(err)) => {
            tracing::warn!(
                ?err,
                peer = %peer_desc,
                role,
                "network handshake refused (closing connection)"
            );
            metrics.incr(Metric::NetworkHandshakeRefused);
            Err(std::io::Error::new(std::io::ErrorKind::InvalidData, err))
        }
        Err(PreambleError::TimedOut) => {
            tracing::warn!(
                peer = %peer_desc,
                role,
                timeout = ?HANDSHAKE_TIMEOUT,
                "network handshake timed out (closing connection)"
            );
            Err(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "network handshake timed out",
            ))
        }
    }
}

/// Spawn the accept loop for `listener` — one reader task per inbound connection,
/// each demuxing length-prefixed frames into a fresh inbox channel. Returns the
/// inbox receiver and the accept task's abort handle (for `shutdown`).
///
/// **A failed `accept()` never stops this loop.** `TcpListener::accept`'s
/// error cases (`EMFILE`/`ENFILE` when the process or system is at its
/// file-descriptor limit, `ECONNABORTED`/`ECONNRESET` from a peer that
/// disconnected mid-handshake, and similar per-connection conditions) are
/// ordinarily transient — the classic accept-loop hazard (well documented
/// for `accept(2)`-style servers) is treating any of them as fatal and
/// exiting, which silently and permanently deafens this node to every
/// future inbound connection despite the process staying alive and
/// otherwise healthy. That is exactly what starved a fresh 3-node
/// control-plane election in practice: a short burst of concurrent
/// DNS-resolution + connect attempts against not-yet-resolvable peer
/// hostnames during cluster bootstrap (every voter re-running pre-vote every
/// election timeout with no leader yet to quiet it) transiently pushed the
/// process to its file-descriptor ulimit, one `accept()` observed `EMFILE`,
/// the old code returned, and that node's Raft peers could never reach it
/// again — with only the eventual `TcpListener` teardown itself as the
/// (never-triggered) way out. This loop instead logs and backs off
/// ([`ACCEPT_ERROR_BACKOFF`]) on every error and keeps accepting; the only
/// way it stops is this env's own `AbortHandle` being aborted
/// ([`ProdEnv::shutdown`]/[`ProdEnv::shutdown_and_wait`]).
///
/// **TLS (ADR 0064):** when `tls` is `Some`, every accepted socket is first
/// run through [`TlsMaterial::acceptor`] (requiring and verifying the peer's
/// client certificate against the cluster CA) before any frame is read. A
/// failed handshake — a plain-TCP dial into a TLS listener, or a peer
/// presenting a cert from a different CA — is logged at `warn` with the
/// peer's address and the connection is simply dropped: exactly like a
/// failed accept, never a panic, and the listener keeps serving every other
/// (genuine) peer without interruption.
///
/// **Keepalive/`TCP_USER_TIMEOUT` (issue #924):** every accepted socket is
/// hardened via [`harden_pooled_socket`] before any frame is read or (for
/// TLS) any handshake is attempted — the accept-side twin of what
/// [`connect_nodelay`] already does on the dial side, so a peer that
/// silently vanishes is detected the same way regardless of which side of
/// the connection this node was on.
///
/// **Network handshake preamble (ADR 0073 Phase 0, workstream D):** once
/// TLS (if any) is established, every accepted socket runs
/// [`perform_handshake`] before a single frame is read — this build writes
/// its own [`handshake::NETWORK_PROTOCOL`] preamble, then reads and checks
/// the peer's, under [`HANDSHAKE_TIMEOUT`]. A bad magic (covering, in
/// particular, a pre-baseline peer whose first bytes are a raw,
/// unversioned frame — it was never going to send this preamble at all),
/// an unsupported version, an oversized declared extension, or an EOF/
/// timeout during the exchange are all logged at `warn` and simply drop
/// the connection without ever entering [`read_frames`] — never a panic,
/// and (like a failed TLS handshake, just above) the listener keeps
/// serving every other peer without interruption. See
/// [`perform_handshake`]'s own doc for exactly which of these count
/// [`Metric::NetworkHandshakeRefused`].
fn spawn_accept(
    listener: TcpListener,
    tls: Option<TlsMaterial>,
    metrics: MetricsHandle,
    hs: Arc<HandshakeCfg>,
) -> (mpsc::UnboundedReceiver<Envelope>, tokio::task::AbortHandle) {
    let (tx, rx) = mpsc::unbounded_channel();
    let accept = tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, peer_addr)) => {
                    harden_pooled_socket(&stream, &peer_addr.to_string());
                    let tx = tx.clone();
                    let tls = tls.clone();
                    let metrics = metrics.clone();
                    let hs = Arc::clone(&hs);
                    tokio::spawn(async move {
                        let stream = match tls {
                            None => MaybeTlsStream::Plain(stream),
                            Some(tls) => match tls.acceptor.accept(stream).await {
                                Ok(tls_stream) => {
                                    // Issue #1253: a certificate admitted only through
                                    // the peer-region CA bundle never speaks Raft.
                                    if tls.classify_peer(tls_stream.get_ref().1.peer_certificates())
                                        == crate::tls::PeerTrust::PeerRegionOnly
                                    {
                                        tracing::warn!(
                                            %peer_addr,
                                            "peer-region certificate on the internal Raft wire (dropping connection)"
                                        );
                                        return;
                                    }
                                    MaybeTlsStream::Tls(Box::new(tls_stream.into()))
                                }
                                Err(err) => {
                                    tracing::warn!(
                                        ?err,
                                        %peer_addr,
                                        "TLS handshake failed (dropping connection)"
                                    );
                                    return;
                                }
                            },
                        };
                        let (stream, peer_ext) = match perform_handshake(
                            stream,
                            "accept",
                            &peer_addr.to_string(),
                            &metrics,
                            &hs,
                        )
                        .await
                        {
                            Ok(ok) => ok,
                            Err(_err) => return, // already logged/counted by perform_handshake
                        };
                        if let Err(err) = read_frames(stream, tx, peer_ext, hs, metrics).await {
                            tracing::debug!(?err, "peer connection closed");
                        }
                    });
                }
                Err(err) => {
                    tracing::warn!(?err, "accept failed (retrying)");
                    tokio::time::sleep(ACCEPT_ERROR_BACKOFF).await;
                }
            }
        }
    });
    (rx, accept.abort_handle())
}

/// What a dial needs beyond the address: TLS material, the metrics sink and
/// the handshake `ext` policy.
#[derive(Clone, Copy)]
struct DialCtx<'a> {
    tls: Option<&'a TlsMaterial>,
    metrics: &'a MetricsHandle,
    hs: &'a HandshakeCfg,
}

/// Send one frame over the cached connection for `addr`, connecting (with
/// `TCP_NODELAY`) if there is none. `addr` is a `host:port` string — a
/// hostname (resolved via async DNS by `TcpStream::connect`'s own
/// `ToSocketAddrs` impl for `&str`) or a numeric address. Resolution only
/// happens on this connect path: a cached, already-live stream is reused
/// with no lookup at all. Holding the per-address lock across the whole
/// frame write is what keeps concurrent senders' frames from interleaving.
///
/// **Pool-lock scope (checked when the handshake preamble was added,
/// ADR 0073 Phase 0 workstream D):** `slot` is the per-*address*
/// `tokio::sync::Mutex` from `Inner.conns` — `send_stream` clones it out
/// from under `Inner.conns`'s outer `StdMutex` and drops that outer guard
/// *before* ever reaching this function (see `Network::send_stream`'s own
/// comment), so nothing here ever holds a lock shared with any other
/// destination address. Waiting out a handshake round trip while holding
/// `slot` therefore only serializes concurrent senders **to this one
/// peer** — which they already are, for frame-interleaving-safety reasons,
/// independent of the handshake — and never delays a send to any other
/// peer, and never risks a cross-peer deadlock. This is exactly the same
/// scope the TLS handshake (just below) has already been paying for since
/// ADR 0064, so adding the network handshake's own round trip inside
/// `connect_maybe_tls` needed no restructuring.
/// On a write error the cached stream is stale (e.g. the peer restarted
/// since the last send, or — for a hostname peer — moved to a new address
/// entirely) — drop it, reconnect **once** (re-resolving `addr` fresh, which
/// is exactly the cache invalidation a moved pod needs), resend the whole
/// frame (the receiver never saw a partial frame: the dead connection took
/// it), then surface the error if that also fails, matching the old
/// connect-per-message fire-and-forget semantics.
///
/// **TLS (ADR 0064):** when `tls` is `Some`, both the initial connect and
/// any reconnect run the outbound handshake through [`TlsMaterial::
/// connector`] (presenting this node's own cert, verifying the peer's
/// against the cluster CA) before the frame is written. A handshake failure
/// (e.g. the peer presents a cert from a different CA) surfaces as a plain
/// `io::Error` from `connect_maybe_tls` — handled by the exact same
/// reconnect-once-then-surface path a failed plain dial already used, no
/// special-casing needed here.
///
/// **Network handshake preamble (ADR 0073 Phase 0, workstream D):**
/// `connect_maybe_tls` also runs [`perform_handshake`] on both the initial
/// connect and any reconnect, right after TLS (if any) is established —
/// same treatment as the TLS handshake failure just above: a preamble
/// mismatch/timeout surfaces as a plain `io::Error`, already logged/
/// counted by `perform_handshake` itself, and this function's existing
/// reconnect-once-then-surface path (and its caller's "don't cache a
/// connection that never carried a frame" contract) handles it with no
/// special-casing — a connection that fails its handshake is simply never
/// stored in `slot`, matching a failed plain/TLS connect exactly.
async fn send_frame_pooled(
    slot: &Mutex<Option<MaybeTlsStream>>,
    addr: &str,
    from: &NodeId,
    msg_stream: u64,
    payload: &[u8],
    dial: DialCtx<'_>,
) -> std::io::Result<()> {
    let DialCtx { tls, metrics, hs } = dial;
    let mut guard = slot.lock().await;
    if guard.is_none() {
        *guard = Some(connect_maybe_tls(addr, tls, metrics, hs).await?);
    }
    let conn = guard.as_mut().expect("connection just ensured");
    if let Err(err) = write_frame(conn, from, msg_stream, payload).await {
        tracing::debug!(?err, %addr, "cached connection failed; reconnecting once");
        *guard = None; // drop the stale stream before dialing afresh
        let mut fresh = connect_maybe_tls(addr, tls, metrics, hs).await?;
        write_frame(&mut fresh, from, msg_stream, payload).await?;
        *guard = Some(fresh); // cache only a stream that just carried a frame
    }
    Ok(())
}

async fn connect_nodelay(addr: &str) -> std::io::Result<TcpStream> {
    // `TcpStream::connect` is generic over `ToSocketAddrs`, which tokio
    // implements for `&str` (a `host:port` string) with an internal async
    // DNS resolution — a numeric `"1.2.3.4:5"` string resolves trivially, a
    // hostname like `"my-pod.my-svc:5"` goes through a real lookup. Either
    // way this is the only place in the send path that ever resolves.
    let stream = TcpStream::connect(addr).await?;
    // Frames are small (heartbeats, votes) and latency-sensitive; never let
    // Nagle hold one back waiting to coalesce.
    stream.set_nodelay(true)?;
    // Issue #924: bound how long this pooled connection can survive its
    // peer vanishing silently (no FIN/RST — a recreated pod's collapsed old
    // endpoint) before a write/keepalive-probe failure surfaces and lets
    // `send_frame_pooled`'s reconnect-once path re-resolve `addr` fresh.
    harden_pooled_socket(&stream, addr);
    Ok(stream)
}

/// Dial `addr` (see [`connect_nodelay`]) and, when `tls` is configured, run
/// the outbound TLS handshake on top — presenting this node's own
/// certificate and verifying the peer's against the cluster CA (ADR 0064).
/// The `ServerName` the handshake verifies against is derived from `addr`
/// itself ([`server_name_for`]), so a peer's certificate SAN must cover
/// whatever string the peer book holds for it (see the `tls` module doc).
///
/// Once TLS (if any) is established, runs [`perform_handshake`] on the
/// resulting stream — this build's half of the network handshake preamble
/// (ADR 0073 Phase 0, workstream D) — before returning it, so every stream
/// this function hands back has already been checked in both directions.
async fn connect_maybe_tls(
    addr: &str,
    tls: Option<&TlsMaterial>,
    metrics: &MetricsHandle,
    hs: &HandshakeCfg,
) -> std::io::Result<MaybeTlsStream> {
    let stream = connect_nodelay(addr).await?;
    let stream = match tls {
        None => MaybeTlsStream::Plain(stream),
        Some(tls) => {
            let server_name = server_name_for(addr)?;
            let tls_stream = tls.connector.connect(server_name, stream).await?;
            MaybeTlsStream::Tls(Box::new(tls_stream.into()))
        }
    };
    // A dialed connection only ever carries this node's outbound frames (the
    // acceptor never writes frames back on it), so the acceptor's `ext` has
    // no frames to be stamped onto; it was still checked above.
    perform_handshake(stream, "dial", addr, metrics, hs)
        .await
        .map(|(stream, _peer_ext)| stream)
}

/// Write one length-prefixed `[from_len: u32][from: utf8 bytes][stream:
/// u64][len: u32][payload]` frame (ADR 0040 PR3 length-prefixed the `from`
/// field to carry a string id instead of a fixed `u64`; ADR 0026 added the
/// `stream` field) over a pooled connection — the receive side
/// (`read_frames`, which already loops until EOF) needs no further change.
async fn write_frame(
    conn: &mut MaybeTlsStream,
    from: &NodeId,
    msg_stream: u64,
    payload: &[u8],
) -> std::io::Result<()> {
    let from_bytes = from.as_str().as_bytes();
    conn.write_u32(from_bytes.len() as u32).await?;
    conn.write_all(from_bytes).await?;
    conn.write_u64(msg_stream).await?;
    conn.write_u32(payload.len() as u32).await?;
    conn.write_all(payload).await?;
    conn.flush().await?;
    Ok(())
}

/// This node's real filesystem `Disk` primitive — everything `impl Disk for
/// ProdEnv` used to do directly, factored out so it can be composed *below*
/// `EncryptedDisk` (ADR 0069) instead of being replaced by it. Deliberately
/// holds only what raw file I/O needs (`data_dir` + its own `dir_synced`
/// memo, in its own fresh `Arc` — **not** a reference back to `ProdEnv`'s
/// `Inner`), so wrapping it in `EncryptedDisk` and storing that inside
/// `Inner` creates no `Arc` reference cycle.
#[derive(Clone)]
struct RawFsDisk {
    data_dir: PathBuf,
    /// Files whose *directory entry* is already durable — see `Inner`'s old
    /// field doc (moved here verbatim); semantics unchanged.
    dir_synced: Arc<StdMutex<BTreeSet<String>>>,
}

impl RawFsDisk {
    fn path(&self, file: &str) -> PathBuf {
        self.data_dir.join(file)
    }

    /// `fsync` every directory from `file`'s parent up to (and including) the
    /// data dir, so a namespace change for `file` (creation, rename-over) is
    /// durable. A file name carrying a subdirectory prefix (`"db/wal"`) needs
    /// the whole chain synced: each intervening directory entry is a separate
    /// namespace record. Bounded by the (tiny) nesting depth.
    async fn sync_parents(&self, file: &str) -> std::io::Result<()> {
        let path = self.path(file);
        let mut dir = path.parent();
        while let Some(d) = dir {
            sync_dir(d).await?;
            if d == self.data_dir || !d.starts_with(&self.data_dir) {
                break;
            }
            dir = d.parent();
        }
        Ok(())
    }
}

/// This node's `Disk` implementation (ADR 0069): plain (byte-identical to
/// pre-ADR-0069 behavior, zero overhead) unless an `--encryption-key` was
/// configured at bind time. `ProdEnv`'s own `impl Disk` is a thin dispatch
/// over this enum — see `bind_with_tls_and_key`'s doc for the loud-refusal
/// check that runs before either variant is ever constructed.
enum DiskBackend {
    Plain(RawFsDisk),
    Encrypted(crate::EncryptedDisk<RawFsDisk, DiskSaltRng>),
}

/// A minimal [`Rng`] drawing real OS randomness, scoped specifically to
/// [`RawFsDisk`]'s per-file encryption salts (ADR 0069) — deliberately
/// **not** `ProdEnv` itself, which would create an `Arc` reference cycle
/// (`ProdEnv`'s `Inner` holding a `DiskBackend::Encrypted` that in turn
/// held a `ProdEnv` pointing back at the same `Inner` would never be freed).
/// Byte-for-byte the same source `ProdEnv`'s own [`Rng`] impl and
/// [`PreBindRng`] already draw from — this is not a *third* real-randomness
/// policy, just a differently-scoped handle onto the identical one.
#[derive(Debug, Default, Clone, Copy)]
struct DiskSaltRng;

#[allow(
    clippy::disallowed_types,
    reason = "OsRng is the sanctioned real-randomness source ProdEnv's own encrypted-disk salts draw from (ADR 0069); see ADR 0061 Decision 4"
)]
impl Rng for DiskSaltRng {
    fn next_u64(&self) -> u64 {
        rand::RngCore::next_u64(&mut rand::rngs::OsRng)
    }

    fn fill_bytes(&self, dst: &mut [u8]) {
        rand::RngCore::fill_bytes(&mut rand::rngs::OsRng, dst);
    }
}

#[async_trait::async_trait]
impl Disk for RawFsDisk {
    async fn append(&self, file: &str, bytes: &[u8]) -> std::io::Result<()> {
        let path = self.path(file);
        // Fast path: the data dir was created at `bind`, so don't pay a
        // `create_dir_all` per append. A file name carrying a not-yet-created
        // subdirectory prefix (e.g. `"db/wal"`) surfaces as `NotFound` —
        // create the parents and retry once.
        let mut f = match open_append(&path).await {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                ensure_parent(&path).await?;
                open_append(&path).await?
            }
            Err(e) => return Err(e),
        };
        f.write_all(bytes).await?;
        // Load-bearing: a `tokio::fs::File` buffers writes in user space and
        // submits them to the blocking pool *in the background*; dropping the
        // handle after `write_all` does NOT wait for that submission. Without
        // this `flush`, `append` can return before the bytes reach the kernel,
        // so (a) a subsequent `sync` — which opens a *different* handle — may
        // fsync a file that does not yet contain them (breaking "ack means
        // durable"), and (b) a subsequent `read`/`read_at` can see a truncated
        // file (observed as `corrupt sstable index` when the LSM read back an
        // SSTable it had just written and synced). `flush` completes the
        // in-flight write, restoring the sequential-consistency contract the
        // `Disk` seam promises (and `SimEnv` models).
        f.flush().await?;
        Ok(())
    }

    async fn sync(&self, file: &str) -> std::io::Result<()> {
        let f = tokio::fs::OpenOptions::new()
            .append(true)
            .open(self.path(file))
            .await?;
        f.sync_all().await?;
        // Also fsync the containing directory chain: if `append` *created* the
        // file, its directory entry is a namespace change that `sync_all` on
        // the file does not persist — without this a just-created WAL segment
        // can vanish on power loss even after `sync` returned. Doing it here
        // (not per-append) makes creation durable exactly when the caller
        // demands durability, at no per-append cost — and only on the *first*
        // `sync` of a file (creation is a one-time namespace change; the
        // `dir_synced` memo keeps the group-commit hot path at one fsync).
        let already = self
            .dir_synced
            .lock()
            .expect("dir_synced poisoned")
            .contains(file);
        if !already {
            self.sync_parents(file).await?;
            self.dir_synced
                .lock()
                .expect("dir_synced poisoned")
                .insert(file.to_string());
        }
        Ok(())
    }

    async fn read(&self, file: &str) -> std::io::Result<Vec<u8>> {
        match tokio::fs::read(self.path(file)).await {
            Ok(bytes) => Ok(bytes),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
            Err(e) => Err(e),
        }
    }

    async fn read_at(&self, file: &str, offset: u64, len: usize) -> std::io::Result<Vec<u8>> {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};
        let mut f = match tokio::fs::File::open(self.path(file)).await {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };
        if f.seek(std::io::SeekFrom::Start(offset)).await? != offset {
            return Ok(Vec::new());
        }
        let mut buf = vec![0u8; len];
        let mut filled = 0;
        while filled < len {
            let n = f.read(&mut buf[filled..]).await?;
            if n == 0 {
                break; // EOF
            }
            filled += n;
        }
        buf.truncate(filled);
        Ok(buf)
    }

    async fn size(&self, file: &str) -> std::io::Result<u64> {
        match tokio::fs::metadata(self.path(file)).await {
            Ok(m) => Ok(m.len()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(e) => Err(e),
        }
    }

    async fn remove(&self, file: &str) -> std::io::Result<()> {
        // Deliberately no directory fsync here: a remove that un-happens on
        // power loss just resurrects a file the owner already forgot (an
        // orphan), which startup/compaction cleanup handles — unlike a lost
        // *creation* or *rename*, it can't lose acknowledged data. Skipping
        // the dir fsync keeps deletes cheap. Do un-memoize the name: if the
        // file is re-created later, that is a fresh namespace change and its
        // next `sync` must fsync the directory again.
        self.dir_synced
            .lock()
            .expect("dir_synced poisoned")
            .remove(file);
        match tokio::fs::remove_file(self.path(file)).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    async fn replace(&self, file: &str, bytes: &[u8]) -> std::io::Result<()> {
        // Write a temp file, fsync it, then atomically rename over the target.
        // `replace` is rare (WAL compaction / manifest swap), so the up-front
        // `ensure_parent` cost is fine here, unlike on the `append` hot path.
        let target = self.path(file);
        let tmp = self.path(&format!("{file}.tmp"));
        ensure_parent(&target).await?;
        {
            let mut f = tokio::fs::File::create(&tmp).await?;
            f.write_all(bytes).await?;
            // Explicit flush before fsync: `tokio::fs::File` buffers writes
            // (see `append`); same-handle ops do serialize, but make the
            // "drain the buffer, then fsync" order explicit rather than
            // implied.
            f.flush().await?;
            f.sync_all().await?;
        }
        tokio::fs::rename(&tmp, &target).await?;
        // The rename is a namespace change: fsync the directory chain or the
        // completed swap can be lost on power loss (POSIX does not persist a
        // rename until the containing directory is synced).
        self.sync_parents(file).await?;
        // The chain is now durable for this name — a subsequent `sync` of the
        // same file need not re-fsync the directory.
        self.dir_synced
            .lock()
            .expect("dir_synced poisoned")
            .insert(file.to_string());
        Ok(())
    }

    // Issue #1116: `replace` split in two so the slow half (write + fsync of
    // the temp file) can run outside the caller's WAL lock. Same `{file}.tmp`
    // name and the same fsync discipline as `replace` above.
    async fn stage_replace(&self, file: &str, bytes: &[u8]) -> std::io::Result<()> {
        let target = self.path(file);
        let tmp = self.path(&format!("{file}.tmp"));
        ensure_parent(&target).await?;
        let mut f = tokio::fs::File::create(&tmp).await?;
        f.write_all(bytes).await?;
        f.flush().await?;
        f.sync_all().await
    }

    async fn stage_extend(&self, file: &str, bytes: &[u8]) -> std::io::Result<()> {
        if bytes.is_empty() {
            return Ok(());
        }
        let mut f = open_append(&self.path(&format!("{file}.tmp"))).await?;
        f.write_all(bytes).await?;
        f.flush().await?;
        f.sync_all().await
    }

    async fn commit_staged(&self, file: &str) -> std::io::Result<()> {
        let target = self.path(file);
        let tmp = self.path(&format!("{file}.tmp"));
        tokio::fs::rename(&tmp, &target).await?;
        // As in `replace`: the rename is not durable until the directory
        // chain is fsynced.
        self.sync_parents(file).await?;
        self.dir_synced
            .lock()
            .expect("dir_synced poisoned")
            .insert(file.to_string());
        Ok(())
    }

    async fn link(&self, src: &str, dst: &str) -> std::io::Result<()> {
        let src_path = self.path(src);
        let dst_path = self.path(dst);
        ensure_parent(&dst_path).await?;
        // Overwrite semantics (idempotent retry): remove any stale entry at
        // `dst` first — `std::fs::hard_link` itself errors `AlreadyExists`
        // rather than replacing. Best-effort: an absent `dst` (the common
        // case) or any other removal failure is not fatal here — the
        // following `hard_link` call is the one whose result matters.
        let _ = tokio::fs::remove_file(&dst_path).await;
        tokio::fs::hard_link(&src_path, &dst_path).await?;
        // The new directory entry is a namespace change: fsync the
        // containing directory chain or the link can be lost on power loss,
        // mirroring `replace`'s post-rename fsync.
        self.sync_parents(dst).await?;
        self.dir_synced
            .lock()
            .expect("dir_synced poisoned")
            .insert(dst.to_string());
        Ok(())
    }

    async fn list(&self) -> std::io::Result<Vec<String>> {
        // Non-recursive: a nested subdirectory is not this env's own top-level
        // disk contents. A data dir that does not exist yet reads as empty —
        // the env creates it lazily on first write.
        let mut dir = match tokio::fs::read_dir(&self.data_dir).await {
            Ok(dir) => dir,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };
        let mut names = Vec::new();
        while let Some(entry) = dir.next_entry().await? {
            if entry.file_type().await?.is_file()
                && let Ok(name) = entry.file_name().into_string()
            {
                names.push(name);
            }
        }
        names.sort_unstable();
        Ok(names)
    }
}

macro_rules! dispatch_disk {
    ($self:ident, $method:ident ( $($arg:expr),* )) => {
        match &$self.inner.disk {
            DiskBackend::Plain(d) => d.$method($($arg),*).await,
            DiskBackend::Encrypted(d) => d.$method($($arg),*).await,
        }
    };
}

/// `ProdEnv`'s own `Disk` impl is a thin dispatch over [`DiskBackend`] (ADR
/// 0069) — every method just routes to whichever variant `bind_with_tls_
/// and_key` constructed, so a node with no `--encryption-key` (the default)
/// runs the exact `RawFsDisk` code path this crate always has, unchanged.
#[async_trait::async_trait]
impl Disk for ProdEnv {
    async fn append(&self, file: &str, bytes: &[u8]) -> std::io::Result<()> {
        dispatch_disk!(self, append(file, bytes))
    }

    async fn sync(&self, file: &str) -> std::io::Result<()> {
        dispatch_disk!(self, sync(file))
    }

    async fn read(&self, file: &str) -> std::io::Result<Vec<u8>> {
        dispatch_disk!(self, read(file))
    }

    async fn read_at(&self, file: &str, offset: u64, len: usize) -> std::io::Result<Vec<u8>> {
        dispatch_disk!(self, read_at(file, offset, len))
    }

    async fn size(&self, file: &str) -> std::io::Result<u64> {
        dispatch_disk!(self, size(file))
    }

    async fn remove(&self, file: &str) -> std::io::Result<()> {
        dispatch_disk!(self, remove(file))
    }

    async fn replace(&self, file: &str, bytes: &[u8]) -> std::io::Result<()> {
        dispatch_disk!(self, replace(file, bytes))
    }

    async fn stage_replace(&self, file: &str, bytes: &[u8]) -> std::io::Result<()> {
        dispatch_disk!(self, stage_replace(file, bytes))
    }

    async fn stage_extend(&self, file: &str, bytes: &[u8]) -> std::io::Result<()> {
        dispatch_disk!(self, stage_extend(file, bytes))
    }

    async fn commit_staged(&self, file: &str) -> std::io::Result<()> {
        dispatch_disk!(self, commit_staged(file))
    }

    async fn discard_staged(&self, file: &str) -> std::io::Result<()> {
        dispatch_disk!(self, discard_staged(file))
    }

    async fn list(&self) -> std::io::Result<Vec<String>> {
        dispatch_disk!(self, list())
    }

    async fn link(&self, src: &str, dst: &str) -> std::io::Result<()> {
        dispatch_disk!(self, link(src, dst))
    }
}

/// A single-directory [`SegmentStore`](crate::SegmentStore) (ADR 0043 §A7):
/// every id is a file under `root`, with `/`-separated ids
/// (`{table}/{label}/{tablet}/{epoch}`, ADR 0043 §A3) mapped to
/// subdirectories, created on demand. `put` follows the same
/// temp-write + fsync + rename + directory-fsync discipline
/// [`ProdEnv`]'s own [`Disk::replace`] uses for its atomic swaps: write a
/// `.tmp` sibling, fsync it, rename over the target, then fsync the
/// directory chain — POSIX does not persist a rename until its containing
/// directory is fsynced, so skipping that step would let a completed `put`
/// vanish on power loss even though the file itself was synced.
///
/// This is the **opt-in** local store (`--segment-store=dir:...`, wired by a
/// later PR) for dev use or a shared mount, and doubles as
/// `ClusterSegmentStore`'s own per-node local building block (ADR 0043
/// §A7b) — the *default* store replicates across `K` nodes' own
/// `FsSegmentStore`-backed directories rather than trusting any single one.
///
/// Cheap to clone: the root path is the only state.
#[derive(Clone)]
pub struct FsSegmentStore {
    root: PathBuf,
}

impl FsSegmentStore {
    /// Root the store at `root`, without touching the filesystem yet — `put`
    /// creates `root` (and any id's subdirectories) on demand.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        FsSegmentStore { root: root.into() }
    }

    /// The root directory this store writes under.
    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Resolve `id` to a path under `root`, rejecting a path-traversal
    /// attempt (a `..` or `.` component) or an absolute id — every
    /// component of `id` must be a plain name, and `id` itself must be
    /// non-empty.
    fn resolve(&self, id: &str) -> std::io::Result<PathBuf> {
        if id.is_empty() {
            return Err(invalid_segment_id(id));
        }
        let rel = Path::new(id);
        if rel.is_absolute() {
            return Err(invalid_segment_id(id));
        }
        for comp in rel.components() {
            match comp {
                std::path::Component::Normal(_) => {}
                _ => return Err(invalid_segment_id(id)),
            }
        }
        Ok(self.root.join(rel))
    }

    /// `fsync` every directory from `path`'s parent up to (and including)
    /// `root` — the same chain-fsync discipline [`ProdEnv::sync_parents`]
    /// uses, rooted at this store's own directory instead of a node's data
    /// dir.
    async fn sync_parents(&self, path: &Path) -> std::io::Result<()> {
        let mut dir = path.parent();
        while let Some(d) = dir {
            sync_dir(d).await?;
            if d == self.root || !d.starts_with(&self.root) {
                break;
            }
            dir = d.parent();
        }
        Ok(())
    }
}

/// The rejected-id error [`FsSegmentStore::resolve`] returns for an empty,
/// absolute, or path-traversing segment id.
fn invalid_segment_id(id: &str) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::InvalidInput,
        format!(
            "invalid segment id {id:?}: must be a non-empty relative path with no \
             `..`/`.` component"
        ),
    )
}

/// [`SegmentStore::put`](crate::SegmentStore::put)'s write-once violation:
/// `id` already holds content that differs from what this call is trying to
/// write. See the trait's own doc for why this is a hard error rather than
/// the last-write-wins overwrite this store used to allow.
fn write_once_violation(id: &str) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::AlreadyExists,
        format!(
            "segment store write-once violation: {id:?} already holds different content \
             (every attempt must write its own unique id — see \
             animus_cp_data::segment::segment_object_id)"
        ),
    )
}

#[async_trait::async_trait]
impl crate::SegmentStore for FsSegmentStore {
    async fn put(&self, id: &str, bytes: &[u8]) -> std::io::Result<()> {
        let target = self.resolve(id)?;
        // Write-once (`SegmentStore::put`'s own amended contract): a
        // differing-content rewrite of an existing id is a hard error; an
        // identical-content rewrite is a safe no-op that skips the
        // temp-write/fsync/rename dance entirely.
        match tokio::fs::read(&target).await {
            Ok(existing) if existing == bytes => return Ok(()),
            Ok(_) => {
                return Err(write_once_violation(id));
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        // Safe to `expect` a file name: `resolve` rejects an empty id and
        // every non-`..`/`.` relative path has one.
        let mut tmp_name = target
            .file_name()
            .expect("resolve guarantees a file name")
            .to_os_string();
        tmp_name.push(".tmp");
        let tmp = target.with_file_name(tmp_name);

        ensure_parent(&target).await?;
        {
            let mut f = tokio::fs::File::create(&tmp).await?;
            f.write_all(bytes).await?;
            // Explicit flush before fsync, matching `ProdEnv::replace`: a
            // `tokio::fs::File` buffers writes and completes an in-flight
            // one on the blocking pool in the background on drop, so a bare
            // `sync_all` without a preceding flush can fsync before the
            // bytes actually land.
            f.flush().await?;
            f.sync_all().await?;
        }
        tokio::fs::rename(&tmp, &target).await?;
        // The rename is a namespace change: fsync the directory chain, or a
        // completed `put` can be lost on power loss even though the file
        // itself was synced above.
        self.sync_parents(&target).await?;
        Ok(())
    }

    async fn get(&self, id: &str) -> std::io::Result<Option<Vec<u8>>> {
        let path = self.resolve(id)?;
        match tokio::fs::read(&path).await {
            Ok(bytes) => Ok(Some(bytes)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn delete(&self, id: &str) -> std::io::Result<()> {
        let path = self.resolve(id)?;
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    async fn list(&self, prefix: &str) -> std::io::Result<Vec<String>> {
        // Recursive (unlike `Disk::list`, which is deliberately
        // non-recursive over a node's flat data dir): segment ids are
        // multi-component paths, so every level under `root` must be
        // walked. Debug/sweep-only, per the trait's own contract — no read
        // path depends on this.
        let mut out = Vec::new();
        let mut stack = vec![self.root.clone()];
        while let Some(dir) = stack.pop() {
            let mut rd = match tokio::fs::read_dir(&dir).await {
                Ok(rd) => rd,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e),
            };
            while let Some(entry) = rd.next_entry().await? {
                let file_type = entry.file_type().await?;
                let path = entry.path();
                if file_type.is_dir() {
                    stack.push(path);
                    continue;
                }
                if !file_type.is_file() {
                    continue;
                }
                let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                    continue;
                };
                if name.ends_with(".tmp") {
                    continue; // an in-flight or crash-orphaned `put` temp file
                }
                let Ok(rel) = path.strip_prefix(&self.root) else {
                    continue;
                };
                let id = rel
                    .components()
                    .filter_map(|c| c.as_os_str().to_str())
                    .collect::<Vec<_>>()
                    .join("/");
                if id.starts_with(prefix) {
                    out.push(id);
                }
            }
        }
        out.sort_unstable();
        Ok(out)
    }
}

/// Render a `catch_unwind` payload as a message, for the common cases
/// (`panic!("...")` / `panic!("{}", fmt)` yield `&str`/`String`) — anything
/// else (a payload built from a non-string `Any`, rare in practice) falls
/// back to a fixed placeholder rather than failing to report at all.
fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "<non-string panic payload>".to_string()
    }
}

impl Spawner for ProdEnv {
    /// Spawns `fut` on the tokio runtime, registering its `AbortHandle` for
    /// [`ProdEnv::shutdown`] exactly as before — **and**, since issue #939,
    /// counting a panic inside `fut` on this env before letting it continue
    /// to unwind.
    ///
    /// **Why**: a spawned task's `JoinHandle` was never kept (only its
    /// `AbortHandle`, needed for shutdown), so a panic inside a background
    /// apply/driver task — e.g. the issue #939 Run-6 panic,
    /// `animus_cp_data::apply_and_compact`'s split-fork seal-marker
    /// `.expect(..)` firing on a real `wal group-commit sync failed` under
    /// disk pressure — killed that task with nothing to observe it: the
    /// default tokio panic hook printed it to stderr and the task simply
    /// stopped running, silently, while the foreground test's own
    /// assertions (which never happened to touch that now-dead replica)
    /// kept passing. A process-global `std::panic::set_hook` was considered
    /// and rejected: tests run in parallel on shared worker threads, so a
    /// global hook cannot attribute a panic to the *test* (or even the
    /// *node*) whose task produced it — counting on the env the task was
    /// spawned from is the one attribution `ProdEnv` can make correctly.
    ///
    /// **Mechanism**: `fut` is wrapped in `futures::FutureExt::
    /// catch_unwind` (needs `AssertUnwindSafe` — a `BoxFuture` gives no
    /// static unwind-safety guarantee, and this wrapper doesn't rely on any:
    /// it never inspects `fut`'s state after a caught panic, only whether
    /// one happened). On `Err(payload)`: bump `task_panics`, remember the
    /// first message (`panic_message`), log it at `error`, then
    /// **`std::panic::resume_unwind(payload)`** — so the task's `JoinHandle`
    /// (on the rare caller that does keep one) still observes a genuine
    /// `JoinError::is_panic()`, and the default panic hook's own stderr
    /// print/backtrace behavior is completely unchanged. Nothing is
    /// swallowed; this only adds an observation point before the same
    /// unwind continues.
    ///
    /// **A cancelled (`abort()`ed) task does not go through this path at
    /// all**: `AbortHandle::abort` drops the task's future without ever
    /// resuming its poll, so `catch_unwind` (which only ever wraps a
    /// *poll*) never runs for it — a `Drop` during cancellation is not an
    /// unwind. So this must never count `ProdEnv::shutdown`'s routine
    /// task-abort as a panic, and doesn't (see
    /// `spawn_aborted_task_never_counts_as_a_panic`).
    fn spawn(&self, fut: crate::BoxFuture<'static, ()>) {
        self.spawn_counted(fut, false);
    }

    /// Issue #1220: as [`spawn`](Self::spawn), and a panic in `fut` also
    /// bumps `consensus_task_panics` / `Metric::ConsensusTaskPanics`.
    fn spawn_critical(&self, fut: crate::BoxFuture<'static, ()>) {
        self.spawn_counted(fut, true);
    }
}

impl ProdEnv {
    /// The body of [`Spawner::spawn`]/[`Spawner::spawn_critical`]; `critical`
    /// marks a consensus-loop task (issue #1220).
    fn spawn_counted(&self, fut: crate::BoxFuture<'static, ()>, critical: bool) {
        let inner = Arc::clone(&self.inner);
        // Created *outside* the async block and moved in, so it is owned by
        // the future's initial state: a task aborted before its first poll
        // drops the future without running any of its body, and a guard
        // created inside the body would never exist for it.
        let guard = CompletionGuard(Arc::clone(&inner));
        let counted = async move {
            // Dropped on every exit path of this future: normal completion,
            // the panic re-raise below, and abort (future dropped, polled or
            // not).
            let _guard = guard;
            let outcome = futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(fut)).await;
            if let Err(payload) = outcome {
                let msg = panic_message(payload.as_ref());
                // Store the message BEFORE bumping the counter: a poller
                // spinning on `spawned_task_panics()` only ever observes a
                // nonzero count after this store's effects are visible
                // (the mutex unlock below happens-before the following
                // `fetch_add`'s `SeqCst` store, which happens-before the
                // poller's own `SeqCst` load of it) — never a nonzero count
                // with `first_spawned_task_panic()` still `None`.
                {
                    let mut first = inner
                        .first_task_panic
                        .lock()
                        .expect("first_task_panic poisoned");
                    if first.is_none() {
                        *first = Some(msg.clone());
                    }
                }
                if critical {
                    inner.critical_task_panics.fetch_add(1, Ordering::SeqCst);
                    inner.metrics.incr(Metric::ConsensusTaskPanics);
                }
                inner.task_panics.fetch_add(1, Ordering::SeqCst);
                inner.metrics.incr(Metric::SpawnedTaskPanics);
                tracing::error!(panic = %msg, critical, "spawned task panicked (issue #939)");
                std::panic::resume_unwind(payload);
            }
        };
        // Register the handle so [`ProdEnv::shutdown`] can abort the task on
        // teardown (the Raft driver, the replica serve loop, etc.) — see
        // `Inner::tasks`'s own doc for why this vec must not simply grow
        // forever, which is exactly what it did before this pruning sweep
        // existed (the ProdEnv task-handle leak: ~94 MB of leaked `tokio::
        // runtime::task::core::Cell`s at a 254 MB RSS peak under heaptrack,
        // almost entirely `send_stream`'s one-task-per-outbound-frame
        // sends).
        // NOTE: `tokio::spawn` is called with the `tasks` lock NOT held — it
        // may drop `counted` inline, running `CompletionGuard::drop`, which
        // takes that lock (see `Inner::tasks`'s deadlock rule).
        let handle = tokio::spawn(counted);
        {
            let mut tasks = self.inner.tasks.lock().expect("tasks poisoned");
            tasks.push(handle.abort_handle());
            self.inner
                .metrics
                .set(Metric::SpawnedTaskHandlesTracked, tasks.len() as u64);
        }
        // The task may already have completed (its guard ran before the push
        // above, so it counted a completion for a handle not yet tracked);
        // re-check now that the handle is in the vec.
        maybe_sweep(&self.inner);
    }
}

/// Drop guard held by every task spawned through `Spawner::spawn`: on drop
/// (completion, panic unwind or abort) it records one more finished task and
/// gives `maybe_sweep` a chance to prune. See `Inner::tasks` for the design
/// and the deadlock rule this relies on.
struct CompletionGuard(Arc<Inner>);

impl Drop for CompletionGuard {
    fn drop(&mut self) {
        self.0.finished_unswept.fetch_add(1, Ordering::AcqRel);
        maybe_sweep(&self.0);
    }
}

/// Sweeps finished handles out of `Inner::tasks` when
/// `finished_unswept >= max(TASK_PRUNE_FLOOR, tasks.len() - finished_unswept)`
/// and refreshes the `SpawnedTaskHandlesTracked` gauge. Never panics (it runs
/// from `Drop`, possibly during unwinding): a poisoned lock is recovered.
fn maybe_sweep(inner: &Inner) {
    // Cheap pre-check: below the floor no sweep can be due, so skip the lock.
    if inner.finished_unswept.load(Ordering::Acquire) < TASK_PRUNE_FLOOR {
        return;
    }
    let mut tasks = inner.tasks.lock().unwrap_or_else(|e| e.into_inner());
    let finished = inner.finished_unswept.load(Ordering::Acquire);
    let live_estimate = tasks.len().saturating_sub(finished);
    if finished >= TASK_PRUNE_FLOOR.max(live_estimate) {
        // Only handles of finished tasks are dropped here, so this can never
        // run a task's future destructor under the lock.
        tasks.retain(|h| !h.is_finished());
        inner.finished_unswept.store(0, Ordering::Release);
    }
    inner
        .metrics
        .set(Metric::SpawnedTaskHandlesTracked, tasks.len() as u64);
}

impl Env for ProdEnv {
    fn node_id(&self) -> NodeId {
        self.inner.node_id.clone()
    }

    fn metrics(&self) -> MetricsHandle {
        self.inner.metrics.clone()
    }

    /// Delegates to the inherent [`ProdEnv::merge_peer`] — Rust's method
    /// resolution prefers an inherent impl over a trait impl, so this call
    /// reaches that method directly rather than recursing into this trait
    /// default.
    fn merge_peer(&self, id: NodeId, addr: String) {
        ProdEnv::merge_peer(self, id, addr);
    }

    /// Delegates to the inherent `refresh_inbox_metrics_inner` — mirrors
    /// `merge_peer`'s own delegation shape immediately above.
    fn refresh_inbox_metrics(&self) {
        self.refresh_inbox_metrics_inner();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Disk, SegmentStore};
    use rustls_pki_types::pem::PemObject;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// A unique temp directory for one test (no extra deps): the system temp dir
    /// plus pid + a process-local counter. Removed at the end of the test.
    fn unique_tmp_dir() -> PathBuf {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("animus-prodenv-{}-{n}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    /// `append` + `sync` + `read` of a file whose name carries a subdirectory
    /// prefix (`"sub/dir/file"`) round-trips: `ProdEnv` creates the intervening
    /// directories rather than silently failing on a missing parent.
    #[tokio::test]
    async fn disk_creates_parent_dirs_for_nested_file() {
        let dir = unique_tmp_dir();
        let (env, _addr) = ProdEnv::bind(nid(0), "127.0.0.1:0".parse().unwrap(), &dir)
            .await
            .expect("bind");

        let file = "sub/dir/file";
        let payload = b"durable-nested-bytes";
        env.append(file, payload).await.expect("append nested");
        env.sync(file).await.expect("sync nested");

        let got = env.read(file).await.expect("read nested");
        assert_eq!(got, payload, "nested append/sync/read must round-trip");

        // The nested directories really exist on disk.
        assert!(dir.join("sub/dir/file").exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `Disk::link` (ADR 0058 rung 2) is a real hard link: the linked file
    /// reads back the source's bytes, the two paths share one inode, and
    /// `remove`ing `src` leaves `dst` intact (the hard-link contract: the
    /// bytes live as long as any name references them). Also covers the
    /// overwrite-on-retry contract: linking again over an already-linked
    /// `dst` (as a crash-retried clone would) succeeds rather than erroring
    /// `AlreadyExists`, and linking a nonexistent source is a clean
    /// `NotFound`.
    #[tokio::test]
    async fn disk_link_is_a_real_hard_link() {
        use std::os::unix::fs::MetadataExt;

        let dir = unique_tmp_dir();
        let (env, _addr) = ProdEnv::bind(nid(0), "127.0.0.1:0".parse().unwrap(), &dir)
            .await
            .expect("bind");

        env.append("src", b"hello").await.expect("append src");
        env.sync("src").await.expect("sync src");
        env.link("src", "dst").await.expect("link");

        assert_eq!(env.read("dst").await.expect("read dst"), b"hello");
        let src_meta = std::fs::metadata(dir.join("src")).expect("src exists");
        let dst_meta = std::fs::metadata(dir.join("dst")).expect("dst exists");
        assert_eq!(
            src_meta.ino(),
            dst_meta.ino(),
            "link must share the source's inode, not copy its bytes"
        );
        assert!(src_meta.nlink() >= 2);

        // Idempotent-on-retry: relinking over an already-linked `dst` must
        // succeed (not `AlreadyExists`), reproducing the same state.
        env.link("src", "dst")
            .await
            .expect("relink over existing dst");
        assert_eq!(env.read("dst").await.expect("read dst again"), b"hello");

        // Removing the source leaves the link's own bytes intact — the
        // classic hard-link guarantee this primitive exists to exploit.
        env.remove("src").await.expect("remove src");
        assert_eq!(
            env.read("dst").await.expect("read dst after src removed"),
            b"hello",
            "dst must survive removal of src — that's the whole point of a hard link"
        );

        // Linking a nonexistent source is a clean NotFound, not a panic or a
        // silent no-op.
        let err = env
            .link("does-not-exist", "also-dst")
            .await
            .expect_err("linking a missing source must fail");
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `Disk::list` returns this env's own files, sorted, non-recursively — a
    /// nested subdirectory's files are not this env's own top-level disk
    /// contents — and reads a not-yet-created data dir as empty.
    #[tokio::test]
    async fn disk_list_is_own_files_sorted_nonrecursive() {
        let dir = unique_tmp_dir();
        let missing = ProdEnv::bind(
            nid(0),
            "127.0.0.1:0".parse().unwrap(),
            dir.join("never-written"),
        )
        .await
        .expect("bind")
        .0;
        assert_eq!(
            missing.list().await.expect("list missing"),
            Vec::<String>::new()
        );

        let (env, _addr) = ProdEnv::bind(nid(1), "127.0.0.1:0".parse().unwrap(), &dir)
            .await
            .expect("bind");
        env.append("db-wal", b"w").await.expect("append");
        env.append("db-MANIFEST", b"m").await.expect("append");
        env.append("nested/db-t2-wal", b"s").await.expect("append");

        let got = env.list().await.expect("list");
        assert_eq!(
            got,
            vec!["db-MANIFEST".to_string(), "db-wal".to_string()],
            "own files sorted; a nested subdirectory's files are not listed"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Multiplexed `(node, stream)` addressing (ADR 0026): two streams from one
    /// sender to one receiver, driven concurrently on a real multi-threaded
    /// `tokio` runtime, must never cross-talk — each stream's consumer sees
    /// exactly its own frames, regardless of how the underlying frames
    /// interleave on the wire. This is the `ProdEnv` counterpart to
    /// `animus-sim`'s `multiplexed_streams_are_isolated_and_deterministic`.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn prod_env_multiplexed_streams_do_not_cross_talk() {
        use crate::Network;

        const STREAM_X: u64 = 11;
        const STREAM_Y: u64 = 22;
        const N: u8 = 50;

        let dir_a = unique_tmp_dir();
        let dir_b = unique_tmp_dir();
        let loop0 = || "127.0.0.1:0".parse::<SocketAddr>().unwrap();
        let (a, a_addr) = ProdEnv::bind(nid(0), loop0(), &dir_a)
            .await
            .expect("bind a");
        let (b, _) = ProdEnv::bind(nid(1), loop0(), &dir_b)
            .await
            .expect("bind b");
        b.set_peers([(nid(0), a_addr.to_string())].into_iter().collect());

        // Two receive loops on `a`, one per stream, each collecting its frames'
        // first payload byte (the sequence number) into its own vector.
        let recv_x = {
            let a = a.clone();
            tokio::spawn(async move {
                let mut got = Vec::new();
                for _ in 0..N {
                    got.push(a.recv_stream(STREAM_X).await.payload[0]);
                }
                got
            })
        };
        let recv_y = {
            let a = a.clone();
            tokio::spawn(async move {
                let mut got = Vec::new();
                for _ in 0..N {
                    got.push(a.recv_stream(STREAM_Y).await.payload[0]);
                }
                got
            })
        };

        // Two concurrent senders on `b`, each hammering its own stream.
        let send_x = {
            let b = b.clone();
            tokio::spawn(async move {
                for i in 0..N {
                    b.send_stream(nid(0), STREAM_X, vec![i]).await;
                }
            })
        };
        let send_y = {
            let b = b.clone();
            tokio::spawn(async move {
                for i in 0..N {
                    b.send_stream(nid(0), STREAM_Y, vec![i]).await;
                }
            })
        };
        send_x.await.expect("send_x task");
        send_y.await.expect("send_y task");

        let mut got_x = tokio::time::timeout(Duration::from_secs(10), recv_x)
            .await
            .expect("stream X recv timed out")
            .expect("recv_x task");
        let mut got_y = tokio::time::timeout(Duration::from_secs(10), recv_y)
            .await
            .expect("stream Y recv timed out")
            .expect("recv_y task");
        got_x.sort_unstable();
        got_y.sort_unstable();

        let expected: Vec<u8> = (0..N).collect();
        assert_eq!(
            got_x, expected,
            "stream X must receive exactly its own N frames, no more, no less \
             (a dropped/duplicated/cross-talked frame would show up as a wrong \
             multiset here)"
        );
        assert_eq!(
            got_y, expected,
            "stream Y must receive exactly its own N frames — isolated from \
             stream X's concurrent traffic to the same (from, to) pair"
        );

        a.shutdown();
        b.shutdown();
        let _ = std::fs::remove_dir_all(&dir_a);
        let _ = std::fs::remove_dir_all(&dir_b);
    }

    /// Demux inbox observability (ADR 0026 inbox-growth investigation): send
    /// `N` frames to a stream nobody ever polls, and assert
    /// [`ProdEnv::inbox_stats`] reports exactly `N` frames / the expected
    /// total bytes for that stream (and that it has never been popped),
    /// then pop them one by one and assert both counters decrement back to
    /// zero and the last-pop time goes from `None` to `Some`. Real loopback
    /// sockets, two `ProdEnv` instances, mirroring
    /// `prod_env_multiplexed_streams_do_not_cross_talk`'s own bring-up.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn inbox_stats_reports_an_unread_streams_frames_and_bytes_then_drains_on_pop() {
        use crate::Network;

        const STREAM: u64 = 77;
        const N: usize = 25;
        const VALUE_LEN: usize = 40;

        let dir_a = unique_tmp_dir();
        let dir_b = unique_tmp_dir();
        let loop0 = || "127.0.0.1:0".parse::<SocketAddr>().unwrap();
        let (a, a_addr) = ProdEnv::bind(nid(0), loop0(), &dir_a)
            .await
            .expect("bind a");
        let (b, _) = ProdEnv::bind(nid(1), loop0(), &dir_b)
            .await
            .expect("bind b");
        b.set_peers([(nid(0), a_addr.to_string())].into_iter().collect());

        // Send N frames to `a` on `STREAM` without ever polling `recv_stream`
        // for it — the exact "nobody reads this stream" shape the pump
        // still queues forever.
        for i in 0..N {
            b.send_stream(nid(0), STREAM, vec![i as u8; VALUE_LEN])
                .await;
        }

        // The pump is a background task; poll (bounded) until it has filed
        // every frame rather than asserting on a fixed sleep.
        let deadline = Instant::now() + Duration::from_secs(10);
        let stats = loop {
            let stats = a.inbox_stats(10);
            if stats.total_frames >= N {
                break stats;
            }
            assert!(
                Instant::now() < deadline,
                "pump never filed all {N} frames for stream {STREAM}"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        };

        assert_eq!(stats.total_frames, N);
        assert_eq!(stats.total_bytes, N * VALUE_LEN);
        let s = stats
            .top_streams
            .iter()
            .find(|s| s.stream == STREAM)
            .expect("stream must appear in the top-N view");
        assert_eq!(s.frames, N);
        assert_eq!(s.bytes, N * VALUE_LEN);
        assert!(
            !s.ever_polled,
            "never polled — nobody has called recv_stream yet"
        );
        assert!(!s.waker_parked);
        assert_eq!(
            s.since_last_pop_ms, None,
            "never popped must report None, not a large elapsed value"
        );

        // Now pop every frame and confirm both counters drain back to zero.
        for _ in 0..N {
            let env = a.recv_stream(STREAM).await;
            assert_eq!(env.payload.len(), VALUE_LEN);
        }
        let after = a.inbox_stats(10);
        assert_eq!(after.total_frames, 0);
        assert_eq!(after.total_bytes, 0);
        let s_after = after
            .top_streams
            .iter()
            .find(|s| s.stream == STREAM)
            .expect("stream stays in the map even once drained (never pruned by this PR)");
        assert_eq!(s_after.frames, 0);
        assert_eq!(s_after.bytes, 0);
        assert!(s_after.ever_polled);
        assert!(
            s_after.since_last_pop_ms.is_some(),
            "popped at least once — must now report Some(elapsed), not None"
        );

        a.shutdown();
        b.shutdown();
        let _ = std::fs::remove_dir_all(&dir_a);
        let _ = std::fs::remove_dir_all(&dir_b);
    }

    /// ADR 0026's 2026-09-28 amendment: `close_stream` drops a stream's
    /// queued frames and marks it closed; a frame arriving afterward is
    /// discarded and counted (`Metric::DemuxFramesDroppedClosed`), never
    /// queued; a later `recv_stream` reopens it and delivery resumes. Real
    /// loopback sockets, two `ProdEnv` instances, mirroring this file's own
    /// `inbox_stats_reports_an_unread_streams_frames_and_bytes_then_drains_
    /// on_pop` bring-up. Converged-or-timeout polling throughout — no fixed
    /// sleeps for frame arrival (the pump is a background task).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn close_stream_drops_queued_frames_and_reopens_on_recv() {
        use crate::{Env, Metric, Network};

        const STREAM: u64 = 91;
        const N: usize = 10;
        const VALUE_LEN: usize = 16;

        let dir_a = unique_tmp_dir();
        let dir_b = unique_tmp_dir();
        let loop0 = || "127.0.0.1:0".parse::<SocketAddr>().unwrap();
        let (a, a_addr) = ProdEnv::bind(nid(0), loop0(), &dir_a)
            .await
            .expect("bind a");
        let (b, _) = ProdEnv::bind(nid(1), loop0(), &dir_b)
            .await
            .expect("bind b");
        b.set_peers([(nid(0), a_addr.to_string())].into_iter().collect());

        // Send N frames to `a` on STREAM and wait (converged-or-timeout, no
        // fixed sleep) until the pump has filed all of them, unread.
        for i in 0..N {
            b.send_stream(nid(0), STREAM, vec![i as u8; VALUE_LEN])
                .await;
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if a.inbox_stats(0).total_frames >= N {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "pump never filed all {N} frames for stream {STREAM}"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }

        // Close the stream: its queue must drop to zero immediately (no
        // consumer was ever polling it here, so the caller contract holds).
        a.close_stream(STREAM);
        let after_close = a.inbox_stats(0);
        assert_eq!(
            after_close.total_frames, 0,
            "close_stream must drop every already-queued frame"
        );
        assert_eq!(after_close.total_bytes, 0);

        // M more frames sent while closed must be dropped-and-counted, never
        // queued.
        const M: usize = 4;
        let before_dropped = a.metrics().get(Metric::DemuxFramesDroppedClosed);
        for i in 0..M {
            b.send_stream(nid(0), STREAM, vec![i as u8; VALUE_LEN])
                .await;
        }
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let dropped = a.metrics().get(Metric::DemuxFramesDroppedClosed) - before_dropped;
            if dropped >= M as u64 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "pump never counted all {M} frames dropped against the closed stream"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(
            a.inbox_stats(0).total_frames,
            0,
            "a frame arriving for a closed stream must never be queued"
        );

        // recv_stream reopens: a subsequent send must now be delivered and
        // received normally.
        //
        // The receiver is spawned and confirmed genuinely PARKED (via
        // `inbox_stats`'s `waker_parked`, which only goes true once
        // `RecvStream::poll`'s pending arm has actually run — after
        // `recv_stream`'s own reopen already cleared the closed mark)
        // before `b` ever sends, rather than sending first and calling
        // `recv_stream` right after: under real OS scheduling (as opposed
        // to this crate's own single-threaded `SimEnv`), the pump task
        // filing the frame and this task reaching its own next line race
        // each other, and a busy host can schedule the pump first — which
        // would see the stream still marked closed (recv_stream's future
        // not yet polled) and drop the very frame this test means to
        // receive, hanging the final `.await` forever. Parking first makes
        // the ordering deterministic regardless of scheduling.
        let a2 = a.clone();
        let recv_task = tokio::spawn(async move { a2.recv_stream(STREAM).await });
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let parked = a
                .inbox_stats(1)
                .top_streams
                .iter()
                .any(|s| s.stream == STREAM && s.waker_parked);
            if parked {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "recv_stream never parked on the reopened stream"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        b.send_stream(nid(0), STREAM, vec![0xAB; VALUE_LEN]).await;
        let env = recv_task.await.expect("recv task panicked");
        assert_eq!(env.payload, vec![0xAB; VALUE_LEN]);

        a.shutdown();
        b.shutdown();
        let _ = std::fs::remove_dir_all(&dir_a);
        let _ = std::fs::remove_dir_all(&dir_b);
    }

    /// ADR 0026's 2026-09-28 inbox-cap amendment: a per-stream byte/frame
    /// cap, drop-oldest on overflow — the fix for the "consumer never
    /// started polling at all" leak `close_stream` cannot reach (nothing
    /// ever calls it for a stream this node never locally hosted). Real
    /// loopback sockets, two `ProdEnv` instances, mirroring this file's own
    /// `close_stream_drops_queued_frames_and_reopens_on_recv` bring-up —
    /// converged-or-timeout polling throughout, no fixed sleeps.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn inbox_cap_drops_oldest_frames_past_the_cap_and_counts_them() {
        use crate::{Env, InboxCap, Metric, Network};

        const STREAM: u64 = 92;
        const VALUE_LEN: usize = 100;
        // A tiny cap so this test never has to send megabytes of real
        // payload — `set_inbox_cap` is exactly the seam this exists for.
        const CAP_FRAMES: usize = 5;
        const N: usize = 20; // well past CAP_FRAMES

        let dir_a = unique_tmp_dir();
        let dir_b = unique_tmp_dir();
        let loop0 = || "127.0.0.1:0".parse::<SocketAddr>().unwrap();
        let (a, a_addr) = ProdEnv::bind(nid(0), loop0(), &dir_a)
            .await
            .expect("bind a");
        let (b, _) = ProdEnv::bind(nid(1), loop0(), &dir_b)
            .await
            .expect("bind b");
        b.set_peers([(nid(0), a_addr.to_string())].into_iter().collect());
        a.set_inbox_cap(InboxCap {
            max_bytes: usize::MAX,
            max_frames: CAP_FRAMES,
        });
        assert_eq!(a.inbox_cap().max_frames, CAP_FRAMES);

        // Send N frames to `a` on STREAM without ever polling `recv_stream`
        // for it — the newest CAP_FRAMES must survive, the rest dropped.
        //
        // `send_stream` itself only *schedules* the real write onto its own
        // spawned task (issue #661 — a slow/unreachable peer must never
        // delay a different one queued behind it in the same caller loop),
        // so nothing here guarantees these N sends are written to the wire
        // in call order under a multi-threaded runtime. This test cares
        // about **delivery** order (which frame the pump actually files
        // first), so each iteration waits (converged-or-timeout) for this
        // exact frame to be accounted for — queued or already evicted —
        // before sending the next, making the whole sequence deterministic
        // regardless of the runtime's own scheduling.
        let before_overflow = a.metrics().get(Metric::DemuxFramesDroppedOverflow);
        for i in 0..N {
            b.send_stream(nid(0), STREAM, vec![i as u8; VALUE_LEN])
                .await;
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                let accounted = a.inbox_stats(0).total_frames
                    + (a.metrics().get(Metric::DemuxFramesDroppedOverflow) - before_overflow)
                        as usize;
                if accounted > i {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "pump never filed/accounted frame {i} for stream {STREAM}"
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }

        // Queued bytes/frames stay at exactly the cap — never above it, and
        // the drop-oldest counter matches exactly what overflowed (no
        // over-counting from the loop above having overshot).
        let stats = a.inbox_stats(10);
        assert_eq!(stats.total_frames, CAP_FRAMES);
        assert_eq!(stats.total_bytes, CAP_FRAMES * VALUE_LEN);
        assert_eq!(
            a.metrics().get(Metric::DemuxFramesDroppedOverflow) - before_overflow,
            (N - CAP_FRAMES) as u64
        );

        // Newest frames retained (drop-oldest): popping every surviving
        // frame must yield exactly the LAST CAP_FRAMES payloads sent, in
        // order, never any of the first N - CAP_FRAMES.
        for expected in (N - CAP_FRAMES)..N {
            let env = a.recv_stream(STREAM).await;
            assert_eq!(
                env.payload,
                vec![expected as u8; VALUE_LEN],
                "drop-oldest must retain the newest frames, in send order"
            );
        }
        assert_eq!(a.inbox_stats(0).total_frames, 0);

        // After close_stream: zero queued, and no growth on further sends
        // while it stays closed (the cap and the close/reopen mechanism
        // compose without surprises — closing wins, exactly like
        // `close_stream_drops_queued_frames_and_reopens_on_recv` proves for
        // the un-capped case). A small, serialized batch (mirroring that
        // test's own `M`, waiting for each frame's drop to be counted
        // before sending the next) rather than another concurrent burst of
        // `N` — under `cargo test --workspace`-level parallel socket load
        // a large fire-and-forget burst can individually miss
        // `SEND_TIMEOUT` and simply never arrive, which is a real
        // scheduling artifact this test has no need to court twice.
        a.close_stream(STREAM);
        const M: usize = 4;
        let before_closed = a.metrics().get(Metric::DemuxFramesDroppedClosed);
        for i in 0..M {
            b.send_stream(nid(0), STREAM, vec![i as u8; VALUE_LEN])
                .await;
            let deadline = Instant::now() + Duration::from_secs(10);
            loop {
                if a.metrics().get(Metric::DemuxFramesDroppedClosed) - before_closed > i as u64 {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "pump never counted frame {i} dropped against the closed stream"
                );
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        }
        assert_eq!(
            a.inbox_stats(0).total_frames,
            0,
            "no queued frames while closed, capped or not"
        );

        a.shutdown();
        b.shutdown();
        let _ = std::fs::remove_dir_all(&dir_a);
        let _ = std::fs::remove_dir_all(&dir_b);
    }

    /// Issue #661 (the S-07d kind-e2e control-plane outage root cause): a
    /// peer whose address is silently unreachable — no RST, no ICMP, packets
    /// just dropped, modelled here with the reserved/unrouted
    /// `10.255.255.1` (a real black hole in this sandbox: a bare
    /// `TcpStream::connect` to it does not fail within several seconds,
    /// confirmed by probing it directly before writing this test) — must
    /// never delay delivery to a *different*, live peer sequenced right
    /// after it in the same caller, the way a Raft driver's own
    /// outbound-dispatch loop sequences `env.send(..).await` once per peer
    /// in one task. Before the `send_stream` fix this regresses, both sends
    /// ran inline on the caller's own `.await`, so the black-holed send
    /// would have stalled the whole loop for the OS's own multi-minute TCP
    /// retry timeout before the live peer ever saw its frame.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_send_to_an_unreachable_peer_does_not_delay_a_live_peers_delivery() {
        use crate::Network;

        let dir_a = unique_tmp_dir();
        let dir_b = unique_tmp_dir();
        let (a, b, _b_addr) = bound_pair(&dir_a, &dir_b).await;
        // A reserved, unrouted address: connecting to it hangs rather than
        // failing fast (see this test's own doc) — exactly the "old pod IP,
        // now a collapsed endpoint" shape the incident hit.
        a.merge_peer(nid(2), "10.255.255.1:1".to_string());

        let before = Instant::now();
        // Mirrors `RaftNode`'s own driver loop: dispatch to the unreachable
        // peer first, then the live one, both sequentially `.await`ed in this
        // one task.
        a.send(nid(2), b"never-arrives".to_vec()).await;
        a.send(nid(1), b"still-prompt".to_vec()).await;
        let dispatch_elapsed = before.elapsed();
        assert!(
            dispatch_elapsed < Duration::from_secs(1),
            "both sends (to the black-holed peer, then the live one) must return \
             promptly — took {dispatch_elapsed:?}. A regression here means \
             `send_stream` is back to running its connect+write inline instead \
             of spawned (issue #661)."
        );

        let env = tokio::time::timeout(Duration::from_secs(5), b.recv())
            .await
            .expect(
                "live peer's frame must still arrive well within SEND_TIMEOUT, \
                     unblocked by the unreachable peer queued ahead of it",
            );
        assert_eq!(env.payload, b"still-prompt");

        a.shutdown();
        b.shutdown();
        let _ = std::fs::remove_dir_all(&dir_a);
        let _ = std::fs::remove_dir_all(&dir_b);
    }

    /// Bind two `ProdEnv`s on ephemeral loopback ports and point `sender` at
    /// `receiver` in the peer book. Returns `(sender, receiver, dirs)`.
    async fn bound_pair(dir_a: &PathBuf, dir_b: &PathBuf) -> (ProdEnv, ProdEnv, SocketAddr) {
        let loop0 = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
        let (a, _) = ProdEnv::bind(nid(0), loop0, dir_a).await.expect("bind a");
        let (b, b_addr) = ProdEnv::bind(nid(1), loop0, dir_b).await.expect("bind b");
        a.set_peers([(nid(1), b_addr.to_string())].into_iter().collect());
        (a, b, b_addr)
    }

    /// Build a self-describing payload for `(task, seq)`: an 8+8 byte header
    /// plus a variable-length filler whose every byte is derived from the
    /// header — so a torn/interleaved frame is detectable on receipt.
    fn framed_payload(task: u64, seq: u64) -> Vec<u8> {
        let fill_len = ((task * 131 + seq * 97) % 4096) as usize;
        let fill_byte = (task.wrapping_mul(31).wrapping_add(seq)) as u8;
        let mut p = Vec::with_capacity(16 + fill_len);
        p.extend_from_slice(&task.to_be_bytes());
        p.extend_from_slice(&seq.to_be_bytes());
        p.extend(std::iter::repeat_n(fill_byte, fill_len));
        p
    }

    /// Concurrent senders to one peer over the pooled per-address connection:
    /// every frame is delivered exactly once and *intact* (the per-peer lock
    /// must prevent two tasks' frames from interleaving mid-write), and the
    /// hammering must not deadlock — the whole test is timeout-guarded.
    /// `multi_thread` on purpose: a lock bug here can pass under a
    /// single-threaded runtime and only bite in production (see repo lore on
    /// determinism vs real-thread liveness).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_sends_to_one_peer_deliver_intact_frames() {
        use crate::Network;

        const TASKS: u64 = 8;
        const MSGS: u64 = 50;

        let dir_a = unique_tmp_dir();
        let dir_b = unique_tmp_dir();
        let (a, b, _) = bound_pair(&dir_a, &dir_b).await;

        let mut senders = Vec::new();
        for task in 0..TASKS {
            let a = a.clone();
            senders.push(tokio::spawn(async move {
                for seq in 0..MSGS {
                    a.send(nid(1), framed_payload(task, seq)).await;
                }
            }));
        }
        for s in senders {
            s.await.expect("sender task");
        }

        let mut seen = std::collections::BTreeSet::new();
        for _ in 0..TASKS * MSGS {
            let env = tokio::time::timeout(Duration::from_secs(30), b.recv())
                .await
                .expect("recv timed out — frames lost or transport deadlocked");
            assert_eq!(env.from, nid(0));
            assert!(env.payload.len() >= 16, "truncated frame");
            let task = u64::from_be_bytes(env.payload[0..8].try_into().unwrap());
            let seq = u64::from_be_bytes(env.payload[8..16].try_into().unwrap());
            // Frame integrity: the whole payload must match what (task, seq)
            // dictates — an interleaved write would corrupt length or filler.
            assert_eq!(
                env.payload,
                framed_payload(task, seq),
                "frame corrupted in flight (task {task}, seq {seq})"
            );
            assert!(
                seen.insert((task, seq)),
                "duplicate delivery of (task {task}, seq {seq})"
            );
        }
        assert_eq!(seen.len() as u64, TASKS * MSGS, "every frame delivered");

        a.shutdown();
        b.shutdown();
        let _ = std::fs::remove_dir_all(&dir_a);
        let _ = std::fs::remove_dir_all(&dir_b);
    }

    /// A cached connection outlives the peer: after the receiving env is torn
    /// down and a new one rebinds the same address, subsequent sends recover
    /// (the pooled sender drops the stale stream and reconnects). Sends are
    /// fire-and-forget, so the frame in flight when the stale stream dies may
    /// be lost — poll (send, short recv) until one lands, per repo lore for
    /// `ProdEnv` tests.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_reconnects_after_peer_restart() {
        use crate::Network;

        let dir_a = unique_tmp_dir();
        let dir_b = unique_tmp_dir();
        let (a, b, b_addr) = bound_pair(&dir_a, &dir_b).await;

        // Establish (and cache) the connection with one delivered frame.
        a.send(nid(1), b"before-restart".to_vec()).await;
        let env = tokio::time::timeout(Duration::from_secs(10), b.recv())
            .await
            .expect("first recv timed out");
        assert_eq!(env.payload, b"before-restart");

        // "Restart" the peer: abort its accept loop and drop the env so its
        // inbox closes, the per-connection reader exits, and the old socket
        // dies — then rebind the *same* address. The freed port can be
        // momentarily contested (port-TOCTOU lore), so retry the rebind.
        b.shutdown();
        drop(b);
        let deadline = Instant::now() + Duration::from_secs(10);
        let b2 = loop {
            match ProdEnv::bind(nid(1), b_addr, &dir_b).await {
                Ok((env, _)) => break env,
                Err(err) => {
                    assert!(
                        Instant::now() < deadline,
                        "could not rebind {b_addr} within budget: {err}"
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        };

        // Sends must recover onto the new listener. The first send after the
        // restart may vanish into the dead socket's buffer (fire-and-forget),
        // so poll until a frame arrives.
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            a.send(nid(1), b"after-restart".to_vec()).await;
            match tokio::time::timeout(Duration::from_millis(200), b2.recv()).await {
                Ok(env) => {
                    assert_eq!(env.from, nid(0));
                    assert_eq!(env.payload, b"after-restart");
                    break;
                }
                Err(_elapsed) => assert!(
                    Instant::now() < deadline,
                    "sends never recovered after peer restart"
                ),
            }
        }

        a.shutdown();
        b2.shutdown();
        let _ = std::fs::remove_dir_all(&dir_a);
        let _ = std::fs::remove_dir_all(&dir_b);
    }

    /// A narrow, `Drop`-cleaned `iptables OUTPUT -d {dest_ip} -j DROP` rule —
    /// the reproduction's stand-in for "this peer's entire network namespace
    /// vanished" (ADR 0060 S-07d's real incident).
    ///
    /// Three other ways to make a peer unresponsive were considered and
    /// rejected as unfaithful to what issue #924 actually needs proven
    /// (**a peer that never acknowledges anything again, with no FIN/RST
    /// ever sent**), all for the same underlying reason: on a single host/
    /// kernel, any technique that leaves the peer's own kernel alive still
    /// gets every send acknowledged (up to buffer capacity) and every
    /// keepalive probe answered — a live kernel always acks a keepalive
    /// probe regardless of whether the *application* ever reads, since the
    /// probe is deliberately crafted within the already-acknowledged
    /// window. Concretely:
    /// - Closing the peer's socket (`shutdown`/drop, what
    ///   `send_reconnects_after_peer_restart` already covers) always emits a
    ///   FIN or RST — that is the reconnect path the pre-#924 code
    ///   *already* handles; it does not exercise this bug at all.
    /// - Accepting a connection and simply never reading it (the socket
    ///   left open) only ever produces a legitimate flow-control stall
    ///   (zero window) — TCP still acks every byte it already buffered, and
    ///   keepalive probes still succeed, so this fix would make no
    ///   observable difference to that scenario (correctly so: a slow
    ///   reader is not a dead peer).
    ///  - Killing the peer's own OS process (even `SIGKILL`) does not avoid
    ///    a FIN/RST either: the *kernel* — shared with this process on a
    ///    single-host test — reclaims every fd on process exit and closes
    ///    the socket as part of that, unconditionally.
    ///
    /// An `iptables OUTPUT` `DROP` on the destination IP is the one
    /// technique that genuinely reproduces "no ack, ever" without any of
    /// those escape hatches: every packet this host's kernel would send to
    /// `dest_ip` is discarded before it ever leaves the machine, so nothing
    /// this sender transmits after the rule is installed is ever
    /// acknowledged — indistinguishable, from the sender's TCP stack's own
    /// point of view, from a pod whose entire network namespace (and every
    /// route to it) was torn down. `try_new` returns `None` (never panics)
    /// when this sandbox can't support it (no `iptables`, no root/
    /// `NET_ADMIN`) — see `HostsEntryGuard`'s own doc for why a
    /// sandbox-dependent real-network test degrades to "skipped" rather
    /// than failing the suite.
    struct IptablesDropGuard {
        dest_ip: String,
    }

    impl IptablesDropGuard {
        fn try_new(dest_ip: &str) -> Option<Self> {
            let status = std::process::Command::new("iptables")
                .args(["-I", "OUTPUT", "-d", dest_ip, "-j", "DROP"])
                .status()
                .ok()?;
            if status.success() {
                Some(Self {
                    dest_ip: dest_ip.to_string(),
                })
            } else {
                None
            }
        }
    }

    impl Drop for IptablesDropGuard {
        fn drop(&mut self) {
            let _ = std::process::Command::new("iptables")
                .args(["-D", "OUTPUT", "-d", &self.dest_ip, "-j", "DROP"])
                .status();
        }
    }

    /// A test-only `/etc/hosts` entry, removed by `Drop` even on panic —
    /// this crate's own copy of `animusd/tests/advertise_host.rs`'s
    /// `HostsEntryGuard` (per this crate's own doc on the test-PKI helper:
    /// a small test-only shape like this is duplicated rather than shared
    /// across a crate boundary). Real DNS (a Kubernetes headless `Service`)
    /// re-points a stable hostname to wherever its target actually is; a
    /// sandboxed test has no real DNS server to control, so this is the
    /// closest honest simulation available, and it exercises exactly the
    /// production assumption this crate's own `Inner::conns` doc states:
    /// resolution happens fresh on every dial, never cached, only the
    /// *connection* is. `try_new` returns `None` (never panics) if
    /// `/etc/hosts` isn't writable here (a non-root sandbox, a read-only
    /// mount) — the one test that needs this degrades to "skipped" rather
    /// than failing the whole suite on an environment it can't assume.
    struct HostsEntryGuard {
        hostname: &'static str,
        /// Held for this guard's whole lifetime: `/etc/hosts` is
        /// process-global, and every mutation below is a non-atomic
        /// truncate-then-write, so a concurrent in-process lookup (any
        /// `localhost:PORT` dial) can read the file empty/half-written and
        /// fail to resolve — the mechanism behind issue #1107. Tests that
        /// resolve a hostname take [`hosts_resolution_lock`] to exclude it.
        _exclusive: std::sync::RwLockWriteGuard<'static, ()>,
    }

    /// Process-wide lock ordering hostname-resolving tests against tests that
    /// rewrite `/etc/hosts` ([`HostsEntryGuard`], write side). Resolvers take
    /// the read side, so they never block each other. Poisoning is ignored
    /// (a panicking test must not cascade into unrelated ones).
    static HOSTS_FILE_LOCK: std::sync::RwLock<()> = std::sync::RwLock::new(());

    fn hosts_resolution_lock() -> std::sync::RwLockReadGuard<'static, ()> {
        HOSTS_FILE_LOCK
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    impl HostsEntryGuard {
        fn try_new(hostname: &'static str, ip: &str) -> Option<Self> {
            let exclusive = HOSTS_FILE_LOCK
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let mut contents = std::fs::read_to_string("/etc/hosts").ok()?;
            if !contents.ends_with('\n') {
                contents.push('\n');
            }
            contents.push_str(&format!("{ip} {hostname}\n"));
            std::fs::write("/etc/hosts", &contents).ok()?;
            Some(Self {
                hostname,
                _exclusive: exclusive,
            })
        }

        /// Re-point this entry to `ip`, simulating a DNS update (a
        /// recreated pod's stable name now resolving to its new address).
        fn repoint(&self, ip: &str) {
            let contents = std::fs::read_to_string("/etc/hosts").expect("read /etc/hosts");
            let marker = format!(" {}", self.hostname);
            let mut new_contents: String = contents
                .lines()
                .map(|line| {
                    if line.trim_end().ends_with(&marker) {
                        format!("{ip} {}", self.hostname)
                    } else {
                        line.to_string()
                    }
                })
                .collect::<Vec<_>>()
                .join("\n");
            new_contents.push('\n');
            std::fs::write("/etc/hosts", new_contents).expect("rewrite /etc/hosts");
        }
    }

    impl Drop for HostsEntryGuard {
        fn drop(&mut self) {
            if let Ok(contents) = std::fs::read_to_string("/etc/hosts") {
                let marker = format!(" {}", self.hostname);
                let mut cleaned: String = contents
                    .lines()
                    .filter(|line| !line.trim_end().ends_with(&marker))
                    .collect::<Vec<_>>()
                    .join("\n");
                if !cleaned.is_empty() {
                    cleaned.push('\n');
                }
                let _ = std::fs::write("/etc/hosts", cleaned);
            }
        }
    }

    /// Find a port number free on both `ip_a` and `ip_b` (distinct loopback
    /// addresses, e.g. `127.0.0.2`/`127.0.0.3`) at the moment of the check —
    /// bounded, best-effort retries; the tiny remaining TOCTOU window before
    /// the caller's own real bind is the same "port-TOCTOU lore" every
    /// rebind-retry loop in this test module already tolerates.
    fn pick_port_free_on_both(ip_a: &str, ip_b: &str) -> Option<u16> {
        for _ in 0..50 {
            let Ok(probe) = std::net::TcpListener::bind((ip_a, 0)) else {
                continue;
            };
            let port = probe.local_addr().expect("local addr").port();
            drop(probe);
            if std::net::TcpListener::bind((ip_b, port)).is_ok() {
                return Some(port);
            }
        }
        None
    }

    /// Issue #924: a pooled connection whose peer vanishes with **no
    /// FIN/RST at all** — a Kubernetes pod recreated at a new IP, its old
    /// network namespace torn down before (or racing) its final FIN — is
    /// never detected by the pre-#924 code. Every write into it still
    /// succeeds (the bytes just land in this host's own kernel send
    /// buffer; nothing about `write()` ever notices the peer is gone), so
    /// `send_frame_pooled`'s reconnect-once path (issue #661 — already
    /// proven above, in [`send_reconnects_after_peer_restart`], for a peer
    /// that *closes*, FIN/RST) never triggers, and the peer's address is
    /// never re-resolved: exactly "sends that never fail and never reach
    /// the new endpoint," the evidence this issue was opened with.
    ///
    /// **Construction** (see [`IptablesDropGuard`]'s own doc for the
    /// alternatives this rejects and why): the peer's *registered* address
    /// is one hostname string (`HOST:PORT`) for the whole test — matching
    /// production, where the pooled-connection cache is keyed by that
    /// registered string, not by whatever it resolves to (see `Inner::
    /// conns`'s own doc), so an address that visibly *changes* in
    /// `set_peers` would trivially "fix itself" via a plain cache miss and
    /// prove nothing about this fix. `HOST` resolves first to `peer_a`
    /// (`127.0.0.2:PORT`, via [`HostsEntryGuard`]); after the connection is
    /// established and cached, this test installs a narrow `iptables
    /// OUTPUT -d 127.0.0.2 DROP` ([`IptablesDropGuard`]) — genuinely
    /// black-holing every packet this sender ever sends there — and
    /// re-points `HOST` at `peer_b` (`127.0.0.3:PORT`, the *same* port,
    /// [`pick_port_free_on_both`]), simulating a recreated pod's new IP
    /// under its old stable DNS name.
    ///
    /// Proves both halves: (RED, still true pre-fix) nothing reaches
    /// `peer_b` within a window well under the fix's own detection bound —
    /// the stale connection to the now-silent `peer_a` is still what gets
    /// reused, i.e. no re-dial happened yet; (GREEN, the fix) something
    /// *does* reach `peer_b` well within a generous recovery budget — the
    /// vanished-peer detection fired and the reconnect-once path
    /// re-resolved `HOST` fresh. `peer_b.recv()` succeeding is the "count
    /// accepts on the new listener" proof this issue asks for: it can only
    /// happen after a real accept, TCP handshake, and one full frame
    /// round-trip through this crate's own wire format — strictly stronger
    /// evidence than a bare accept counter.
    ///
    /// Skips (never fails outright) if this sandbox can't support the
    /// construction: not Linux, `iptables` unusable, `/etc/hosts`
    /// unwritable, or `127.0.0.2`/`127.0.0.3` unbindable/no mutually-free
    /// port — see each guard's own doc for why that's the right failure
    /// mode for a real-network test whose infrastructure this crate cannot
    /// assume (mirrors `animusd/tests/advertise_host.rs`'s identical
    /// posture for the same reason).
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn send_reconnects_after_peer_vanishes_without_fin_or_rst() {
        use crate::Network;

        if !cfg!(target_os = "linux") {
            eprintln!("skipping: this reproduction needs Linux (iptables + TCP_USER_TIMEOUT)");
            return;
        }
        if std::net::TcpListener::bind("127.0.0.2:0").is_err()
            || std::net::TcpListener::bind("127.0.0.3:0").is_err()
        {
            eprintln!("skipping: 127.0.0.2/127.0.0.3 are not bindable in this sandbox");
            return;
        }
        const HOST: &str = "animus-issue924-vanished-peer.invalid";
        let Some(hosts_entry) = HostsEntryGuard::try_new(HOST, "127.0.0.2") else {
            eprintln!("skipping: /etc/hosts is not writable in this sandbox");
            return;
        };
        let Some(port) = pick_port_free_on_both("127.0.0.2", "127.0.0.3") else {
            eprintln!("skipping: no port free on both 127.0.0.2 and 127.0.0.3");
            return;
        };

        let dir_sender = unique_tmp_dir();
        let dir_peer_a = unique_tmp_dir();
        let dir_peer_b = unique_tmp_dir();

        let (peer_a, _) = ProdEnv::bind(
            nid(1),
            format!("127.0.0.2:{port}").parse().unwrap(),
            &dir_peer_a,
        )
        .await
        .expect("bind peer_a on 127.0.0.2");

        let loop0 = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
        let (sender, _) = ProdEnv::bind(nid(0), loop0, &dir_sender)
            .await
            .expect("bind sender");
        sender.set_peers([(nid(1), format!("{HOST}:{port}"))].into_iter().collect());

        // Establish (and cache) the connection with one delivered frame —
        // proves the peer really was reachable, exactly like the incident's
        // own "everything was healthy" starting state.
        sender.send(nid(1), b"before-vanish".to_vec()).await;
        let env = tokio::time::timeout(Duration::from_secs(10), peer_a.recv())
            .await
            .expect("first recv (via peer_a) timed out");
        assert_eq!(env.payload, b"before-vanish");

        // The peer vanishes: black-holed at the network layer (no socket
        // anywhere is ever closed) and its stable name re-pointed at the
        // recreated pod's new address.
        let Some(_drop_guard) = IptablesDropGuard::try_new("127.0.0.2") else {
            eprintln!("skipping: iptables is not usable in this sandbox (needs root/NET_ADMIN)");
            return;
        };
        hosts_entry.repoint("127.0.0.3");
        let (peer_b, _) = ProdEnv::bind(
            nid(1),
            format!("127.0.0.3:{port}").parse().unwrap(),
            &dir_peer_b,
        )
        .await
        .expect("bind peer_b on 127.0.0.3");

        // RED evidence: well under the fix's documented detection bound
        // (`POOLED_SOCKET_DEAD_PEER_TIMEOUT`), nothing must reach `peer_b`
        // — the sender is still blindly reusing its cached, now-silently-
        // dead connection to `peer_a`.
        let short_deadline = Instant::now() + Duration::from_secs(2);
        let mut reached_before_bound = false;
        while Instant::now() < short_deadline {
            sender.send(nid(1), b"during-vanish".to_vec()).await;
            if tokio::time::timeout(Duration::from_millis(100), peer_b.recv())
                .await
                .is_ok()
            {
                reached_before_bound = true;
                break;
            }
        }
        assert!(
            !reached_before_bound,
            "a send reached peer_b before the detection bound elapsed — this \
             reproduction is not exercising issue #924's silent-vanish path \
             at all (a real bug here would be a *different*, more suspicious \
             regression: an immediate, unconditional re-dial on every send)"
        );

        // GREEN evidence: within a generous recovery budget comfortably
        // above the documented bound, sends recover and reach `peer_b` —
        // the vanished-peer detection fired, `send_frame_pooled`'s
        // reconnect-once path re-resolved `HOST` (now 127.0.0.3), and the
        // *new* connection is what carries this frame.
        let recover_deadline = Instant::now() + Duration::from_secs(20);
        let started = Instant::now();
        loop {
            sender.send(nid(1), b"after-vanish".to_vec()).await;
            match tokio::time::timeout(Duration::from_millis(200), peer_b.recv()).await {
                Ok(env) => {
                    assert_eq!(env.from, nid(0));
                    assert_eq!(env.payload, b"after-vanish");
                    eprintln!(
                        "issue #924 repro: recovered {:?} after the silent \
                         vanish (documented bound: {:?})",
                        started.elapsed(),
                        POOLED_SOCKET_DEAD_PEER_TIMEOUT
                    );
                    break;
                }
                Err(_elapsed) => assert!(
                    Instant::now() < recover_deadline,
                    "sends never reached the recreated peer within the \
                     recovery budget — the silent vanish was never detected"
                ),
            }
        }

        sender.shutdown();
        peer_a.shutdown();
        peer_b.shutdown();
        let _ = std::fs::remove_dir_all(&dir_sender);
        let _ = std::fs::remove_dir_all(&dir_peer_a);
        let _ = std::fs::remove_dir_all(&dir_peer_b);
    }

    /// `merge_peer` adds a reachable entry without disturbing any other
    /// existing entry (ADR 0037) — the incremental dual of `set_peers`'s full
    /// replace. Binds three envs: `a` starts with a peer book containing only
    /// `b`, then `merge_peer`s in `c` — `a` must now be able to reach *both*
    /// `b` (untouched) and `c` (newly added), never just one.
    #[tokio::test]
    async fn merge_peer_adds_one_entry_without_disturbing_others() {
        use crate::Network;

        let dir_a = unique_tmp_dir();
        let dir_b = unique_tmp_dir();
        let dir_c = unique_tmp_dir();
        let (a, b, _b_addr) = bound_pair(&dir_a, &dir_b).await;
        let loop0 = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
        let (c, c_addr) = ProdEnv::bind(nid(2), loop0, &dir_c).await.expect("bind c");

        // Before merging, `a` has no route to `c` at all — a send is simply
        // dropped (see `Network::send`'s doc), not an error.
        a.send(nid(2), b"too-early".to_vec()).await;

        a.merge_peer(nid(2), c_addr.to_string());

        // The pre-existing entry for `b` still works...
        a.send(nid(1), b"still-reachable".to_vec()).await;
        let env = tokio::time::timeout(Duration::from_secs(10), b.recv())
            .await
            .expect("recv from b timed out");
        assert_eq!(env.payload, b"still-reachable");

        // ...and the newly merged entry for `c` now works too.
        a.send(nid(2), b"newly-reachable".to_vec()).await;
        let env = tokio::time::timeout(Duration::from_secs(10), c.recv())
            .await
            .expect("recv from c timed out");
        assert_eq!(env.payload, b"newly-reachable");

        a.shutdown();
        b.shutdown();
        c.shutdown();
        let _ = std::fs::remove_dir_all(&dir_a);
        let _ = std::fs::remove_dir_all(&dir_b);
        let _ = std::fs::remove_dir_all(&dir_c);
    }

    /// A peer registered by **hostname**, not a numeric address (the
    /// advertise/dial split's whole point — a Kubernetes pod advertises its
    /// stable DNS name), is genuinely reachable: `TcpStream::connect`'s own
    /// `ToSocketAddrs` impl for `&str` resolves it. `"localhost"` is a
    /// hostname every sandbox can resolve without a real DNS server, so this
    /// exercises the actual resolution path rather than a numeric string
    /// that merely happens to parse.
    #[tokio::test]
    #[allow(
        clippy::await_holding_lock,
        reason = "the std RwLock read guard deliberately spans the dial: it orders this test against /etc/hosts rewrites (#1107); the writers are sync std guards so an async lock could not exclude them"
    )]
    async fn send_delivers_to_a_peer_registered_by_hostname() {
        use crate::Network;

        // Issue #1107: `localhost` resolves through `/etc/hosts`, which the
        // hosts-rewriting tests mutate non-atomically; exclude them for the
        // whole send-and-receive so a torn read can't drop the (fire-and-
        // forget) frame. Bind first so the lock is held only across the dial.
        let dir_a = unique_tmp_dir();
        let dir_b = unique_tmp_dir();
        let loop0 = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
        let (a, _) = ProdEnv::bind(nid(0), loop0, &dir_a).await.expect("bind a");
        let (b, b_addr) = ProdEnv::bind(nid(1), loop0, &dir_b).await.expect("bind b");
        let _hosts = hosts_resolution_lock();

        // Register `b` by hostname:port rather than its numeric address.
        a.set_peers(
            [(nid(1), format!("localhost:{}", b_addr.port()))]
                .into_iter()
                .collect(),
        );

        a.send(nid(1), b"via-hostname".to_vec()).await;
        let env = tokio::time::timeout(Duration::from_secs(10), b.recv())
            .await
            .expect("recv via hostname-registered peer timed out");
        assert_eq!(env.from, nid(0));
        assert_eq!(env.payload, b"via-hostname");

        a.shutdown();
        b.shutdown();
        let _ = std::fs::remove_dir_all(&dir_a);
        let _ = std::fs::remove_dir_all(&dir_b);
    }

    /// Issue #1107 regression: a `/etc/hosts` mutation must exclude every
    /// hostname-resolving test for as long as it is in flight. Deterministic:
    /// while a [`HostsEntryGuard`] is alive the resolution lock cannot be
    /// acquired, and it can once the guard is dropped. (Before the fix there
    /// was no exclusion at all, so a resolver could observe the truncated
    /// file and the hostname send test flaked.) Skips like its siblings if
    /// `/etc/hosts` is not writable.
    #[test]
    fn hosts_mutation_excludes_hostname_resolution() {
        let Some(guard) = HostsEntryGuard::try_new("animus-lock-probe.invalid", "127.0.0.9") else {
            eprintln!("skipping: /etc/hosts not writable");
            return;
        };
        assert!(
            HOSTS_FILE_LOCK.try_read().is_err(),
            "a resolver could run while /etc/hosts was being rewritten"
        );
        drop(guard);
        assert!(
            HOSTS_FILE_LOCK.try_read().is_ok(),
            "lock must release when the hosts guard drops"
        );
    }

    /// Durability smoke test for the directory-fsync paths: `replace` (rename +
    /// dir fsync) and `append`+`sync` (creation + dir fsync) execute end-to-end
    /// and read back, including for a file in a nested (chain-synced)
    /// subdirectory. Power loss itself is untestable here; what this pins is
    /// that the fsync-the-parent code path runs and stays compatible with
    /// lazily-created directories.
    #[tokio::test]
    async fn replace_and_sync_fsync_dirs_and_read_back() {
        let dir = unique_tmp_dir();
        let (env, _addr) = ProdEnv::bind(nid(0), "127.0.0.1:0".parse().unwrap(), &dir)
            .await
            .expect("bind");

        // replace: create-by-rename, then overwrite-by-rename.
        env.replace("db-MANIFEST", b"v1").await.expect("replace v1");
        assert_eq!(env.read("db-MANIFEST").await.expect("read v1"), b"v1");
        env.replace("db-MANIFEST", b"v2-longer")
            .await
            .expect("replace v2");
        assert_eq!(
            env.read("db-MANIFEST").await.expect("read v2"),
            b"v2-longer"
        );

        // append + sync on a freshly-created nested file: the sync must fsync
        // the whole directory chain (each parent up to the data dir).
        env.append("nested/dir/db-wal", b"segment-bytes")
            .await
            .expect("append nested");
        env.sync("nested/dir/db-wal").await.expect("sync nested");
        assert_eq!(
            env.read("nested/dir/db-wal").await.expect("read nested"),
            b"segment-bytes"
        );

        // And replace into a nested dir (rename + chain fsync) works too.
        env.replace("nested/dir/db-MANIFEST", b"m1")
            .await
            .expect("replace nested");
        assert_eq!(
            env.read("nested/dir/db-MANIFEST").await.expect("read"),
            b"m1"
        );

        env.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Issue #1116: the split replace (`stage_replace` + `stage_extend` +
    /// `commit_staged`) leaves the target untouched until the swap, then
    /// yields `staged ++ extended` and consumes the staging file — over the
    /// plain disk, a nested path, and the encrypted-disk default impls.
    #[tokio::test]
    async fn staged_replace_swaps_atomically_and_leaves_the_target_until_commit() {
        let dir = unique_tmp_dir();
        let (env, _addr) = ProdEnv::bind(nid(0), "127.0.0.1:0".parse().unwrap(), &dir)
            .await
            .expect("bind");
        for file in ["wal", "nested/dir/wal"] {
            env.append(file, b"old").await.expect("append");
            env.sync(file).await.expect("sync");

            env.stage_replace(file, b"image").await.expect("stage");
            env.stage_extend(file, b"+tail1").await.expect("extend");
            env.stage_extend(file, b"").await.expect("empty extend");
            env.stage_extend(file, b"+tail2").await.expect("extend");
            assert_eq!(env.read(file).await.unwrap(), b"old", "target untouched");

            env.commit_staged(file).await.expect("commit");
            assert_eq!(env.read(file).await.unwrap(), b"image+tail1+tail2");
            assert!(
                env.read(&format!("{file}.tmp")).await.unwrap().is_empty(),
                "staging file consumed"
            );
            // Appends after the swap land on the new file.
            env.append(file, b"!").await.expect("append after");
            env.sync(file).await.expect("sync after");
            assert_eq!(env.read(file).await.unwrap(), b"image+tail1+tail2!");

            // A re-stage overwrites a leftover staged file; discard drops it.
            env.stage_replace(file, b"x").await.expect("restage");
            env.stage_replace(file, b"y").await.expect("restage over");
            env.discard_staged(file).await.expect("discard");
            assert!(env.read(&format!("{file}.tmp")).await.unwrap().is_empty());
        }
        env.shutdown();
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The shared cross-crate contract (`animus_env::test_support`) holds
    /// for `FsSegmentStore` over a real temp directory: put/get round-trip,
    /// idempotent overwrite, delete semantics, `list` filtering, and
    /// resurrect-after-delete.
    #[tokio::test]
    async fn fs_segment_store_satisfies_the_contract() {
        let dir = unique_tmp_dir();
        let store = FsSegmentStore::new(&dir);
        crate::test_support::assert_segment_store_contract(&store).await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `EncryptedSegmentStore<FsSegmentStore, DiskSaltRng>` over a real
    /// temp directory satisfies the identical shared contract (ADR 0069,
    /// S-03 PR 2) — the `SegmentStore` sibling of this file's own
    /// `FsSegmentStore` contract test above, and of `EncryptedDisk`'s own
    /// `Disk`-seam contract coverage.
    #[tokio::test]
    async fn encrypted_fs_segment_store_satisfies_the_contract() {
        let dir = unique_tmp_dir();
        let raw = FsSegmentStore::new(&dir);
        let key = crate::EncryptionKey::from_bytes([0x42; 32]);
        let store = crate::EncryptedSegmentStore::open(raw, DiskSaltRng, key)
            .await
            .expect("open a fresh store with a key");
        crate::test_support::assert_segment_store_contract(&store).await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The real filesystem, real-disk end-to-end proof of the three loud
    /// mismatch directions plus "the raw bytes on disk are not plaintext" —
    /// the `SegmentStore` counterpart of `EncryptedDisk`'s own `ProdEnv`
    /// mismatch coverage.
    #[tokio::test]
    async fn encrypted_fs_segment_store_marker_mismatch_and_ciphertext_on_disk() {
        let dir = unique_tmp_dir();
        let key_a = crate::EncryptionKey::from_bytes([0xAA; 32]);
        let key_b = crate::EncryptionKey::from_bytes([0xBB; 32]);

        // Fresh store, key A: initializes and writes real ciphertext to disk.
        {
            let raw = FsSegmentStore::new(&dir);
            let store = crate::EncryptedSegmentStore::open(raw, DiskSaltRng, key_a.clone())
                .await
                .expect("open with key A");
            store
                .put(
                    "t/label/1/0",
                    b"a plaintext value nobody should see on disk",
                )
                .await
                .expect("put");
        }
        let on_disk = tokio::fs::read(dir.join("t/label/1/0"))
            .await
            .expect("read raw file");
        assert!(
            !on_disk
                .windows(b"a plaintext value".len())
                .any(|w| w == b"a plaintext value"),
            "the plaintext value must never appear verbatim on disk"
        );

        // Reopening with the wrong key is refused.
        {
            let raw = FsSegmentStore::new(&dir);
            let err = crate::EncryptedSegmentStore::open(raw, DiskSaltRng, key_b)
                .await
                .map(|_| ())
                .expect_err("the wrong key must be refused");
            assert!(err.to_string().contains("does not match the key"));
        }

        // Reopening with no key at all is refused (encrypted store, no key).
        {
            let raw = FsSegmentStore::new(&dir);
            let err = crate::verify_or_init_segment_store_marker(&raw, &DiskSaltRng, None)
                .await
                .expect_err("no key against an encrypted store must be refused");
            assert!(err.to_string().contains("no --encryption-key was given"));
        }

        let _ = std::fs::remove_dir_all(&dir);

        // A key against a fresh, genuinely plaintext store (one real object,
        // no marker) is refused too.
        let dir2 = unique_tmp_dir();
        {
            let raw = FsSegmentStore::new(&dir2);
            raw.put("some/object", b"plaintext")
                .await
                .expect("put plaintext");
            let err = crate::EncryptedSegmentStore::open(raw, DiskSaltRng, key_a)
                .await
                .map(|_| ())
                .expect_err("a key against a plaintext store must be refused");
            assert!(err.to_string().contains("already holds unencrypted"));
        }
        let _ = std::fs::remove_dir_all(&dir2);
    }

    /// Ids map to nested subdirectories (the production shape,
    /// `{table}/{label}/{tablet}/{epoch}`), created on demand, and the bytes
    /// really land on disk at the expected nested path.
    #[tokio::test]
    async fn fs_segment_store_nested_id_creates_subdirectories() {
        let dir = unique_tmp_dir();
        let store = FsSegmentStore::new(&dir);
        let id = "orders/label-1/17/3";

        store
            .put(id, b"segment-bytes")
            .await
            .expect("put nested id");
        assert_eq!(
            store.get(id).await.expect("get nested id"),
            Some(b"segment-bytes".to_vec())
        );
        assert!(
            dir.join("orders/label-1/17/3").exists(),
            "put must create the intervening directories"
        );
        // No stray `.tmp` sibling left behind after a successful put.
        assert!(!dir.join("orders/label-1/17/3.tmp").exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Write-once (the ledger-named-object amendment): a second `put` to the
    /// same id with different bytes is a hard error and leaves the file on
    /// disk untouched; a second `put` with byte-identical content is a safe
    /// no-op (skips the temp-write/fsync/rename dance entirely).
    #[tokio::test]
    async fn fs_segment_store_put_is_write_once_except_for_identical_content() {
        let dir = unique_tmp_dir();
        let store = FsSegmentStore::new(&dir);
        let id = "orders/label-1/17/3";

        store.put(id, b"first").await.expect("first put");

        store
            .put(id, b"first")
            .await
            .expect("identical-content put must succeed");
        assert_eq!(store.get(id).await.expect("get"), Some(b"first".to_vec()));

        let err = store
            .put(id, b"second")
            .await
            .expect_err("a write-once violation must be rejected");
        drop(err);
        assert_eq!(
            store.get(id).await.expect("get"),
            Some(b"first".to_vec()),
            "a rejected write-once violation must not change the file on disk"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A path-traversal or absolute id is rejected outright, never resolved
    /// to a path outside `root`.
    #[tokio::test]
    async fn fs_segment_store_rejects_path_traversal_and_absolute_ids() {
        let dir = unique_tmp_dir();
        let store = FsSegmentStore::new(&dir);

        for bad_id in ["../escape", "table/../../escape", "/absolute/escape", ""] {
            assert!(
                store.put(bad_id, b"x").await.is_err(),
                "put must reject {bad_id:?}"
            );
            assert!(
                store.get(bad_id).await.is_err(),
                "get must reject {bad_id:?}"
            );
            assert!(
                store.delete(bad_id).await.is_err(),
                "delete must reject {bad_id:?}"
            );
        }
        // Nothing escaped the root: no file exists above/outside it.
        assert!(!dir.parent().unwrap().join("escape").exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `list` recurses through every nested level under `root`, filters by
    /// prefix, and never surfaces an in-flight/crash-orphaned `.tmp` sibling
    /// as if it were a real id.
    #[tokio::test]
    async fn fs_segment_store_list_recurses_and_filters_and_hides_tmp_files() {
        let dir = unique_tmp_dir();
        let store = FsSegmentStore::new(&dir);

        store.put("t/label/1/0", b"a").await.expect("put a");
        store.put("t/label/1/1", b"b").await.expect("put b");
        store.put("t/label/2/0", b"c").await.expect("put c");
        store.put("other/label/1/0", b"d").await.expect("put d");

        // A crash-orphaned temp file (as `put` would leave one mid-write) is
        // never surfaced by `list`.
        let orphan = dir.join("t/label/1/9.tmp");
        tokio::fs::write(&orphan, b"partial")
            .await
            .expect("write orphan tmp");

        let mut all = store.list("t/").await.expect("list t/");
        all.sort();
        assert_eq!(
            all,
            vec![
                "t/label/1/0".to_string(),
                "t/label/1/1".to_string(),
                "t/label/2/0".to_string(),
            ],
            "list must recurse every level, filter by prefix, and hide .tmp files"
        );

        let narrower = store.list("t/label/1").await.expect("list t/label/1");
        assert_eq!(
            narrower,
            vec!["t/label/1/0".to_string(), "t/label/1/1".to_string()]
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- TLS (ADR 0064, S-01 step 1) -------------------------------------
    //
    // Every test below drives a real self-signed CA + node certs through
    // `rcgen` (dev-only), writes them to a real temp dir, and exercises
    // `ProdEnv::bind_with_tls` end to end over real loopback sockets — the
    // same "real thread, real socket" shape as the plain-TCP tests above,
    // not a mock of rustls. The plain-TCP tests above are unmodified and
    // stay green: `ProdEnv::bind` (all of them) still takes the identical
    // path it always did (`bind_with_tls(..., None)`), so TLS existing at
    // all in this crate changes nothing for a caller that never asks for it.

    /// Generate a self-signed test CA plus one leaf certificate per entry in
    /// `names`, each leaf's Subject Alternative Name (and CN, for
    /// readability) set to that exact string — so a leaf minted for
    /// `"127.0.0.1"` satisfies [`server_name_for`]'s derivation for a
    /// loopback dial address on any port, matching the SAN requirement
    /// documented on the `tls` module. Returns the CA's own PEM plus one
    /// `(cert_pem, key_pem)` pair per name, in the same order as `names`.
    fn test_pki(names: &[&str]) -> (String, Vec<(String, String)>) {
        use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, KeyPair};

        let mut ca_params = CertificateParams::new(Vec::<String>::new()).expect("ca params");
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca_params
            .distinguished_name
            .push(DnType::CommonName, "animus-env test CA");
        let ca_key = KeyPair::generate().expect("generate ca key");
        let ca_cert = ca_params.self_signed(&ca_key).expect("self-sign ca");
        let ca_pem = ca_cert.pem();

        let leafs = names
            .iter()
            .map(|name| {
                let mut leaf_params =
                    CertificateParams::new(vec![(*name).to_string()]).expect("leaf params");
                leaf_params
                    .distinguished_name
                    .push(DnType::CommonName, *name);
                let leaf_key = KeyPair::generate().expect("generate leaf key");
                let leaf_cert = leaf_params
                    .signed_by(&leaf_key, &ca_cert, &ca_key)
                    .expect("sign leaf with ca");
                (leaf_cert.pem(), leaf_key.serialize_pem())
            })
            .collect();

        (ca_pem, leafs)
    }

    /// [`test_pki`], written to real PEM files under `dir` and wrapped as one
    /// [`TlsConfig`] per name — the file-path shape [`TlsConfig::load`]
    /// actually reads, so these tests exercise the real load path, not the
    /// in-memory PEM strings directly.
    fn write_test_pki(dir: &Path, names: &[&str]) -> (PathBuf, Vec<TlsConfig>) {
        let (ca_pem, leafs) = test_pki(names);
        let ca_path = dir.join("ca.pem");
        std::fs::write(&ca_path, &ca_pem).expect("write ca.pem");

        let configs = leafs
            .into_iter()
            .enumerate()
            .map(|(i, (cert_pem, key_pem))| {
                let cert_path = dir.join(format!("node{i}.cert.pem"));
                let key_path = dir.join(format!("node{i}.key.pem"));
                std::fs::write(&cert_path, cert_pem).expect("write cert pem");
                std::fs::write(&key_path, key_pem).expect("write key pem");
                TlsConfig {
                    cert_path,
                    key_path,
                    ca_path: Some(ca_path.clone()),
                    peer_ca_path: None,
                }
            })
            .collect();
        (ca_path, configs)
    }

    /// The TLS counterpart to [`bound_pair`]: two `ProdEnv`s, both trusting
    /// the same CA and each presenting a cert naming `"127.0.0.1"` (matching
    /// what [`server_name_for`] derives from a loopback dial address on any
    /// port), with `sender` pointed at `receiver` in the peer book.
    async fn bound_tls_pair(dir_a: &PathBuf, dir_b: &PathBuf) -> (ProdEnv, ProdEnv, SocketAddr) {
        let pki_dir = unique_tmp_dir();
        let (_ca_path, mut configs) = write_test_pki(&pki_dir, &["127.0.0.1", "127.0.0.1"]);
        let cfg_b = configs.pop().expect("node b tls config");
        let cfg_a = configs.pop().expect("node a tls config");

        let loop0 = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
        let (a, _) = ProdEnv::bind_with_tls(nid(0), loop0, dir_a, Some(cfg_a))
            .await
            .expect("bind a with tls");
        let (b, b_addr) = ProdEnv::bind_with_tls(nid(1), loop0, dir_b, Some(cfg_b))
            .await
            .expect("bind b with tls");
        a.set_peers([(nid(1), b_addr.to_string())].into_iter().collect());
        (a, b, b_addr)
    }

    /// A TLS-configured `bound_pair`: frames flow both ways over the mutual
    /// TLS handshake, exactly like the plain-TCP `bound_pair` tests above.
    #[tokio::test]
    async fn tls_bound_pair_frames_flow_both_ways() {
        use crate::Network;

        let dir_a = unique_tmp_dir();
        let dir_b = unique_tmp_dir();
        let (a, b, _b_addr) = bound_tls_pair(&dir_a, &dir_b).await;

        a.send(nid(1), b"hello-over-tls".to_vec()).await;
        let env = tokio::time::timeout(Duration::from_secs(10), b.recv())
            .await
            .expect("recv over tls timed out");
        assert_eq!(env.from, nid(0));
        assert_eq!(env.payload, b"hello-over-tls");

        a.shutdown();
        b.shutdown();
        let _ = std::fs::remove_dir_all(&dir_a);
        let _ = std::fs::remove_dir_all(&dir_b);
    }

    /// A peer presenting a certificate signed by a **different** CA is
    /// refused: the sender's own handshake fails (its `ClientConfig` trusts
    /// only its own CA), the receiver never sees the frame, and neither side
    /// panics — a rejected handshake is handled exactly like a failed dial.
    #[tokio::test]
    async fn tls_peer_from_different_ca_is_refused() {
        use crate::Network;

        let dir_a = unique_tmp_dir();
        let dir_b = unique_tmp_dir();
        let pki_dir_a = unique_tmp_dir();
        let pki_dir_b = unique_tmp_dir();

        // Two independent CAs — `a` and `b` each trust only their own.
        let (_ca_a, mut leafs_a) = write_test_pki(&pki_dir_a, &["127.0.0.1"]);
        let (_ca_b, mut leafs_b) = write_test_pki(&pki_dir_b, &["127.0.0.1"]);
        let cfg_a = leafs_a.pop().expect("a's tls config");
        let cfg_b = leafs_b.pop().expect("b's tls config");

        let loop0 = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
        let (a, _) = ProdEnv::bind_with_tls(nid(0), loop0, &dir_a, Some(cfg_a))
            .await
            .expect("bind a with tls");
        let (b, b_addr) = ProdEnv::bind_with_tls(nid(1), loop0, &dir_b, Some(cfg_b))
            .await
            .expect("bind b with tls");
        a.set_peers([(nid(1), b_addr.to_string())].into_iter().collect());

        a.send(nid(1), b"should-never-arrive".to_vec()).await;

        // `b` must never see this frame — a short bounded wait, not a
        // fixed-deadline race: any delivery at all within the window is a
        // failure of the CA-mismatch rejection.
        let never_arrived = tokio::time::timeout(Duration::from_millis(500), b.recv()).await;
        assert!(
            never_arrived.is_err(),
            "a frame from a different-CA peer must never be delivered"
        );

        a.shutdown();
        b.shutdown();
        let _ = std::fs::remove_dir_all(&dir_a);
        let _ = std::fs::remove_dir_all(&dir_b);
        let _ = std::fs::remove_dir_all(&pki_dir_a);
        let _ = std::fs::remove_dir_all(&pki_dir_b);
    }

    /// A plain-TCP dial into a TLS listener fails cleanly (no valid TLS
    /// handshake ever completes) and — the important part — the listener
    /// keeps right on serving genuine TLS peers afterward, exactly as
    /// `spawn_accept`'s "never stop on one failed connection" contract
    /// already guarantees for a failed `accept()` itself.
    #[tokio::test]
    async fn tls_listener_rejects_plain_dial_and_keeps_serving_tls_peers() {
        use crate::Network;

        let dir_a = unique_tmp_dir();
        let dir_b = unique_tmp_dir();
        let pki_dir = unique_tmp_dir();
        let (_ca_path, mut configs) = write_test_pki(&pki_dir, &["127.0.0.1", "127.0.0.1"]);
        let cfg_b = configs.pop().expect("node b tls config");
        let cfg_a = configs.pop().expect("node a tls config");

        let loop0 = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
        let (a, _) = ProdEnv::bind_with_tls(nid(0), loop0, &dir_a, Some(cfg_a))
            .await
            .expect("bind a with tls");
        let (b, b_addr) = ProdEnv::bind_with_tls(nid(1), loop0, &dir_b, Some(cfg_b))
            .await
            .expect("bind b with tls");

        // A raw, non-TLS dial: write a few plaintext bytes (not a TLS
        // ClientHello) and drop the connection. The listener's handshake
        // must fail and be logged/dropped, never panic or wedge the loop.
        {
            let mut plain = TcpStream::connect(b_addr).await.expect("plain dial");
            let _ = plain.write_all(b"not-a-tls-hello").await;
            drop(plain);
        }
        // Give the accept loop a moment to observe and drop the bad
        // connection before proving the listener still works.
        tokio::time::sleep(Duration::from_millis(100)).await;

        a.set_peers([(nid(1), b_addr.to_string())].into_iter().collect());
        a.send(nid(1), b"still-serving-tls".to_vec()).await;
        let env = tokio::time::timeout(Duration::from_secs(10), b.recv())
            .await
            .expect("recv after bad plain dial timed out");
        assert_eq!(env.payload, b"still-serving-tls");

        a.shutdown();
        b.shutdown();
        let _ = std::fs::remove_dir_all(&dir_a);
        let _ = std::fs::remove_dir_all(&dir_b);
        let _ = std::fs::remove_dir_all(&pki_dir);
    }

    /// The TLS counterpart to [`send_reconnects_after_peer_restart`]: after
    /// the receiving env is torn down and a new one rebinds the same address
    /// with the same TLS material, sends recover — the pooled sender drops
    /// the stale (now-broken) TLS stream and re-handshakes.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn tls_send_reconnects_after_peer_restart() {
        use crate::Network;

        let dir_a = unique_tmp_dir();
        let dir_b = unique_tmp_dir();
        let pki_dir = unique_tmp_dir();
        let (_ca_path, mut configs) = write_test_pki(&pki_dir, &["127.0.0.1", "127.0.0.1"]);
        let cfg_b = configs.pop().expect("node b tls config");
        let cfg_a = configs.pop().expect("node a tls config");

        let loop0 = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
        let (a, _) = ProdEnv::bind_with_tls(nid(0), loop0, &dir_a, Some(cfg_a))
            .await
            .expect("bind a with tls");
        let (b, b_addr) = ProdEnv::bind_with_tls(nid(1), loop0, &dir_b, Some(cfg_b.clone()))
            .await
            .expect("bind b with tls");
        a.set_peers([(nid(1), b_addr.to_string())].into_iter().collect());

        a.send(nid(1), b"before-restart".to_vec()).await;
        let env = tokio::time::timeout(Duration::from_secs(10), b.recv())
            .await
            .expect("first recv timed out");
        assert_eq!(env.payload, b"before-restart");

        b.shutdown();
        drop(b);
        let deadline = Instant::now() + Duration::from_secs(10);
        let b2 = loop {
            match ProdEnv::bind_with_tls(nid(1), b_addr, &dir_b, Some(cfg_b.clone())).await {
                Ok((env, _)) => break env,
                Err(err) => {
                    assert!(
                        Instant::now() < deadline,
                        "could not rebind {b_addr} within budget: {err}"
                    );
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        };

        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            a.send(nid(1), b"after-restart".to_vec()).await;
            match tokio::time::timeout(Duration::from_millis(200), b2.recv()).await {
                Ok(env) => {
                    assert_eq!(env.from, nid(0));
                    assert_eq!(env.payload, b"after-restart");
                    break;
                }
                Err(_elapsed) => assert!(
                    Instant::now() < deadline,
                    "sends never recovered after tls peer restart"
                ),
            }
        }

        a.shutdown();
        b2.shutdown();
        let _ = std::fs::remove_dir_all(&dir_a);
        let _ = std::fs::remove_dir_all(&dir_b);
        let _ = std::fs::remove_dir_all(&pki_dir);
    }

    /// Issue #913: `POST /admin/control/member/add`'s own dial address is
    /// now a hostname (`animus-operator`'s `desired::pod_fqdn(name, ns,
    /// ordinal):internal_port` — the same per-pod stable DNS name every
    /// other Kubernetes address surface in this codebase already uses),
    /// never a resolved `status.podIP`. This proves the actual payoff end
    /// to end: a real loopback TLS handshake dialed by exactly that
    /// hostname shape succeeds when the peer's certificate SAN covers it —
    /// through the same `TlsMaterial::acceptor`/`connector` and
    /// `server_name_for` derivation `spawn_accept`/`connect_maybe_tls` use
    /// in production, not a unit-level `server_name_for` parse check alone.
    /// Before this fix, `AddControlMemberReq.addr` being typed `SocketAddr`
    /// forced the caller to resolve and dial the pod's numeric IP instead,
    /// which a certificate carrying only DNS SANs (this one included) can
    /// never satisfy — `ServerName::IpAddress` against a cert with no IP
    /// SAN fails the handshake outright, exactly the `AlertReceived(
    /// BadCertificate)` this issue's own investigation traced back to this
    /// dial address.
    #[tokio::test]
    async fn tls_dial_by_member_add_style_pod_hostname_succeeds() {
        // The exact shape `desired::pod_fqdn` produces for ordinal 3 of a
        // cluster named "e2e" in namespace "animus-e2e" — literal, not a
        // wildcard: this PR's own fix is the address becoming a hostname
        // at all, independent of the separate wildcard-SAN fix that makes
        // the *certificate* stable across a scale-up.
        let pod_hostname = "e2e-3.e2e-internal.animus-e2e.svc.cluster.local";
        let pki_dir = unique_tmp_dir();
        let (_ca_path, mut configs) = write_test_pki(&pki_dir, &[pod_hostname, "127.0.0.1"]);
        let cfg_client = configs.pop().expect("client tls config");
        let cfg_server = configs.remove(0);
        let server_material = cfg_server.load().expect("load server tls material");
        let client_material = cfg_client.load().expect("load client tls material");

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let addr = listener.local_addr().expect("local addr");

        let accept_task = tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.expect("accept");
            server_material
                .acceptor
                .accept(stream)
                .await
                .expect("server-side handshake must succeed for the pod's own hostname SAN")
        });

        let stream = TcpStream::connect(addr).await.expect("connect");
        // `member/add`'s own `addr` field, dialed exactly as `add_control_
        // voter` constructs it (`{pod_fqdn}:{internal_port}`) and exactly
        // as `connect_maybe_tls` derives a `ServerName` from any dial
        // address.
        let server_name = server_name_for(&format!("{pod_hostname}:14000"))
            .expect("derive server name from the member-add-style hostname:port");
        client_material
            .connector
            .connect(server_name, stream)
            .await
            .expect(
                "client-side hostname verification must accept the pod's own \
                 member-add-style hostname",
            );

        accept_task.await.expect("accept task panicked");
        let _ = std::fs::remove_dir_all(&pki_dir);
    }

    /// [`TlsMaterial::server_acceptor`] (ADR 0064 commit 2) accepts a TLS
    /// client that presents **no** client certificate at all — unlike
    /// [`TlsMaterial::acceptor`] (mutual, exercised by every test above,
    /// which would refuse this same client). This is a raw loopback
    /// listener/dial, not a `ProdEnv` — `animusd`'s own client/dynamo/
    /// admin/console listeners are the real consumer of this acceptor
    /// (commit 2), but the acceptor itself is this crate's surface, so its
    /// server-only behavior is proven here directly.
    #[tokio::test]
    async fn server_only_acceptor_accepts_a_client_with_no_certificate() {
        let pki_dir = unique_tmp_dir();
        let (_ca_path, mut configs) = write_test_pki(&pki_dir, &["127.0.0.1"]);
        let cfg = configs.pop().expect("node tls config");
        let material = cfg.load().expect("load tls material");

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let addr = listener.local_addr().expect("local addr");

        let accept_task = tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.expect("accept");
            let mut tls_stream = material
                .server_acceptor
                .accept(stream)
                .await
                .expect("server-only handshake must succeed with no client cert");
            let mut buf = [0u8; 5];
            tokio::io::AsyncReadExt::read_exact(&mut tls_stream, &mut buf)
                .await
                .expect("read client hello payload");
            assert_eq!(&buf, b"hello");
        });

        // A bare rustls `ClientConfig` trusting the CA but presenting no
        // client certificate — exactly the shape a server-only-TLS client
        // (a DynamoDB caller, `animus-cli --tls-ca`) uses, deliberately
        // built independently of `TlsConfig::load()` (which always builds
        // a *mutual* `ClientConfig`) to prove the acceptor imposes no
        // client-cert requirement.
        let (ca_pem, _leafs) = {
            let ca_bytes = std::fs::read(&_ca_path).expect("read ca pem");
            (ca_bytes, ())
        };
        let mut root_store = rustls::RootCertStore::empty();
        for cert in rustls_pki_types::CertificateDer::pem_slice_iter(&ca_pem)
            .collect::<Result<Vec<_>, _>>()
            .expect("parse ca certs")
        {
            root_store.add(cert).expect("add ca cert");
        }
        let client_config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .expect("default protocol versions")
        .with_root_certificates(root_store)
        .with_no_client_auth();
        let connector = tokio_rustls::TlsConnector::from(Arc::new(client_config));

        let stream = TcpStream::connect(addr).await.expect("connect");
        let server_name = crate::tls::server_name_for(&addr.to_string()).expect("server name");
        let mut tls_stream = connector
            .connect(server_name, stream)
            .await
            .expect("client-side server-only handshake must succeed");
        tokio::io::AsyncWriteExt::write_all(&mut tls_stream, b"hello")
            .await
            .expect("write hello");
        tls_stream.flush().await.expect("flush");

        accept_task.await.expect("accept task panicked");
        let _ = std::fs::remove_dir_all(&pki_dir);
    }

    /// A wildcard SAN (`*.<label>.<label>...`, the shape `animus-operator`'s
    /// `desired::certificate::dns_names` issues since issue #913 to cover
    /// every pod ordinal of a headless `Service` without depending on node
    /// count) is honored by this crate's own real handshake path — proven
    /// against the exact hostname a Kubernetes headless `Service` gives a
    /// pod (`desired::pod_fqdn`'s own shape: `{cluster}-{ordinal}.
    /// {internal-svc}.{ns}.svc.cluster.local`), not merely asserted. This is
    /// deliberately a full loopback handshake through [`TlsMaterial::
    /// acceptor`]/[`TlsMaterial::connector`] (the identical code
    /// `spawn_accept`/`connect_maybe_tls` use), with the client's
    /// `ServerName` derived from [`server_name_for`] exactly the way a real
    /// peer-book dial derives it — the one detail a unit test against
    /// `rustls-webpki` directly could get subtly wrong by not exercising
    /// this crate's own derivation. issue #913's investigation needed this
    /// pinned decisively rather than assumed from RFC 6125 wildcard rules
    /// in the abstract.
    #[tokio::test]
    async fn tls_wildcard_san_matches_a_per_ordinal_pod_hostname() {
        // The server's leaf SAN is a single-label wildcard scoped to one
        // headless Service's own DNS zone — `desired::certificate::
        // dns_names`'s own shape (`*.<internal-svc>.<ns>.svc.cluster.local`)
        // — and the client dials a concrete per-ordinal hostname under that
        // same zone (`desired::pod_fqdn`'s own shape), never the wildcard
        // pattern itself.
        let pki_dir = unique_tmp_dir();
        let (_ca_path, mut configs) = write_test_pki(
            &pki_dir,
            &["*.e2e-internal.animus-e2e.svc.cluster.local", "127.0.0.1"],
        );
        let cfg_client = configs.pop().expect("client tls config");
        let cfg_server = configs.remove(0);
        let server_material = cfg_server.load().expect("load server tls material");
        let client_material = cfg_client.load().expect("load client tls material");

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let addr = listener.local_addr().expect("local addr");

        let accept_task = tokio::spawn(async move {
            let (stream, _peer) = listener.accept().await.expect("accept");
            server_material
                .acceptor
                .accept(stream)
                .await
                .expect("server-side handshake must succeed under a matching wildcard SAN")
        });

        let stream = TcpStream::connect(addr).await.expect("connect");
        // The exact hostname a peer book entry carries for pod ordinal 3
        // (`desired::pod_fqdn("e2e", "animus-e2e", 3)`) — one label deeper
        // than the wildcard's own `*.` position, and never itself in the
        // certificate's SAN list.
        let server_name = server_name_for("e2e-3.e2e-internal.animus-e2e.svc.cluster.local:14000")
            .expect("derive server name from the pod's own hostname");
        client_material
            .connector
            .connect(server_name, stream)
            .await
            .expect(
                "client-side hostname verification must accept the per-ordinal \
                 hostname against the wildcard SAN",
            );

        accept_task.await.expect("accept task panicked");
        let _ = std::fs::remove_dir_all(&pki_dir);
    }

    // -----------------------------------------------------------------
    // Encryption at rest (ADR 0069, S-03 PR 1) over a real filesystem.
    // -----------------------------------------------------------------

    fn test_key(byte: u8) -> crate::EncryptionKey {
        crate::EncryptionKey::from_bytes([byte; 32])
    }

    #[tokio::test]
    async fn encrypted_prod_env_round_trips_and_plaintext_never_hits_disk() {
        let dir = unique_tmp_dir();
        let (env, _addr) = ProdEnv::bind_with_tls_and_key(
            nid(0),
            "127.0.0.1:0".parse().unwrap(),
            &dir,
            None,
            Some(test_key(1)),
        )
        .await
        .expect("bind encrypted");

        env.append("db-wal", b"super-secret-payload")
            .await
            .expect("append");
        env.sync("db-wal").await.expect("sync");
        assert_eq!(
            env.read("db-wal").await.expect("read"),
            b"super-secret-payload"
        );

        // The raw bytes on disk must never contain the plaintext.
        let raw = std::fs::read(dir.join("db-wal")).expect("raw read");
        assert!(
            !raw.windows(b"super-secret".len())
                .any(|w| w == b"super-secret"),
            "plaintext leaked onto disk: {raw:?}"
        );
        // And the marker file is really there, on the real filesystem.
        assert!(dir.join(crate::MARKER_FILE).exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn encrypted_prod_env_wrong_key_on_reopen_is_a_loud_refusal() {
        let dir = unique_tmp_dir();
        {
            let (env, _addr) = ProdEnv::bind_with_tls_and_key(
                nid(0),
                "127.0.0.1:0".parse().unwrap(),
                &dir,
                None,
                Some(test_key(2)),
            )
            .await
            .expect("bind encrypted");
            env.append("db-wal", b"x").await.expect("append");
            env.sync("db-wal").await.expect("sync");
        }

        let err = match ProdEnv::bind_with_tls_and_key(
            nid(0),
            "127.0.0.1:0".parse().unwrap(),
            &dir,
            None,
            Some(test_key(3)),
        )
        .await
        {
            Ok(_) => panic!("wrong key must be refused"),
            Err(e) => e,
        };
        assert_eq!(
            err.to_string(),
            "--encryption-key does not match the key this data directory was encrypted with — \
             refusing to start. Use the original key, or point at a fresh data directory."
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn encrypted_prod_env_missing_key_on_reopen_is_a_loud_refusal() {
        let dir = unique_tmp_dir();
        {
            let (env, _addr) = ProdEnv::bind_with_tls_and_key(
                nid(0),
                "127.0.0.1:0".parse().unwrap(),
                &dir,
                None,
                Some(test_key(4)),
            )
            .await
            .expect("bind encrypted");
            env.append("db-wal", b"x").await.expect("append");
            env.sync("db-wal").await.expect("sync");
        }

        let err = match ProdEnv::bind(nid(0), "127.0.0.1:0".parse().unwrap(), &dir).await {
            Ok(_) => panic!("missing key against an encrypted directory must be refused"),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("no --encryption-key was given"),
            "unexpected message: {err}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn encrypted_prod_env_key_against_existing_plaintext_dir_is_a_loud_refusal() {
        let dir = unique_tmp_dir();
        {
            let (env, _addr) = ProdEnv::bind(nid(0), "127.0.0.1:0".parse().unwrap(), &dir)
                .await
                .expect("bind plaintext");
            env.append("db-wal", b"x").await.expect("append");
            env.sync("db-wal").await.expect("sync");
        }

        let err = match ProdEnv::bind_with_tls_and_key(
            nid(0),
            "127.0.0.1:0".parse().unwrap(),
            &dir,
            None,
            Some(test_key(5)),
        )
        .await
        {
            Ok(_) => panic!("key against an existing plaintext directory must be refused"),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("already holds unencrypted files"),
            "unexpected message: {err}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// No key at all is byte-identical to pre-ADR-0069 `ProdEnv`: no marker
    /// file is ever written, and nothing about the on-disk bytes changes.
    #[tokio::test]
    async fn no_key_writes_no_marker_and_stays_byte_identical() {
        let dir = unique_tmp_dir();
        let (env, _addr) = ProdEnv::bind(nid(0), "127.0.0.1:0".parse().unwrap(), &dir)
            .await
            .expect("bind");
        env.append("f", b"plain-bytes").await.expect("append");
        env.sync("f").await.expect("sync");

        let raw = std::fs::read(dir.join("f")).expect("raw read");
        assert_eq!(
            raw, b"plain-bytes",
            "plaintext path must write bytes verbatim"
        );
        assert!(!dir.join(crate::MARKER_FILE).exists());

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Issue #939's red-before/green-after proof, half A: a task spawned
    /// through `env.spawn_task` that panics is counted on the env — the
    /// exact observation the Run-6 apply-task panic needed and didn't have.
    /// `spawned_task_panics()` is polled (bounded) rather than joined
    /// directly: `Spawner::spawn` deliberately keeps no `JoinHandle` (only
    /// an `AbortHandle`, see that impl's own doc), so the only way a test
    /// can know the spawned task actually finished is to observe its
    /// side effect.
    #[tokio::test(flavor = "multi_thread")]
    async fn spawned_task_panic_is_counted_and_the_message_is_captured() {
        use crate::EnvExt;

        let dir = unique_tmp_dir();
        let (env, _addr) = ProdEnv::bind(nid(0), "127.0.0.1:0".parse().unwrap(), &dir)
            .await
            .expect("bind");

        env.spawn_task(async {
            panic!("issue-939 injected panic");
        });

        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while env.spawned_task_panics() == 0 {
            assert!(
                std::time::Instant::now() < deadline,
                "spawned task panic was never counted"
            );
            tokio::task::yield_now().await;
        }

        assert_eq!(env.spawned_task_panics(), 1);
        assert_eq!(
            env.first_spawned_task_panic().as_deref(),
            Some("issue-939 injected panic")
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Issue #1220: only a `spawn_critical` (consensus-loop) task's panic
    /// bumps `consensus_task_panics` / `Metric::ConsensusTaskPanics`; both
    /// kinds bump `spawned_task_panics` / `Metric::SpawnedTaskPanics`.
    #[tokio::test(flavor = "multi_thread")]
    async fn critical_task_panic_is_counted_separately_and_exported() {
        use crate::EnvExt;

        let dir = unique_tmp_dir();
        let (env, _addr) = ProdEnv::bind(nid(0), "127.0.0.1:0".parse().unwrap(), &dir)
            .await
            .expect("bind");
        let wait_for = |want: u64| {
            let env = env.clone();
            async move {
                let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
                while env.spawned_task_panics() < want {
                    assert!(std::time::Instant::now() < deadline, "panic not counted");
                    tokio::task::yield_now().await;
                }
            }
        };

        env.spawn_task(async { panic!("issue-1220 ordinary") });
        wait_for(1).await;
        assert_eq!(env.consensus_task_panics(), 0);
        assert_eq!(env.metrics().get(Metric::SpawnedTaskPanics), 1);
        assert_eq!(env.metrics().get(Metric::ConsensusTaskPanics), 0);

        env.spawn_critical_task(async { panic!("issue-1220 critical") });
        wait_for(2).await;
        assert_eq!(env.consensus_task_panics(), 1);
        assert_eq!(env.metrics().get(Metric::SpawnedTaskPanics), 2);
        assert_eq!(env.metrics().get(Metric::ConsensusTaskPanics), 1);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Issue #939's red-before/green-after proof, half B: an aborted
    /// (cancelled), never-panicking task must NOT count as a panic —
    /// `AbortHandle::abort` drops the task's future without resuming its
    /// poll, so `catch_unwind` (which only ever wraps a poll) never runs
    /// for it. This is exactly the routine-shutdown path (`ProdEnv::
    /// shutdown`/`shutdown_and_wait`, and every simulated-crash test that
    /// calls them) — it must stay silent, or every ordinary kill-node test
    /// would start failing this new check.
    #[tokio::test(flavor = "multi_thread")]
    async fn spawn_aborted_task_never_counts_as_a_panic() {
        use crate::EnvExt;

        let dir = unique_tmp_dir();
        let (env, _addr) = ProdEnv::bind(nid(0), "127.0.0.1:0".parse().unwrap(), &dir)
            .await
            .expect("bind");

        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        env.spawn_task(async move {
            let _ = started_tx.send(());
            std::future::pending::<()>().await;
        });
        started_rx
            .await
            .expect("task must start before it's aborted");

        // Aborts every task this env owns, including the one above — the
        // same path `ProdEnv::shutdown` uses.
        env.shutdown();

        // Give the runtime a few yields to actually drop the cancelled
        // task's future (abort() only requests cancellation, see
        // `ProdEnv::shutdown`'s own doc) before asserting the negative.
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            env.spawned_task_panics(),
            0,
            "an aborted task must never be counted as a panic"
        );
        assert_eq!(env.first_spawned_task_panic(), None);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Slack over `TASK_PRUNE_FLOOR` the completion-driven sweep may leave
    /// tracked at quiescence: a handful of tasks whose `CompletionGuard` ran
    /// (so they were counted, then reset by the sweep's `store(0)`) but which
    /// were not yet `is_finished()` when the sweep's `retain` looked — bounded
    /// by the worker count — plus the accept loop's and demux pump's own two
    /// permanent handles. See `Inner::finished_unswept`.
    const PRUNE_SLACK: usize = 64;

    /// Converged-or-timeout poll (30s cap) for `tracked_task_handles()` to
    /// come down to `bound`, with no spawn helping it along.
    async fn wait_tracked_at_most(env: &ProdEnv, bound: usize, what: &str) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while env.tracked_task_handles() > bound {
            assert!(
                std::time::Instant::now() < deadline,
                "{what}: tracked task handles never converged to <= {bound}: {} still tracked",
                env.tracked_task_handles()
            );
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    }

    /// The ProdEnv task-handle-leak fix's red-before/green-after proof:
    /// spawning a large number of short tasks and letting every one finish
    /// must NOT leave `Inner::tasks` growing without bound. Since issue
    /// #1105 pruning is driven by task *completion*, so the bound is reached
    /// with no further spawn at all — the poll below deliberately spawns
    /// nothing (the old spawn-only sweep needed one, and could still strand
    /// handles when none came).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn spawn_prunes_finished_handles_and_stays_bounded() {
        use crate::EnvExt;

        let dir = unique_tmp_dir();
        let (env, _addr) = ProdEnv::bind(nid(0), "127.0.0.1:0".parse().unwrap(), &dir)
            .await
            .expect("bind");

        const N: usize = 100_000;
        for _ in 0..N {
            env.spawn_task(async {});
        }

        wait_tracked_at_most(&env, TASK_PRUNE_FLOOR + PRUNE_SLACK, "100k no-op tasks").await;

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Issue #1105 regression (deterministic, no scheduler luck): a burst of
    /// M live tasks drives the *old* spawn-only sweep's high-water threshold
    /// to ~2M; after the burst finishes and NOTHING further is spawned, the
    /// old code kept all ~M finished handles pinned forever. The
    /// completion-driven sweep must bring the count back down on its own.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn finished_handles_are_pruned_after_a_burst_with_no_further_spawns() {
        use crate::EnvExt;

        let dir = unique_tmp_dir();
        let (env, _addr) = ProdEnv::bind(nid(0), "127.0.0.1:0".parse().unwrap(), &dir)
            .await
            .expect("bind");

        const M: usize = 4 * TASK_PRUNE_FLOOR;
        let (gate_tx, gate_rx) = tokio::sync::watch::channel(false);
        for _ in 0..M {
            let mut rx = gate_rx.clone();
            env.spawn_task(async move {
                let _ = rx.wait_for(|open| *open).await;
            });
        }
        // Every one of the M tasks is live (gated), so each sweep that
        // happened during the spawn loop saw them all live.
        assert!(env.tracked_task_handles() >= M);

        gate_tx.send(true).expect("open gate");
        // Spawn NOTHING further.
        wait_tracked_at_most(&env, TASK_PRUNE_FLOOR + PRUNE_SLACK, "post-burst").await;

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The completion guard must fire on the panic and abort paths too, not
    /// just normal completion.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn panicked_and_aborted_tasks_are_pruned() {
        use crate::EnvExt;

        let dir = unique_tmp_dir();
        let (env, _addr) = ProdEnv::bind(nid(0), "127.0.0.1:0".parse().unwrap(), &dir)
            .await
            .expect("bind");

        const N: usize = 3 * TASK_PRUNE_FLOOR;
        for _ in 0..N {
            env.spawn_task(async { panic!("expected: pruning-test panic") });
        }
        wait_tracked_at_most(&env, TASK_PRUNE_FLOOR + PRUNE_SLACK, "panicked tasks").await;

        // Aborted: spawn never-finishing tasks, then abort clones of their
        // handles (outside the `tasks` lock, per its deadlock rule).
        for _ in 0..N {
            env.spawn_task(std::future::pending::<()>());
        }
        let handles: Vec<tokio::task::AbortHandle> = env
            .inner
            .tasks
            .lock()
            .expect("tasks poisoned")
            .iter()
            .rev()
            .take(N)
            .cloned()
            .collect();
        assert_eq!(handles.len(), N);
        for h in &handles {
            h.abort();
        }
        wait_tracked_at_most(&env, TASK_PRUNE_FLOOR + PRUNE_SLACK + 2, "aborted tasks").await;

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `shutdown` must still abort a long-running task even after many
    /// short-lived ones spawned earlier were pruned out of `tasks` — pruning
    /// a *finished* handle must never accidentally drop or skip a *still
    /// running* one's handle.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn shutdown_still_aborts_a_long_running_task_after_pruning() {
        use crate::EnvExt;

        let dir = unique_tmp_dir();
        let (env, _addr) = ProdEnv::bind(nid(0), "127.0.0.1:0".parse().unwrap(), &dir)
            .await
            .expect("bind");

        // Spawn enough short tasks to cross the prune threshold at least
        // once, and let them finish. Pruning is completion-driven (issue
        // #1105), so no further spawn is needed for the count to drop.
        for _ in 0..(TASK_PRUNE_FLOOR * 2) {
            env.spawn_task(async {});
        }
        wait_tracked_at_most(&env, TASK_PRUNE_FLOOR + PRUNE_SLACK, "short tasks").await;

        // Now spawn one long-running task, confirmed started, then shut down.
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (aborted_tx, mut aborted_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        env.spawn_task(async move {
            let _ = started_tx.send(());
            // Runs until cancelled; on cancellation the future is dropped
            // without resuming — `aborted_tx`'s own `Drop` (never an
            // explicit send) is what a real cancellation looks like here,
            // so this task deliberately never sends on `aborted_tx` itself.
            std::future::pending::<()>().await;
            drop(aborted_tx);
        });
        started_rx
            .await
            .expect("long-running task must start before shutdown");

        env.shutdown();

        // If the task were NOT aborted, `aborted_rx` would hang forever
        // (nothing ever sends on `aborted_tx`, and it's only dropped when
        // the task itself is dropped by cancellation) — so a bounded recv
        // observing a closed channel (`None`) is a direct proof that the
        // long-running task's future was actually dropped by `shutdown`'s
        // abort, not merely left running.
        match tokio::time::timeout(std::time::Duration::from_secs(5), aborted_rx.recv()).await {
            Ok(None) => {}
            Ok(Some(())) => panic!("unexpected value received on aborted_rx"),
            Err(_) => {
                panic!("long-running task must be aborted by shutdown even after pruning")
            }
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    // --- ADR 0073 Phase 0, workstream D: the network handshake preamble
    // wired into `ProdEnv`'s accept/connect paths. ---

    /// A raw dialer speaking the network handshake's right magic but a
    /// **wrong version** against a real `ProdEnv` listener: the acceptor
    /// must still write its own `NHS1` v1 preamble first — so the raw peer
    /// can see what it's not agreeing with, even though it's about to be
    /// refused — then close the connection without ever entering the frame
    /// loop (the raw peer's next read is a clean EOF, never a frame).
    /// `Metric::NetworkHandshakeRefused` counts the refusal, and the
    /// listener keeps serving a genuine peer right after.
    #[tokio::test(flavor = "multi_thread")]
    async fn accept_refuses_mismatched_version_and_keeps_serving() {
        use crate::Network;

        let dir_b = unique_tmp_dir();
        let loop0 = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
        let (b, b_addr) = ProdEnv::bind(nid(1), loop0, &dir_b).await.expect("bind b");

        let bad = handshake::Preamble {
            magic: handshake::NETWORK_PROTOCOL.magic,
            version: handshake::NETWORK_PROTOCOL.version + 1,
            extensions: Vec::new(),
        };
        let mut raw = tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(b_addr))
            .await
            .expect("connect timed out")
            .expect("raw connect");
        raw.write_all(&handshake::encode(&bad))
            .await
            .expect("write mismatched-version preamble");

        // The acceptor writes its own preamble first, regardless of what it
        // is about to decide about ours.
        let mut header = [0u8; handshake::HEADER_LEN];
        tokio::time::timeout(Duration::from_secs(5), raw.read_exact(&mut header))
            .await
            .expect("read of acceptor's own preamble timed out")
            .expect("read acceptor preamble");
        let (their_preamble, _) = handshake::decode(&header).expect("decode acceptor preamble");
        assert_eq!(their_preamble.magic, handshake::NETWORK_PROTOCOL.magic);
        assert_eq!(their_preamble.version, handshake::NETWORK_PROTOCOL.version);

        // Having refused, the acceptor closes the connection without ever
        // reaching the frame loop.
        let mut trailing = [0u8; 1];
        let n = tokio::time::timeout(Duration::from_secs(5), raw.read(&mut trailing))
            .await
            .expect("read for EOF timed out")
            .expect("read for EOF");
        assert_eq!(
            n, 0,
            "acceptor must close the connection on a version mismatch"
        );

        // The refusal's metric increment happens on the accept task, not
        // synchronously with this test's own read of the EOF — poll rather
        // than assert immediately.
        let metrics = b.metrics();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if metrics.get(Metric::NetworkHandshakeRefused) >= 1 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "NetworkHandshakeRefused never incremented for a version mismatch"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        // The listener keeps accepting and serving a real peer afterward.
        let dir_a = unique_tmp_dir();
        let (a, _) = ProdEnv::bind(nid(0), loop0, &dir_a).await.expect("bind a");
        a.set_peers([(nid(1), b_addr.to_string())].into_iter().collect());
        a.send(nid(1), b"still-serving".to_vec()).await;
        let env = tokio::time::timeout(Duration::from_secs(10), b.recv())
            .await
            .expect("recv after a refused peer timed out");
        assert_eq!(env.payload, b"still-serving");

        drop(raw);
        a.shutdown();
        b.shutdown();
        let _ = std::fs::remove_dir_all(&dir_a);
        let _ = std::fs::remove_dir_all(&dir_b);
    }

    /// The pre-baseline case: a raw dialer that never sends this handshake
    /// preamble at all, just a plausible-looking raw frame header (exactly
    /// what a pre-ADR-0073 peer's first bytes on this wire used to be) —
    /// this must decode as `BadMagic`, refuse, count, and close, the same
    /// as an explicit version mismatch above, and the listener must keep
    /// serving genuine peers afterward.
    #[tokio::test(flavor = "multi_thread")]
    async fn accept_refuses_bad_magic_pre_baseline_frame_and_keeps_serving() {
        use crate::Network;

        let dir_b = unique_tmp_dir();
        let loop0 = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
        let (b, b_addr) = ProdEnv::bind(nid(1), loop0, &dir_b).await.expect("bind b");

        // A plausible pre-handshake frame-length prefix, never this magic
        // (mirrors `handshake.rs`'s own `check_peer_rejects_a_raw_pre_
        // baseline_frame_as_bad_magic` fixture).
        let raw_frame_start = [0x00, 0x00, 0x00, 0x10, 0xFF, 0x00, 0x00];
        let mut raw = tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(b_addr))
            .await
            .expect("connect timed out")
            .expect("raw connect");
        raw.write_all(&raw_frame_start)
            .await
            .expect("write raw pre-baseline bytes");

        let mut header = [0u8; handshake::HEADER_LEN];
        tokio::time::timeout(Duration::from_secs(5), raw.read_exact(&mut header))
            .await
            .expect("read of acceptor's own preamble timed out")
            .expect("read acceptor preamble");
        let (their_preamble, _) = handshake::decode(&header).expect("decode acceptor preamble");
        assert_eq!(their_preamble.magic, handshake::NETWORK_PROTOCOL.magic);

        let mut trailing = [0u8; 1];
        let n = tokio::time::timeout(Duration::from_secs(5), raw.read(&mut trailing))
            .await
            .expect("read for EOF timed out")
            .expect("read for EOF");
        assert_eq!(
            n, 0,
            "acceptor must close the connection on a bad-magic pre-baseline peer"
        );

        let metrics = b.metrics();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if metrics.get(Metric::NetworkHandshakeRefused) >= 1 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "NetworkHandshakeRefused never incremented for a bad-magic peer"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let dir_a = unique_tmp_dir();
        let (a, _) = ProdEnv::bind(nid(0), loop0, &dir_a).await.expect("bind a");
        a.set_peers([(nid(1), b_addr.to_string())].into_iter().collect());
        a.send(nid(1), b"still-serving-2".to_vec()).await;
        let env = tokio::time::timeout(Duration::from_secs(10), b.recv())
            .await
            .expect("recv after a refused peer timed out");
        assert_eq!(env.payload, b"still-serving-2");

        drop(raw);
        a.shutdown();
        b.shutdown();
        let _ = std::fs::remove_dir_all(&dir_a);
        let _ = std::fs::remove_dir_all(&dir_b);
    }

    /// A non-preamble peer whose bytes 5..7 (where `ext_len` sits) happen
    /// to declare a large-but-legal extension, and which then sends nothing
    /// more: the acceptor must refuse it as `BadMagic` straight off the
    /// header — counted, and well inside [`HANDSHAKE_TIMEOUT`] — rather than
    /// trusting the garbage `ext_len` and parking until the timeout fires
    /// (which would be logged but never counted as a refusal).
    #[tokio::test(flavor = "multi_thread")]
    async fn accept_refuses_bad_magic_before_waiting_on_its_declared_extension() {
        let dir_b = unique_tmp_dir();
        let loop0 = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
        let (b, b_addr) = ProdEnv::bind(nid(1), loop0, &dir_b).await.expect("bind b");

        // Wrong magic, any version, ext_len = 1000 (<= MAX_EXTENSION_LEN), and
        // no extension bytes ever follow.
        let [lo, hi] = 1000u16.to_le_bytes();
        let header = [b'X', b'X', b'X', b'X', 1, lo, hi];
        let mut raw = tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(b_addr))
            .await
            .expect("connect timed out")
            .expect("raw connect");
        raw.write_all(&header).await.expect("write bad header");

        // Well under HANDSHAKE_TIMEOUT: a reader that trusted `ext_len`
        // first would still be waiting here.
        let refused_within = Duration::from_secs(5);
        assert!(refused_within < HANDSHAKE_TIMEOUT);
        let metrics = b.metrics();
        let deadline = Instant::now() + refused_within;
        loop {
            if metrics.get(Metric::NetworkHandshakeRefused) >= 1 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "a bad-magic header must be refused before waiting on its declared extension"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        drop(raw);
        b.shutdown();
        let _ = std::fs::remove_dir_all(&dir_b);
    }

    /// The dial side's mirror: a raw fake "acceptor" that writes back a
    /// wrong-version preamble to every connection it accepts. A real
    /// `ProdEnv::send` to it must never panic, must count the refusal, and
    /// — since a failed handshake is never cached (see `send_frame_pooled`'s
    /// own doc) — a second send re-dials rather than reusing anything, which
    /// this proves two ways: the refusal counter reaches 2 (not 1), and the
    /// fake acceptor itself sees 2 separate inbound connections.
    #[tokio::test(flavor = "multi_thread")]
    async fn dial_refused_by_mismatched_fake_acceptor_is_never_cached() {
        use crate::Network;

        let fake = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind fake acceptor");
        let fake_addr = fake.local_addr().expect("fake acceptor addr");
        let accepted = Arc::new(AtomicU64::new(0));
        let accepted_for_task = Arc::clone(&accepted);
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = fake.accept().await else {
                    return;
                };
                accepted_for_task.fetch_add(1, Ordering::SeqCst);
                let bad = handshake::Preamble {
                    magic: handshake::NETWORK_PROTOCOL.magic,
                    version: handshake::NETWORK_PROTOCOL.version + 1,
                    extensions: Vec::new(),
                };
                // Best-effort: the dialer may already have given up and
                // closed its side by the time this writes; that's just
                // another way the dialer's own read fails, not a bug here.
                let _ = sock.write_all(&handshake::encode(&bad)).await;
            }
        });

        let dir_a = unique_tmp_dir();
        let (a, _) = ProdEnv::bind(nid(0), "127.0.0.1:0".parse().unwrap(), &dir_a)
            .await
            .expect("bind a");
        a.set_peers([(nid(1), fake_addr.to_string())].into_iter().collect());

        let metrics = a.metrics();
        for expected in 1..=2u64 {
            a.send(nid(1), b"never-arrives".to_vec()).await;
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                if metrics.get(Metric::NetworkHandshakeRefused) >= expected {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "NetworkHandshakeRefused never reached {expected} \
                     (dialer must re-dial and re-refuse on every send, \
                     never cache a failed handshake)"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
        assert_eq!(
            accepted.load(Ordering::SeqCst),
            2,
            "each send must dial fresh — a failed handshake must never be cached"
        );

        a.shutdown();
        let _ = std::fs::remove_dir_all(&dir_a);
    }

    /// Drive `read_frames` over an in-memory stream carrying `header` (and
    /// nothing after it, never EOF), returning its result. A reader that
    /// trusted the declared length would allocate it and then block forever
    /// in `read_exact` waiting for bytes that never come, so the 5s timeout
    /// failing is the "not rejected before allocating" signal.
    async fn run_read_frames(header: Vec<u8>) -> std::io::Result<()> {
        use tokio::io::AsyncWriteExt;
        let (mut client, server) = tokio::io::duplex(64 * 1024);
        client.write_all(&header).await.unwrap();
        let (tx, _rx) = mpsc::unbounded_channel();
        let res = tokio::time::timeout(
            Duration::from_secs(5),
            read_frames(
                server,
                tx,
                Arc::from(Vec::<u8>::new()),
                Arc::new(HandshakeCfg::default()),
                MetricsHandle::recording(),
            ),
        )
        .await
        .expect("read_frames must reject an oversized length promptly, not allocate and wait");
        drop(client);
        res
    }

    /// A peer claiming a ~4 GiB sender id is refused before any allocation.
    #[tokio::test]
    async fn read_frames_rejects_oversized_sender_id_length() {
        let err = run_read_frames(u32::MAX.to_be_bytes().to_vec())
            .await
            .expect_err("oversized sender id must close the connection");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("sender id"), "{err}");
    }

    /// A valid sender id followed by a ~4 GiB payload length is refused
    /// before the payload buffer is allocated.
    #[tokio::test]
    async fn read_frames_rejects_oversized_payload_length() {
        let mut h = Vec::new();
        h.extend_from_slice(&2u32.to_be_bytes());
        h.extend_from_slice(b"n0");
        h.extend_from_slice(&7u64.to_be_bytes());
        h.extend_from_slice(&u32::MAX.to_be_bytes());
        let err = run_read_frames(h)
            .await
            .expect_err("oversized payload must close the connection");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("payload"), "{err}");
    }

    /// A normal small frame still flows through the capped reader.
    #[tokio::test]
    async fn read_frames_still_delivers_a_normal_frame() {
        use tokio::io::AsyncWriteExt;
        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let mut h = Vec::new();
        h.extend_from_slice(&2u32.to_be_bytes());
        h.extend_from_slice(b"n0");
        h.extend_from_slice(&7u64.to_be_bytes());
        h.extend_from_slice(&3u32.to_be_bytes());
        h.extend_from_slice(b"abc");
        client.write_all(&h).await.unwrap();
        drop(client); // EOF after the one frame
        let (tx, mut rx) = mpsc::unbounded_channel();
        read_frames(
            server,
            tx,
            Arc::from(Vec::<u8>::new()),
            Arc::new(HandshakeCfg::default()),
            MetricsHandle::recording(),
        )
        .await
        .expect("clean EOF");
        let env = rx.recv().await.expect("frame delivered");
        assert_eq!(env.stream, 7);
        assert_eq!(env.payload, b"abc");
    }
}
