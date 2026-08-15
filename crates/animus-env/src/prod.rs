//! Production [`Env`] implementation.
//!
//! Real wall-derived monotonic clock, OS randomness, `tokio` task spawning,
//! length-prefixed TCP messaging, and `tokio::fs` with real `fsync`. This is the
//! non-deterministic side of the seam: it is **not** exercised by the
//! simulation tests, which run against `animus-sim`'s `SimEnv`. Keep production
//! behavior here so the rest of the codebase stays environment-agnostic.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, Mutex as StdMutex};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, mpsc};

#[cfg(test)]
use crate::nid;
use crate::{Clock, Disk, Env, Envelope, MetricsHandle, Nanos, Network, NodeId, Rng, Spawner};

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
}

struct Inner {
    node_id: NodeId,
    start: Instant,
    /// The peer address book.
    peers: Arc<StdMutex<BTreeMap<NodeId, SocketAddr>>>,
    /// This env's own listener address.
    local_addr: SocketAddr,
    /// Cached outbound connections, one per destination address, so `send`/
    /// `send_stream` do not pay a TCP handshake per message (Raft heartbeats/
    /// AppendEntries/votes are the hot path). Keyed by `SocketAddr`, not
    /// `NodeId`: the frame header carries `from` per message and the receiver
    /// demuxes per *listener*, so one connection per address is correct even
    /// when several ids map to it, and a re-mapped peer id naturally picks up
    /// a fresh connection. The outer `StdMutex` only guards map lookup/insert
    /// (never held across `.await`); the per-address `tokio::sync::Mutex`
    /// serializes whole-frame writes so concurrent senders to one peer never
    /// interleave frames, without head-of-line blocking *across* peers.
    #[allow(clippy::type_complexity)]
    conns: Arc<StdMutex<BTreeMap<SocketAddr, Arc<Mutex<Option<TcpStream>>>>>>,
    data_dir: PathBuf,
    /// Files whose *directory entry* is already durable — i.e. whose containing
    /// directory chain has been fsynced since the file was (re)created. A file's
    /// creation is a one-time namespace change: the first `sync` of a file pays
    /// the directory fsync, later `sync`s (the WAL group-commit hot path) skip
    /// it. `remove` un-memoizes (a re-created file is a new namespace change);
    /// `replace` re-memoizes (its rename just got the chain fsynced).
    dir_synced: StdMutex<BTreeSet<String>>,
    /// This env's multiplexed inbox (ADR 0026): a background pump task (spawned
    /// alongside the accept loop, see `spawn_pump`) drains the accept loop's raw
    /// per-connection frames and files each into `demux.queues[frame.stream]`.
    demux: Arc<StdMutex<Demux>>,
    /// Abort handles for every task this env owns — the inbound-connection
    /// accept loop, its demux pump, and everything spawned through
    /// [`Spawner::spawn`] (the Raft driver, the replica serve loop, etc.).
    /// [`shutdown`](ProdEnv::shutdown) aborts them all so the node can be torn
    /// down and its listener port freed.
    tasks: StdMutex<Vec<tokio::task::AbortHandle>>,
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
        let data_dir = data_dir.into();
        tokio::fs::create_dir_all(&data_dir).await?;
        let listener = TcpListener::bind(listen).await?;
        let local_addr = listener.local_addr()?;
        let (raw_rx, accept_abort) = spawn_accept(listener);
        let demux = Arc::new(StdMutex::new(Demux::default()));
        let pump_abort = spawn_pump(raw_rx, Arc::clone(&demux));

        let env = Self {
            inner: Arc::new(Inner {
                node_id,
                start: Instant::now(),
                peers: Arc::new(StdMutex::new(BTreeMap::new())),
                local_addr,
                conns: Arc::new(StdMutex::new(BTreeMap::new())),
                data_dir,
                dir_synced: StdMutex::new(BTreeSet::new()),
                demux,
                tasks: StdMutex::new(vec![accept_abort, pump_abort]),
                metrics: MetricsHandle::recording(),
            }),
        };
        Ok((env, local_addr))
    }

    /// This env's own listener address.
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.inner.local_addr
    }

    /// Install (or replace) the peer address book: a map from node id to socket
    /// address for every node this env may send to.
    pub fn set_peers(&self, peers: BTreeMap<NodeId, SocketAddr>) {
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
    pub fn merge_peer(&self, id: NodeId, addr: SocketAddr) {
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

    fn path(&self, file: &str) -> PathBuf {
        self.inner.data_dir.join(file)
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
            if d == self.inner.data_dir || !d.starts_with(&self.inner.data_dir) {
                break;
            }
            dir = d.parent();
        }
        Ok(())
    }

    /// A point-in-time text export of this env's recorded metrics (ADR 0015):
    /// one `name value` line per counter plus the leadership gauge, in stable
    /// order. This is what an integration-level `/metrics` endpoint serves; the
    /// `Env` seam itself does no HTTP. A pure read of the atomic sink.
    #[must_use]
    pub fn metrics_text(&self) -> String {
        self.inner.metrics.snapshot().to_text()
    }
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

/// Read length-prefixed `[from_len: u32][from: utf8 bytes][stream: u64][len:
/// u32][payload]` frames until EOF (ADR 0040 PR3 changed `from` from a fixed
/// `u64` to a length-prefixed UTF-8 string, since node ids are strings now;
/// ADR 0026 added the `stream` field; the rest of the frame is unchanged).
/// These are the *raw*, not-yet-demultiplexed frames off one accepted
/// connection — `spawn_pump` fans them out by `stream` into an env's
/// [`Demux`].
async fn read_frames(
    mut stream: TcpStream,
    tx: mpsc::UnboundedSender<Envelope>,
) -> std::io::Result<()> {
    loop {
        let from_len = match stream.read_u32().await {
            Ok(v) => v as usize,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e),
        };
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
        let mut payload = vec![0u8; len];
        stream.read_exact(&mut payload).await?;
        if tx
            .send(Envelope {
                from,
                stream: msg_stream,
                payload,
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
fn spawn_pump(
    mut raw_rx: mpsc::UnboundedReceiver<Envelope>,
    demux: Arc<StdMutex<Demux>>,
) -> tokio::task::AbortHandle {
    let handle = tokio::spawn(async move {
        while let Some(env) = raw_rx.recv().await {
            let stream = env.stream;
            let waker = {
                let mut d = demux.lock().expect("demux poisoned");
                d.queues.entry(stream).or_default().push_back(env);
                d.wakers.remove(&stream)
            };
            if let Some(w) = waker {
                w.wake();
            }
        }
    });
    handle.abort_handle()
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
            Poll::Ready(env)
        } else {
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

    async fn sleep(&self, dur: Duration) {
        tokio::time::sleep(dur).await;
    }
}

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

impl Rng for PreBindRng {
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
                Some(&addr) => addr,
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
        let from = &self.inner.node_id;
        // Grab (or create) this address's connection slot. The map lock is a
        // `StdMutex` and must not be held across an `.await` — clone the
        // per-address `Arc` out and drop the guard before any I/O.
        let slot = {
            let mut conns = self.inner.conns.lock().expect("conns poisoned");
            Arc::clone(conns.entry(addr).or_default())
        };
        if let Err(err) = send_frame_pooled(&slot, addr, from, stream, &payload).await {
            tracing::debug!(?err, to = %to, "send failed (dropped)");
        }
    }

    async fn recv_stream(&self, stream: u64) -> Envelope {
        RecvStream {
            demux: Arc::clone(&self.inner.demux),
            stream,
        }
        .await
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

/// Spawn the accept loop for `listener` — one reader task per inbound connection,
/// each demuxing length-prefixed frames into a fresh inbox channel. Returns the
/// inbox receiver and the accept task's abort handle (for `shutdown`).
fn spawn_accept(
    listener: TcpListener,
) -> (mpsc::UnboundedReceiver<Envelope>, tokio::task::AbortHandle) {
    let (tx, rx) = mpsc::unbounded_channel();
    let accept = tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((stream, _addr)) => {
                    let tx = tx.clone();
                    tokio::spawn(async move {
                        if let Err(err) = read_frames(stream, tx).await {
                            tracing::debug!(?err, "peer connection closed");
                        }
                    });
                }
                Err(err) => {
                    tracing::warn!(?err, "accept failed");
                    return;
                }
            }
        }
    });
    (rx, accept.abort_handle())
}

/// Send one frame over the cached connection for `addr`, connecting (with
/// `TCP_NODELAY`) if there is none. Holding the per-address lock across the
/// whole frame write is what keeps concurrent senders' frames from
/// interleaving. On a write error the cached stream is stale (e.g. the peer
/// restarted since the last send) — drop it, reconnect **once**, resend the
/// whole frame (the receiver never saw a partial frame: the dead connection
/// took it), then surface the error if that also fails, matching the old
/// connect-per-message fire-and-forget semantics.
async fn send_frame_pooled(
    slot: &Mutex<Option<TcpStream>>,
    addr: SocketAddr,
    from: &NodeId,
    msg_stream: u64,
    payload: &[u8],
) -> std::io::Result<()> {
    let mut guard = slot.lock().await;
    if guard.is_none() {
        *guard = Some(connect_nodelay(addr).await?);
    }
    let conn = guard.as_mut().expect("connection just ensured");
    if let Err(err) = write_frame(conn, from, msg_stream, payload).await {
        tracing::debug!(?err, %addr, "cached connection failed; reconnecting once");
        *guard = None; // drop the stale stream before dialing afresh
        let mut fresh = connect_nodelay(addr).await?;
        write_frame(&mut fresh, from, msg_stream, payload).await?;
        *guard = Some(fresh); // cache only a stream that just carried a frame
    }
    Ok(())
}

async fn connect_nodelay(addr: SocketAddr) -> std::io::Result<TcpStream> {
    let stream = TcpStream::connect(addr).await?;
    // Frames are small (heartbeats, votes) and latency-sensitive; never let
    // Nagle hold one back waiting to coalesce.
    stream.set_nodelay(true)?;
    Ok(stream)
}

/// Write one length-prefixed `[from_len: u32][from: utf8 bytes][stream:
/// u64][len: u32][payload]` frame (ADR 0040 PR3 length-prefixed the `from`
/// field to carry a string id instead of a fixed `u64`; ADR 0026 added the
/// `stream` field) over a pooled connection — the receive side
/// (`read_frames`, which already loops until EOF) needs no further change.
async fn write_frame(
    conn: &mut TcpStream,
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

#[async_trait::async_trait]
impl Disk for ProdEnv {
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
            .inner
            .dir_synced
            .lock()
            .expect("dir_synced poisoned")
            .contains(file);
        if !already {
            self.sync_parents(file).await?;
            self.inner
                .dir_synced
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
        self.inner
            .dir_synced
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
        self.inner
            .dir_synced
            .lock()
            .expect("dir_synced poisoned")
            .insert(file.to_string());
        Ok(())
    }

    async fn list(&self) -> std::io::Result<Vec<String>> {
        // Non-recursive: a nested subdirectory is not this env's own top-level
        // disk contents. A data dir that does not exist yet reads as empty —
        // the env creates it lazily on first write.
        let mut dir = match tokio::fs::read_dir(&self.inner.data_dir).await {
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

impl Spawner for ProdEnv {
    fn spawn(&self, fut: crate::BoxFuture<'static, ()>) {
        // Register the handle so [`ProdEnv::shutdown`] can abort the task on
        // teardown (the Raft driver, the replica serve loop, etc.).
        let handle = tokio::spawn(fut);
        self.inner
            .tasks
            .lock()
            .expect("tasks poisoned")
            .push(handle.abort_handle());
    }
}

impl Env for ProdEnv {
    fn node_id(&self) -> NodeId {
        self.inner.node_id.clone()
    }

    fn metrics(&self) -> MetricsHandle {
        self.inner.metrics.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Disk, SegmentStore};
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
        b.set_peers([(nid(0), a_addr)].into_iter().collect());

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

    /// Bind two `ProdEnv`s on ephemeral loopback ports and point `sender` at
    /// `receiver` in the peer book. Returns `(sender, receiver, dirs)`.
    async fn bound_pair(dir_a: &PathBuf, dir_b: &PathBuf) -> (ProdEnv, ProdEnv, SocketAddr) {
        let loop0 = "127.0.0.1:0".parse::<SocketAddr>().unwrap();
        let (a, _) = ProdEnv::bind(nid(0), loop0, dir_a).await.expect("bind a");
        let (b, b_addr) = ProdEnv::bind(nid(1), loop0, dir_b).await.expect("bind b");
        a.set_peers([(nid(1), b_addr)].into_iter().collect());
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

        a.merge_peer(nid(2), c_addr);

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
}
