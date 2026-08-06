//! Production [`Env`] implementation.
//!
//! Real wall-derived monotonic clock, OS randomness, `tokio` task spawning,
//! length-prefixed TCP messaging, and `tokio::fs` with real `fsync`. This is the
//! non-deterministic side of the seam: it is **not** exercised by the
//! simulation tests, which run against `animus-sim`'s `SimEnv`. Keep production
//! behavior here so the rest of the codebase stays environment-agnostic.

use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex as StdMutex};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use crate::{
    Clock, Coresident, Disk, Env, Envelope, MetricsHandle, Nanos, Network, NodeId, Rng, Spawner,
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

/// A pre-bound spare listener a [`ProdEnv`] holds for [`Coresident::sibling`]
/// (ADR 0017 #3b): minting a co-resident handle at runtime needs a new
/// id-addressable inbox, but binding a socket is `async`/fallible while `sibling`
/// is sync/infallible — so the listeners are bound up front (at `bind` time) and
/// handed out synchronously. Each slot is a bound listener + its accept loop's
/// inbox + the abort handle for that loop.
struct PoolSlot {
    addr: SocketAddr,
    inbox: mpsc::UnboundedReceiver<Envelope>,
    accept_abort: tokio::task::AbortHandle,
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
    /// The peer address book, **shared** (via `Arc`) with every sibling minted off
    /// this env (ADR 0017 #3b), so an address-distribution update (`set_peers` from
    /// replicated `Metadata`) reaches the co-resident CP groups too.
    peers: Arc<StdMutex<BTreeMap<NodeId, SocketAddr>>>,
    /// This env's own listener address (so a caller can publish a freshly-minted
    /// sibling's `id → addr` for distribution).
    local_addr: SocketAddr,
    /// Unclaimed pre-bound listeners for [`Coresident::sibling`], shared so any
    /// clone of this env can mint a sibling. Empty for an env bound without a pool.
    pool: Arc<StdMutex<Vec<PoolSlot>>>,
    data_dir: PathBuf,
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
        Self::bind_with_pool(node_id, listen, &[], data_dir).await
    }

    /// Like [`bind`](Self::bind), but also binds a **pool** of spare listeners (one
    /// per address in `pool_listens`, port 0 for OS-assigned) that
    /// [`Coresident::sibling`] hands out at runtime (ADR 0017 #3b). A node that may
    /// host co-resident CP per-tablet Raft groups (ADR 0017) binds a pool sized to
    /// the maximum groups it will host; an env bound with an empty pool is not
    /// usefully `Coresident` (the first `sibling` call panics on exhaustion).
    ///
    /// # Errors
    /// As [`bind`](Self::bind), for the main listener or any pool listener.
    pub async fn bind_with_pool(
        node_id: NodeId,
        listen: SocketAddr,
        pool_listens: &[SocketAddr],
        data_dir: impl Into<PathBuf>,
    ) -> std::io::Result<(Self, SocketAddr)> {
        let data_dir = data_dir.into();
        tokio::fs::create_dir_all(&data_dir).await?;
        let listener = TcpListener::bind(listen).await?;
        let local_addr = listener.local_addr()?;
        let (raw_rx, accept_abort) = spawn_accept(listener);
        let demux = Arc::new(StdMutex::new(Demux::default()));
        let pump_abort = spawn_pump(raw_rx, Arc::clone(&demux));

        // Pre-bind the sibling pool: each slot is a bound listener + its accept
        // loop's inbox, held until `sibling` claims it.
        let mut pool = Vec::with_capacity(pool_listens.len());
        for &addr in pool_listens {
            let l = TcpListener::bind(addr).await?;
            let slot_addr = l.local_addr()?;
            let (slot_rx, slot_abort) = spawn_accept(l);
            pool.push(PoolSlot {
                addr: slot_addr,
                inbox: slot_rx,
                accept_abort: slot_abort,
            });
        }

        let env = Self {
            inner: Arc::new(Inner {
                node_id,
                start: Instant::now(),
                peers: Arc::new(StdMutex::new(BTreeMap::new())),
                local_addr,
                pool: Arc::new(StdMutex::new(pool)),
                data_dir,
                demux,
                tasks: StdMutex::new(vec![accept_abort, pump_abort]),
                metrics: MetricsHandle::recording(),
            }),
        };
        Ok((env, local_addr))
    }

    /// This env's own listener address — so a caller can publish a freshly-minted
    /// [`sibling`](Coresident::sibling)'s `id → addr` for address distribution
    /// (ADR 0017 #3b).
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.inner.local_addr
    }

    /// Install (or replace) the peer address book: a map from node id to socket
    /// address for every node this env may send to.
    pub fn set_peers(&self, peers: BTreeMap<NodeId, SocketAddr>) {
        *self.inner.peers.lock().expect("peers poisoned") = peers;
    }

    /// Abort every task this env owns — its inbound-connection accept loop and
    /// everything spawned through [`Spawner::spawn`] — so the node can be torn
    /// down cleanly and its listener port freed for a restart. Idempotent; once
    /// called, this env should no longer be used to spawn or receive.
    pub fn shutdown(&self) {
        let handles = std::mem::take(&mut *self.inner.tasks.lock().expect("tasks poisoned"));
        for h in handles {
            h.abort();
        }
        // Also abort any still-unclaimed sibling-pool accept loops so their
        // listener ports are freed too (a claimed slot's abort moved into the
        // sibling's own `tasks`, torn down when that sibling shuts down).
        for slot in std::mem::take(&mut *self.inner.pool.lock().expect("pool poisoned")) {
            slot.accept_abort.abort();
        }
    }

    /// Abort only the tasks **this env itself** owns — its accept loop and
    /// everything spawned through [`Spawner::spawn`] on this handle — leaving the
    /// **shared** sibling listener pool untouched. The per-sibling teardown a
    /// tablet GC needs: [`shutdown`](Self::shutdown) on a sibling would drain the
    /// pool it shares with its parent and every other sibling, killing the
    /// unclaimed slots future co-resident groups (splits) still need. Idempotent.
    pub fn shutdown_tasks(&self) {
        let handles = std::mem::take(&mut *self.inner.tasks.lock().expect("tasks poisoned"));
        for h in handles {
            h.abort();
        }
    }

    fn path(&self, file: &str) -> PathBuf {
        self.inner.data_dir.join(file)
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
async fn ensure_parent(path: &std::path::Path) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    Ok(())
}

/// Read length-prefixed `[from: u64][stream: u64][len: u32][payload]` frames
/// until EOF (ADR 0026 added the `stream` field; the rest of the frame is
/// unchanged). These are the *raw*, not-yet-demultiplexed frames off one
/// accepted connection — `spawn_pump` fans them out by `stream` into an env's
/// [`Demux`].
async fn read_frames(
    mut stream: TcpStream,
    tx: mpsc::UnboundedSender<Envelope>,
) -> std::io::Result<()> {
    loop {
        let from = match stream.read_u64().await {
            Ok(v) => v,
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(e),
        };
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
                    tracing::debug!(to, "send to peer with no known address (dropped)");
                    return;
                }
            }
        };
        // Fire-and-forget semantics: a transport error is the network dropping
        // the message, not an error to the caller (see `Network::send`).
        let from = self.inner.node_id;
        if let Err(err) = send_frame(addr, from, stream, &payload).await {
            tracing::debug!(?err, to, "send failed (dropped)");
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

impl Coresident for ProdEnv {
    /// Mint a co-resident handle bound to `id` (ADR 0017 #3b): claim a pre-bound
    /// listener from the pool ([`bind_with_pool`](Self::bind_with_pool)) and return
    /// a `ProdEnv` on it. The handle **shares this env's peer address book** (so a
    /// later `set_peers` from distributed `Metadata` reaches it) and the same
    /// listener pool (so it too can mint siblings), but has its own inbox, id, and
    /// a distinct data dir (`<dir>/sib-<id>`, created lazily on first write) so its
    /// WAL never collides with the parent's. The caller publishes the new addr
    /// ([`local_addr`](Self::local_addr)) for distribution.
    ///
    /// **Panics** if the pool is exhausted — the pool size bounds how many
    /// co-resident groups a node hosts; size it accordingly at `bind_with_pool`.
    fn sibling(&self, id: NodeId) -> Self {
        let slot = self.inner.pool.lock().expect("pool poisoned").pop().expect(
            "Coresident::sibling: ProdEnv listener pool exhausted — too many \
                 co-resident groups for the pre-bound pool; increase the pool size \
                 (animusd's CP_SIBLING_POOL / bind_with_pool's pool_listens)",
        );
        let demux = Arc::new(StdMutex::new(Demux::default()));
        let pump_abort = spawn_pump(slot.inbox, Arc::clone(&demux));
        Self {
            inner: Arc::new(Inner {
                node_id: id,
                start: self.inner.start,
                peers: Arc::clone(&self.inner.peers),
                local_addr: slot.addr,
                pool: Arc::clone(&self.inner.pool),
                data_dir: self.inner.data_dir.join(format!("sib-{id}")),
                demux,
                tasks: StdMutex::new(vec![slot.accept_abort, pump_abort]),
                metrics: self.inner.metrics.clone(),
            }),
        }
    }
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

async fn send_frame(
    addr: SocketAddr,
    from: NodeId,
    msg_stream: u64,
    payload: &[u8],
) -> std::io::Result<()> {
    let mut conn = TcpStream::connect(addr).await?;
    conn.write_u64(from).await?;
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
        ensure_parent(&path).await?;
        let mut f = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;
        f.write_all(bytes).await?;
        Ok(())
    }

    async fn sync(&self, file: &str) -> std::io::Result<()> {
        let f = tokio::fs::OpenOptions::new()
            .append(true)
            .open(self.path(file))
            .await?;
        f.sync_all().await
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
        match tokio::fs::remove_file(self.path(file)).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    async fn replace(&self, file: &str, bytes: &[u8]) -> std::io::Result<()> {
        // Write a temp file, fsync it, then atomically rename over the target.
        let target = self.path(file);
        let tmp = self.path(&format!("{file}.tmp"));
        ensure_parent(&target).await?;
        {
            let mut f = tokio::fs::File::create(&tmp).await?;
            f.write_all(bytes).await?;
            f.sync_all().await?;
        }
        tokio::fs::rename(&tmp, &target).await
    }

    async fn list(&self) -> std::io::Result<Vec<String>> {
        // Non-recursive: a subdirectory (e.g. a sibling's `sib-<id>/`) is another
        // env's disk. A data dir that does not exist yet reads as empty — the env
        // creates it lazily on first write.
        let mut dir = match tokio::fs::read_dir(&self.inner.data_dir).await {
            Ok(dir) => dir,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e),
        };
        let mut names = Vec::new();
        while let Some(entry) = dir.next_entry().await? {
            if entry.file_type().await?.is_file() {
                if let Ok(name) = entry.file_name().into_string() {
                    names.push(name);
                }
            }
        }
        names.sort_unstable();
        Ok(names)
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
        self.inner.node_id
    }

    fn metrics(&self) -> MetricsHandle {
        self.inner.metrics.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Disk;
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
        let (env, _addr) = ProdEnv::bind(0, "127.0.0.1:0".parse().unwrap(), &dir)
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
    /// subdirectory (a sibling's dir) is not this env's disk — and reads a
    /// not-yet-created data dir as empty.
    #[tokio::test]
    async fn disk_list_is_own_files_sorted_nonrecursive() {
        let dir = unique_tmp_dir();
        let missing = ProdEnv::bind(0, "127.0.0.1:0".parse().unwrap(), dir.join("never-written"))
            .await
            .expect("bind")
            .0;
        assert_eq!(
            missing.list().await.expect("list missing"),
            Vec::<String>::new()
        );

        let (env, _addr) = ProdEnv::bind(1, "127.0.0.1:0".parse().unwrap(), &dir)
            .await
            .expect("bind");
        env.append("db-wal", b"w").await.expect("append");
        env.append("db-MANIFEST", b"m").await.expect("append");
        env.append("sib-300/db-t2-wal", b"s").await.expect("append");

        let got = env.list().await.expect("list");
        assert_eq!(
            got,
            vec!["db-MANIFEST".to_string(), "db-wal".to_string()],
            "own files sorted; the sibling subdirectory's files are not listed"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `Coresident::sibling` over `ProdEnv` (ADR 0017 #3b): an env bound with a
    /// listener pool mints a sibling at runtime with its own id-addressable inbox,
    /// and — once the freshly-minted addresses are distributed (`set_peers`) — two
    /// siblings on different physical nodes exchange a message over real TCP.
    #[tokio::test]
    async fn coresident_siblings_address_and_message_each_other() {
        use crate::Network;

        let dir_a = unique_tmp_dir();
        let dir_b = unique_tmp_dir();
        let loop0 = || "127.0.0.1:0".parse::<SocketAddr>().unwrap();
        // Two physical nodes, each with a one-slot sibling pool.
        let (a, _) = ProdEnv::bind_with_pool(0, loop0(), &[loop0()], &dir_a)
            .await
            .expect("bind a");
        let (b, _) = ProdEnv::bind_with_pool(1, loop0(), &[loop0()], &dir_b)
            .await
            .expect("bind b");

        // Mint a co-resident group member on each node (ids 300, 301).
        let a_sib = a.sibling(300);
        let b_sib = b.sibling(301);
        assert_eq!(a_sib.node_id(), 300);
        assert_ne!(
            a_sib.local_addr(),
            a.local_addr(),
            "sibling has its own port"
        );

        // Distribute the freshly-minted addresses (what the 3b peer-sync loop does
        // from replicated Metadata) onto the parents — siblings share the book.
        let book: BTreeMap<NodeId, SocketAddr> =
            [(300, a_sib.local_addr()), (301, b_sib.local_addr())]
                .into_iter()
                .collect();
        a.set_peers(book.clone());
        b.set_peers(book);

        // 301 → 300 over real TCP, received on the sibling's own inbox.
        let recv_handle = {
            let a_sib = a_sib.clone();
            tokio::spawn(async move { a_sib.recv().await })
        };
        // Give the receiver a moment to park on its inbox, then send.
        tokio::time::sleep(Duration::from_millis(50)).await;
        b_sib.send(300, b"hello-cp-sibling".to_vec()).await;

        let env = tokio::time::timeout(Duration::from_secs(5), recv_handle)
            .await
            .expect("recv timed out")
            .expect("recv task");
        assert_eq!(env.from, 301);
        assert_eq!(env.payload, b"hello-cp-sibling");

        a.shutdown();
        b.shutdown();
        a_sib.shutdown();
        b_sib.shutdown();
        let _ = std::fs::remove_dir_all(&dir_a);
        let _ = std::fs::remove_dir_all(&dir_b);
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
        let (a, a_addr) = ProdEnv::bind(0, loop0(), &dir_a).await.expect("bind a");
        let (b, _) = ProdEnv::bind(1, loop0(), &dir_b).await.expect("bind b");
        b.set_peers([(0, a_addr)].into_iter().collect());

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
                    b.send_stream(0, STREAM_X, vec![i]).await;
                }
            })
        };
        let send_y = {
            let b = b.clone();
            tokio::spawn(async move {
                for i in 0..N {
                    b.send_stream(0, STREAM_Y, vec![i]).await;
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
}
