//! Production [`Env`] implementation.
//!
//! Real wall-derived monotonic clock, OS randomness, `tokio` task spawning,
//! length-prefixed TCP messaging, and `tokio::fs` with real `fsync`. This is the
//! non-deterministic side of the seam: it is **not** exercised by the
//! simulation tests, which run against `custos-sim`'s `SimEnv`. Keep production
//! behavior here so the rest of the codebase stays environment-agnostic.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Mutex, mpsc};

use crate::{Clock, Disk, Env, Envelope, Nanos, Network, NodeId, Rng, Spawner};

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

struct Inner {
    node_id: NodeId,
    start: Instant,
    peers: StdMutex<BTreeMap<NodeId, SocketAddr>>,
    data_dir: PathBuf,
    inbox: Mutex<mpsc::UnboundedReceiver<Envelope>>,
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
        let (tx, rx) = mpsc::unbounded_channel();

        // Accept loop: one reader task per inbound connection, each demuxing
        // length-prefixed frames into the shared inbox.
        tokio::spawn(async move {
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

        let env = Self {
            inner: Arc::new(Inner {
                node_id,
                start: Instant::now(),
                peers: StdMutex::new(BTreeMap::new()),
                data_dir,
                inbox: Mutex::new(rx),
            }),
        };
        Ok((env, local_addr))
    }

    /// Install (or replace) the peer address book: a map from node id to socket
    /// address for every node this env may send to.
    pub fn set_peers(&self, peers: BTreeMap<NodeId, SocketAddr>) {
        *self.inner.peers.lock().expect("peers poisoned") = peers;
    }

    fn path(&self, file: &str) -> PathBuf {
        self.inner.data_dir.join(file)
    }
}

/// Read length-prefixed `[from: u64][len: u32][payload]` frames until EOF.
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
        let len = stream.read_u32().await? as usize;
        let mut payload = vec![0u8; len];
        stream.read_exact(&mut payload).await?;
        if tx.send(Envelope { from, payload }).is_err() {
            return Ok(()); // receiver gone; node shutting down
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
    async fn send(&self, to: NodeId, payload: Vec<u8>) {
        let addr = {
            let peers = self.inner.peers.lock().expect("peers poisoned");
            match peers.get(&to) {
                Some(&addr) => addr,
                None => {
                    tracing::warn!(to, "send to unknown peer");
                    return;
                }
            }
        };
        // Fire-and-forget semantics: a transport error is the network dropping
        // the message, not an error to the caller (see `Network::send`).
        let from = self.inner.node_id;
        if let Err(err) = send_frame(addr, from, &payload).await {
            tracing::debug!(?err, to, "send failed (dropped)");
        }
    }

    async fn recv(&self) -> Envelope {
        let mut rx = self.inner.inbox.lock().await;
        rx.recv()
            .await
            .expect("inbox sender dropped while env is alive")
    }
}

async fn send_frame(addr: SocketAddr, from: NodeId, payload: &[u8]) -> std::io::Result<()> {
    let mut stream = TcpStream::connect(addr).await?;
    stream.write_u64(from).await?;
    stream.write_u32(payload.len() as u32).await?;
    stream.write_all(payload).await?;
    stream.flush().await?;
    Ok(())
}

#[async_trait::async_trait]
impl Disk for ProdEnv {
    async fn append(&self, file: &str, bytes: &[u8]) -> std::io::Result<()> {
        let mut f = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.path(file))
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
        {
            let mut f = tokio::fs::File::create(&tmp).await?;
            f.write_all(bytes).await?;
            f.sync_all().await?;
        }
        tokio::fs::rename(&tmp, &target).await
    }
}

impl Spawner for ProdEnv {
    fn spawn(&self, fut: crate::BoxFuture<'static, ()>) {
        tokio::spawn(fut);
    }
}

impl Env for ProdEnv {
    fn node_id(&self) -> NodeId {
        self.inner.node_id
    }
}
