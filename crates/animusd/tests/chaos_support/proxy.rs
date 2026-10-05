//! Userspace loopback TCP fault proxy (R-01 sub-track b).
//!
//! One listener per (node, port) sits in front of that node's real bind
//! address; the node advertises the proxy's address (`advertise_host`) and
//! every peer's static config entry points at it too, so all node-to-node
//! traffic crosses a proxy without root, netns or `tc`.
//!
//! Fault model (all dynamic, flipped through [`Faults`]):
//!
//! - **cut / stall** — a cut link stops forwarding in both directions but does
//!   NOT discard bytes: it stops reading, so TCP backpressure builds exactly
//!   as under real packet loss, and delivery resumes in order on heal. (A
//!   discarding blackhole would corrupt the length-prefixed framing mid-frame
//!   and test the harness, not the database.)
//! - **cut / reset** — a cut link's connections are closed, new ones refused.
//! - **delay** — every forwarded chunk is held `delay` before delivery.
//!
//! Per-link granularity: the `internal` port carries the Raft wire. Its
//! frames name the sender (`[from_len u32 BE][from][stream u64][len u32]..`,
//! after the `magic|ver|ext_len|ext` handshake preamble), which the proxy
//! sniffs from each connection's first bytes, so a cut is really per directed
//! `(src, dst)` pair. The `intra` port (client-framed forwarding RPC) names
//! no sender, so it is cut per destination: see [`Faults::partition`].

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Instant;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum PortKind {
    /// Raft wire: per-link cuts via sender sniffing.
    Internal,
    /// Node-to-node forwarding RPC: per-destination cuts.
    Intra,
    /// client / dynamo / admin / console: always passed through.
    Other,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum CutMode {
    /// Hold bytes (TCP backpressure), resume in order on heal.
    #[default]
    Stall,
    /// Close the connection; refuse new ones while cut.
    Reset,
}

#[derive(Default, Clone)]
struct Rules {
    cut_links: BTreeSet<(usize, usize)>,
    intra_cut_dst: BTreeSet<usize>,
    mode: CutMode,
    delay: Duration,
}

enum Verdict {
    Pass,
    Stall,
    Reset,
}

/// The shared, dynamically-updated fault table all proxies consult.
pub struct Faults {
    rules: Mutex<Rules>,
    gen_tx: watch::Sender<u64>,
}

impl Faults {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            rules: Mutex::new(Rules::default()),
            gen_tx: watch::channel(0).0,
        })
    }

    fn bump(&self) {
        self.gen_tx.send_modify(|g| *g += 1);
    }

    /// Partition the cluster into `groups`: every directed internal link
    /// between different groups is cut, and `intra` traffic *to* every node
    /// outside the largest group is cut (the intra wire carries no sender,
    /// so a node on the minority side can still forward its own clients'
    /// requests outward - documented in `docs/chaos.md`).
    pub fn partition(&self, groups: &[Vec<usize>], mode: CutMode) {
        let mut r = self.rules.lock().unwrap();
        r.cut_links.clear();
        r.intra_cut_dst.clear();
        for (gi, g) in groups.iter().enumerate() {
            for (hi, h) in groups.iter().enumerate() {
                if gi != hi {
                    for &a in g {
                        for &b in h {
                            r.cut_links.insert((a, b));
                        }
                    }
                }
            }
        }
        let largest = groups
            .iter()
            .enumerate()
            .max_by_key(|(i, g)| (g.len(), std::cmp::Reverse(*i)))
            .map(|(i, _)| i);
        for (i, g) in groups.iter().enumerate() {
            if Some(i) != largest {
                r.intra_cut_dst.extend(g.iter().copied());
            }
        }
        r.mode = mode;
        drop(r);
        self.bump();
    }

    /// Cut one direction of one link (`from` -> `to`) on the Raft wire only.
    pub fn cut_one_way(&self, from: usize, to: usize, mode: CutMode) {
        let mut r = self.rules.lock().unwrap();
        r.cut_links.insert((from, to));
        r.mode = mode;
        drop(r);
        self.bump();
    }

    pub fn set_delay(&self, delay: Duration) {
        self.rules.lock().unwrap().delay = delay;
        self.bump();
    }

    /// Remove every cut and delay.
    pub fn heal(&self) {
        *self.rules.lock().unwrap() = Rules::default();
        self.bump();
    }

    fn delay(&self) -> Duration {
        self.rules.lock().unwrap().delay
    }

    fn verdict(&self, kind: PortKind, src: Option<usize>, dst: usize) -> Verdict {
        let r = self.rules.lock().unwrap();
        let cut = match kind {
            PortKind::Other => false,
            PortKind::Intra => r.intra_cut_dst.contains(&dst),
            PortKind::Internal => src.is_some_and(|s| r.cut_links.contains(&(s, dst))),
        };
        match (cut, r.mode) {
            (false, _) => Verdict::Pass,
            (true, CutMode::Stall) => Verdict::Stall,
            (true, CutMode::Reset) => Verdict::Reset,
        }
    }
}

const UNKNOWN: usize = usize::MAX;

#[derive(Clone)]
struct Conn {
    faults: Arc<Faults>,
    kind: PortKind,
    dst: usize,
    src: Arc<AtomicUsize>,
}

impl Conn {
    fn src(&self) -> Option<usize> {
        match self.src.load(Ordering::Relaxed) {
            UNKNOWN => None,
            s => Some(s),
        }
    }

    /// Wait until this connection may forward. `Err` = reset it.
    async fn wait_pass(&self) -> Result<(), ()> {
        loop {
            // Subscribe before judging so a heal between the two is seen.
            let mut rx = self.faults.gen_tx.subscribe();
            match self.faults.verdict(self.kind, self.src(), self.dst) {
                Verdict::Pass => return Ok(()),
                Verdict::Reset => return Err(()),
                Verdict::Stall => {
                    let _ = rx.changed().await;
                }
            }
        }
    }
}

/// Incrementally decodes the sender id from a connection's first bytes.
#[derive(Default)]
struct Sniffer {
    buf: Vec<u8>,
}

enum Sniff {
    Need,
    Found(String),
    GiveUp,
}

impl Sniffer {
    fn feed(&mut self, bytes: &[u8]) -> Sniff {
        self.buf.extend_from_slice(bytes);
        let b = &self.buf;
        if b.len() < 7 {
            return Sniff::Need;
        }
        let ext_len = u16::from_le_bytes([b[5], b[6]]) as usize;
        let from_at = 7 + ext_len;
        if b.len() < from_at + 4 {
            return Sniff::Need;
        }
        let from_len =
            u32::from_be_bytes([b[from_at], b[from_at + 1], b[from_at + 2], b[from_at + 3]])
                as usize;
        if from_len == 0 || from_len > 256 {
            return Sniff::GiveUp;
        }
        if b.len() < from_at + 4 + from_len {
            return Sniff::Need;
        }
        match std::str::from_utf8(&b[from_at + 4..from_at + 4 + from_len]) {
            Ok(s) => Sniff::Found(s.to_string()),
            Err(_) => Sniff::GiveUp,
        }
    }
}

/// Aborts its task when dropped.
struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Start a proxy: accept on `listen`, forward to `target`. Runs until the
/// returned handle is aborted (dropping the accept loop drops every
/// connection task it owns).
pub async fn spawn_proxy(
    listen: SocketAddr,
    target: SocketAddr,
    kind: PortKind,
    dst: usize,
    faults: Arc<Faults>,
    names: Arc<Vec<String>>,
) -> std::io::Result<JoinHandle<()>> {
    let listener = TcpListener::bind(listen).await?;
    Ok(tokio::spawn(async move {
        let mut conns = JoinSet::new();
        loop {
            tokio::select! {
                acc = listener.accept() => {
                    let Ok((client, _)) = acc else { continue };
                    let _ = client.set_nodelay(true);
                    let conn = Conn {
                        faults: Arc::clone(&faults),
                        kind,
                        dst,
                        src: Arc::new(AtomicUsize::new(UNKNOWN)),
                    };
                    conns.spawn(handle(client, target, conn, Arc::clone(&names)));
                }
                // Reap finished connection tasks so the set stays small.
                Some(_) = conns.join_next(), if !conns.is_empty() => {}
            }
        }
    }))
}

async fn handle(client: TcpStream, target: SocketAddr, conn: Conn, names: Arc<Vec<String>>) {
    // Admission: an `intra` connection to a cut destination never reaches
    // the node (stalled until heal, or closed in reset mode).
    if conn.kind == PortKind::Intra && conn.wait_pass().await.is_err() {
        return;
    }
    // Node down => refuse like a dead port would.
    let Ok(upstream) = TcpStream::connect(target).await else {
        return;
    };
    let _ = upstream.set_nodelay(true);
    let (crd, cwr) = client.into_split();
    let (urd, uwr) = upstream.into_split();
    let sniff = conn.kind == PortKind::Internal;
    let mut c2s = Box::pin(pump(crd, uwr, conn.clone(), sniff, Arc::clone(&names)));
    let mut s2c = Box::pin(pump(urd, cwr, conn, false, names));
    tokio::select! {
        r = &mut c2s => { if r.is_ok() { let _ = s2c.await; } }
        r = &mut s2c => { if r.is_ok() { let _ = c2s.await; } }
    }
}

/// Copy `rd` -> `wr` honouring delay and cuts. `Err` = reset the connection.
async fn pump(
    mut rd: OwnedReadHalf,
    mut wr: OwnedWriteHalf,
    conn: Conn,
    sniff: bool,
    names: Arc<Vec<String>>,
) -> Result<(), ()> {
    let (tx, mut rx) = mpsc::channel::<(Instant, Vec<u8>)>(64);
    let reader_conn = conn.clone();
    let _reader = AbortOnDrop(tokio::spawn(async move {
        let mut buf = vec![0u8; 16 * 1024];
        let mut sniffer = sniff.then(Sniffer::default);
        let mut held: Vec<u8> = Vec::new();
        loop {
            let n = match rd.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(n) => n,
            };
            let mut out = buf[..n].to_vec();
            if let Some(s) = sniffer.as_mut() {
                match s.feed(&out) {
                    Sniff::Need => {
                        // Hold until the sender is known so a cut link can't
                        // leak its first frame.
                        held.extend_from_slice(&out);
                        continue;
                    }
                    Sniff::Found(name) => {
                        if let Some(i) = names.iter().position(|x| *x == name) {
                            reader_conn.src.store(i, Ordering::Relaxed);
                        }
                        sniffer = None;
                    }
                    Sniff::GiveUp => sniffer = None,
                }
                if !held.is_empty() {
                    let mut all = std::mem::take(&mut held);
                    all.extend_from_slice(&out);
                    out = all;
                }
            }
            let deadline = Instant::now() + reader_conn.faults.delay();
            if tx.send((deadline, out)).await.is_err() {
                return;
            }
        }
        if !held.is_empty() {
            let _ = tx.send((Instant::now(), held)).await;
        }
    }));
    while let Some((deadline, data)) = rx.recv().await {
        tokio::time::sleep_until(deadline).await;
        conn.wait_pass().await?;
        if wr.write_all(&data).await.is_err() {
            return Err(());
        }
    }
    let _ = wr.shutdown().await;
    Ok(())
}
