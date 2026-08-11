//! The `Env` seam: the single point through which AnimusDB accesses time,
//! randomness, the network, disk, and task scheduling.
//!
//! This is the load-bearing constraint of the whole project (see
//! `docs/adr/0003-deterministic-simulation.md`). System code is written generic
//! over `E: Env` — monomorphized, never `dyn` — and *never* calls the wall
//! clock, spawns raw tasks, touches real sockets or files, or uses unseeded
//! randomness directly. In production it is instantiated with [`ProdEnv`]; under
//! test it is instantiated with the deterministic `SimEnv` from `animus-sim`.
//!
//! [`Env`] is a *supertrait*: a handle that implements [`Clock`], [`Rng`],
//! [`Network`], [`Disk`], and [`Spawner`] all at once, scoped to a single node
//! (its [`NodeId`]). Components hold an `E: Env` and call `env.now()`,
//! `env.send(..)`, `env.recv()`, `env.spawn(..)`, and so on directly.

use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

use serde::{Deserialize, Serialize};

pub mod prod;
pub use prod::ProdEnv;

pub mod metrics;
pub use metrics::{Metric, MetricSink, MetricSnapshot, MetricsHandle};

/// Stable identifier for a node in the cluster.
///
/// **ADR 0040 PR2**: an opaque newtype over `u64` — the *representation*
/// stays a plain `u64` (this PR is behavior-, wire-, and WAL-byte-neutral;
/// `#[serde(transparent)]` means every JSON/WAL byte this type touches is
/// identical to the bare `u64` this replaces), but the *type* no longer
/// supports arithmetic. That's deliberate: it lets the compiler enumerate
/// every remaining numeric-coupling site in one mechanical sweep, before PR3
/// changes the representation to a validated string and none of those sites
/// can quietly keep assuming "`NodeId` is a small dense integer" — see
/// `docs/adr/0040-self-minted-string-node-ids.md`.
///
/// Construct one with [`NodeId::new`] (or `From<u64>`/[`nid`]); recover the
/// raw value with [`NodeId::as_u64`] — kept deliberately narrow (only the few
/// sites that must serialize/format the id as a number: metrics labels,
/// dashboard JSON, `ProdEnv`'s wire frame, `syskv`'s big-endian key encoding,
/// and the one pre-existing `ALLOC_ID_BASE` allocator arithmetic PR4 retires)
/// rather than a broad numeric API that would defeat the point of this PR.
#[derive(Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NodeId(u64);

impl NodeId {
    /// Wrap a raw `u64` as a [`NodeId`].
    #[must_use]
    pub const fn new(id: u64) -> Self {
        NodeId(id)
    }

    /// Recover the raw `u64` — the narrow escape hatch for the handful of
    /// sites that must serialize/format the id as a number (see the type's
    /// doc comment).
    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl From<u64> for NodeId {
    fn from(id: u64) -> Self {
        NodeId(id)
    }
}

impl std::str::FromStr for NodeId {
    type Err = std::num::ParseIntError;

    /// Parses a `u64` and wraps it — this PR keeps CLI/config/wire id
    /// parsing semantics identical (parse the number, then wrap); PR3 is
    /// where the accepted charset changes to a validated string.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse::<u64>().map(NodeId)
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl std::fmt::Debug for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self.0, f)
    }
}

/// Test-support constructor: `nid(n)` builds the `n`th test node id.
///
/// Homed here (no feature gate — it's trivial) so every crate's test code can
/// reach it without duplicating a helper. Introduced in ADR 0040 PR2 so the
/// mechanical sweep of `~195 sim.env(...)` and `~89 RaftCore::new`/
/// `RaftNode::start` call sites across the test fleet happens exactly once:
/// PR3 reformats this function's body to mint `"n{n}"` strings and no test
/// call site needs to change again.
#[must_use]
pub const fn nid(n: u64) -> NodeId {
    NodeId::new(n)
}

/// A monotonic instant, measured in nanoseconds since the environment started.
///
/// Under simulation this is virtual time; under production it is measured from a
/// fixed start instant. It is deliberately *not* a wall-clock time: system code
/// must never depend on calendar time.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Nanos(pub u64);

impl Nanos {
    /// Saturating addition of a [`Duration`].
    #[must_use]
    pub fn saturating_add(self, dur: Duration) -> Nanos {
        Nanos(
            self.0
                .saturating_add(dur.as_nanos().min(u128::from(u64::MAX)) as u64),
        )
    }

    /// Duration elapsed since an earlier instant (saturating at zero).
    #[must_use]
    pub fn duration_since(self, earlier: Nanos) -> Duration {
        Duration::from_nanos(self.0.saturating_sub(earlier.0))
    }
}

/// A boxed, `Send` future — the unit of work accepted by [`Spawner`].
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// The well-known stream every pre-multiplexing call site is implicitly on
/// (ADR 0026). Every protocol that predates multiplexed addressing — the
/// control plane, a non-split tablet's CP group, everything that only ever
/// calls [`Network::send`]/[`Network::recv`] — sends and receives on this
/// stream, so it needs zero call-site changes when a node gains a second
/// stream for some other protocol instance.
pub const PRIMARY_STREAM: u64 = 0;

/// A message delivered to a node over the [`Network`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Envelope {
    /// The node that sent this message.
    pub from: NodeId,
    /// Which logical stream on the destination node this message is for
    /// (ADR 0026). `(node, stream)` is single-consumer, generalizing the
    /// pre-multiplexing single-consumer-per-node invariant.
    pub stream: u64,
    /// The opaque payload. Higher layers define and (de)serialize their own
    /// message types; the network moves bytes.
    pub payload: Vec<u8>,
}

/// Monotonic clock and sleeping.
#[async_trait::async_trait]
pub trait Clock: Send + Sync {
    /// The current monotonic instant.
    fn now(&self) -> Nanos;

    /// Sleep until at least `dur` of (virtual or real) time has elapsed.
    async fn sleep(&self, dur: Duration);
}

/// Seeded pseudo-randomness. Uses interior mutability so a shared `&self` env
/// handle can advance the stream; under simulation the stream is fully
/// determined by the run's seed.
pub trait Rng: Send + Sync {
    /// Draw the next 64-bit value from the stream.
    fn next_u64(&self) -> u64;

    /// Fill `dst` with bytes from the stream.
    fn fill_bytes(&self, dst: &mut [u8]);

    /// Draw a value in `[0, n)`. Returns 0 when `n == 0`.
    ///
    /// Uses Lemire's debiased multiply-shift so the distribution is uniform, not
    /// modulo-biased. Determinism is preserved because every step is a pure
    /// function of the stream.
    fn gen_below(&self, n: u64) -> u64 {
        if n == 0 {
            return 0;
        }
        let mut x = self.next_u64();
        let mut m = u128::from(x) * u128::from(n);
        let mut low = m as u64;
        if low < n {
            let threshold = n.wrapping_neg() % n;
            while low < threshold {
                x = self.next_u64();
                m = u128::from(x) * u128::from(n);
                low = m as u64;
            }
        }
        (m >> 64) as u64
    }

    /// Draw a [`Duration`] in `[lo, hi]` nanoseconds inclusive.
    fn gen_duration(&self, lo: Duration, hi: Duration) -> Duration {
        let lo = lo.as_nanos().min(u128::from(u64::MAX)) as u64;
        let hi = hi.as_nanos().min(u128::from(u64::MAX)) as u64;
        if hi <= lo {
            return Duration::from_nanos(lo);
        }
        Duration::from_nanos(lo + self.gen_below(hi - lo + 1))
    }
}

/// Point-to-point message passing between nodes, scoped to *this* node.
///
/// `send`/`send_stream` are fire-and-forget: they never report delivery (the
/// network may delay, reorder, or drop, and under partition it silently
/// discards), matching the reality that a successful local send guarantees
/// nothing. `recv`/`recv_stream` yield the next message addressed to this node
/// (on a given stream).
///
/// **Multiplexed addressing (ADR 0026).** [`send_stream`](Network::send_stream)
/// and [`recv_stream`](Network::recv_stream) are the primitive methods a
/// `Network` implementation provides; `send`/`recv` are convenience defaults
/// over [`PRIMARY_STREAM`] so every call site written before this axis
/// existed — which is nearly all of them — needs no change and behaves
/// identically. A component that wants a *second* protocol instance
/// addressable on the same node (e.g. a per-tablet Raft group after a split,
/// ADR 0017 D) can now open a second stream on the existing env instead of
/// minting a whole new `NodeId` (the `Coresident` escape hatch this ADR aims to
/// eventually retire).
#[async_trait::async_trait]
pub trait Network: Send + Sync {
    /// Hand a payload to the network for delivery to `to` on `stream`.
    async fn send_stream(&self, to: NodeId, stream: u64, payload: Vec<u8>);

    /// Await the next message addressed to this node on `stream`. `(node,
    /// stream)` is single-consumer — never run two receive loops on the same
    /// node id and stream.
    async fn recv_stream(&self, stream: u64) -> Envelope;

    /// Send on [`PRIMARY_STREAM`] — the whole pre-multiplexing API surface.
    async fn send(&self, to: NodeId, payload: Vec<u8>) {
        self.send_stream(to, PRIMARY_STREAM, payload).await;
    }

    /// Receive on [`PRIMARY_STREAM`] — the whole pre-multiplexing API surface.
    async fn recv(&self) -> Envelope {
        self.recv_stream(PRIMARY_STREAM).await
    }
}

/// Append-structured durable storage, scoped to *this* node.
///
/// The model is intentionally minimal: named files that are appended to and
/// explicitly synced. Bytes written by [`append`](Disk::append) are *not*
/// durable until [`sync`](Disk::sync) returns; a crash loses un-synced bytes.
/// The simulator models exactly this, so durability bugs surface under test.
#[async_trait::async_trait]
pub trait Disk: Send + Sync {
    /// Append `bytes` to `file` (buffered; not yet durable).
    async fn append(&self, file: &str, bytes: &[u8]) -> std::io::Result<()>;

    /// Flush and fsync `file` so previously appended bytes become durable.
    async fn sync(&self, file: &str) -> std::io::Result<()>;

    /// Read the current contents of `file` (durable + buffered), or an empty
    /// vector if it does not exist.
    async fn read(&self, file: &str) -> std::io::Result<Vec<u8>>;

    /// Read up to `len` bytes from `file` starting at byte `offset` (over the
    /// durable + buffered view, like [`read`](Disk::read)). Returns fewer bytes
    /// at end-of-file and an empty vector if the file does not exist or `offset`
    /// is past the end. This is the random-access primitive an on-disk LSM uses
    /// to fetch a single SSTable block without loading the whole file.
    async fn read_at(&self, file: &str, offset: u64, len: usize) -> std::io::Result<Vec<u8>>;

    /// The current length of `file` in bytes (durable + buffered), or 0 if it
    /// does not exist.
    async fn size(&self, file: &str) -> std::io::Result<u64>;

    /// Delete `file`. A no-op (not an error) if it does not exist. Used to drop
    /// SSTables that compaction has superseded.
    async fn remove(&self, file: &str) -> std::io::Result<()>;

    /// Atomically replace `file`'s durable contents with `bytes`. On return the
    /// new contents are durable; a crash before or after sees the whole old or
    /// whole new contents, never a mix (production does this with a temp file +
    /// rename). Used for log/WAL compaction.
    async fn replace(&self, file: &str, bytes: &[u8]) -> std::io::Result<()>;

    /// The names of every file on this env's disk, in lexicographic order (empty
    /// if none exist yet). Only files this handle's `Disk` methods could open —
    /// production lists the env's own data directory, non-recursively (a
    /// co-resident sibling's `sib-<id>/` subdirectory is that sibling's disk, not
    /// this one's). The enumeration primitive teardown paths need to find every
    /// artifact of a prefix-named component (e.g. a dropped tablet's
    /// `db-t{id}-*` LSM files) without knowing the exact set.
    async fn list(&self) -> std::io::Result<Vec<String>>;
}

/// Task spawning. Under production this is `tokio::spawn`; under simulation it
/// enqueues the future on the cooperative, single-threaded run-queue so that
/// scheduling is deterministic.
pub trait Spawner: Send + Sync {
    /// Spawn a future to run concurrently. The future must be `Send + 'static`.
    fn spawn(&self, fut: BoxFuture<'static, ()>);
}

/// The environment supertrait: a cheap-to-clone handle, scoped to one node,
/// that provides all sources of nondeterminism behind one boundary.
pub trait Env: Clock + Rng + Network + Disk + Spawner + Clone + Send + Sync + 'static {
    /// The identity of the node this handle acts as.
    fn node_id(&self) -> NodeId;

    /// The metrics sink for this env (ADR 0015). Additive with a default: an env
    /// that does not record metrics returns the shared no-op handle
    /// ([`MetricsHandle::noop`]), so every existing `E: Env` implementation —
    /// `SimEnv` included — keeps compiling and behaving identically without
    /// change. `ProdEnv` overrides this to return its own recording handle; a
    /// component that wants to record into a test-readable sink under simulation
    /// is handed a recording [`MetricsHandle`] directly rather than relying on
    /// this default. Returning a handle (not `Option`) means recording sites need
    /// no `if let Some(..)` guard.
    fn metrics(&self) -> MetricsHandle {
        MetricsHandle::noop()
    }
}

/// Convenience extension for spawning an `async` block without writing
/// `Box::pin` at every call site.
pub trait EnvExt: Env {
    /// Spawn an `async` block as a background task on this environment.
    fn spawn_task<F>(&self, fut: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.spawn(Box::pin(fut));
    }
}

impl<E: Env> EnvExt for E {}

/// An `Env` that can mint a **sibling** handle on the same physical node bound to
/// a different [`NodeId`] — its own inbox, clock-, disk- and spawn-context-shared
/// with this one.
///
/// The `Network` inbox is single-consumer per `NodeId` (one `recv` loop per id),
/// so a node that hosts a *second* protocol instance — e.g. the new tablet's Raft
/// group after a split (ADR 0017 D) — needs a second id with its own inbox.
/// `sibling` is how a running component mints that id **in band** (from inside an
/// apply step) rather than relying on the test harness / process bootstrap to
/// pre-allocate every id up front.
///
/// This is a **separate** trait, not part of the [`Env`] supertrait: only the few
/// co-residency-aware components (the leaderful split path) bound on it, so every
/// other `E: Env` impl is unaffected and an `Env` that cannot multiplex inboxes
/// (a production transport keyed by one address) is simply not `Coresident`.
pub trait Coresident: Env {
    /// A fresh handle on this physical node bound to `id`, with its own inbox.
    /// `id` must be distinct from this handle's and from any other live instance
    /// on the node (the caller owns id allocation, as with the initial group).
    fn sibling(&self, id: NodeId) -> Self;
}
