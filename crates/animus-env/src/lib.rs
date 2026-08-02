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

pub mod prod;
pub use prod::ProdEnv;

pub mod metrics;
pub use metrics::{Metric, MetricSink, MetricSnapshot, MetricsHandle};

/// Stable identifier for a node in the cluster.
pub type NodeId = u64;

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

/// A message delivered to a node over the [`Network`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Envelope {
    /// The node that sent this message.
    pub from: NodeId,
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
/// `send` is fire-and-forget: it never reports delivery (the network may delay,
/// reorder, or drop, and under partition it silently discards), matching the
/// reality that a successful local send guarantees nothing. `recv` yields the
/// next message addressed to this node.
#[async_trait::async_trait]
pub trait Network: Send + Sync {
    /// Hand a payload to the network for delivery to `to`.
    async fn send(&self, to: NodeId, payload: Vec<u8>);

    /// Await the next message addressed to this node.
    async fn recv(&self) -> Envelope;
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
