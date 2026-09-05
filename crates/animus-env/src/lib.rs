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
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Real-time/real-IO/real-RNG implementation ([`ProdEnv`], [`FsSegmentStore`],
/// `PreBindRng`) — gated behind the default-off `prod` feature (ADR 0061
/// rung C0) so a crate that depends on `animus-env` with
/// `default-features = false` genuinely cannot name `ProdEnv`: the boundary
/// `animus-node` (ADR 0061 Phase C) needs is compiler-enforced, not just a
/// convention. Every current consumer that actually constructs a `ProdEnv`
/// opts in explicitly with `features = ["prod"]`.
#[cfg(feature = "prod")]
pub mod prod;
#[cfg(feature = "prod")]
pub use prod::{FsSegmentStore, ProdEnv};

/// TLS material for the intra-node wire (ADR 0064, S-01 step 1) — gated
/// alongside `prod.rs` since it exists only to serve `ProdEnv`'s real
/// sockets. See the module's own doc for `TlsConfig`/`TlsMaterial`/
/// `MaybeTlsStream` and the certificate SAN requirement.
#[cfg(feature = "prod")]
pub mod tls;
#[cfg(feature = "prod")]
pub use tls::{MaybeTlsStream, TlsConfig, TlsMaterial};

pub mod metrics;
pub use metrics::{Metric, MetricSink, MetricSnapshot, MetricsHandle};

pub mod test_support;

/// Stable identifier for a node in the cluster.
///
/// **ADR 0040 PR3**: `NodeId` is now a validated, opaque **string** wrapped in
/// an `Arc<str>` (dropping `Copy` — every `.copied()` over a `NodeId` became
/// `.cloned()`; the clone is one refcount bump, not a byte copy). Three ways
/// to build one:
///
/// - [`NodeId::propose`] — validates the charset (`[A-Za-z0-9._-]{1,64}`,
///   rejecting `@` — the leader-hint wire format is `leader_hint={id}@{addr}`
///   — and any other punctuation/whitespace/`/`) and is the **only** path a
///   node-supplied identity (config `id` field, `--id`, an admin `add`
///   request) may go through. Every intake boundary must call this, not
///   construct a `NodeId` some other way.
/// - [`NodeId::mint`] (ADR 0040 PR4) — self-mints a random 22-char id off the
///   `Rng` seam for a node that doesn't propose an explicit one. Never
///   trusted probabilistically unique on its own — see its own doc and
///   `animus-control`'s `MetaCommand::RegisterNode` (the registration CAS
///   that makes uniqueness structural, not statistical).
/// - [`NodeId::new_unchecked`] — bypasses validation. Reserved for
///   deserializing an id that was already validated once (wire frames, WAL
///   replay, `serde` round-trips of already-stored `Metadata`) and for the
///   test-support [`nid`] helper. Never call this on untrusted input.
///
/// `Display`/`Debug` print the raw string; `serde` is `#[serde(transparent)]`
/// (a plain JSON string) but note this means `serde`-driven deserialization
/// (e.g. `serde_json::from_str` on a whole config file) does **not** run
/// [`NodeId::propose`]'s charset check — callers that accept configuration
/// from outside this process must explicitly re-validate every parsed id
/// (see `animusd::config` for the config-load call site).
///
/// See `docs/adr/0040-self-minted-string-node-ids.md`.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NodeId(Arc<str>);

/// A proposed node id failed [`NodeId::propose`]'s charset validation.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error(
    "invalid node id {raw:?}: ids must be 1-64 chars of [A-Za-z0-9._-] \
     (no '@', '/', or whitespace)"
)]
pub struct InvalidNodeId {
    /// The rejected input, for the error message.
    pub raw: String,
}

/// The accepted charset for a *proposed* node id (config/CLI/admin intake):
/// ASCII letters, digits, `.`, `_`, `-`. Deliberately excludes `@` (the
/// leader-hint wire format is `leader_hint={id}@{addr}` — `topology.rs`),
/// `/`, and whitespace.
fn is_valid_node_id_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-'
}

/// Maximum length (chars) of a proposed node id.
pub const NODE_ID_MAX_LEN: usize = 64;

impl NodeId {
    /// Validate and wrap a node-proposed id. The only sanctioned entry point
    /// for an identity a human or config file chose — see the type's doc
    /// comment for why every intake boundary must go through this.
    ///
    /// # Errors
    /// Returns [`InvalidNodeId`] if `s` is empty, longer than
    /// [`NODE_ID_MAX_LEN`] chars, or contains any character outside
    /// `[A-Za-z0-9._-]`.
    pub fn propose(s: &str) -> Result<NodeId, InvalidNodeId> {
        if s.is_empty()
            || s.chars().count() > NODE_ID_MAX_LEN
            || !s.chars().all(is_valid_node_id_char)
        {
            return Err(InvalidNodeId { raw: s.to_string() });
        }
        Ok(NodeId(Arc::from(s)))
    }

    /// Wrap a string as a [`NodeId`] **without** charset validation.
    ///
    /// Reserved for: deserializing an id that was already validated once
    /// (wire frames, WAL/snapshot replay, an already-stored `Metadata`
    /// round-tripping through `serde`), and the [`nid`] test-support
    /// constructor. Never call this on a node/operator-supplied string —
    /// use [`NodeId::propose`] instead.
    #[must_use]
    pub fn new_unchecked(s: impl Into<Arc<str>>) -> Self {
        NodeId(s.into())
    }

    /// The raw string, borrowed.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Self-mint a fresh id from 128 bits drawn off the [`Rng`] seam (ADR
    /// 0040 Decision B/C): two [`Rng::next_u64`] draws packed into 16 bytes,
    /// base64url-encoded (unpadded) into a 22-char string. Every character of
    /// the base64url alphabet (`A-Za-z0-9-_`) already lies within
    /// [`NodeId::propose`]'s accepted charset, so a minted id is always
    /// structurally valid without re-running that check — this bypasses it
    /// directly via [`NodeId::new_unchecked`], the same way the test-support
    /// [`nid`] helper does.
    ///
    /// **Never trusted probabilistically as globally unique on its own** —
    /// uniqueness is enforced by a registration compare-and-swap on the
    /// replicated cluster state (`MetaCommand::RegisterNode`, `animus-control`),
    /// not by this draw. A caller that hits a collision (astronomically
    /// unlikely, but structurally possible) re-mints and retries. See
    /// `docs/adr/0040-self-minted-string-node-ids.md` Decision C.
    ///
    /// Sim callers pass a `SimEnv` handle (its own seeded `Rng`, so minting
    /// stays a pure function of the run's seed); production join paths mint
    /// at the CLI boundary via [`prod::PreBindRng`] — the sanctioned home of
    /// real entropy for a process that has no bound `Env` yet.
    #[must_use]
    pub fn mint<R: Rng + ?Sized>(rng: &R) -> NodeId {
        let hi = rng.next_u64();
        let lo = rng.next_u64();
        let mut bytes = [0u8; 16];
        bytes[0..8].copy_from_slice(&hi.to_be_bytes());
        bytes[8..16].copy_from_slice(&lo.to_be_bytes());
        NodeId::new_unchecked(base64url_nopad(&bytes))
    }
}

/// Unpadded base64url-encode `bytes` (RFC 4648 §5 alphabet: `A-Za-z0-9-_`,
/// no `=` padding) — hand-rolled rather than a new dependency, mirroring this
/// codebase's existing hand-rolled-primitives convention (e.g. `animusd`'s
/// HTTP parser). The only caller is [`NodeId::mint`] (16 bytes in, 22 chars
/// out); kept general over the input length since there is nothing
/// `NodeId`-specific about the encoding itself.
fn base64url_nopad(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b0 = u32::from(chunk[0]);
        let b1 = u32::from(*chunk.get(1).unwrap_or(&0));
        let b2 = u32::from(*chunk.get(2).unwrap_or(&0));
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(ALPHABET[((n >> 18) & 0x3F) as usize] as char);
        out.push(ALPHABET[((n >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            out.push(ALPHABET[((n >> 6) & 0x3F) as usize] as char);
        }
        if chunk.len() > 2 {
            out.push(ALPHABET[(n & 0x3F) as usize] as char);
        }
    }
    out
}

impl std::str::FromStr for NodeId {
    type Err = InvalidNodeId;

    /// Parses via [`NodeId::propose`] — CLI/config text always goes through
    /// validation.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        NodeId::propose(s)
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&*self.0, f)
    }
}

impl std::fmt::Debug for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&*self.0, f)
    }
}

/// Test-support constructor: `nid(n)` builds the `n`th test node id, formatted
/// `"n{n}"` (e.g. `nid(2)` is `"n2"`).
///
/// Homed here (no feature gate — it's trivial) so every crate's test code can
/// reach it without duplicating a helper. Introduced in ADR 0040 PR2 as
/// `NodeId::new(n)` (then a bare `u64` newtype) so the mechanical sweep of
/// `~195 sim.env(...)` and `~89 RaftCore::new`/`RaftNode::start` call sites
/// across the test fleet happened exactly once; ADR 0040 PR3 reformats this
/// function's body to mint the `"n{n}"` string and no test call site needed to
/// change again. Bypasses [`NodeId::propose`] validation via
/// [`NodeId::new_unchecked`] (deliberately — the `"n{n}"` shape is always
/// valid, and this must stay infallible for the ~89 call sites that use it in
/// non-`Result` contexts).
#[must_use]
pub fn nid(n: u64) -> NodeId {
    NodeId::new_unchecked(format!("n{n}"))
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

/// A **wall-clock** instant: milliseconds since the Unix epoch (1970-01-01
/// UTC).
///
/// Deliberately a separate type from [`Nanos`], which is monotonic-only and
/// carries no calendar meaning. The two are not interchangeable and neither
/// converts into the other: `Nanos` answers "how long since this env started",
/// `UnixMillis` answers "what time is it". Reach for this **only** where a
/// calendar timestamp is part of an external contract the database does not
/// get to define — a DynamoDB TTL attribute is an absolute epoch second chosen
/// by the client (ADR 0051), so no monotonic reading can interpret it. System
/// timing (timeouts, elections, retries, backoff) must keep using
/// [`Clock::now`]: it is monotonic, and a wall clock can jump backwards.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct UnixMillis(pub u64);

impl UnixMillis {
    /// Whole seconds since the epoch (truncating) — the unit DynamoDB's TTL
    /// attribute itself is denominated in.
    #[must_use]
    pub fn as_secs(self) -> u64 {
        self.0 / 1_000
    }

    /// A wall-clock instant from whole epoch seconds (saturating).
    #[must_use]
    pub fn from_secs(secs: u64) -> Self {
        UnixMillis(secs.saturating_mul(1_000))
    }

    /// Saturating addition of a [`Duration`].
    #[must_use]
    pub fn saturating_add(self, dur: Duration) -> UnixMillis {
        UnixMillis(
            self.0
                .saturating_add(dur.as_millis().min(u128::from(u64::MAX)) as u64),
        )
    }
}

/// Monotonic clock and sleeping.
#[async_trait::async_trait]
pub trait Clock: Send + Sync {
    /// The current monotonic instant.
    fn now(&self) -> Nanos;

    /// The current **wall-clock** time (see [`UnixMillis`]).
    ///
    /// This is the one seam through which calendar time enters the system, and
    /// it is still a seam: under simulation it is a pure function of the run's
    /// virtual clock (a fixed epoch base plus elapsed virtual time, plus any
    /// per-node clock skew), so a TTL sweep stays as reproducible from its seed
    /// as everything else (ADR 0003). Under production it is the host's real
    /// clock and may therefore jump — forwards or backwards — with NTP.
    ///
    /// **Never** use this for timing. Deadlines, timeouts, elections, backoff,
    /// and every other interval measurement use [`now`](Clock::now); a
    /// backwards wall-clock step must never be able to stall the system.
    fn wall_now(&self) -> UnixMillis;

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
/// ADR 0017 D) opens a second stream on the existing env instead of minting a
/// whole new `NodeId` — this superseded the old `Coresident`/`sibling`
/// escape hatch outright, which is why that trait is gone (ADR 0040 PR5).
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
    /// production lists the env's own data directory, non-recursively. The
    /// enumeration primitive teardown paths need to find every artifact of a
    /// prefix-named component (e.g. a dropped tablet's `db-t{id}-*` LSM files)
    /// without knowing the exact set.
    async fn list(&self) -> std::io::Result<Vec<String>>;

    /// Create a hard link at `dst` over `src`'s current durable bytes — a
    /// directory-entry-only operation, no data copy (ADR 0058 rung 2: the
    /// primitive an SSTable-granularity engine clone needs to share immutable
    /// files between the source and a new target engine instead of copying
    /// their bytes).
    ///
    /// `dst` becomes an independent name over the same durable bytes `src`
    /// names *at the moment of this call*. A later [`remove`](Disk::remove)
    /// of either name never affects the other — the underlying bytes persist
    /// as long as at least one name references them, exactly like a real
    /// filesystem hard link. This is safe to use for sharing a file between
    /// two live engines specifically *because* the caller only ever links
    /// files it treats as immutable once written (an SSTable, never mutated
    /// in place after the manifest swap that makes it live) — this trait
    /// does not itself enforce that discipline, the same way it does not
    /// enforce anything else about a caller's file-naming convention.
    ///
    /// **Overwrites `dst` if it already exists** (like
    /// [`replace`](Disk::replace), but backed by a link rather than a byte
    /// copy), so the primitive is idempotent on retry: relinking the same
    /// `(src, dst)` pair after a crash mid-clone reproduces the same durable
    /// state rather than erroring on an already-present name.
    ///
    /// On return the new directory entry is durable — production fsyncs the
    /// containing directory, mirroring [`append`](Disk::append)/
    /// [`replace`](Disk::replace)'s "namespace changes are fsynced" rule —
    /// so no follow-up [`sync`](Disk::sync) call is needed for the link
    /// itself.
    ///
    /// Returns a `NotFound` error if `src` does not exist.
    async fn link(&self, src: &str, dst: &str) -> std::io::Result<()>;
}

/// A store for immutable, content-addressed byte blobs — the stream-shard
/// subsystem's sealed segments (ADR 0043 §A7), addressed by an opaque `id`
/// (production ids are `{table}/{label}/{tablet}/{epoch}`, ADR 0043 §A3, but
/// this trait imposes no structure on `id` beyond treating it as a string a
/// filesystem-backed implementation may map to a path).
///
/// Lives beside the other seams (`Clock`/`Rng`/`Network`/`Disk`/`Spawner`)
/// but is **deliberately not** part of the [`Env`] supertrait (ADR 0043 §A7,
/// decision F5): every call site threads an explicit `SegmentStore` handle,
/// the same way a `StorageEngine` handle is threaded rather than folded into
/// `Env`. This lets a component's choice of store vary independently of its
/// `Env` (a sim test pairs a `SimEnv` with a fault-injecting store;
/// production pairs `ProdEnv` with the cluster-replicated default), and
/// keeps `Env` itself free of a dependency on the stream subsystem.
///
/// **Consistency contract** (binding on every implementation):
/// - **Read-after-put**: once [`put`](SegmentStore::put) returns `Ok`, every
///   reader's subsequent [`get`](SegmentStore::get) of that `id` sees the
///   bytes just written — never a value older than the last acknowledged
///   put.
/// - **Write-once (as-built amendment — this was originally "idempotent
///   overwrite, last-write-wins"; see below for why that changed).**
///   Putting an id that already holds **byte-identical** content is `Ok`, a
///   safe no-op (a same-attempt retry after a lost ack, or a repair sweep
///   copying the exact same bytes to a fresh replica that lacks them, land
///   here). Putting an id that already holds **different** content is a
///   hard `Err` — this trait now enforces write-once itself, rather than
///   leaving "first write wins" as a caller-owned policy on top of a
///   last-write-wins store. **Why this changed**: two independently-computed
///   seal attempts for the same catalog `(tablet, epoch)` used to derive the
///   identical deterministic id and race their physical `put`s — the
///   replicated catalog's own `SealStreamShard` apply arm correctly picked
///   one winner (first-committer-wins on content), but this store had no
///   matching adjudication, so whichever `put` physically landed *last* won
///   the bytes, independent of which attempt's *catalog proposal* won. When
///   the chronologically-later `put` carried a *smaller* range than the
///   catalog's own committed one, the gap was silently, permanently lost —
///   a real bug that shipped undetected (see
///   `docs/engineering-lessons.md` and `animus_cp_data::segment`'s own doc
///   for the full incident). The structural fix is upstream of this trait
///   (every caller now writes each attempt at its own unique id,
///   `animus_cp_data::segment::segment_object_id` — no two attempts can ever
///   collide on the same id again), but this trait's own contract is
///   tightened too: a caller that *does* accidentally reuse an id for
///   genuinely different content now gets a loud `Err` instead of a silent
///   overwrite, closing the hazard by construction rather than by every
///   caller's continued discipline.
/// - **Immutability once cataloged, as defense-in-depth.** An id recorded in
///   the replicated segment catalog (`MetaCommand::SealStreamShard`) is
///   treated as immutable by every reader. With write-once enforced by this
///   trait, a cataloged id's bytes can now never change at all after the
///   fact — the "superset-slice rule" (ADR 0042 §10 / ADR 0043 §A3, a reader
///   slicing a fetched object down to the catalog row's own committed
///   range) predates this amendment and is no longer load-bearing for the
///   race it was written for, but stays in place as cheap, harmless
///   defense-in-depth (see `animus_cp_data::segment`'s own module doc).
/// - **`get` returning `None` after a [`delete`](SegmentStore::delete)** is
///   a defined, expected outcome (surfaces to a stream consumer as
///   `TrimmedDataAccess`), never an error. `None` is also the answer for an
///   id that was never written.
/// - [`delete`](SegmentStore::delete) is idempotent: deleting an absent id
///   is `Ok`, not an error.
/// - [`list`](SegmentStore::list) is **debug/sweep-only** (retention,
///   repair, operator introspection). It is never on the read path for
///   serving a stream record, and no caller may treat its result as
///   authoritative for correctness — only the replicated catalog is.
///
/// See `docs/adr/0043-stream-shard-subsystem.md` §A7/§A7b for the full
/// design and each implementation's own contract:
/// [`FsSegmentStore`](crate::FsSegmentStore) (single directory, opt-in) and
/// `SimSegmentStore` (`animus-sim`, seeded + fault-injectable); the default
/// `ClusterSegmentStore` (K-way replicated) lands in a later PR.
#[async_trait::async_trait]
pub trait SegmentStore: Send + Sync {
    /// Write `bytes` at `id`. Durable, per this implementation's own
    /// durability contract, once this returns `Ok`. **Write-once**: `Ok` if
    /// `id` is unwritten or already holds byte-identical content (a safe
    /// no-op); `Err` if `id` already holds *different* content — see the
    /// trait's own doc for why. Every real caller should be writing each
    /// attempt at its own unique id (`animus_cp_data::segment::
    /// segment_object_id`) and never actually hit the `Err` case in
    /// practice.
    async fn put(&self, id: &str, bytes: &[u8]) -> io::Result<()>;

    /// Fetch the bytes at `id`, or `None` if `id` was never written or has
    /// since been [`delete`](SegmentStore::delete)d — a defined outcome, not
    /// an error.
    async fn get(&self, id: &str) -> io::Result<Option<Vec<u8>>>;

    /// Remove `id`. Idempotent: deleting an absent id is `Ok`.
    async fn delete(&self, id: &str) -> io::Result<()>;

    /// List every id currently starting with `prefix` — for debugging or a
    /// sweep (retention, repair), never load-bearing for serving a read.
    async fn list(&self, prefix: &str) -> io::Result<Vec<String>>;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    /// A deterministic, scripted [`Rng`] for testing `mint`'s draw shape —
    /// yields a fixed sequence of `u64`s, then falls back to a trivial
    /// incrementing counter once exhausted (so a test can script only the
    /// draws it cares about and let the rest be "whatever, just distinct").
    /// Atomics, not `Cell`, since [`Rng`] requires `Send + Sync`.
    struct ScriptedRng {
        script: Vec<u64>,
        pos: AtomicUsize,
        fallback: AtomicU64,
    }

    impl ScriptedRng {
        fn new(script: Vec<u64>) -> Self {
            ScriptedRng {
                script,
                pos: AtomicUsize::new(0),
                fallback: AtomicU64::new(0xF000_0000_0000_0000),
            }
        }
    }

    impl Rng for ScriptedRng {
        fn next_u64(&self) -> u64 {
            let i = self.pos.fetch_add(1, Ordering::Relaxed);
            if i < self.script.len() {
                self.script[i]
            } else {
                self.fallback.fetch_add(1, Ordering::Relaxed)
            }
        }

        fn fill_bytes(&self, dst: &mut [u8]) {
            for b in dst {
                *b = self.next_u64() as u8;
            }
        }
    }

    #[test]
    fn base64url_nopad_matches_known_vectors() {
        // RFC 4648 test vectors (base64url is identical to base64 except for
        // the last two alphabet characters, none of which these hit).
        assert_eq!(base64url_nopad(b""), "");
        assert_eq!(base64url_nopad(b"f"), "Zg");
        assert_eq!(base64url_nopad(b"fo"), "Zm8");
        assert_eq!(base64url_nopad(b"foo"), "Zm9v");
        assert_eq!(base64url_nopad(b"foob"), "Zm9vYg");
        assert_eq!(base64url_nopad(b"fooba"), "Zm9vYmE");
        assert_eq!(base64url_nopad(b"foobar"), "Zm9vYmFy");
    }

    #[test]
    fn base64url_nopad_never_emits_padding_or_standard_base64_chars() {
        let bytes: Vec<u8> = (0..=255).collect();
        let out = base64url_nopad(&bytes);
        assert!(!out.contains('='), "must be unpadded");
        assert!(!out.contains('+') && !out.contains('/'), "must be url-safe");
    }

    #[test]
    fn mint_produces_a_22_char_id_within_the_proposed_charset() {
        let rng = ScriptedRng::new(vec![0x1234_5678_9abc_def0, 0x0fed_cba9_8765_4321]);
        let id = NodeId::mint(&rng);
        assert_eq!(id.as_str().chars().count(), 22, "id: {id}");
        // Every minted id must independently pass `propose`'s own charset
        // check — a minted id that failed this would be silently invalid
        // wherever it later round-trips through `propose` (e.g. re-parsed
        // from a config file after being written there once).
        assert!(
            NodeId::propose(id.as_str()).is_ok(),
            "minted id {id} must satisfy NodeId::propose's charset"
        );
    }

    #[test]
    fn mint_is_a_pure_function_of_the_rng_draws() {
        // Same scripted draws in, same id out — minting itself has no hidden
        // state; determinism (under `SimEnv`) rests entirely on the `Rng`
        // seam being deterministic, not on anything in `mint`.
        let a = NodeId::mint(&ScriptedRng::new(vec![1, 2]));
        let b = NodeId::mint(&ScriptedRng::new(vec![1, 2]));
        assert_eq!(a, b);

        let c = NodeId::mint(&ScriptedRng::new(vec![1, 3]));
        assert_ne!(a, c, "a different draw must mint a different id");
    }

    #[test]
    fn many_mints_never_collide() {
        // Not a uniqueness *proof* (that's the registration CAS's job, ADR
        // 0040 Decision C) — just a sanity check that ordinary draws don't
        // trivially alias each other (e.g. an off-by-one in the byte
        // packing that silently discards entropy).
        let rng = ScriptedRng::new(Vec::new()); // pure fallback-counter draws
        let mut seen = std::collections::BTreeSet::new();
        for _ in 0..2000 {
            assert!(seen.insert(NodeId::mint(&rng)), "unexpected mint collision");
        }
    }
}
