# CLAUDE.md — animus-env

This file provides guidance to Claude Code (claude.ai/code) when working in this crate.

## Purpose

The `Env` seam: the single boundary through which all AnimusDB code reaches
time, randomness, the network, disk, and task spawning. Defines the traits and
the production implementation; the deterministic implementation lives in
`animus-sim`.

## Entry points

- `lib.rs` — the traits `Clock`, `Rng`, `Network`, `Disk`, `Spawner`, combined
  into the **`Env` supertrait** (scoped to one `NodeId`), plus `Nanos`,
  `Envelope`, `BoxFuture`, `PRIMARY_STREAM` (the default stream id the
  `send`/`recv` defaults ride on, ADR 0026), and the `EnvExt::spawn_task`
  convenience. Also `SegmentStore` (ADR 0043 §A7, below) — a seam that is
  deliberately **not** part of `Env`. All of this is available
  **unconditionally** (no feature needed) — none of it depends on `prod.rs`.
- `prod.rs` — `ProdEnv`: real monotonic clock, `OsRng`, `tokio::spawn`,
  length-prefixed TCP, `tokio::fs` + `fsync`. Owns a real recording metrics sink
  and exposes `metrics_text()` (ADR 0015). Also `FsSegmentStore` (below),
  the single-directory `SegmentStore` impl, since it does the same real
  `tokio::fs` I/O `ProdEnv` does.
- `tls.rs` — `TlsConfig`/`TlsMaterial`/`MaybeTlsStream` (ADR 0064, S-01):
  the intra-node wire's TLS material and transport wrapper. See "TLS on the
  intra-node wire" below.
- **`prod.rs`/`tls.rs` are gated behind a default-off `prod` Cargo feature
  (ADR 0061 rung C0)**, added specifically so `ProdEnv`/`FsSegmentStore`
  can be made compiler-unreachable from a crate's manifest, not just
  avoided by convention — the prerequisite for the `animus-node` carve-out
  (ADR 0061 Phase C) to be an enforced boundary. `pub mod prod;`/`pub mod
  tls;` and `pub use prod::{FsSegmentStore, ProdEnv};`/`pub use
  tls::{MaybeTlsStream, TlsConfig, TlsMaterial};` in `lib.rs` all carry
  `#[cfg(feature = "prod")]`. The trait definitions, `NodeId`, and
  `metrics.rs` need none of this and are never gated.
  **Only `tokio`, `rand`, `tracing`, `rustls`, `tokio-rustls`,
  `rustls-pemfile`, and `rustls-pki-types` are `prod`-only** and became
  `optional = true` dependencies pulled in by the feature
  (`async-trait`/`serde`/`thiserror` back the trait definitions and `NodeId`
  themselves, so they stay unconditional). Every current consumer that
  actually constructs a `ProdEnv`/`FsSegmentStore` opts in explicitly with
  `features = ["prod"]` — on the `[dependencies]` entry for a crate whose
  own *library* does the constructing (`animusd`, the one real process
  boundary before Phase C), or on a separate `[dev-dependencies]` entry for
  a crate that only needs it in tests/benches (`animus-control`,
  `animus-cp-data`, `animus-storage` — each has a real-thread `ProdEnv`
  liveness/regression test or, for `animus-storage`, `engine_bench` too).
  The dev-only placement doesn't yet *enforce* anything on its own — Cargo's
  resolver keeps a workspace member's dev-dependency features off its
  library build (verify with `cargo tree -e features -p <crate>
  --no-dev-dependencies`, which is exactly how this was checked), but a
  `cargo build --workspace` that also builds test/bench targets in the same
  invocation unifies features across everything being built in that
  invocation — it documents the real boundary correctly regardless. Prove
  the gate itself holds with a scratch crate depending on `animus-env { path
  = "...", default-features = false }`: naming `animus_env::ProdEnv` fails
  to compile with `error[E0425]: cannot find type ProdEnv in crate
  animus_env ... found an item that was configured out ... gated behind the
  prod feature`.
- `test_support.rs` — `assert_segment_store_contract` (below): a
  `#[doc(hidden)]`, always-compiled (not `#[cfg(test)]`) cross-crate test
  helper, since `#[cfg(test)]` only gates this crate's own test binaries and
  `animus-sim`'s `SimSegmentStore` tests need the same assertions.
- `metrics.rs` — the **observability seam** (ADR 0015): a closed `Metric` enum
  (`control_*` Raft + `storage_*` LSM-engine counters, plus legacy `data_*`
  leaderless-AP counters that are **dormant** — the AP plane was deleted, ADR
  0019, but the enum is append-only so the variants stay), a fixed-array
  lock-free `MetricSink`, the cheap-to-clone `MetricsHandle`, and the
  `MetricSnapshot` text export. The enum is **append-only**: add new variants
  *after* the existing ones (and a matching row in `Metric::ALL`) so slots and
  the export order stay stable and the snapshot remains byte-reproducible.

## What's non-obvious

- **`NodeId` is a validated, opaque string newtype (`Arc<str>`, ADR 0040
  PR3), not a type alias and not numeric.** It went through two mechanical
  stages before landing here: PR2 made it an opaque `u64` newtype (proving,
  via the compiler, that no call site outside a short sanctioned list did
  numeric arithmetic on it); PR3 then swapped the representation to
  `Arc<str>` (dropping `Copy` — every `.copied()` over a `NodeId` became
  `.cloned()`, one refcount bump, not a byte copy). `Clone, PartialEq, Eq,
  PartialOrd, Ord, Hash` + `Display`/`Debug` (the raw string) +
  `#[serde(transparent)]` (a plain JSON string — but this means `serde`
  deserialization does **not** run charset validation; see below). Three
  ways to build one — see the type's own doc for the full contract:
  - `NodeId::propose(s: &str) -> Result<NodeId, InvalidNodeId>` — the
    **only** sanctioned path for a node/operator/config-supplied identity
    (config `id` field, CLI `--id`, an admin `add` request). Validates
    `[A-Za-z0-9._-]{1,64}` (excludes `@` — the leader-hint wire format is
    `leader_hint={id}@{addr}` — and `/`/whitespace). `FromStr` delegates to
    this, so `s.parse::<NodeId>()` at a CLI boundary is the idiom.
  - `NodeId::mint<R: Rng + ?Sized>(rng: &R) -> NodeId` (ADR 0040 Decision
    B/C, PR4) — self-mints a fresh id for a node that doesn't propose an
    explicit one: two `Rng::next_u64` draws packed into 16 bytes,
    base64url-encoded (unpadded, hand-rolled — no new dependency) into a
    22-char string; every base64url character already satisfies
    `propose`'s charset, so this bypasses that check via `new_unchecked`
    directly. **Never trusted probabilistically unique on its own** —
    uniqueness is enforced by `animus-control`'s `MetaCommand::RegisterNode`
    registration CAS on the replicated cluster state; a caller that hits a
    (astronomically unlikely) collision re-mints and retries. Sim callers
    pass a `SimEnv` handle (its own seeded `Rng`, so minting stays a pure
    function of the run's seed); production join paths mint at the CLI
    boundary via `prod::PreBindRng` (below) — the sanctioned pre-bind
    entropy source, replacing ADR 0036's old `generate_join_nonce`
    bespoke-exception with one reusable, documented seam.
  - `NodeId::new_unchecked(s) -> NodeId` — bypasses validation. Reserved for
    deserializing an id already validated once (wire frames, WAL/snapshot
    replay, a `serde` round-trip of already-stored `Metadata`) and the
    test-support `nid(n: u64) -> NodeId` (formats `"n{n}"`, also exported
    from here, ungated). Never call this on untrusted input.
  `.as_str()` recovers the raw string — the sole accessor, deliberately
  narrow (mirrors the old `.as_u64()` discipline from the PR2 stage: grep
  every call site before adding a new one, and prefer fixing the call site
  to carry a `NodeId` instead of unwrapping early).
- **`prod::PreBindRng` (ADR 0040 PR4)** is a minimal `Rng` impl (real
  `OsRng`, byte-for-byte the same source `ProdEnv`'s own `Rng` impl draws
  from) for the **one** narrow, still-sanctioned pre-bind exception to the
  `Env`-seam rule (ADR 0003): a joining CLI process minting its own
  `NodeId` *before* `Node::bind`/`ProdEnv::bind` exist, so there is no bound
  `Env` to draw from yet. It replaces ADR 0036's `generate_join_nonce`
  (deleted) with a reusable, documented seam instead of a one-off bespoke
  function. **Scope discipline**: anything that runs in-process on an
  already-bound node (e.g. `animusd`'s `admin_add_control_member`'s
  minted-id path) must keep drawing from its own bound env's `Rng`
  (`leader.env().next_u64()`/`NodeId::mint(leader.env())`), never
  `PreBindRng` — a `SimEnv` test can and does drive that code path
  deterministically, and `PreBindRng` would silently break that.
- `Env` is a *supertrait*, not a bag of accessors: a handle **is** a `Clock` +
  `Rng` + `Network` + `Disk` + `Spawner`. Callers write `env.now()`,
  `env.send(..)`, `env.recv()` directly. Because components are `<E: Env>`, the
  supertrait's methods are in scope from the bound — you do **not** need to
  `use animus_env::Clock` etc. in generic code (doing so trips an unused-import
  warning).
- `Network::send` is fire-and-forget (no delivery result); `recv` is
  **single-consumer per node** — never run two receive loops on one `NodeId`.
- **`ProdEnv`'s peer book is keyed by `host:port` string, not `SocketAddr`
  (ADR 0060's advertise/dial split)** — `set_peers`/`merge_peer` both take
  `String`, and `Network::send`'s dial path resolves it (numeric parse or a
  real async DNS lookup, via `TcpStream::connect`'s own `ToSocketAddrs` impl
  for `&str`) only when it needs a fresh connection; an already-cached live
  stream is reused with zero resolution cost, and a write failure drops the
  stale cache entry and reconnects once, re-resolving fresh — the mechanism
  that lets a peer registered by a stable hostname (a Kubernetes pod's own
  DNS name) recover after moving to a new address, which a `SocketAddr`-
  keyed book could never express. Every caller building this crate's own
  peer/route maps from a bind address still has a plain `SocketAddr` in hand
  and stringifies it at the boundary (`animusd`'s own `advertised_addr`
  helper is the one place that decides whether that string is the bind
  address itself or an operator-supplied advertised host) — this seam
  itself has no opinion on *which* string it's handed, only that it's a
  dialable one.
- **`ProdEnv::merge_peer(id, addr)` (ADR 0037 PR3) adds/replaces a single peer
  entry without disturbing the rest of the book** — the incremental dual of
  `set_peers`'s full replace, with no `get_peers` to read-modify-write around
  by design (a full periodic rebuild from a known-good source, like
  `animusd::peer_sync_loop` does, is the intended pattern for anything that
  needs to *converge*; `merge_peer` is for a one-off "make this one id
  reachable right now" case). It updates only the *calling* env's own book,
  so its remaining live callers are deliberately narrow:
  `admin_add_control_member` calls it on the local leader's own env right
  after registering a new/promoted voter, so that voter is reachable before
  the next periodic sync tick, without waiting on it.
  **Historical scope limit, since structurally closed rather than fixed in
  place (ADR 0037 PR3 → PR4 → ADR 0040 PR1)**: PR3 shipped with a
  runtime-added control voter's address known only via this one-off
  `merge_peer` call on whichever node happened to be leader at add time — a
  *later* leader had no path to independently rediscover it. PR4's fix (a
  second periodic loop, `control_peer_sync_loop`, syncing a now-deleted
  `NodeAddrs.control` field into `ControlHandle::merge_control_peer`) is
  itself gone: ADR 0040 PR1 merged the `control`/`raftkv` address pair into
  one `internal` field, so there is no separate control-only address left
  to sync — the single, already-existing `animusd::peer_sync_loop` (which
  every node, control/data/combined alike, already runs off
  `Metadata.node_addrs[*].internal`) now closes this gap for free, with no
  dedicated control-role loop needed. See `animusd/CLAUDE.md`'s
  "Control-plane membership change" gotcha for the full historical arc and
  `docs/engineering-lessons.md` for the war story.
- **`ProdEnv` pools one outbound TCP connection per destination *address***
  (`TCP_NODELAY` set) instead of dialing per message — a Raft heartbeat no
  longer pays a handshake. Frames are unchanged and carry `from` per message,
  so one connection per addr is correct even with several ids mapping to it;
  the per-address `tokio::sync::Mutex` is held across the whole frame write
  (frame integrity) without head-of-line blocking across peers. On a write
  error (peer restarted) the stale stream is dropped and the send reconnects
  **once**, then surfaces the error — still fire-and-forget, and the frame in
  flight when a peer dies can be lost (higher layers retry, as before).
- **`Coresident`/`sibling` (ADR 0017 D) is gone (ADR 0040 PR5)**, superseded
  by multiplexed streams (ADR 0026, below): co-hosting a second protocol
  instance (a tablet's CP Raft group) rides a distinct *stream* on the node's
  own id instead of a whole second `NodeId`+inbox minted at runtime off a
  pre-bound listener pool. It was already dead code by the time it was
  removed — the per-tablet CP groups had migrated onto streams (ADR 0026
  Stage B), leaving zero live call sites — so this was a pure deletion:
  `Coresident`, `ProdEnv::bind_with_pool`/`PoolSlot`/the pool field,
  `SimEnv`'s impl, and `ProdEnv::shutdown_tasks()` (which existed only to
  spare a sibling's shared listener pool from a full `shutdown()`) all went
  together. If a future need for a second addressable identity on one
  physical node appears, prefer a stream — that is what this trait's own
  existence proved unnecessary the first time.
- `Disk` is append + explicit `sync`; bytes are not durable until `sync`
  returns. This models real crash semantics and is what `animus-sim` exploits.
  `Disk::replace` atomically swaps a file's whole contents (temp-file + rename
  in `ProdEnv`) — used for WAL compaction. In `ProdEnv`, the file-creating paths
  (`append`/`replace`) create missing parent directories, so a filename
  carrying a subdirectory prefix (e.g. `"db/wal"`) works instead of silently
  failing on a missing parent (`append` does it lazily — retry-on-`NotFound`,
  not a `create_dir_all` per call). **Namespace changes are fsynced**: `sync`
  and `replace` fsync the containing directory chain after the file
  `sync_all`/rename (POSIX requires it — without it a just-created WAL segment
  or completed manifest swap can vanish on power loss); `remove` deliberately
  does not (a resurrected orphan is harmless and cleanup handles it). The dir
  fsync runs only on the **first** `sync` of a file — creation is a one-time
  namespace change; a per-env `dir_synced` memo (invalidated by `remove`,
  refreshed by `replace`) keeps the WAL group-commit hot path at one fsync per
  commit. **`append` must `flush` before returning**: `tokio::fs::File`
  buffers, `write_all` can return with the write still in flight on the
  blocking pool, and dropping the handle completes it in the *background* — so
  without the flush two sequential `append`s (separate handles) can land in
  the file in **inverted order** (observed: an SSTable whose index preceded
  its data block, the long-standing `lsm_concurrent` flake), and a following
  `sync()` (a different fd) can fsync before the buffered write reaches the
  page cache. `read_at(file, offset, len)` /
  `size(file)` / `remove(file)` are the random-access + delete primitives an
  on-disk LSM needs (SSTable block reads, file sizing, compaction cleanup); they
  view the same durable + buffered bytes as `read`, so a crash drops an un-synced
  tail consistently across all of them.
- `ProdEnv` is *not* covered by the simulation tests (it's the nondeterministic
  side). Don't add logic here that the deterministic path needs to share.
- **`prod.rs` carries a module-level `#![allow(clippy::disallowed_methods,
  reason = "...")]`** (ADR 0061 rung B5) — this file is the one sanctioned
  place `Instant::now`/`SystemTime::now`/`tokio::spawn`/
  `tokio::time::{sleep,timeout}` are the correct call, so the workspace-wide
  `clippy.toml` `disallowed-methods` lint is exempted here at the module
  level rather than at each of its ~30 call sites. The two `OsRng` sites
  (`impl Rng for ProdEnv`/`impl Rng for PreBindRng`) are separately
  `disallowed_types`-allowed at their own `impl` blocks, not folded into
  that same module-level allow — `OsRng` is a unit struct (trips
  `disallowed-types`, not `disallowed-methods`) and keeping the two allows
  apart means an accidental future `HashMap` elsewhere in this file would
  still trip the lint. Don't widen either allow's scope without checking
  whether the new code is actually more `ProdEnv`-sanctioned real I/O, or
  just convenient to exempt.
- `ProdEnv::shutdown()` aborts every task the env owns — its inbound-connection
  accept loop plus everything spawned through `Spawner::spawn` (so the env tracks
  spawned `AbortHandle`s). `animusd`'s `Node::shutdown` calls it on each of the
  node's three role envs to tear the node down and free its listener ports for a
  restart in the same runtime. Production-edge only; determinism is unaffected.
  **`abort()` only requests cancellation — it does not wait for the task (and
  the `TcpListener` it may own) to actually be dropped**, so a bare `shutdown`
  followed immediately by a same-address rebind can race this same process's
  own not-yet-unwound accept-loop task for the port; under `cargo test
  --workspace`-level CPU contention that race can lag long enough to flake a
  restart test even behind a generous retry bound (see the "abort() is a
  request, not a guarantee" entry in `docs/engineering-lessons.md`). Use
  **`ProdEnv::shutdown_and_wait()`** (async) instead when the caller needs the
  listener genuinely gone before proceeding — it aborts the same way, then
  polls `AbortHandle::is_finished()` (bounded) before returning.
  `animusd::Node::shutdown_graceful` uses the `Node`-level dual
  (`shutdown_and_wait`) for exactly this reason.
- **Metrics are additive and determinism-safe (ADR 0015).** `Env::metrics()` has
  a **default** returning a shared no-op `MetricsHandle`, so the supertrait is
  unchanged and every `E: Env` impl (`SimEnv` included) compiles untouched.
  Recording is a relaxed atomic add — no wall clock (a timestamped metric takes
  `Clock::now`), no I/O, no `HashMap` (a snapshot uses `BTreeMap`). `ProdEnv`
  overrides `metrics()` with a recording sink; a sim test that wants to *read*
  counters threads a recording handle into the component (e.g.
  `RaftNode::start_with_metrics`) rather than relying on the no-op default — so
  no change to `animus-sim` is needed to observe metrics. The storage engine
  follows the same pattern: `LsmEngine::open`/`open_with` forward
  `env.metrics()`, and the additive `LsmEngine::open_with_metrics` threads a
  recording handle in for a sim test. (The deleted AP data plane's
  `DataClient::with_metrics`/`serve_*_with_metrics` variants followed it too —
  gone with `animus-data`, ADR 0019.)
- **`SegmentStore` (ADR 0043 §A7) is a seam like `Disk`/`Network`, but
  deliberately excluded from the `Env` supertrait itself (decision F5).**
  Every call site threads an explicit handle — the same way a
  `StorageEngine` handle is threaded rather than folded into `Env` — instead
  of gaining a `env.segment_store()`-shaped accessor. This keeps `Env`
  itself free of a dependency on the stream-shard subsystem, and lets a
  component's choice of store vary independently of its `Env` (a sim test
  pairs a `SimEnv` with `animus-sim`'s fault-injecting `SimSegmentStore`;
  production pairs `ProdEnv` with the cluster-replicated default). The
  trait is four methods — `put`/`get`/`delete`/`list`, all `io::Result`,
  `#[async_trait]` like `Disk` — over an opaque string `id` (production ids
  are `{table}/{label}/{tablet}/{epoch}/{attempt-suffix}`, ADR 0043 §A3's
  ledger-named-object amendment — a per-*attempt* unique id, not the bare
  per-*shard* `{table}/{label}/{tablet}/{epoch}` prefix a reader/sweep
  resolves from the catalog row instead of recomputing). Its **consistency
  contract** (read-after-put, **write-once** — an identical-content re-put
  is a safe no-op, a differing-content re-put is a hard `Err` — `get` after
  `delete` is a defined `None` not an error, `list` is debug/sweep-only and
  never load-bearing for a read) is spelled out on the trait's own doc.
  **As-built amendment**: this used to say "idempotent overwrite,
  last-write-wins," with a documented "superset-slice rule" reader-side
  exception for a crash-retried `put`'s late arrival — that design let two
  independently-computed seal attempts for the same shard silently
  overwrite each other's bytes at the shared deterministic id, a real
  data-loss bug (see `animus_cp_data::segment`'s own module doc for the
  incident). Write-once, unique-per-attempt ids close it structurally: two
  attempts can no longer share a storage key at all.
- **`FsSegmentStore` (single directory) is `ProdEnv`'s `SegmentStore`
  sibling, opt-in** (`--segment-store=dir:...`, wired by a later PR) for dev
  use or a shared mount, and doubles as the default
  `ClusterSegmentStore`'s own per-node local building block (ADR 0043
  §A7b — a later PR; the default replicates across `K` nodes' own
  `FsSegmentStore`-backed directories). `put` reuses `ProdEnv`'s own private
  `ensure_parent`/`sync_dir` free functions and follows the identical
  temp-write + fsync + rename + directory-fsync discipline
  `ProdEnv::replace` uses for its atomic swaps — the same "POSIX doesn't
  persist a rename until its directory is fsynced" reasoning applies here.
  Ids contain `/` separators mapped to subdirectories (created on demand by
  `put`); `resolve` rejects an empty id, an absolute id, or one with a
  `..`/`.` leading component — `Path::components()` already lexically
  normalizes away a *non-leading* `.` (e.g. `"table/./epoch"` parses as just
  `["table", "epoch"]`, no traversal risk), so the guard only needs to catch
  `ParentDir`/`RootDir`/`Prefix`/a leading `CurDir`, not scan for a literal
  `".."` substring. `list` recurses the whole tree under `root` (unlike
  `Disk::list`, deliberately non-recursive over a flat per-node data dir)
  and filters out any `.tmp` sibling a crash mid-`put` could have
  orphaned, so a debug/sweep caller never mistakes a half-written temp file
  for a real segment.
- **`assert_segment_store_contract` (`test_support.rs`) is the one place the
  `SegmentStore` trait contract is pinned**, exercised against both
  `FsSegmentStore` (this crate's own `#[tokio::test]`, real temp dir) and
  `animus-sim`'s `SimSegmentStore` (a `#[test]` driven through the
  simulator). Every id it writes is scoped under `"contract-test/"` and
  cleaned up before returning, so it composes with a store a caller has
  already put other data into.
- **`Disk::link(src, dst)` (ADR 0058 rung 2) is a hard link, not a copy** —
  added specifically for `animus-storage`'s `LsmEngine::clone_to`, which
  needs to share an immutable SSTable file between a source engine and a
  freshly cloned one without paying a byte copy. `dst` becomes an
  independent directory entry over `src`'s current durable bytes; a later
  `remove` of either name never affects the other (real hard-link
  semantics — the underlying bytes persist until every name referencing
  them is gone). **Overwrites `dst` if it already exists** (like `replace`,
  but backed by a link) so a caller can safely relink the same `(src, dst)`
  pair on retry after a crash — this is what makes `clone_to` idempotent to
  retry. Durable on return, same as `append`/`replace`'s "namespace changes
  are fsynced" rule — no follow-up `sync` needed. `ProdEnv` implements it
  with `std::fs::hard_link` (removing any stale `dst` first, since
  `hard_link` itself errors `AlreadyExists` rather than replacing) plus the
  usual containing-directory fsync. `SimEnv` has no inode/directory model,
  so it models a link as a snapshot copy of `src`'s current `FileState` into
  `dst`'s own independent map slot — behaviorally indistinguishable from a
  real hard link for this trait's sanctioned use (an already-fully-synced,
  never-mutated-in-place file), and it participates in the disk fault model
  exactly like `append`/`sync`/`read`/`read_at`/`replace` (an injected error
  makes no state change). Only three `Disk` implementors exist
  (`ProdEnv`, `SimEnv`, and `animus-storage`'s `lsm_group_commit.rs` test
  double `CrashEnv`, which just delegates) — grep `impl Disk for` before
  assuming there might be more to update if this trait grows again.
- **`Disk::list` is per-env and non-recursive** (ADR 0024): it enumerates only
  the files this handle's own `Disk` methods could open — production reads the
  env's data dir without descending into any nested subdirectory. It exists so
  a teardown path (drop-table GC) can find every file of a prefix-named
  component; deletion stays on the seam (`remove`), so teardown is
  sim-testable.

- **Multiplexed `(node, stream)` addressing (ADR 0026).** `Network` gained a
  second addressing axis so a node can host more than one protocol instance
  without minting a whole new `NodeId`: `send_stream`/`recv_stream` are the
  primitive methods every implementor provides; `send`/`recv` are **default**
  methods over `PRIMARY_STREAM` (`= 0`), so every call site that predates this
  axis — which is nearly everything — needs no change and behaves identically.
  `SimEnv` re-keys its inbox `BTreeMap` from `NodeId` to `(NodeId, u64)` (no new
  RNG draw or timeline event, so determinism is unaffected). `ProdEnv` demuxes
  by a `Demux` (`BTreeMap<u64, VecDeque<Envelope>>` + per-stream `Waker`s)
  behind one `Arc<StdMutex<_>>` per env, fed by a background pump task that
  drains the accept loop's raw frames (now `[from][stream][len][payload]`,
  the `stream` field ADR 0026 added) and routes each into its stream's queue,
  waking a parked `recv_stream(stream)`. The staged plan in the ADR completed:
  the per-tablet CP Raft groups (`animus-cp-data`) migrated onto streams
  (Stage B), which is what made `Coresident`/the sibling pool vestigial —
  and, ADR 0040 PR5, deleted outright.
  This is the same "additive default over a well-known constant" shape the
  metrics seam (`Env::metrics()`) uses — extend the trait so nothing existing
  has to change, not by widening every implementor's required surface.

- **TLS on the intra-node wire (ADR 0064, S-01 step 1) — `tls.rs`, config-gated,
  default off, byte-for-byte unchanged when unconfigured.** `TlsConfig
  {cert_path, key_path, ca_path: Option<PathBuf>}` names three PEM files —
  file-based like `--dynamo-auth`, no inline PEM in config/CLI. `TlsConfig::
  load()` reads them once and builds `TlsMaterial{acceptor, connector}` —
  today, **always mutual TLS**: a `ServerConfig` requiring and verifying a
  peer's client cert against `ca_path`, and a `ClientConfig` presenting this
  node's own cert and verifying the peer's against the same CA. `ca_path` is
  `Option` only because a future server-only mode (the client/admin/console
  ports, commit 2) has no peer cert to verify; `load()` errors if it's
  `None` today, since the internal wire has no server-only mode. Crypto
  provider is pinned to `ring` (`rustls::crypto::ring::default_provider()`)
  — the same provider already in the graph via `kube`'s `rustls-tls`
  feature, so the workspace carries one crypto backend, not two.
  - **`MaybeTlsStream { Plain(TcpStream), Tls(Box<tokio_rustls::
    TlsStream<TcpStream>>) }`** is the transport wrapper every accept/dial
    path in `prod.rs` moves through, implementing `AsyncRead + AsyncWrite`
    by delegating to whichever variant is live. `tokio_rustls::TlsStream<T>`
    already unifies the client- and server-initiated cases, so this enum
    needs no third variant. Boxed only on the `Tls` arm (a plain `TcpStream`
    needs no heap allocation at all); chosen over a `Box<dyn ..>` for the
    whole enum because this sits on the hot path every frame — every Raft
    heartbeat included — moves through, and over a Cargo feature because a
    feature would bifurcate this crate's own build for what is a runtime
    config choice, not a compile-time one. Lives in `animus-env` (not
    `prod.rs`'s own concern folded away) specifically so `animusd` can reuse
    it verbatim for its own client/intra/admin/console listeners in commit 2.
  - **`ProdEnv::bind` is untouched — it now just calls the new, general
    `ProdEnv::bind_with_tls(node_id, listen, data_dir, tls: Option<
    TlsConfig>)` with `None`.** Every one of this workspace's ~25+ existing
    `ProdEnv::bind` call sites (across `animusd`, `animus-control`,
    `animus-cp-data`, `animus-storage`) keeps compiling and behaving
    identically with zero changes — only a caller that actually wants TLS
    reaches for `bind_with_tls` instead. `Inner` gained one field
    (`tls: Option<TlsMaterial>`, shared across the env's clones) and the
    pooled-connection map's value type changed from `Option<TcpStream>` to
    `Option<MaybeTlsStream>` — an internal representation change, invisible
    at every call site.
  - **Accept-side**: `spawn_accept` takes `Option<TlsMaterial>`; when set,
    every accepted socket runs through `TlsMaterial::acceptor.accept(..)`
    before a single frame is read. **A failed handshake — a plain-TCP dial
    into a TLS listener, or a peer presenting a cert from a different CA —
    is logged at `warn` with the peer's address and the connection is
    simply dropped**, handled exactly like a failed `accept()` itself (see
    `spawn_accept`'s own doc on why a failed accept never stops the loop):
    no panic, and the listener keeps serving every other genuine peer.
  - **Dial-side**: `connect_maybe_tls` (replacing the bare `connect_nodelay`
    at the top of the pooled-send path) runs the outbound handshake through
    `TlsMaterial::connector` when configured, presenting this node's cert
    and verifying the peer's. A handshake failure surfaces as a plain
    `io::Error` — `send_frame_pooled`'s existing reconnect-once-then-
    surface path handles it with zero special-casing, the same as any other
    dial failure.
  - **Certificate SAN requirement**: a node's cert must carry a Subject
    Alternative Name for every string a peer might dial it by — its bind
    address's IP, and, if registered by hostname (a Kubernetes pod's stable
    DNS name, ADR 0060's advertise/dial split), that DNS name too.
    `server_name_for(addr)` derives the `ServerName` the handshake verifies
    against from exactly the string `ProdEnv`'s peer book holds (a numeric
    address becomes `ServerName::IpAddress`, anything else a DNS name) — a
    cert missing the SAN the caller actually dials by fails the handshake,
    a deployment/cert-issuance concern this module's doc records rather
    than papering over.
  - **The test PKI helper** (`prod::tests::test_pki`/`write_test_pki`) —
    `rcgen` (dev-dependency only, never shipped) generates a self-signed CA
    plus one leaf cert per name, each leaf's SAN/CN set to that exact
    string (e.g. `"127.0.0.1"`, matching what `server_name_for` derives
    from a loopback dial address on any port); `write_test_pki` writes the
    PEMs to real files under a temp dir and returns one `TlsConfig` per
    leaf, so the tests exercise the real file-path `load()` path, not
    in-memory PEM strings. **`#[cfg(test)]`-private to this crate** — a
    downstream crate needing the same shape (`animusd`'s own
    `tests/support/mod.rs::tls_pki`, ADR 0064 commit 2) writes a small
    independent copy rather than reusing this one across the crate
    boundary.

- **Server-only TLS + a second `TlsMaterial` acceptor (ADR 0064 commit 2)**
  — `TlsMaterial` now carries `server_acceptor: tokio_rustls::TlsAcceptor`
  alongside the original `acceptor` (still mutual) and `connector`:
  `TlsConfig::load()` builds both `ServerConfig`s from the identical
  cert/key (`with_client_cert_verifier` for `acceptor`, `with_no_client_
  auth()` for `server_acceptor`), so `animusd`'s client/dynamo/admin/
  console listeners (server-only TLS, ADR 0064 Decision 2) get an acceptor
  from the same one `TlsConfig::load()` call the internal wire's mutual
  mode already made — no second PEM read/parse for the server-only side.
  `ca_path` stays required unconditionally in `load()` even though a
  server-only port never reads it: a node's internal wire is always
  mutual the moment TLS is configured at all, so there is no legitimate
  case where a `TlsConfig` exists with nothing to verify peers against.
  Tested directly here: `prod::tests::
  server_only_acceptor_accepts_a_client_with_no_certificate` (a raw
  loopback listener/dial, not a `ProdEnv` — proves the acceptor itself
  imposes no client-cert requirement, independent of who ends up using
  it).
- **`server_name_for` is now `pub`, not `pub(crate)`** (ADR 0064 commit
  2) — `animusd`'s own relay dialers (the intra `ClientRequest` port,
  mutual TLS just like the internal wire) need the identical `ServerName`
  derivation from the identical peer-book string, so it's shared via
  `animus_env::tls::server_name_for` rather than duplicated.

## Tests

The seam is exercised end-to-end through `animus-sim` (`cargo test -p
animus-sim`). One `ProdEnv` unit test (`prod::tests`, real temp dir) asserts a
nested `"sub/dir/file"` `append`+`sync`+`read` round-trips — i.e. the disk
creates parent directories. `cargo test -p animus-env` runs the
feature-independent tests (`lib.rs`/`metrics.rs`); `prod::tests` only exists
(and only runs) with the `prod` feature on — `cargo test -p animus-env
--features prod` (or `--all-features`, which the workspace gate already
runs). `metrics.rs::tests`
cover incr/snapshot round-trips, that clones share one sink, and that the text
export is stable + ordered. `lib.rs::tests` (ADR 0040 PR4) covers
`NodeId::mint`/`base64url_nopad`: known base64url test vectors, no padding/
non-url-safe characters ever emitted, a minted id is always exactly 22 chars
and independently passes `NodeId::propose`'s own charset check, minting is a
pure function of the `Rng` draws (same scripted draws in ⇒ same id out), and
2000 draws off a plain incrementing-fallback scripted `Rng` never collide —
via a hand-rolled `ScriptedRng` test double (atomics, not `Cell`, since `Rng`
requires `Send + Sync` even for a single-threaded test double — see
`docs/engineering-lessons.md` if this surprises you). `prod::tests` also
covers `FsSegmentStore` (ADR 0043 §A7): the shared contract via
`assert_segment_store_contract`; a nested (`{table}/{label}/{tablet}/{epoch}`-
shaped) id creates its intervening subdirectories and leaves no stray `.tmp`
sibling behind; a path-traversal/absolute/empty id is rejected by every
method (`put`/`get`/`delete`), never resolved outside `root`; and `list`
recurses every nested level, filters by prefix, and hides a crash-orphaned
`.tmp` file from the result.

**TLS (ADR 0064)**, also `prod::tests`, also real loopback sockets, no
mocking of `rustls` itself: `tls_bound_pair_frames_flow_both_ways` (a real
mutual-TLS handshake, frames flow both ways); `tls_peer_from_different_ca_
is_refused` (two independent CAs — a peer never sees a frame from a sender
whose cert its own `ClientConfig` doesn't trust, and neither side panics);
`tls_listener_rejects_plain_dial_and_keeps_serving_tls_peers` (a raw,
non-TLS `TcpStream` dial into a TLS listener fails the handshake cleanly,
and the listener keeps serving genuine TLS peers right after); and
`tls_send_reconnects_after_peer_restart` (the TLS counterpart to
`send_reconnects_after_peer_restart` — a torn-down-and-rebound peer's new
TLS material is picked up by the pooled sender's reconnect-once path).
`tls::tests` covers `server_name_for`'s IP-vs-DNS-name derivation
directly. Every plain-TCP test above this section is unmodified by ADR
0064 and stays green under the same `cargo test -p animus-env --features
prod` run.

**Commit 2** adds one more: `prod::tests::
server_only_acceptor_accepts_a_client_with_no_certificate` — a raw
loopback listener/dial (no `ProdEnv`) proving `TlsMaterial::
server_acceptor` imposes no client-cert requirement, unlike `acceptor`
(mutual) on the exact same cert/key. `animusd`'s own `tests/tls_e2e.rs` is
the real end-to-end regression net for this acceptor actually serving the
client/dynamo/admin/console ports — see that crate's `CLAUDE.md`.
