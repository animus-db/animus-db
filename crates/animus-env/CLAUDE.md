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
  convenience.
- `prod.rs` — `ProdEnv`: real monotonic clock, `OsRng`, `tokio::spawn`,
  length-prefixed TCP, `tokio::fs` + `fsync`. Owns a real recording metrics sink
  and exposes `metrics_text()` (ADR 0015).
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

## Tests

The seam is exercised end-to-end through `animus-sim` (`cargo test -p
animus-sim`). One `ProdEnv` unit test (`prod::tests`, real temp dir) asserts a
nested `"sub/dir/file"` `append`+`sync`+`read` round-trips — i.e. the disk
creates parent directories. `cargo test -p animus-env`. `metrics.rs::tests`
cover incr/snapshot round-trips, that clones share one sink, and that the text
export is stable + ordered. `lib.rs::tests` (ADR 0040 PR4) covers
`NodeId::mint`/`base64url_nopad`: known base64url test vectors, no padding/
non-url-safe characters ever emitted, a minted id is always exactly 22 chars
and independently passes `NodeId::propose`'s own charset check, minting is a
pure function of the `Rng` draws (same scripted draws in ⇒ same id out), and
2000 draws off a plain incrementing-fallback scripted `Rng` never collide —
via a hand-rolled `ScriptedRng` test double (atomics, not `Cell`, since `Rng`
requires `Send + Sync` even for a single-threaded test double — see
`docs/engineering-lessons.md` if this surprises you).
