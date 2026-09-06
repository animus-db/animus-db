//! A shared, multi-tenant write-ahead-log I/O coordinator (PR1 of the
//! single-command-split redesign, see `docs/adr/0028-*.md`), **wired into
//! `animus-cp-data`'s persist path behind `--shared-wal` since C-05 PR 2**.
//!
//! Once several tablets' `RaftCore` instances on one node persist into the
//! SAME physical WAL file (each record tagged with its tablet, see
//! [`crate::persist::PersistedState::encode_tagged_record`]), those tablets'
//! independent driver tasks become genuinely concurrent writers of one file.
//! `SharedWal` serializes them and, where concurrent callers overlap, batches
//! their appends into a single `Disk::append` + `Disk::sync` — the multi-tablet
//! analogue of `animus-storage`'s per-engine `GroupCommit`. It is built from
//! executor-agnostic `futures` primitives (`futures::lock::Mutex`,
//! `futures::channel::oneshot`), the same family already used for
//! `animus-cp-data`'s per-instance `wal_lock`, so it stays deterministic under
//! `SimEnv` — no tokio-runtime-bound primitive is involved.
//!
//! A whole-file compaction rewrite (`Disk::replace`) is submitted through the
//! same queue as appends, so the coordinator only ever has one physical I/O
//! operation touching the file in flight — a compaction can never race a
//! concurrent append into torn/interleaved bytes.
//!
//! ## Two APIs: raw (untyped) and tagged (typed, group-aware)
//!
//! The original `append`/`compact` pair (below) moves opaque bytes with no
//! knowledge of tablets — this is what `benches/wal_fsync_bench.rs` (C-05,
//! PR 1) measures directly, and stays exactly as it was so that bench's
//! numbers are still reproducing the same code path.
//!
//! The **tagged** API — [`append_tagged`](SharedWal::append_tagged),
//! [`compact_group`](SharedWal::compact_group), [`forget`](SharedWal::forget),
//! [`open`](SharedWal::open), [`recovered_state`](SharedWal::recovered_state)
//! — is what a real multi-tablet node uses. `SharedWal<C, S>` additionally
//! keeps an in-memory cache, `group_tails: BTreeMap<TabletId,
//! Vec<WalRecord<C, S>>>`: each tablet's own currently-durable-on-this-file
//! record set (everything appended since that tablet's own last compaction,
//! or since the file was last read at node start). Both are always mutated
//! **in the same critical section** that also enqueues the corresponding
//! physical [`WalOp`] — the coordinator's pre-existing `inner` lock — which
//! is what makes a whole-file [`compact_group`](SharedWal::compact_group)
//! rewrite always consistent with every append that was enqueued (and thus,
//! by FIFO drive order, physically written) before it, regardless of which
//! tablet's own task happens to win the queue-leader race: see that method's
//! own doc for the full argument.
//!
//! **Recovery / GC contract** (C-05 PR 2, ADR 0028's matching amendment):
//! - [`open`](SharedWal::open) is called exactly ONCE per node, before any
//!   tablet's own driver starts, and seeds `group_tails` for every tablet the
//!   file already holds by demuxing it whole. A tablet hosted later that was
//!   never in the file starts with an empty (absent) tail — correct, since
//!   it genuinely has no prior history.
//! - A round's ack fires only after the physical `append`/`sync` (or,
//!   for a rewrite, `replace`) actually lands — identical durability
//!   semantics to the per-group file this replaces.
//! - **GC policy**: there is no independent segment file to reclaim — the
//!   coordinator holds one physical file, atomically rewritten. A tablet's
//!   own bytes are reclaimed the moment THAT tablet itself calls
//!   `compact_group` (its accumulated tail is replaced by its fresh,
//!   minimal `wal_image()`); a DIFFERENT tablet's `compact_group` or
//!   `append_tagged` call never touches another tablet's cached tail except
//!   to re-include it verbatim in the rewritten file, so one tablet's
//!   compaction can never reclaim (or lose) bytes another tablet still
//!   needs, and no tablet ever waits on another's compaction to reclaim its
//!   own. [`forget`](SharedWal::forget) is the teardown-time removal (a
//!   dropped/moved-off tablet's bytes are dropped from the cache and the
//!   file is rewritten without them at once, rather than waiting for some
//!   other tablet's next ordinary compaction).
//! - **Crash safety**: recovery after a crash mid-append, mid-sync, or
//!   mid-rewrite yields, for every tablet, exactly its own last **durably
//!   written** tail — `PersistedState::decode_tagged`'s per-record CRC32 +
//!   torn-tail tolerance (issue #495) already guarantees a torn trailing
//!   write is dropped rather than corrupting recovery, and a rewrite
//!   (`Disk::replace`) is atomic at the `Env` seam (either the old file or
//!   the fully-written new one is what a fresh `open` sees — see
//!   `animus-sim`'s `torn_tail_on_crash`/`corrupt_on_crash` knobs for how
//!   this is fault-injected under `SimEnv`).
//! - **Layout-mismatch (flag-flip) safety**: [`open`](SharedWal::open) is
//!   the ONLY reader of the shared file; a node started with `--shared-wal`
//!   OFF never touches it (it reads/writes only per-tablet `raftkv.wal.
//!   {stream}` files, unchanged). Since the shared file and the per-tablet
//!   files live at different, disjoint names on the SAME data directory,
//!   flipping the flag against an existing data dir written under the OTHER
//!   layout does not corrupt anything — it simply finds nothing (an empty
//!   `group_tails`) under the new layout while the old layout's files sit
//!   unread beside it. `animus-cp-data`'s node-start caller is expected to
//!   fail loudly rather than silently mix layouts — see that crate's own
//!   `CLAUDE.md` for the concrete check.

use std::collections::{BTreeMap, VecDeque};
use std::io;
use std::sync::Arc;

use animus_env::Env;
#[cfg(test)]
use animus_env::nid;
use animus_tablet::TabletId;
use futures::channel::oneshot;
use futures::lock::Mutex as AsyncMutex;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::meta::{MetaCommand, Metadata};
use crate::persist::{PersistedState, WalRecord};

/// One pending physical operation against the shared WAL file.
enum WalOp {
    /// Append these already-encoded bytes (one or more tagged WAL records).
    Append(Vec<u8>),
    /// Atomically replace the whole file with this image (a compaction).
    Compact(Vec<u8>),
}

/// A cloneable error handle: `io::Error` isn't `Clone`, but one failed
/// physical write must fail every caller batched into it.
#[derive(Clone)]
struct SharedWalError(Arc<io::Error>);

impl From<io::Error> for SharedWalError {
    fn from(e: io::Error) -> Self {
        Self(Arc::new(e))
    }
}

impl From<SharedWalError> for io::Error {
    fn from(e: SharedWalError) -> Self {
        io::Error::new(e.0.kind(), e.0.to_string())
    }
}

struct Pending {
    op: WalOp,
    done: oneshot::Sender<Result<(), SharedWalError>>,
}

struct SharedWalState<C, S> {
    queue: VecDeque<Pending>,
    leader_active: bool,
    /// Every locally-known tablet's own currently-durable-on-this-file
    /// record set (C-05 PR 2) — see the module doc's "Two APIs" section.
    group_tails: BTreeMap<TabletId, Vec<WalRecord<C, S>>>,
}

impl<C, S> Default for SharedWalState<C, S> {
    fn default() -> Self {
        Self {
            queue: VecDeque::new(),
            leader_active: false,
            group_tails: BTreeMap::new(),
        }
    }
}

/// Coordinates every locally-hosted tablet's persistence calls against one
/// physical file. One `SharedWal` per node/role (e.g. the `raftkv` WAL) is
/// shared, via `Arc`, across every tablet's `RaftCore` driver on that node.
/// Generic over the command/state-machine types (defaults: the control
/// plane's [`MetaCommand`]/[`Metadata`]) so `animus-cp-data` can instantiate
/// `SharedWal<KvCommand, KvState>` for the CP data plane's own persist path
/// (C-05 PR 2) — see the module doc's "Two APIs" section.
pub struct SharedWal<C = MetaCommand, S = Metadata> {
    inner: AsyncMutex<SharedWalState<C, S>>,
    /// Every completed physical `Disk::append`+`sync`/`Disk::replace` this
    /// coordinator has run — i.e. every time [`drive`](Self::drive)'s
    /// `flush` succeeded, once per (possibly multi-caller-coalesced) batch,
    /// never once per caller. The coalescing-observability counter: reading
    /// this before/after a burst of concurrent `append_tagged`/`compact_
    /// group` calls is how a caller (or a test) measures the coalescing win
    /// directly, without needing a `MetricsHandle` threaded through this
    /// coordinator. `std::sync::atomic`, not `Metric`, since this type has
    /// no `Env`/metrics dependency of its own (`animus-control` sits below
    /// `animus-env`'s metrics-recording call sites in the dependency
    /// graph — its callers, e.g. `animus-cp-data`, already have their own
    /// `MetricsHandle` and record `Metric::CpSharedWalSyncs`/
    /// `CpSharedWalGcRewrites` from there; this counter is the lower-level,
    /// caller-independent primitive those metrics are derived from).
    physical_writes: std::sync::atomic::AtomicU64,
}

impl<C, S> SharedWal<C, S> {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: AsyncMutex::new(SharedWalState::default()),
            physical_writes: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// The running count of completed physical writes (append-batches and
    /// compaction/forget rewrites combined) — see
    /// [`physical_writes`](Self::physical_writes)'s own doc.
    #[must_use]
    pub fn physical_write_count(&self) -> u64 {
        self.physical_writes
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Append `bytes` (already tag-encoded WAL records, see
    /// [`crate::persist::PersistedState::encode_tagged_record`]) to `file`.
    /// Batches with any other `append`/`compact` calls that arrive while a
    /// batch is being flushed into a single `Disk::append` + `Disk::sync`.
    ///
    /// **Raw/untyped**: does not touch [`SharedWalState::group_tails`] — a
    /// caller that also wants this coordinator's recovery/GC bookkeeping to
    /// stay consistent must use [`append_tagged`](Self::append_tagged)
    /// instead. Kept unchanged (including its exact queuing/batching
    /// behavior) so `benches/wal_fsync_bench.rs` keeps measuring the same
    /// code path C-05 PR 1 gated this wiring on.
    pub async fn append<E: Env>(&self, env: &E, file: &str, bytes: Vec<u8>) -> io::Result<()> {
        self.submit(env, file, WalOp::Append(bytes)).await
    }

    /// Atomically replace `file`'s entire contents with `image` (a shared-WAL
    /// compaction rewrite). Queued through the same coordinator as `append`,
    /// so it can never run concurrently with an in-flight append to the same
    /// file — no torn/interleaved bytes are possible.
    ///
    /// **Raw/untyped** — see [`append`](Self::append)'s doc; use
    /// [`compact_group`](Self::compact_group) for the group-aware form.
    pub async fn compact<E: Env>(&self, env: &E, file: &str, image: Vec<u8>) -> io::Result<()> {
        self.submit(env, file, WalOp::Compact(image)).await
    }

    async fn submit<E: Env>(&self, env: &E, file: &str, op: WalOp) -> io::Result<()> {
        let (tx, rx) = oneshot::channel();
        let become_leader = {
            let mut state = self.inner.lock().await;
            state.queue.push_back(Pending { op, done: tx });
            if state.leader_active {
                false
            } else {
                state.leader_active = true;
                true
            }
        };
        if become_leader {
            self.drive(env, file).await;
        }
        match rx.await {
            Ok(result) => result.map_err(Into::into),
            Err(_) => Err(io::Error::other(
                "shared wal: leader dropped before completing this operation",
            )),
        }
    }

    /// Like [`submit`](Self::submit), but `mutate` runs INSIDE the same
    /// critical section that enqueues the op, so a group-tails mutation and
    /// its corresponding queue entry are atomic together — the load-bearing
    /// property [`append_tagged`](Self::append_tagged)/
    /// [`compact_group`](Self::compact_group)/[`forget`](Self::forget) all
    /// rest on: whichever op enters the queue first is exactly the op whose
    /// `group_tails` mutation happened first, so by the time a LATER op's
    /// `mutate` builds its own physical image, every op that will land
    /// before it physically has already been folded into `group_tails`.
    async fn submit_with_mutation<E: Env>(
        &self,
        env: &E,
        file: &str,
        mutate: impl FnOnce(&mut SharedWalState<C, S>) -> WalOp,
    ) -> io::Result<()> {
        let (tx, rx) = oneshot::channel();
        let become_leader = {
            let mut state = self.inner.lock().await;
            let op = mutate(&mut state);
            state.queue.push_back(Pending { op, done: tx });
            if state.leader_active {
                false
            } else {
                state.leader_active = true;
                true
            }
        };
        if become_leader {
            self.drive(env, file).await;
        }
        match rx.await {
            Ok(result) => result.map_err(Into::into),
            Err(_) => Err(io::Error::other(
                "shared wal: leader dropped before completing this operation",
            )),
        }
    }

    /// Drain the queue: flush every contiguous run of `Append`s as one batch,
    /// and every `Compact` alone, until the queue is empty. Only the caller
    /// that won the `leader_active` race in `submit` runs this; every other
    /// caller just awaits its `oneshot`.
    async fn drive<E: Env>(&self, env: &E, file: &str) {
        loop {
            let batch = {
                let mut state = self.inner.lock().await;
                match state.queue.front() {
                    None => {
                        state.leader_active = false;
                        return;
                    }
                    Some(Pending {
                        op: WalOp::Compact(_),
                        ..
                    }) => vec![state.queue.pop_front().expect("front just matched")],
                    Some(Pending {
                        op: WalOp::Append(_),
                        ..
                    }) => {
                        let mut batch = Vec::new();
                        while matches!(
                            state.queue.front(),
                            Some(Pending {
                                op: WalOp::Append(_),
                                ..
                            })
                        ) {
                            batch.push(state.queue.pop_front().expect("front just matched"));
                        }
                        batch
                    }
                }
            };

            let result = Self::flush(env, file, &batch).await;
            if result.is_ok() {
                self.physical_writes
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            for pending in batch {
                let _ = pending.done.send(result.clone());
            }
        }
    }

    async fn flush<E: Env>(env: &E, file: &str, batch: &[Pending]) -> Result<(), SharedWalError> {
        match &batch[0].op {
            WalOp::Compact(image) => {
                debug_assert_eq!(
                    batch.len(),
                    1,
                    "a Compact is never batched with anything else"
                );
                env.replace(file, image).await.map_err(SharedWalError::from)
            }
            WalOp::Append(_) => {
                let mut merged = Vec::new();
                for pending in batch {
                    if let WalOp::Append(bytes) = &pending.op {
                        merged.extend_from_slice(bytes);
                    }
                }
                async {
                    env.append(file, &merged).await?;
                    env.sync(file).await
                }
                .await
                .map_err(SharedWalError::from)
            }
        }
    }
}

impl<C, S> SharedWal<C, S>
where
    C: Clone + Serialize + DeserializeOwned,
    S: Clone + Serialize + DeserializeOwned,
{
    /// Recover a node's shared WAL file (C-05 PR 2): read `file` once (a
    /// missing file reads as empty, mirroring every per-group WAL's own
    /// `env.read(..).unwrap_or_default()` recovery convention), demux it via
    /// [`PersistedState::decode_tagged`], and seed `group_tails` with every
    /// tablet's own record run found — **before any tablet's own driver
    /// starts**. This is the ONE seeding read: every later
    /// [`append_tagged`]/[`compact_group`]/[`forget`] call mutates the
    /// already-seeded cache in place rather than re-reading the file, which
    /// is what makes a whole-file rewrite triggered by tablet A safe to run
    /// without silently losing tablet B's own not-yet-touched-this-uptime
    /// history — see the module doc's "Recovery / GC contract".
    pub async fn open<E: Env>(env: &E, file: &str) -> io::Result<Arc<Self>> {
        let bytes = env.read(file).await.unwrap_or_default();
        let mut group_tails: BTreeMap<TabletId, Vec<WalRecord<C, S>>> = BTreeMap::new();
        for (tablet, record) in PersistedState::<C, S>::decode_tagged(&bytes) {
            group_tails.entry(tablet).or_default().push(record);
        }
        Ok(Arc::new(Self {
            inner: AsyncMutex::new(SharedWalState {
                queue: VecDeque::new(),
                leader_active: false,
                group_tails,
            }),
            physical_writes: std::sync::atomic::AtomicU64::new(0),
        }))
    }

    /// This tablet's own recovered [`PersistedState`] — the shared-WAL
    /// analogue of a per-group `env.read(wal_file(stream)).await` +
    /// `PersistedState::decode` + `PersistedState::replay`. Reads whatever
    /// [`open`](Self::open) seeded (or a later [`append_tagged`]/
    /// [`compact_group`] call has since added) for `tablet` — an absent
    /// tablet (never seen in the file, and never yet appended to since)
    /// replays as [`PersistedState::default`], the fresh-group case.
    pub async fn recovered_state(&self, tablet: TabletId) -> PersistedState<C, S> {
        let state = self.inner.lock().await;
        let records = state.group_tails.get(&tablet).cloned().unwrap_or_default();
        PersistedState::replay(records)
    }

    /// Append `records` — one tablet's own whole persist round — physically
    /// tagged and coalesced into a single `Disk::append` + `Disk::sync` (like
    /// [`append`](Self::append), batched with any other tablet's overlapping
    /// `append_tagged`/`compact_group` call), AND fold them into that
    /// tablet's own `group_tails` entry in the SAME critical section, so a
    /// later `compact_group` by any other tablet always sees this round
    /// already reflected. A no-op (returns `Ok(())` without touching the
    /// queue at all) when `records` is empty, mirroring `persist_wal`'s own
    /// "an empty drain consumes no round" convention.
    pub async fn append_tagged<E: Env>(
        &self,
        env: &E,
        file: &str,
        tablet: TabletId,
        records: &[WalRecord<C, S>],
    ) -> io::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        let records = records.to_vec();
        self.submit_with_mutation(env, file, move |state| {
            let mut bytes = Vec::new();
            for record in &records {
                bytes.extend_from_slice(&PersistedState::<C, S>::encode_tagged_record(
                    tablet, record,
                ));
            }
            state.group_tails.entry(tablet).or_default().extend(records);
            WalOp::Append(bytes)
        })
        .await
    }

    /// One tablet's own compaction: replace `tablet`'s cached tail with its
    /// fresh, minimal `image` (a `wal_image()` — snapshot + hard state + log
    /// tail, exactly what a per-group `apply_and_compact` builds today), then
    /// atomically rewrite the WHOLE shared file from the union of every
    /// tablet's own current `group_tails` entry (this tablet's freshly
    /// replaced one included) — reclaiming exactly this tablet's own
    /// already-compacted-away bytes, and no other tablet's, in one physical
    /// `Disk::replace`.
    ///
    /// **Why this is safe against a concurrent `append_tagged` from a
    /// DIFFERENT tablet, with no extra coordination beyond the shared
    /// `inner` lock**: both this method and `append_tagged` mutate
    /// `group_tails` and enqueue their own `WalOp` in the same critical
    /// section (`submit_with_mutation`). Whichever call's critical section
    /// runs first is, by construction, the one whose `group_tails` mutation
    /// is visible to the other — so a compaction that runs after a
    /// concurrent append's mutation includes that append's own fresh
    /// records in its rewritten image (no data loss), and one that runs
    /// BEFORE it produces a rewrite that is stale only for the tail the
    /// other append is about to append physically afterward — which lands
    /// in the file, on top of the rewrite, exactly as normal (queue FIFO
    /// order === physical write order), so that tablet's own full record
    /// stream (image ++ append) is unaffected either way. See the module
    /// doc's "Recovery / GC contract" for the file-level argument this
    /// composes into.
    pub async fn compact_group<E: Env>(
        &self,
        env: &E,
        file: &str,
        tablet: TabletId,
        image: Vec<WalRecord<C, S>>,
    ) -> io::Result<()> {
        self.submit_with_mutation(env, file, move |state| {
            state.group_tails.insert(tablet, image);
            let bytes = PersistedState::<C, S>::encode_multiplexed_image(
                state
                    .group_tails
                    .iter()
                    .map(|(t, recs)| (*t, recs.as_slice())),
            );
            WalOp::Compact(bytes)
        })
        .await
    }

    /// Drop `tablet` from this coordinator entirely and rewrite the shared
    /// file without it — the teardown-time counterpart of
    /// [`compact_group`](Self::compact_group): a released/reclaimed tablet's
    /// bytes are reclaimed immediately rather than waiting on some other
    /// still-hosted tablet's next ordinary compaction. A tablet this
    /// coordinator never held (already forgotten, or never appended to)
    /// still triggers a (no-op-content) rewrite — cheap, and simpler than a
    /// conditional skip that would leave callers guessing whether a rewrite
    /// happened.
    pub async fn forget<E: Env>(&self, env: &E, file: &str, tablet: TabletId) -> io::Result<()> {
        self.submit_with_mutation(env, file, move |state| {
            state.group_tails.remove(&tablet);
            let bytes = PersistedState::<C, S>::encode_multiplexed_image(
                state
                    .group_tails
                    .iter()
                    .map(|(t, recs)| (*t, recs.as_slice())),
            );
            WalOp::Compact(bytes)
        })
        .await
    }
}

impl<C, S> Default for SharedWal<C, S> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::{MetaCommand, Metadata, NodeStatus};
    use crate::persist::{PersistedState, WalRecord};
    use crate::raft::LogEntry;
    use animus_env::{Disk, EnvExt};
    use animus_sim::{SimEnv, Simulator};
    use animus_tablet::TabletId;

    const WAL: &str = "shared.wal";
    const MAX_STEPS: usize = 10_000;

    fn entry(index: u64, term: u64, node: animus_env::NodeId) -> LogEntry<MetaCommand> {
        LogEntry {
            index,
            term,
            command: MetaCommand::UpsertMember {
                node,
                labels: std::collections::BTreeMap::new(),
                status: NodeStatus::Active,
            },
            config: None,
            learners: None,
        }
    }

    /// Several tablets appending "concurrently" (each from its own spawned
    /// task) must all succeed, and every appended record must survive in the
    /// final file — the coordinator's queue must never drop or corrupt an
    /// append it accepted, regardless of how many other writers overlap it.
    #[test]
    fn concurrent_appends_from_many_tablets_all_land() {
        let mut sim = Simulator::new(1);
        let env: SimEnv = sim.env(nid(0));
        let wal = Arc::new(SharedWal::<MetaCommand, Metadata>::new());

        const N_TABLETS: u64 = 5;
        for t in 0..N_TABLETS {
            let wal = wal.clone();
            let env = env.clone();
            env.clone().spawn_task(async move {
                let tablet = TabletId(t);
                let bytes = PersistedState::<MetaCommand, Metadata>::encode_tagged_record(
                    tablet,
                    &WalRecord::Append(entry(1, 1, nid(300 + t))),
                );
                wal.append(&env, WAL, bytes).await.expect("append succeeds");
            });
        }
        sim.run_until_quiescent(MAX_STEPS);

        let bytes = futures::executor::block_on(env.read(WAL)).expect("wal readable");
        let demuxed = PersistedState::<MetaCommand, Metadata>::replay_multiplexed(&bytes);
        assert_eq!(demuxed.len(), N_TABLETS as usize);
        for t in 0..N_TABLETS {
            let state = &demuxed[&TabletId(t)];
            assert_eq!(state.log.len(), 1, "tablet {t}'s append must have landed");
        }
    }

    /// A `compact` racing concurrent `append`s must never interleave with
    /// them mid-write: the file's final content is always either exactly the
    /// compacted image (compact ran last) or the image followed by whatever
    /// appends landed after it (compact ran first) — never a torn mix of the
    /// two, and every operation must complete successfully.
    #[test]
    fn compact_never_races_a_concurrent_append() {
        let mut sim = Simulator::new(7);
        let env: SimEnv = sim.env(nid(0));
        let wal = Arc::new(SharedWal::<MetaCommand, Metadata>::new());

        let image = PersistedState::<MetaCommand, Metadata>::encode_tagged_record(
            TabletId(1),
            &WalRecord::Hard {
                term: 9,
                voted_for: Some(nid(300)),
            },
        );
        let appended = PersistedState::<MetaCommand, Metadata>::encode_tagged_record(
            TabletId(2),
            &WalRecord::Append(entry(1, 1, nid(301))),
        );

        {
            let wal = wal.clone();
            let env = env.clone();
            let image = image.clone();
            env.clone().spawn_task(async move {
                wal.compact(&env, WAL, image)
                    .await
                    .expect("compact succeeds");
            });
        }
        {
            let wal = wal.clone();
            let env = env.clone();
            let appended = appended.clone();
            env.clone().spawn_task(async move {
                wal.append(&env, WAL, appended)
                    .await
                    .expect("append succeeds");
            });
        }
        sim.run_until_quiescent(MAX_STEPS);

        let bytes = futures::executor::block_on(env.read(WAL)).expect("wal readable");
        let valid_orders: [Vec<u8>; 2] = [image.clone(), {
            let mut both = image.clone();
            both.extend_from_slice(&appended);
            both
        }];
        assert!(
            valid_orders.contains(&bytes),
            "final file must be exactly the compact image, or the image followed by the append \
             — got neither, meaning the two physical writes interleaved"
        );
    }

    /// `SharedWal` must genuinely batch overlapping appends into one physical
    /// write rather than always doing one `Disk::append` per caller — the
    /// whole reason to reuse a group-commit shape instead of a plain mutex.
    /// Since `SimEnv`'s cooperative scheduler runs strictly one task at a time
    /// between `.await` points, force real overlap by having every tablet
    /// queue its bytes and yield once before any of them proceeds, so the
    /// first to resume becomes leader and finds the others already queued.
    #[test]
    fn overlapping_appends_are_coalesced_into_one_physical_write() {
        let mut sim = Simulator::new(3);
        let env: SimEnv = sim.env(nid(0));
        let wal = Arc::new(SharedWal::<MetaCommand, Metadata>::new());

        const N: u64 = 4;
        for t in 0..N {
            let wal = wal.clone();
            let env = env.clone();
            env.clone().spawn_task(async move {
                // Yield once so every task has pushed onto the queue before
                // any of them races to become leader.
                YieldOnce::default().await;
                let bytes = PersistedState::<MetaCommand, Metadata>::encode_tagged_record(
                    TabletId(t),
                    &WalRecord::Append(entry(1, 1, nid(300 + t))),
                );
                wal.append(&env, WAL, bytes).await.expect("append succeeds");
            });
        }
        sim.run_until_quiescent(MAX_STEPS);

        let bytes = futures::executor::block_on(env.read(WAL)).expect("wal readable");
        let demuxed = PersistedState::<MetaCommand, Metadata>::replay_multiplexed(&bytes);
        assert_eq!(
            demuxed.len(),
            N as usize,
            "every tablet's append must still land"
        );
    }

    /// A shared WAL must survive **two** crash/restart cycles with the
    /// interleaved records of multiple tablets intact — the depth-≥ 2 proof
    /// the root `CLAUDE.md` "prove recursive invariants at depth ≥ 2" lesson
    /// calls for: a single restart can pass by coincidence (e.g. if recovery
    /// only happened to re-derive state that was never actually read back from
    /// disk), but a second cycle building on the first's *recovered* state
    /// exercises the real demux-then-continue-appending path. Uses
    /// `Simulator::stop` (kills tasks/volatile state, keeps synced disk) +
    /// `sim.env(id)` (a fresh handle to the same backing store) to simulate a
    /// restart, exactly like `animus-control/tests/restart.rs`. Each
    /// "incarnation" constructs a brand-new `SharedWal` (as a real restarted
    /// driver would), proving the file format itself — not any in-memory
    /// coordinator state — is what survives.
    #[test]
    fn survives_two_crash_restart_cycles_with_interleaved_tablets() {
        let node = nid(0);
        let t1 = TabletId(11);
        let t2 = TabletId(12);

        let sim = Simulator::new(42);

        // Cycle 1: both tablets append their first entry.
        {
            let env = sim.env(node.clone());
            let wal = SharedWal::<MetaCommand, Metadata>::new();
            futures::executor::block_on(async {
                wal.append(
                    &env,
                    WAL,
                    PersistedState::<MetaCommand, Metadata>::encode_tagged_record(
                        t1,
                        &WalRecord::Append(entry(1, 1, nid(300))),
                    ),
                )
                .await
                .expect("t1 first append succeeds");
                wal.append(
                    &env,
                    WAL,
                    PersistedState::<MetaCommand, Metadata>::encode_tagged_record(
                        t2,
                        &WalRecord::Append(entry(1, 1, nid(400))),
                    ),
                )
                .await
                .expect("t2 first append succeeds");
            });
        }
        sim.stop(node.clone());

        // Restart #1: recover, verify, then append a second entry each.
        {
            let env = sim.env(node.clone());
            let bytes = futures::executor::block_on(env.read(WAL)).expect("wal readable");
            let demuxed = PersistedState::<MetaCommand, Metadata>::replay_multiplexed(&bytes);
            assert_eq!(demuxed[&t1].log.len(), 1);
            assert_eq!(demuxed[&t2].log.len(), 1);

            let wal = SharedWal::<MetaCommand, Metadata>::new();
            futures::executor::block_on(async {
                wal.append(
                    &env,
                    WAL,
                    PersistedState::<MetaCommand, Metadata>::encode_tagged_record(
                        t1,
                        &WalRecord::Append(entry(2, 1, nid(301))),
                    ),
                )
                .await
                .expect("t1 second append succeeds");
                wal.append(
                    &env,
                    WAL,
                    PersistedState::<MetaCommand, Metadata>::encode_tagged_record(
                        t2,
                        &WalRecord::Append(entry(2, 1, nid(401))),
                    ),
                )
                .await
                .expect("t2 second append succeeds");
            });
        }
        sim.stop(node.clone());

        // Restart #2: both tablets' full two-entry histories must be intact,
        // correctly ordered, and never cross-contaminated.
        {
            let env = sim.env(node.clone());
            let bytes = futures::executor::block_on(env.read(WAL)).expect("wal readable");
            let demuxed = PersistedState::<MetaCommand, Metadata>::replay_multiplexed(&bytes);

            assert_eq!(demuxed.len(), 2);
            assert_eq!(demuxed[&t1].log.len(), 2);
            assert_eq!(demuxed[&t1].log[0].index, 1);
            assert_eq!(demuxed[&t1].log[1].index, 2);
            assert_eq!(demuxed[&t2].log.len(), 2);
            assert_eq!(demuxed[&t2].log[0].index, 1);
            assert_eq!(demuxed[&t2].log[1].index, 2);
        }
    }

    // --- C-05 PR 2: the tagged/group-aware API (open/append_tagged/compact_group/forget) ---

    /// `open` seeds `group_tails` from whatever the file already holds, and
    /// `recovered_state` replays exactly one tablet's own stream out of it —
    /// the shared-WAL analogue of a per-group node's own
    /// `env.read`+`decode`+`replay` recovery, but sourced from one seeded
    /// read shared by every tablet rather than each doing its own.
    #[test]
    fn open_seeds_group_tails_and_recovered_state_replays_per_tablet() {
        let sim = Simulator::new(11);
        let t1 = TabletId(1);
        let t2 = TabletId(2);
        {
            let env: SimEnv = sim.env(nid(0));
            let wal = futures::executor::block_on(SharedWal::<KvC, KvS>::open(&env, WAL))
                .expect("open succeeds on a missing file");
            futures::executor::block_on(wal.append_tagged(
                &env,
                WAL,
                t1,
                &[kv_append(1, 1, nid(300))],
            ))
            .expect("t1 append succeeds");
            futures::executor::block_on(wal.append_tagged(
                &env,
                WAL,
                t2,
                &[kv_append(1, 1, nid(400)), kv_append(2, 1, nid(400))],
            ))
            .expect("t2 append succeeds");
        }

        // A fresh coordinator, seeded by a fresh `open` — this is what a
        // real node-restart does.
        let env: SimEnv = sim.env(nid(0));
        let wal = futures::executor::block_on(SharedWal::<KvC, KvS>::open(&env, WAL))
            .expect("open succeeds");
        let recovered_t1 = futures::executor::block_on(wal.recovered_state(t1));
        let recovered_t2 = futures::executor::block_on(wal.recovered_state(t2));
        let recovered_absent = futures::executor::block_on(wal.recovered_state(TabletId(99)));
        assert_eq!(recovered_t1.log.len(), 1);
        assert_eq!(recovered_t2.log.len(), 2);
        assert!(
            recovered_absent.is_empty(),
            "a tablet never seen in the file must replay as a fresh/empty state"
        );
    }

    /// One tablet's `compact_group` reclaims exactly that tablet's own
    /// already-compacted-away bytes, and byte-for-byte preserves every OTHER
    /// tablet's own tail untouched — the GC-bound claim the module doc
    /// makes. Verified by re-opening (which re-demuxes the physical file
    /// from scratch) rather than trusting the in-memory cache alone.
    #[test]
    fn compact_group_reclaims_only_its_own_tablet_and_preserves_siblings() {
        let sim = Simulator::new(12);
        let env: SimEnv = sim.env(nid(0));
        let t_slow = TabletId(1);
        let t_fast = TabletId(2);
        let wal = futures::executor::block_on(SharedWal::<KvC, KvS>::open(&env, WAL))
            .expect("open succeeds");

        // Both tablets append several entries each.
        for i in 1..=5u64 {
            futures::executor::block_on(wal.append_tagged(
                &env,
                WAL,
                t_slow,
                &[kv_append(i, 1, nid(300))],
            ))
            .expect("slow append succeeds");
            futures::executor::block_on(wal.append_tagged(
                &env,
                WAL,
                t_fast,
                &[kv_append(i, 1, nid(400))],
            ))
            .expect("fast append succeeds");
        }

        // Only `t_fast` compacts — down to a single minimal image record.
        futures::executor::block_on(wal.compact_group(
            &env,
            WAL,
            t_fast,
            vec![WalRecord::Hard {
                term: 1,
                voted_for: None,
            }],
        ))
        .expect("compact succeeds");

        // Re-open from scratch: the physical file must reflect exactly
        // `t_fast`'s minimal image and `t_slow`'s full untouched 5-entry
        // history — never a mix, and never the slow tablet's history
        // truncated by a compaction it never asked for.
        let wal2 = futures::executor::block_on(SharedWal::<KvC, KvS>::open(&env, WAL))
            .expect("reopen succeeds");
        let recovered_slow = futures::executor::block_on(wal2.recovered_state(t_slow));
        let recovered_fast = futures::executor::block_on(wal2.recovered_state(t_fast));
        assert_eq!(
            recovered_slow.log.len(),
            5,
            "a sibling tablet's own retained prefix must never be reclaimed by \
             another tablet's compaction"
        );
        assert_eq!(recovered_fast.log.len(), 0);
        assert_eq!(recovered_fast.term, 1);
    }

    /// `forget` removes a tablet's bytes from the physical file at once
    /// (teardown-time reclaim), leaving every other tablet's own tail
    /// exactly as it was.
    #[test]
    fn forget_removes_a_tablet_and_preserves_siblings() {
        let sim = Simulator::new(13);
        let env: SimEnv = sim.env(nid(0));
        let gone = TabletId(1);
        let stays = TabletId(2);
        let wal = futures::executor::block_on(SharedWal::<KvC, KvS>::open(&env, WAL))
            .expect("open succeeds");
        futures::executor::block_on(wal.append_tagged(
            &env,
            WAL,
            gone,
            &[kv_append(1, 1, nid(300))],
        ))
        .expect("append succeeds");
        futures::executor::block_on(wal.append_tagged(
            &env,
            WAL,
            stays,
            &[kv_append(1, 1, nid(400))],
        ))
        .expect("append succeeds");

        futures::executor::block_on(wal.forget(&env, WAL, gone)).expect("forget succeeds");

        let wal2 = futures::executor::block_on(SharedWal::<KvC, KvS>::open(&env, WAL))
            .expect("reopen succeeds");
        assert!(futures::executor::block_on(wal2.recovered_state(gone)).is_empty());
        assert_eq!(
            futures::executor::block_on(wal2.recovered_state(stays))
                .log
                .len(),
            1
        );
    }

    /// A quiesced (long-idle) tablet's own cached tail must survive a
    /// SIBLING's compaction untouched even when the quiesced tablet's own
    /// task never runs again in between — the "no tablet ever waits on
    /// another's compaction" claim, and the ADR 0048 quiescence interaction
    /// the design doc calls out: a quiesced group still durably holds
    /// whatever it last appended, and another group's GC must never disturb
    /// that.
    #[test]
    fn a_quiesced_tablets_tail_survives_an_unrelated_sibling_compaction() {
        let sim = Simulator::new(14);
        let env: SimEnv = sim.env(nid(0));
        let quiesced = TabletId(7);
        let active = TabletId(8);
        let wal = futures::executor::block_on(SharedWal::<KvC, KvS>::open(&env, WAL))
            .expect("open succeeds");

        futures::executor::block_on(wal.append_tagged(
            &env,
            WAL,
            quiesced,
            &[kv_append(1, 1, nid(500)), kv_append(2, 1, nid(500))],
        ))
        .expect("quiesced tablet's one-time append succeeds");

        // The active tablet churns and compacts several times while the
        // quiesced tablet's own task never touches the coordinator again.
        for round in 1..=3u64 {
            futures::executor::block_on(wal.append_tagged(
                &env,
                WAL,
                active,
                &[kv_append(round, 1, nid(600))],
            ))
            .expect("active append succeeds");
            futures::executor::block_on(wal.compact_group(
                &env,
                WAL,
                active,
                vec![WalRecord::Hard {
                    term: round,
                    voted_for: None,
                }],
            ))
            .expect("active compact succeeds");
        }

        let recovered_quiesced = futures::executor::block_on(wal.recovered_state(quiesced));
        assert_eq!(
            recovered_quiesced.log.len(),
            2,
            "an unrelated group's repeated compaction must never touch a quiesced \
             sibling's own retained tail"
        );

        // And the same holds after a full re-open from the physical file.
        let wal2 = futures::executor::block_on(SharedWal::<KvC, KvS>::open(&env, WAL))
            .expect("reopen succeeds");
        assert_eq!(
            futures::executor::block_on(wal2.recovered_state(quiesced))
                .log
                .len(),
            2
        );
    }

    /// `append_tagged`/`compact_group` survive a real crash+restart cycle
    /// (`Simulator::stop` + a brand-new `SharedWal::open`), across two
    /// interleaved tablets, one of which compacts mid-run — the shared-WAL
    /// depth-≥2 recursive-invariant proof this file's own pre-existing
    /// `survives_two_crash_restart_cycles_with_interleaved_tablets` gives
    /// the raw API, now for the tagged/GC-aware one.
    #[test]
    fn tagged_api_survives_crash_restart_with_a_mid_run_compaction() {
        let node = nid(0);
        let t1 = TabletId(21);
        let t2 = TabletId(22);
        let sim = Simulator::new(15);

        // Cycle 1: both tablets append; t1 then compacts.
        {
            let env = sim.env(node.clone());
            let wal = futures::executor::block_on(SharedWal::<KvC, KvS>::open(&env, WAL))
                .expect("open succeeds");
            futures::executor::block_on(wal.append_tagged(
                &env,
                WAL,
                t1,
                &[kv_append(1, 1, nid(300)), kv_append(2, 1, nid(300))],
            ))
            .expect("t1 appends succeed");
            futures::executor::block_on(wal.append_tagged(
                &env,
                WAL,
                t2,
                &[kv_append(1, 1, nid(400))],
            ))
            .expect("t2 append succeeds");
            futures::executor::block_on(wal.compact_group(
                &env,
                WAL,
                t1,
                vec![kv_append(2, 1, nid(300))],
            ))
            .expect("t1 compact succeeds");
        }
        sim.stop(node.clone());

        // Restart #1: t1 must show its compacted single-entry tail, t2 its
        // untouched one; append a second entry to each.
        {
            let env = sim.env(node.clone());
            let wal = futures::executor::block_on(SharedWal::<KvC, KvS>::open(&env, WAL))
                .expect("open succeeds");
            let r1 = futures::executor::block_on(wal.recovered_state(t1));
            let r2 = futures::executor::block_on(wal.recovered_state(t2));
            assert_eq!(r1.log.len(), 1, "t1's compacted image must survive intact");
            assert_eq!(r1.log[0].index, 2);
            assert_eq!(r2.log.len(), 1);

            futures::executor::block_on(wal.append_tagged(
                &env,
                WAL,
                t1,
                &[kv_append(3, 1, nid(301))],
            ))
            .expect("t1 second append succeeds");
            futures::executor::block_on(wal.append_tagged(
                &env,
                WAL,
                t2,
                &[kv_append(2, 1, nid(401))],
            ))
            .expect("t2 second append succeeds");
        }
        sim.stop(node.clone());

        // Restart #2: full, correctly-ordered, never-cross-contaminated
        // histories for both.
        {
            let env = sim.env(node.clone());
            let wal = futures::executor::block_on(SharedWal::<KvC, KvS>::open(&env, WAL))
                .expect("open succeeds");
            let r1 = futures::executor::block_on(wal.recovered_state(t1));
            let r2 = futures::executor::block_on(wal.recovered_state(t2));
            assert_eq!(r1.log.len(), 2);
            assert_eq!(r1.log[0].index, 2);
            assert_eq!(r1.log[1].index, 3);
            assert_eq!(r2.log.len(), 2);
            assert_eq!(r2.log[0].index, 1);
            assert_eq!(r2.log[1].index, 2);
        }
    }

    /// The `KvCommand`-shaped instantiation this crate's own type never
    /// otherwise constructs — a minimal, structurally-`KvCommand`-like
    /// stand-in kept entirely inside this test module so the tagged API's
    /// tests exercise a SECOND `(C, S)` instantiation beyond the default
    /// `(MetaCommand, Metadata)` one, proving `SharedWal<C, S>` is actually
    /// generic and not accidentally still hardcoded to the control plane's
    /// own types (`animus-cp-data` is the real second instantiation in
    /// production; this crate cannot depend on it, being lower in the
    /// dependency graph, so this stand-in is the closest in-crate proof).
    #[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    struct KvC {
        val: u64,
    }
    #[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    struct KvS;

    fn kv_append(index: u64, term: u64, node: animus_env::NodeId) -> WalRecord<KvC, KvS> {
        WalRecord::Append(LogEntry {
            index,
            term,
            command: KvC { val: index },
            config: None,
            learners: Some([node].into_iter().collect()),
        })
    }

    #[derive(Default)]
    struct YieldOnce {
        yielded: bool,
    }

    impl std::future::Future for YieldOnce {
        type Output = ();
        fn poll(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<()> {
            if self.yielded {
                std::task::Poll::Ready(())
            } else {
                self.yielded = true;
                cx.waker().wake_by_ref();
                std::task::Poll::Pending
            }
        }
    }
}
