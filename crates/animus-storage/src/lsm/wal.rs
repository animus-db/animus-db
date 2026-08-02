//! WAL **group commit** over **rotating numbered segments**: many concurrent
//! writes share one `fsync`, and the log is split into bounded segment files so a
//! flush can drop whole covered segments instead of rewriting one growing file.
//!
//! ## Why
//!
//! The naive WAL path is `append(record)` + `sync()` per write, so every write
//! pays a full `fsync` before it returns. Under `ProdEnv` (a real `fsync`) that
//! dominates the write cost. **Group commit** amortizes it: concurrent writers
//! each append their record to a shared pending buffer, then exactly one of them
//! (the *leader*) performs a single `append` of the whole batch followed by one
//! `sync`, and wakes every writer whose record the sync covered. An ack still
//! means durable — a writer's [`commit`](GroupCommit::commit) returns only after
//! a `sync` that included its record has completed.
//!
//! ## Segment rotation
//!
//! The WAL is a sequence of numbered files `<prefix>wal-NNNNNN`. The leader
//! appends each batch to the **active** segment; once that segment exceeds a byte
//! threshold the next batch opens a **fresh** segment (a new file) and the old
//! one is *sealed*. Every WAL record carries a strictly increasing `wal_seq`, and
//! each sealed segment records the highest `wal_seq` it contains. On a memtable
//! flush the engine learns which segments are **fully covered** by the flush (all
//! their records folded into the new SSTable) via
//! [`segments_covered_by`](GroupCommit::segments_covered_by) and simply `remove`s
//! those files — bounding total WAL size and avoiding any whole-file rewrite. The
//! active (partially covered) segment is always retained. The live segment set is
//! recorded in the durable MANIFEST so recovery knows which files to replay.
//!
//! ## Determinism (ADR 0003)
//!
//! All disk I/O flows through the `Env` [`Disk`] seam. The coordination state is a
//! plain `std::sync::Mutex<Inner>` whose guard is **never held across an
//! `.await`**: the I/O (`append`/`sync`) happens lock-free, and the lock is taken
//! only for brief synchronous buffer mutations / waker bookkeeping. Ordering is a
//! deterministic function of the scheduler: writers are assigned a strictly
//! increasing `wal_seq` under the lock in call order, the leader is whichever
//! writer first observes no flush in progress, segment rotation is decided by the
//! leader under the lock, and waiters / their wakers / the sealed-segment map live
//! in `BTreeMap`s (no `HashMap`). Under the cooperative single-threaded `SimEnv`
//! executor a writer yields once after enqueueing, which lets every other writer
//! that is *already ready in the same drain cycle* enqueue into the same batch
//! before the leader flushes — so batching is observable and reproducible from the
//! seed.
//!
//! ## Crash safety
//!
//! The durability boundary is unchanged: a record is durable iff a `sync` that
//! covered it has returned. The memtable is mutated by the caller **only after**
//! [`commit`](GroupCommit::commit) resolves, so an un-synced batch tail dropped by
//! a crash is exactly the set of writes whose `commit` had not yet returned — they
//! were never acked, never made visible to reads, and recovery (replaying the live
//! segments) sees only the synced prefix. A leader that crashes mid-flush syncs
//! nothing past the prior durable point, so the whole in-flight batch is lost
//! together; no waiter is woken, so no such write is ever reported committed.
//! Segment GC is crash-safe at the manifest swap: a segment file is `remove`d only
//! **after** the manifest that no longer names it is durable, so a crash mid-GC
//! recovers a manifest that still lists the segment (whose bytes are intact) and
//! replay is correct — replaying an already-flushed record just re-inserts an
//! identical `(key, version)` slot (idempotent).
//!
//! [`Disk`]: animus_env::Disk

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};

use animus_env::Env;

use crate::{Result, StorageError};

/// Coordinates group-committed appends to a rotating set of WAL segment files
/// named `<prefix>wal-NNNNNN`.
pub(super) struct GroupCommit {
    /// Filename prefix shared by every segment (`{prefix}wal-{seg:06}`).
    prefix: String,
    /// Byte threshold: once the active segment's appended bytes reach this, the
    /// next batch rotates to a fresh segment.
    seg_threshold: u64,
    inner: Mutex<Inner>,
    /// Count of batch `fsync`s performed (one per group commit). Introspection:
    /// fewer than the number of writes proves coalescing happened.
    batch_syncs: AtomicU64,
}

struct Inner {
    /// Next WAL sequence number to hand out. Strictly increasing; assigned to a
    /// writer's record under the lock in call order. **Monotonic for the life of
    /// the engine** (segment rotation/GC never resets it).
    next_seq: u64,
    /// Highest sequence number made durable (an enclosing `sync` returned).
    durable_seq: u64,
    /// Records appended but not yet flushed, oldest first: `(seq, bytes)`.
    pending: Vec<(u64, Vec<u8>)>,
    /// Whether a leader is currently performing the batch `append` + `sync`.
    flushing: bool,
    /// Writers parked waiting for their sequence to become durable, keyed by the
    /// sequence they are waiting on (`BTreeMap` for deterministic iteration).
    waiters: BTreeMap<u64, Vec<Waker>>,
    /// Set when a leader's batch `append`/`sync` failed: every writer whose record
    /// was in that lost batch must surface the failure rather than claim durability.
    failed_through: u64,
    /// The segment number currently being appended to.
    active_seg: u64,
    /// Bytes the leader has appended to the active segment so far (drives
    /// rotation). Counts bytes handed to `append`, so it advances as batches are
    /// written, not only once they sync.
    active_seg_bytes: u64,
    /// Sealed (no-longer-active) segments: segment number → the highest `wal_seq`
    /// that segment contains. `BTreeMap` for deterministic iteration. The active
    /// segment is never in this map (its max seq is `durable_seq`).
    sealed: BTreeMap<u64, u64>,
}

impl GroupCommit {
    /// A fresh coordinator writing segments under `prefix`, with `live_segments`
    /// being the segments the recovered manifest names (in ascending order) and
    /// `seg_threshold` the per-segment byte budget.
    ///
    /// The sequence space resumes after recovery: recovered records already live
    /// in the memtable, so the next durable record is the first *new* write. The
    /// highest live segment is reopened as the active segment (further appends ride
    /// it until it crosses `seg_threshold`); the rest are sealed. `live_segments`
    /// empty means a fresh engine — the first write opens segment 0.
    pub(super) fn new(prefix: String, live_segments: &[u64], seg_threshold: u64) -> Self {
        // All recovered records are folded into the memtable already, so the
        // resumed sequence space starts at 0; the active segment is the highest
        // live one (or 0 for a fresh engine). Older live segments are sealed with
        // max_seq 0 — they hold only recovered (pre-resume) records, so no *new*
        // record can ever be "covered" by their seq, and they survive until a
        // flush captures the whole memtable and GCs them by membership.
        let active_seg = live_segments.last().copied().unwrap_or(0);
        let mut sealed = BTreeMap::new();
        for &seg in live_segments {
            if seg != active_seg {
                sealed.insert(seg, 0);
            }
        }
        Self {
            prefix,
            seg_threshold: seg_threshold.max(1),
            batch_syncs: AtomicU64::new(0),
            inner: Mutex::new(Inner {
                next_seq: 0,
                durable_seq: 0,
                pending: Vec::new(),
                flushing: false,
                waiters: BTreeMap::new(),
                failed_through: 0,
                active_seg,
                active_seg_bytes: 0,
                sealed,
            }),
        }
    }

    /// The on-disk file name for segment `seg`.
    pub(super) fn segment_file(&self, seg: u64) -> String {
        format!("{}wal-{seg:06}", self.prefix)
    }

    /// The highest WAL sequence currently durable. A flush samples this when it
    /// snapshots the memtable: it is the watermark up to which every WAL record is
    /// reflected in the flushed SSTable.
    pub(super) fn durable_seq(&self) -> u64 {
        self.lock().durable_seq
    }

    /// The live segment set (ascending): every sealed segment plus the active one.
    /// Recorded in the durable manifest so recovery replays exactly these files.
    pub(super) fn live_segments(&self) -> Vec<u64> {
        let inner = self.lock();
        let mut segs: Vec<u64> = inner.sealed.keys().copied().collect();
        segs.push(inner.active_seg);
        segs
    }

    /// Given a flush watermark (`durable_seq` at memtable-snapshot time), return
    /// the **sealed** segments fully covered by the flush — every record they hold
    /// has `wal_seq <= watermark`, so it is now in the SSTable and the segment file
    /// can be removed. The active segment is never returned (it may carry records
    /// beyond the watermark, and is where new writes land). The caller removes the
    /// files **after** a manifest no longer naming them is durable, then calls
    /// [`forget_segments`](Self::forget_segments).
    pub(super) fn segments_covered_by(&self, watermark: u64) -> Vec<u64> {
        let inner = self.lock();
        inner
            .sealed
            .iter()
            .filter(|&(_, &max_seq)| max_seq <= watermark)
            .map(|(&seg, _)| seg)
            .collect()
    }

    /// Drop the given sealed segments from the live set, after their files have
    /// been removed and the new manifest is durable. Idempotent; only ever removes
    /// sealed segments (never the active one).
    pub(super) fn forget_segments(&self, segs: &[u64]) {
        let mut inner = self.lock();
        for seg in segs {
            inner.sealed.remove(seg);
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().expect("wal group-commit poisoned")
    }

    /// Durably append `bytes` (one encoded WAL record) as part of a shared group
    /// commit, returning only once a `sync` covering this record has completed.
    ///
    /// Multiple tasks calling this concurrently coalesce: their records ride one
    /// `append` and one `sync`. The caller must not mutate the memtable until this
    /// resolves (durability precedes visibility).
    ///
    /// # Errors
    /// Returns [`StorageError::Backend`] if the underlying `append`/`sync` failed
    /// for the batch carrying this record (nothing past the prior durable point
    /// became durable).
    pub(super) async fn commit<E: Env>(&self, env: &E, bytes: Vec<u8>) -> Result<()> {
        // Enqueue under the lock, taking a strictly increasing sequence number.
        let my_seq = {
            let mut inner = self.lock();
            inner.next_seq += 1;
            let seq = inner.next_seq;
            inner.pending.push((seq, bytes));
            seq
        };

        // Yield once so any writer already ready in this scheduler drain cycle can
        // enqueue into the same batch before we (potentially) become the leader.
        // Under SimEnv this is a single cooperative turn; under ProdEnv it lets a
        // sibling task on the runtime make progress. Cheap and deterministic.
        YieldOnce::default().await;

        loop {
            let action = {
                let mut inner = self.lock();
                if inner.failed_through >= my_seq {
                    Action::Failed
                } else if inner.durable_seq >= my_seq {
                    Action::Done
                } else if inner.flushing {
                    Action::Wait
                } else {
                    // Become the leader: claim the whole pending buffer.
                    inner.flushing = true;
                    let mut batch = Vec::new();
                    let mut up_to = inner.durable_seq;
                    for (seq, rec) in inner.pending.drain(..) {
                        batch.extend_from_slice(&rec);
                        up_to = up_to.max(seq);
                    }
                    // Decide the target segment: rotate to a fresh one if the
                    // active segment is over threshold. Rotation only happens
                    // between batches (no flush is in progress here), so every
                    // record already in the active segment is durable: seal it at
                    // the current durable seq.
                    if inner.active_seg_bytes >= self.seg_threshold {
                        let sealed_seg = inner.active_seg;
                        let sealed_max = inner.durable_seq;
                        inner.sealed.insert(sealed_seg, sealed_max);
                        inner.active_seg += 1;
                        inner.active_seg_bytes = 0;
                    }
                    let seg = inner.active_seg;
                    // Account the bytes now so the *next* batch's rotation decision
                    // sees this batch's contribution.
                    inner.active_seg_bytes += batch.len() as u64;
                    Action::Lead { batch, up_to, seg }
                }
            };

            match action {
                Action::Done => return Ok(()),
                Action::Failed => {
                    return Err(StorageError::Backend("wal group-commit sync failed".into()));
                }
                Action::Wait => {
                    DurableUpTo {
                        gc: self,
                        seq: my_seq,
                    }
                    .await;
                }
                Action::Lead { batch, up_to, seg } => {
                    // Perform the single batched append + sync, lock-free, to the
                    // chosen segment file.
                    let res = self.flush_batch(env, seg, &batch).await;
                    let woken = {
                        let mut inner = self.lock();
                        inner.flushing = false;
                        match &res {
                            Ok(()) => inner.durable_seq = inner.durable_seq.max(up_to),
                            // The append/sync failed: nothing past the prior durable
                            // point is durable, and the claimed records are gone from
                            // `pending`. Mark the whole lost batch failed so every
                            // writer it carried surfaces the error rather than waiting
                            // forever or falsely claiming durability.
                            Err(_) => inner.failed_through = inner.failed_through.max(up_to),
                        }
                        // Wake **all** parked writers, not only the ones this batch
                        // made durable: a writer whose record arrived *after* we
                        // claimed the batch is now durable-or-not but still parked,
                        // and one of them must re-poll to lead the next batch. They
                        // re-register if they still must wait, so this cannot lose a
                        // wakeup (the alternative — waking only `<= durable_seq` —
                        // would strand a later record with no leader: a deadlock).
                        inner.take_all_wakers()
                    };
                    for w in woken {
                        w.wake();
                    }
                    // Loop: re-evaluate (we now observe Done / Failed, or, if our
                    // record was not in this batch, re-lead or wait).
                }
            }
        }
    }

    /// Append the whole batch to segment `seg`'s file then `sync` it once. The
    /// lock is **not** held here.
    async fn flush_batch<E: Env>(&self, env: &E, seg: u64, batch: &[u8]) -> Result<()> {
        let file = self.segment_file(seg);
        if !batch.is_empty() {
            env.append(&file, batch)
                .await
                .map_err(|e| StorageError::Backend(e.to_string()))?;
        }
        env.sync(&file)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        self.batch_syncs.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Number of batch `fsync`s performed since open (introspection / tests).
    pub(super) fn batch_sync_count(&self) -> u64 {
        self.batch_syncs.load(Ordering::Relaxed)
    }

    /// Number of live WAL segments (sealed + active). Introspection / tests.
    pub(super) fn segment_count(&self) -> usize {
        let inner = self.lock();
        inner.sealed.len() + 1
    }
}

/// What a `commit` poll iteration should do, decided under the lock.
enum Action {
    /// Our record is already durable.
    Done,
    /// Our record's batch failed to sync; surface the error.
    Failed,
    /// Lead the flush of this claimed `batch` to segment `seg`, which makes records
    /// `<= up_to` durable on success.
    Lead {
        batch: Vec<u8>,
        up_to: u64,
        seg: u64,
    },
    /// Another writer is leading; park until our sequence is durable.
    Wait,
}

impl Inner {
    /// Remove and return every parked waker. Called after a batch flush so each
    /// waiter re-polls: durable ones complete, the rest re-park or one leads the
    /// next batch. Draining all of them is what prevents a stranded record (a
    /// record enqueued after the leader claimed its batch) from deadlocking.
    fn take_all_wakers(&mut self) -> Vec<Waker> {
        std::mem::take(&mut self.waiters)
            .into_values()
            .flatten()
            .collect()
    }
}

/// Parks a non-leading writer **only while a flush is actually in progress**,
/// then resolves so the `commit` loop re-decides its action. Registers its waker
/// under the lock so the leader's post-`sync` wake re-readies it.
///
/// Resolving as soon as `!flushing` (rather than only on `durable_seq >= seq`) is
/// load-bearing for liveness under real multithreading: a writer whose record was
/// enqueued *after* the current leader claimed its batch is not covered by that
/// flush, so once the leader finishes this must return to the loop and become the
/// **next** leader for its own record. Parking until `durable_seq >= seq` here
/// stranded it — nothing else would ever flush its record — which deadlocked the
/// multi-threaded `ProdEnv` path (the single-threaded `SimEnv` cannot produce that
/// interleaving). The `commit` loop re-checks `durable`/`failed`/`flushing` under
/// the lock, so an early resolve just causes a re-decision (it re-parks if a flush
/// is still running, or leads otherwise).
struct DurableUpTo<'a> {
    gc: &'a GroupCommit,
    seq: u64,
}

impl Future for DurableUpTo<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut inner = self.gc.lock();
        if inner.durable_seq >= self.seq || inner.failed_through >= self.seq || !inner.flushing {
            Poll::Ready(())
        } else {
            inner
                .waiters
                .entry(self.seq)
                .or_default()
                .push(cx.waker().clone());
            Poll::Pending
        }
    }
}

/// Yields control exactly once: the first poll re-readies the task and returns
/// `Pending`; the second poll returns `Ready`. Lets a sibling task that is already
/// ready run before we proceed (the group-commit accumulation window).
#[derive(Default)]
struct YieldOnce {
    yielded: bool,
}

impl Future for YieldOnce {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.yielded {
            Poll::Ready(())
        } else {
            self.yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}
