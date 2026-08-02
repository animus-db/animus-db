//! WAL **group commit**: many concurrent writes share one `fsync`.
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
//! ## Determinism (ADR 0003)
//!
//! All disk I/O flows through the `Env` [`Disk`] seam, exactly as before. The
//! coordination state is a plain `std::sync::Mutex<Inner>` whose guard is **never
//! held across an `.await`**: the I/O (`append`/`sync`) happens lock-free, and
//! the lock is taken only for brief synchronous buffer mutations / waker
//! bookkeeping. Ordering is a deterministic function of the scheduler: writers
//! are assigned a strictly increasing `wal_seq` under the lock in call order, the
//! leader is whichever writer first observes no flush in progress, and waiters /
//! their wakers live in `BTreeMap`s (no `HashMap`). Under the cooperative
//! single-threaded `SimEnv` executor a writer yields once after enqueueing, which
//! lets every other writer that is *already ready in the same drain cycle* enqueue
//! into the same batch before the leader flushes — so batching is observable and
//! reproducible from the seed.
//!
//! ## Crash safety
//!
//! The durability boundary is unchanged: a record is durable iff a `sync` that
//! covered it has returned. The memtable is mutated by the caller **only after**
//! [`commit`](GroupCommit::commit) resolves, so an un-synced batch tail dropped by
//! a crash is exactly the set of writes whose `commit` had not yet returned — they
//! were never acked, never made visible to reads, and recovery (WAL replay) sees
//! only the synced prefix. A leader that crashes mid-flush syncs nothing past the
//! prior durable point, so the whole in-flight batch is lost together; no waiter
//! is woken, so no such write is ever reported committed.
//!
//! [`Disk`]: custos_env::Disk

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};

use custos_env::Env;

use crate::{Result, StorageError};

/// Coordinates group-committed appends to one WAL file.
pub(super) struct GroupCommit {
    /// The WAL file name this coordinator writes to.
    file: String,
    inner: Mutex<Inner>,
    /// Count of batch `fsync`s performed (one per group commit). Introspection:
    /// fewer than the number of writes proves coalescing happened.
    batch_syncs: AtomicU64,
}

struct Inner {
    /// Next WAL sequence number to hand out. Strictly increasing; assigned to a
    /// writer's record under the lock in call order.
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
    /// Set while a flush is truncating the WAL file and resetting this coordinator.
    /// A new `commit` parks before enqueueing until it clears, so a record can
    /// never be appended into the WAL between the truncate and the reset (which
    /// would otherwise be lost / mis-sequenced). Truncation only begins when no
    /// writer is mid-`commit`, so this never deadlocks a live writer.
    truncating: bool,
    /// Wakers of `commit`s parked because [`truncating`](Inner::truncating) is set.
    truncate_waiters: Vec<Waker>,
}

impl GroupCommit {
    /// A fresh coordinator for `file` with the sequence space starting at 0 (a
    /// fresh or fully-recovered WAL: recovered records already live in the
    /// memtable, and the next durable record is the first new write).
    pub(super) fn new(file: String) -> Self {
        Self {
            file,
            batch_syncs: AtomicU64::new(0),
            inner: Mutex::new(Inner {
                next_seq: 0,
                durable_seq: 0,
                pending: Vec::new(),
                flushing: false,
                waiters: BTreeMap::new(),
                failed_through: 0,
                truncating: false,
                truncate_waiters: Vec::new(),
            }),
        }
    }

    /// The highest WAL sequence currently durable. A flush samples this when it
    /// snapshots the memtable and again before truncating: equality means nothing
    /// new became durable in between, so the WAL holds exactly the flushed records
    /// and is safe to truncate.
    pub(super) fn durable_seq(&self) -> u64 {
        self.lock().durable_seq
    }

    /// Begin a WAL truncation: latch out new `commit`s. Returns `false` (refusing)
    /// if any writer is mid-`commit` (pending records or an active flusher) or a
    /// truncation is already in progress — the caller then leaves the WAL intact.
    /// On `true` the caller must `replace` the WAL file then call
    /// [`finish_truncate`](Self::finish_truncate).
    pub(super) fn begin_truncate(&self) -> bool {
        let mut inner = self.lock();
        if inner.truncating || !inner.pending.is_empty() || inner.flushing {
            return false;
        }
        inner.truncating = true;
        true
    }

    /// Finish a truncation begun by [`begin_truncate`](Self::begin_truncate): the
    /// WAL file is now fresh (empty), so restart the sequence space at 0 and let
    /// parked `commit`s proceed. All prior records are durable in the SSTable.
    pub(super) fn finish_truncate(&self) {
        let mut inner = self.lock();
        debug_assert!(inner.pending.is_empty(), "truncate with pending records");
        inner.next_seq = 0;
        inner.durable_seq = 0;
        inner.failed_through = 0;
        inner.truncating = false;
        // Wake any writer that parked waiting for the truncation to clear.
        let woken = std::mem::take(&mut inner.truncate_waiters);
        drop(inner);
        for w in woken {
            w.wake();
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
        // If the WAL is being truncated, park until it finishes so we never append
        // into a file that is about to be replaced (and so our sequence is assigned
        // in the post-reset space). Under the cooperative SimEnv this never blocks a
        // live write — truncation only begins with no `commit` in flight.
        NotTruncating { gc: self }.await;

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
                    Action::Lead { batch, up_to }
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
                Action::Lead { batch, up_to } => {
                    // Perform the single batched append + sync, lock-free.
                    let res = self.flush_batch(env, &batch).await;
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

    /// Append the whole batch then `sync` once. The lock is **not** held here.
    async fn flush_batch<E: Env>(&self, env: &E, batch: &[u8]) -> Result<()> {
        if !batch.is_empty() {
            env.append(&self.file, batch)
                .await
                .map_err(|e| StorageError::Backend(e.to_string()))?;
        }
        env.sync(&self.file)
            .await
            .map_err(|e| StorageError::Backend(e.to_string()))?;
        self.batch_syncs.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Number of batch `fsync`s performed since open (introspection / tests).
    pub(super) fn batch_sync_count(&self) -> u64 {
        self.batch_syncs.load(Ordering::Relaxed)
    }
}

/// What a `commit` poll iteration should do, decided under the lock.
enum Action {
    /// Our record is already durable.
    Done,
    /// Our record's batch failed to sync; surface the error.
    Failed,
    /// Lead the flush of this claimed `batch`, which makes records `<= up_to`
    /// durable on success.
    Lead { batch: Vec<u8>, up_to: u64 },
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

/// Parks until `gc.durable_seq >= seq` (or the batch failed). Registers its waker
/// under the lock so the leader's post-`sync` `take_wakers_up_to` re-readies it.
struct DurableUpTo<'a> {
    gc: &'a GroupCommit,
    seq: u64,
}

impl Future for DurableUpTo<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut inner = self.gc.lock();
        if inner.durable_seq >= self.seq || inner.failed_through >= self.seq {
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

/// Parks until no WAL truncation is in progress.
struct NotTruncating<'a> {
    gc: &'a GroupCommit,
}

impl Future for NotTruncating<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut inner = self.gc.lock();
        if inner.truncating {
            inner.truncate_waiters.push(cx.waker().clone());
            Poll::Pending
        } else {
            Poll::Ready(())
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
