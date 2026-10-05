//! A minimal FIFO-fair async mutex for the WAL file lock (`wal_lock`).
//!
//! Why not `futures::lock::Mutex`: it is *unfair* — when the holder drops, a
//! waiter is woken but the lock is merely released, so a task that calls
//! `lock()` again before the woken waiter is polled takes it (barging).
//! `drive` re-locks for its next persist round the instant the previous one
//! lands (under continuous proposals `has_unflushed_wal()` never goes false
//! and its `select` polls the persist arm first, with no yield point in
//! between), so the ADR 0038 apply task's compaction section could wait on
//! the lock forever: no further `merge_batch`, a frozen `engine_applied_index`,
//! a stale `metadata()` cache and an unbounded `pending_apply`.
//!
//! This lock hands ownership over in strict arrival order: a `lock()` call
//! that finds the lock held *or any earlier waiter queued* enqueues itself,
//! and release reserves the lock for the queue head. A waiter therefore waits
//! for at most the holders ahead of it, which makes the apply task's wait one
//! persist round, not unbounded. Chosen over a waiter-flag/yield scheme
//! because it needs no timer (no `env.sleep`, so identical behaviour and no
//! added virtual/real latency under both `SimEnv` and `ProdEnv`), is
//! deterministic (order is by poll order, no wall clock or randomness), and
//! adds no dependency (`tokio::sync::Mutex` is dev-only here).

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::task::{Context, Poll, Waker};

struct Inner {
    held: bool,
    /// The waiter the lock has been handed to, not yet polled to claim it.
    reserved_for: Option<u64>,
    next_ticket: u64,
    queue: VecDeque<(u64, Waker)>,
}

/// FIFO-fair async mutex over `()` (it guards a file, not data).
pub struct FairMutex {
    inner: Mutex<Inner>,
    /// The guarded WAL file's piggybacked sync-marker state (issue #1132). It
    /// rides with the lock that serializes the file's writers so the two cannot
    /// be separated; only touch it while holding the lock.
    markers: crate::persist::SyncMarkerState,
    /// The bytes appended to the guarded file while a compaction rewrite is
    /// staging its replacement outside the lock (issue #1116). See
    /// [`RewriteTail`].
    tail: RewriteTail,
}

/// The records durably appended to the live WAL **since a rewrite's image was
/// captured**, recorded so the rewrite can re-append them to its staged file
/// before swapping it in (issue #1116).
///
/// A WAL compaction used to hold the lock across the whole `Disk::replace`
/// (temp write + `fsync` + rename + directory `fsync`), so one slow `fsync`
/// froze every persist round. Now the rewrite stages the image outside the
/// lock; persist rounds that land meanwhile append to the *live* file as
/// usual (so they stay durable-before-ack) and push the same encoded bytes
/// here. The rewrite then drains this tail into the staged file and swaps, so
/// the result is byte-identical to a rewrite done under the lock. Inactive
/// (`None`) outside a rewrite, so [`push`](Self::push) costs nothing then.
///
/// Only [`begin`](Self::begin)/[`take`](Self::take) under the lock are
/// ordering-significant; the inner `Mutex` is just interior mutability.
#[derive(Default)]
pub struct RewriteTail {
    inner: Mutex<Option<Vec<u8>>>,
}

impl RewriteTail {
    /// Start recording. **Call while holding the lock, in the same hold that
    /// captured the rewrite's image**, so no persisted round falls between.
    pub fn begin(&self) {
        *self.inner.lock().expect("rewrite tail poisoned") = Some(Vec::new());
    }

    /// Record `bytes` (one durable persist round's encoded records) if a
    /// rewrite is staging. **Call while holding the lock**, after the round's
    /// `fsync` succeeded.
    pub fn push(&self, bytes: &[u8]) {
        if let Some(t) = self.inner.lock().expect("rewrite tail poisoned").as_mut() {
            t.extend_from_slice(bytes);
        }
    }

    /// Take everything recorded so far (recording continues). **Call while
    /// holding the lock.**
    pub fn take(&self) -> Vec<u8> {
        self.inner
            .lock()
            .expect("rewrite tail poisoned")
            .as_mut()
            .map(std::mem::take)
            .unwrap_or_default()
    }

    /// Whether a staged rewrite is recording right now. **Call while holding
    /// the lock** (the answer is only stable under it).
    pub fn is_active(&self) -> bool {
        self.inner.lock().expect("rewrite tail poisoned").is_some()
    }

    /// Stop recording (the rewrite swapped or was abandoned).
    pub fn end(&self) {
        *self.inner.lock().expect("rewrite tail poisoned") = None;
    }
}

impl Default for FairMutex {
    fn default() -> Self {
        Self::new()
    }
}

impl FairMutex {
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                held: false,
                reserved_for: None,
                next_ticket: 0,
                queue: VecDeque::new(),
            }),
            markers: crate::persist::SyncMarkerState::default(),
            tail: RewriteTail::default(),
        }
    }

    /// The guarded file's piggybacked sync-marker state (see
    /// [`crate::persist::SyncMarkerState`]). Use only while holding the lock.
    pub fn markers(&self) -> &crate::persist::SyncMarkerState {
        &self.markers
    }

    /// The guarded file's [`RewriteTail`] (issue #1116).
    pub fn tail(&self) -> &RewriteTail {
        &self.tail
    }

    /// Acquire the lock, queueing behind every earlier caller.
    pub fn lock(&self) -> LockFuture<'_> {
        LockFuture {
            mutex: self,
            ticket: None,
        }
    }

    /// Release; reserve the lock for the queue head and wake it.
    fn release(inner: &mut Inner) {
        inner.held = false;
        if inner.reserved_for.is_none()
            && let Some((ticket, waker)) = inner.queue.pop_front()
        {
            inner.reserved_for = Some(ticket);
            waker.wake();
        }
    }
}

pub struct LockFuture<'a> {
    mutex: &'a FairMutex,
    ticket: Option<u64>,
}

impl<'a> Future for LockFuture<'a> {
    type Output = FairGuard<'a>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut inner = self.mutex.inner.lock().expect("fair lock poisoned");
        match self.ticket {
            None => {
                if !inner.held && inner.reserved_for.is_none() && inner.queue.is_empty() {
                    inner.held = true;
                    return Poll::Ready(FairGuard { mutex: self.mutex });
                }
                let ticket = inner.next_ticket;
                inner.next_ticket += 1;
                inner.queue.push_back((ticket, cx.waker().clone()));
                drop(inner);
                self.ticket = Some(ticket);
                Poll::Pending
            }
            Some(ticket) => {
                if inner.reserved_for == Some(ticket) {
                    inner.reserved_for = None;
                    inner.held = true;
                    drop(inner);
                    self.ticket = None;
                    return Poll::Ready(FairGuard { mutex: self.mutex });
                }
                // Spurious poll: refresh the stored waker, keep the place.
                if let Some(entry) = inner.queue.iter_mut().find(|(t, _)| *t == ticket) {
                    entry.1 = cx.waker().clone();
                }
                Poll::Pending
            }
        }
    }
}

impl Drop for LockFuture<'_> {
    /// A cancelled waiter must not strand the lock: leave the queue, and if
    /// the lock had been reserved for it, pass the reservation on.
    fn drop(&mut self) {
        let Some(ticket) = self.ticket else { return };
        let mut inner = self.mutex.inner.lock().expect("fair lock poisoned");
        if inner.reserved_for == Some(ticket) {
            inner.reserved_for = None;
            if !inner.held {
                FairMutex::release(&mut inner);
            }
        } else {
            inner.queue.retain(|(t, _)| *t != ticket);
        }
    }
}

pub struct FairGuard<'a> {
    mutex: &'a FairMutex,
}

impl Drop for FairGuard<'_> {
    fn drop(&mut self) {
        let mut inner = self.mutex.inner.lock().expect("fair lock poisoned");
        FairMutex::release(&mut inner);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::FutureExt;
    use futures::task::noop_waker_ref;

    #[test]
    fn rewrite_tail_records_only_while_active_and_takes_in_order() {
        let t = RewriteTail::default();
        t.push(b"ignored");
        assert!(t.take().is_empty());
        t.begin();
        t.push(b"ab");
        t.push(b"cd");
        assert_eq!(t.take(), b"abcd");
        assert!(t.take().is_empty());
        t.push(b"ef");
        assert_eq!(t.take(), b"ef");
        t.end();
        t.push(b"gone");
        assert!(t.take().is_empty());
    }

    fn poll<F: Future + Unpin>(f: &mut F) -> Poll<F::Output> {
        f.poll_unpin(&mut Context::from_waker(noop_waker_ref()))
    }

    #[test]
    fn a_new_locker_cannot_barge_ahead_of_a_woken_waiter() {
        let m = FairMutex::new();
        let Poll::Ready(g) = poll(&mut m.lock()) else {
            panic!("free lock");
        };
        let mut waiter = m.lock();
        assert!(poll(&mut waiter).is_pending());
        drop(g);
        // The waiter has not been polled since the release; a barger must
        // still queue behind it.
        let mut barger = m.lock();
        assert!(poll(&mut barger).is_pending());
        let Poll::Ready(g) = poll(&mut waiter) else {
            panic!("waiter owns the handoff");
        };
        assert!(poll(&mut barger).is_pending());
        drop(g);
        assert!(poll(&mut barger).is_ready());
    }

    #[test]
    fn a_cancelled_reserved_waiter_passes_the_lock_on() {
        let m = FairMutex::new();
        let Poll::Ready(g) = poll(&mut m.lock()) else {
            panic!("free lock");
        };
        let mut a = m.lock();
        let mut b = m.lock();
        assert!(poll(&mut a).is_pending());
        assert!(poll(&mut b).is_pending());
        drop(g);
        drop(a);
        assert!(poll(&mut b).is_ready());
    }
}
