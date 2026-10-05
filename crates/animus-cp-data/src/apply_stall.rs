//! The apply task's **ENOSPC pause** (R-01 (d) residual, issue #1218; ADR 0074
//! §2 amendment).
//!
//! The apply task is the only writer of a tablet's engine, and every engine
//! write it makes is for an entry Raft already committed: skipping one loses
//! an acked write, re-ordering one breaks the apply order. So when the engine
//! reports **out of space** ([`StorageError::StorageFull`]) the only correct
//! response is to *stop and wait* — never panic (the old behaviour: a
//! `merge_batch`/marker `.expect`), never skip, never carry on past it.
//!
//! [`StallingEngine`] wraps the apply task's engine handle and gives every
//! operation exactly that behaviour: on `StorageFull` it raises the shared
//! `stalled` flag (surfaced as [`crate::RaftKvNode::is_storage_full`], so
//! `animusd` refuses new writes with the named 503 `StorageFull` and
//! `/admin/health` reports it), sleeps a poll interval on the `Env` clock, and
//! retries the *identical* call. That is sound because the engine contract is
//! that a `StorageFull` operation made no durable or visible change (see
//! `StorageError::StorageFull`), and every apply-task write is idempotent per
//! `(key, version)` besides. Because the task is blocked inside the one engine
//! call, nothing after it can run: apply order is preserved by construction.
//!
//! Reads are retried too: `SimEnv`'s injector fails reads with ENOSPC, and a
//! read is trivially idempotent.
//!
//! On shutdown (`halted`) a stalled call must not panic its caller's
//! `.expect` nor spin forever: it raises `apply_stopped` (the task has no
//! engine I/O in flight and never will have) and parks, to be dropped with the
//! runtime — the same teardown guarantee the loop's own `halted` exit gives.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use animus_env::Env;
use animus_storage::{
    Key, MergeOp, Result, StorageEngine, Value, Version, VersionedValue, WriteBatch,
};

/// Poll interval while the apply task is paused on a full disk.
const STALL_POLL: Duration = Duration::from_millis(100);

/// The apply task's engine handle: `inner` with ENOSPC turned into a pause.
pub(crate) struct StallingEngine<E: Env, S: StorageEngine> {
    inner: S,
    env: E,
    halted: Arc<AtomicBool>,
    apply_stopped: Arc<AtomicBool>,
    /// Shared with the node: true while a call is paused on ENOSPC.
    stalled: Arc<AtomicBool>,
}

impl<E: Env, S: StorageEngine> Clone for StallingEngine<E, S> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
            env: self.env.clone(),
            halted: Arc::clone(&self.halted),
            apply_stopped: Arc::clone(&self.apply_stopped),
            stalled: Arc::clone(&self.stalled),
        }
    }
}

impl<E: Env, S: StorageEngine> StallingEngine<E, S> {
    pub(crate) fn new(
        inner: S,
        env: E,
        halted: Arc<AtomicBool>,
        apply_stopped: Arc<AtomicBool>,
        stalled: Arc<AtomicBool>,
    ) -> Self {
        Self {
            inner,
            env,
            halted,
            apply_stopped,
            stalled,
        }
    }

    /// Run `op` until it stops failing with `StorageFull`.
    async fn retry<T, F, Fut>(&self, what: &'static str, mut op: F) -> Result<T>
    where
        F: FnMut() -> Fut + Send,
        Fut: std::future::Future<Output = Result<T>> + Send,
        T: Send,
    {
        loop {
            match op().await {
                Err(e) if e.is_storage_full() => {
                    if self.halted.load(Ordering::SeqCst) {
                        self.apply_stopped.store(true, Ordering::SeqCst);
                        std::future::pending::<()>().await;
                    }
                    if !self.stalled.swap(true, Ordering::SeqCst) {
                        tracing::error!(
                            error = %e,
                            op = what,
                            "raftkv apply hit ENOSPC; paused (group is StorageFull) until space returns"
                        );
                    }
                    self.env.sleep(STALL_POLL).await;
                }
                other => {
                    if self.stalled.swap(false, Ordering::SeqCst) {
                        tracing::warn!(op = what, "raftkv apply resumed after ENOSPC pause");
                    }
                    return other;
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl<E: Env, S: StorageEngine> StorageEngine for StallingEngine<E, S> {
    type Snapshot = S::Snapshot;

    async fn put(&self, key: &[u8], value: &[u8], version: Version) -> Result<()> {
        self.retry("put", || self.inner.put(key, value, version))
            .await
    }

    async fn merge(&self, key: &[u8], value: &[u8], version: Version) -> Result<bool> {
        self.retry("merge", || self.inner.merge(key, value, version))
            .await
    }

    async fn merge_tombstone(&self, key: &[u8], version: Version) -> Result<bool> {
        self.retry("merge_tombstone", || {
            self.inner.merge_tombstone(key, version)
        })
        .await
    }

    async fn merge_batch(&self, ops: Vec<MergeOp>) -> Result<()> {
        self.retry("merge_batch", || self.inner.merge_batch(ops.clone()))
            .await
    }

    async fn delete(&self, key: &[u8], version: Version) -> Result<()> {
        self.retry("delete", || self.inner.delete(key, version))
            .await
    }

    async fn delete_range(&self, start: &[u8], end: &[u8], version: Version) -> Result<()> {
        self.retry("delete_range", || {
            self.inner.delete_range(start, end, version)
        })
        .await
    }

    async fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        self.retry("write_batch", || self.inner.write_batch(batch.clone()))
            .await
    }

    async fn get(&self, key: &[u8]) -> Result<Option<VersionedValue>> {
        self.retry("get", || self.inner.get(key)).await
    }

    async fn get_at(&self, key: &[u8], version: Version) -> Result<Option<VersionedValue>> {
        self.retry("get_at", || self.inner.get_at(key, version))
            .await
    }

    async fn scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Key, VersionedValue)>> {
        self.retry("scan", || self.inner.scan(start, end)).await
    }

    async fn scan_at(
        &self,
        start: &[u8],
        end: &[u8],
        version: Version,
    ) -> Result<Vec<(Key, VersionedValue)>> {
        self.retry("scan_at", || self.inner.scan_at(start, end, version))
            .await
    }

    async fn entries(&self) -> Result<Vec<(Key, VersionedValue)>> {
        self.retry("entries", || self.inner.entries()).await
    }

    async fn entries_at(&self, version: Version) -> Result<Vec<(Key, VersionedValue)>> {
        self.retry("entries_at", || self.inner.entries_at(version))
            .await
    }

    async fn entries_with_tombstones(&self) -> Result<Vec<(Key, Option<Value>, Version)>> {
        self.retry("entries_with_tombstones", || {
            self.inner.entries_with_tombstones()
        })
        .await
    }

    async fn scan_with_tombstones(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Result<Vec<(Key, Option<Value>, Version)>> {
        self.retry("scan_with_tombstones", || {
            self.inner.scan_with_tombstones(start, end)
        })
        .await
    }

    async fn approx_bytes_in_range(&self, start: &[u8], end: Option<&[u8]>) -> Result<u64> {
        self.retry("approx_bytes_in_range", || {
            self.inner.approx_bytes_in_range(start, end)
        })
        .await
    }

    fn snapshot(&self) -> Self::Snapshot {
        self.inner.snapshot()
    }

    fn latest_version(&self) -> Version {
        self.inner.latest_version()
    }
}
