//! An ack must mean the write durably applied.
//!
//! A replica whose storage engine returns `Err` from `merge`/`merge_tombstone`
//! must reply `WriteAck { ok: false }` / `DeleteAck { ok: false }` so the
//! coordinator does **not** count it toward the W quorum — otherwise the
//! coordinator would falsely report a write/delete as succeeded that never hit
//! durable storage. Healthy replicas still let a quorum succeed; once enough
//! replicas fail, the quorum write/delete fails.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_data::{DataClient, TabletView, serve_replica};
use animus_env::EnvExt;
use animus_sim::{SimEnv, Simulator};
use animus_storage::{
    Key, MemoryEngine, MemorySnapshot, Result, StorageEngine, StorageError, Value, Version,
    VersionedValue, WriteBatch,
};
use animus_tablet::{Epoch, TabletId};
use async_trait::async_trait;

const REPLICAS: [u64; 3] = [0, 1, 2];
const CLIENT: u64 = 10;
const TIMEOUT: Duration = Duration::from_secs(2);

/// A `StorageEngine` wrapping a `MemoryEngine` whose convergence mutations
/// (`merge` / `merge_tombstone`) fail when `fail` is set, modelling a replica
/// that cannot durably persist (e.g. a full disk or an I/O error). Every other
/// operation delegates to the inner engine, so reads still work.
#[derive(Clone)]
struct FailingEngine {
    inner: MemoryEngine,
    fail: bool,
}

impl FailingEngine {
    /// A replica that fails every `merge` / `merge_tombstone`.
    fn failing() -> Self {
        Self {
            inner: MemoryEngine::new(),
            fail: true,
        }
    }
}

#[async_trait]
impl StorageEngine for FailingEngine {
    type Snapshot = MemorySnapshot;

    async fn put(&self, key: &[u8], value: &[u8], version: Version) -> Result<()> {
        self.inner.put(key, value, version).await
    }

    async fn merge(&self, key: &[u8], value: &[u8], version: Version) -> Result<bool> {
        if self.fail {
            return Err(StorageError::Backend("injected merge failure".into()));
        }
        self.inner.merge(key, value, version).await
    }

    async fn merge_tombstone(&self, key: &[u8], version: Version) -> Result<bool> {
        if self.fail {
            return Err(StorageError::Backend("injected tombstone failure".into()));
        }
        self.inner.merge_tombstone(key, version).await
    }

    async fn delete(&self, key: &[u8], version: Version) -> Result<()> {
        self.inner.delete(key, version).await
    }

    async fn delete_range(&self, start: &[u8], end: &[u8], version: Version) -> Result<()> {
        self.inner.delete_range(start, end, version).await
    }

    async fn write_batch(&self, batch: WriteBatch) -> Result<()> {
        self.inner.write_batch(batch).await
    }

    async fn get(&self, key: &[u8]) -> Result<Option<VersionedValue>> {
        self.inner.get(key).await
    }

    async fn get_at(&self, key: &[u8], version: Version) -> Result<Option<VersionedValue>> {
        self.inner.get_at(key, version).await
    }

    async fn scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Key, VersionedValue)>> {
        self.inner.scan(start, end).await
    }

    async fn entries(&self) -> Result<Vec<(Key, VersionedValue)>> {
        self.inner.entries().await
    }

    async fn entries_with_tombstones(&self) -> Result<Vec<(Key, Option<Value>, Version)>> {
        self.inner.entries_with_tombstones().await
    }

    fn snapshot(&self) -> Self::Snapshot {
        self.inner.snapshot()
    }

    fn latest_version(&self) -> Version {
        self.inner.latest_version()
    }
}

fn view(epoch: Epoch) -> TabletView {
    // R + W = 4 > N = 3.
    TabletView {
        tablet: TabletId(1),
        replicas: REPLICAS.to_vec(),
        epoch,
        r: 2,
        w: 2,
    }
}

/// Run a single client op to completion against a cluster where `n_failing` of
/// the three replicas have a failing storage engine.
fn run_with_failing<T: Clone + Send + 'static>(
    seed: u64,
    n_failing: usize,
    op: impl FnOnce(DataClient<SimEnv>) -> futures::future::BoxFuture<'static, T> + Send + 'static,
) -> T {
    let sim = Simulator::new(seed);
    for (i, &id) in REPLICAS.iter().enumerate() {
        if i < n_failing {
            serve_replica(sim.env(id), FailingEngine::failing(), Epoch::INITIAL);
        } else {
            serve_replica(sim.env(id), MemoryEngine::new(), Epoch::INITIAL);
        }
    }

    let result: Arc<Mutex<Option<T>>> = Arc::new(Mutex::new(None));
    let client_env = sim.env(CLIENT);
    let out = Arc::clone(&result);
    let mut sim = sim;
    client_env.clone().spawn_task(async move {
        let client = DataClient::new(client_env);
        let value = op(client).await;
        *out.lock().unwrap() = Some(value);
    });
    sim.run();
    result
        .lock()
        .unwrap()
        .clone()
        .expect("client op did not complete")
}

#[test]
fn write_fails_when_too_many_replicas_cannot_persist() {
    // Two of three replicas fail to persist ⇒ only one ack possible < W (=2).
    let ok = run_with_failing(0x5151, 2, |client| {
        Box::pin(async move {
            client
                .write(&view(Epoch::INITIAL), b"k", b"v", 1, TIMEOUT)
                .await
        })
    });
    assert!(
        !ok,
        "quorum write must FAIL when fewer than W replicas durably persisted \
         (an Err from storage must not be counted as an ack)"
    );
}

#[test]
fn write_succeeds_when_a_quorum_can_persist() {
    // One replica fails, two heal ⇒ W (=2) acks still reachable.
    let ok = run_with_failing(0x5252, 1, |client| {
        Box::pin(async move {
            client
                .write(&view(Epoch::INITIAL), b"k", b"v", 1, TIMEOUT)
                .await
        })
    });
    assert!(
        ok,
        "quorum write should still succeed while W healthy replicas persist"
    );
}

#[test]
fn delete_fails_when_too_many_replicas_cannot_persist() {
    let ok = run_with_failing(0x6161, 2, |client| {
        Box::pin(async move { client.delete(&view(Epoch::INITIAL), b"k", 1, TIMEOUT).await })
    });
    assert!(
        !ok,
        "quorum delete must FAIL when fewer than W replicas durably tombstoned"
    );
}

#[test]
fn delete_succeeds_when_a_quorum_can_persist() {
    let ok = run_with_failing(0x6262, 1, |client| {
        Box::pin(async move { client.delete(&view(Epoch::INITIAL), b"k", 1, TIMEOUT).await })
    });
    assert!(
        ok,
        "quorum delete should still succeed while W healthy replicas tombstone"
    );
}
