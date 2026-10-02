//! `animus_node::ttl_reaper::ttl_reaper_loop`, driven deterministically
//! under `SimEnv` (ADR 0061 rung C2) against a **fully synthetic**
//! [`FakeTtlHost`] — no `CpGroup`, no real Raft, no `ClientCtx`, not even a
//! real control-plane `RaftNode` (unlike `index_backfill_sim.rs`, this
//! loop's own [`animus_node::host::TtlScanHost`] never hands back a
//! concrete Raft type, so the host here is genuinely just data structures).
//! This is the first deterministic coverage the TTL reaper has ever had —
//! previously it was reachable only through `animusd`'s real-TCP,
//! real-wall-clock `tests/dynamo_ttl.rs`.
//!
//! What this proves that a pure unit test of `is_expired` alone wouldn't:
//! the loop's own scan-cursor/wake/delete control flow, run against a
//! virtual wall clock advancing via `env.wall_now()` (never `env.now()`,
//! ADR 0051 §1), correctly reaps an item that expired before the run
//! started while leaving a not-yet-expired sibling alone, and converges
//! (the item is gone, not just "a delete was attempted") within a bounded
//! number of sweep ticks.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use animus_control::{ApplyOutcome, ColumnType, MetaCommand, Metadata, TableSchema, TtlSpec};
use animus_dynamo::{AttributeValue, wire};
use animus_env::{EnvExt, nid};
use animus_node::host::{TtlReaperProgressHost, TtlScanHost};
use animus_node::ttl_reaper::{TtlReaperProgress, ttl_reaper_loop};
use animus_sim::Simulator;
use animus_tablet::{KeyRange, TabletId};
use async_trait::async_trait;

/// A tablet's own local base-row storage: raw key bytes → encoded stored
/// item bytes, exactly the shape a real `CpGroup::local_scan_kind_capped`
/// would return.
type TabletStore = BTreeMap<Vec<u8>, Vec<u8>>;

struct Inner {
    meta: Metadata,
    tablets: Mutex<BTreeMap<TabletId, TabletStore>>,
    /// Every delete attempt this host ever received, `(tablet, table,
    /// partition key)` — lets a test assert *what* was deleted, not just
    /// that the tablet's row count dropped.
    delete_calls: Mutex<Vec<(TabletId, String, AttributeValue)>>,
    /// The loop's own published progress (roadmap U-07) — lets this fixture
    /// double as a proof that `ttl_reaper_loop` actually publishes through
    /// [`TtlReaperProgressHost`], not just that it deletes the right rows.
    progress: Mutex<TtlReaperProgress>,
}

/// A synthetic [`TtlScanHost`] — no `CpGroup`, no Raft, no `ClientCtx`.
/// "Deleting" an item is a plain map removal keyed by the item's own
/// storage-key bytes (`animus_dynamo::storage_key`, the same function the
/// real write path derives a base row's key from). Also backs
/// [`TtlReaperProgressHost`] with its own `TtlReaperProgress` slot (roadmap
/// U-07), so this double can drive and observe the progress-reporting
/// mechanism too.
#[derive(Clone)]
struct FakeTtlHost(std::sync::Arc<Inner>);

impl FakeTtlHost {
    fn new(meta: Metadata, tablets: BTreeMap<TabletId, TabletStore>) -> Self {
        FakeTtlHost(std::sync::Arc::new(Inner {
            meta,
            tablets: Mutex::new(tablets),
            delete_calls: Mutex::new(Vec::new()),
            progress: Mutex::new(TtlReaperProgress::default()),
        }))
    }

    fn row_count(&self, tablet: TabletId) -> usize {
        self.0
            .tablets
            .lock()
            .unwrap()
            .get(&tablet)
            .map_or(0, TabletStore::len)
    }

    fn delete_calls(&self) -> Vec<(TabletId, String, AttributeValue)> {
        self.0.delete_calls.lock().unwrap().clone()
    }

    fn progress(&self) -> TtlReaperProgress {
        self.0.progress.lock().unwrap().clone()
    }
}

impl TtlReaperProgressHost for FakeTtlHost {
    fn update_ttl_reaper_progress(&self, update: &mut dyn FnMut(&mut TtlReaperProgress)) {
        update(&mut self.0.progress.lock().unwrap());
    }
}

#[async_trait]
impl TtlScanHost for FakeTtlHost {
    fn ttl_metadata(&self) -> Metadata {
        self.0.meta.clone()
    }

    fn led_tablets(&self) -> Vec<TabletId> {
        self.0.tablets.lock().unwrap().keys().copied().collect()
    }

    async fn scan_base_capped(
        &self,
        tablet: TabletId,
        start: &[u8],
        limit: usize,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let tablets = self.0.tablets.lock().unwrap();
        let Some(store) = tablets.get(&tablet) else {
            return Vec::new();
        };
        store
            .range(start.to_vec()..)
            .take(limit)
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    async fn ttl_delete_if_attribute_equals(
        &self,
        tablet: TabletId,
        table: &str,
        pk: &AttributeValue,
        sk: Option<&AttributeValue>,
        attribute: &str,
        expected: AttributeValue,
    ) -> Result<bool, String> {
        self.0
            .delete_calls
            .lock()
            .unwrap()
            .push((tablet, table.to_owned(), pk.clone()));
        let key = animus_dynamo::storage_key(pk, sk);
        let mut tablets = self.0.tablets.lock().unwrap();
        let Some(store) = tablets.get_mut(&tablet) else {
            return Ok(false);
        };
        let Some(bytes) = store.get(&key) else {
            return Ok(false); // already gone — a concurrent delete raced us
        };
        let Ok(Some(item)) = wire::decode_stored_item(bytes) else {
            return Ok(false);
        };
        if item.get(attribute) != Some(&expected) {
            return Ok(false); // condition failed — TTL changed since the scan
        }
        store.remove(&key);
        Ok(true)
    }
}

fn item(pk: &str, ttl_epoch_secs: i64) -> BTreeMap<String, AttributeValue> {
    let mut m = BTreeMap::new();
    m.insert("id".to_owned(), AttributeValue::S(pk.to_owned()));
    m.insert(
        "expiresAt".to_owned(),
        AttributeValue::N(ttl_epoch_secs.to_string()),
    );
    m
}

fn base_meta_with_ttl_table(table: &str, tablet: TabletId) -> Metadata {
    let mut m = Metadata::default();
    assert_eq!(
        m.apply(&MetaCommand::CreateTableSchema {
            table: table.to_owned(),
            schema: TableSchema::simple("id", ColumnType::String),
        }),
        ApplyOutcome::Applied
    );
    assert_eq!(
        m.apply(&MetaCommand::SetTableTtl {
            table: table.to_owned(),
            spec: Some(TtlSpec {
                attribute_name: "expiresAt".to_owned(),
            }),
        }),
        ApplyOutcome::Applied
    );
    assert_eq!(
        m.apply(&MetaCommand::CreateTablet {
            tablet,
            table: Some(table.to_owned()),
            range: KeyRange::whole(),
            replicas: vec![nid(0)],
        }),
        ApplyOutcome::Applied
    );
    m
}

/// `SIM_WALL_EPOCH_MS` (`animus_sim`'s virtual wall-clock base) as whole
/// epoch seconds — 2020-01-01T00:00:00Z. Items are TTL-stamped relative to
/// this so the fixture's "expired"/"not yet expired" split is exact at
/// `t=0`, not dependent on how long the sim happens to run before its first
/// tick.
const SIM_WALL_EPOCH_SECS: i64 = 1_577_836_800;

#[test]
fn an_expired_item_is_reaped_while_a_future_item_is_left_alone() {
    let seed = 0x7717_0001;
    let sim = Simulator::new(seed);
    let env = sim.env(nid(0));

    let table = "sessions";
    let tablet = TabletId(1);
    let meta = base_meta_with_ttl_table(table, tablet);

    let expired = item("expired-one", SIM_WALL_EPOCH_SECS - 10);
    let not_yet = item("future-one", SIM_WALL_EPOCH_SECS + 10_000);
    let mut store = TabletStore::new();
    store.insert(
        animus_dynamo::storage_key(&AttributeValue::S("expired-one".to_owned()), None),
        wire::encode_stored_item(&expired),
    );
    store.insert(
        animus_dynamo::storage_key(&AttributeValue::S("future-one".to_owned()), None),
        wire::encode_stored_item(&not_yet),
    );
    let mut tablets = BTreeMap::new();
    tablets.insert(tablet, store);

    let host = FakeTtlHost::new(meta, tablets);
    assert_eq!(host.row_count(tablet), 2, "sanity: both rows start present");

    env.clone().spawn_task(ttl_reaper_loop(
        env,
        host.clone(),
        Duration::from_millis(10),
    ));

    let mut sim = sim;
    sim.run_for(Duration::from_secs(2));

    assert_eq!(
        host.row_count(tablet),
        1,
        "the expired row must be reaped; the not-yet-expired row must survive (seed={seed})"
    );
    let calls = host.delete_calls();
    assert!(
        calls.iter().any(|(t, tb, pk)| *t == tablet
            && tb == table
            && *pk == AttributeValue::S("expired-one".to_owned())),
        "the loop must have attempted to delete exactly the expired item (seed={seed}): {calls:?}"
    );
    assert!(
        !calls
            .iter()
            .any(|(_, _, pk)| *pk == AttributeValue::S("future-one".to_owned())),
        "the loop must never attempt to delete the not-yet-expired item (seed={seed})"
    );

    // Roadmap U-07: the published progress snapshot reflects the reap —
    // one item deleted cumulatively, at least one expired item observed,
    // one TTL-enabled table, and no error. `deleted_last_tick` is
    // deliberately NOT asserted here: it resets to 0 at the start of every
    // tick (see `TtlReaperProgress::deleted_last_tick`'s own doc) and this
    // sim runs for 2 real-world seconds' worth of 10ms ticks — by the run's
    // end, many idle ticks have elapsed since the one tick that actually
    // deleted the row, so `deleted_last_tick` has long since settled back
    // to 0, exactly as designed.
    let progress = host.progress();
    assert_eq!(progress.deleted_total, 1, "seed={seed}");
    assert!(progress.expired_seen_total >= 1, "seed={seed}");
    assert_eq!(progress.tables_with_ttl, 1, "seed={seed}");
    assert_eq!(progress.last_error, None, "seed={seed}");
    assert!(progress.last_tick_at_ms.is_some(), "seed={seed}");
}

#[test]
fn a_table_with_no_expired_rows_never_calls_delete() {
    let seed = 0x7717_0002;
    let sim = Simulator::new(seed);
    let env = sim.env(nid(0));

    let table = "sessions";
    let tablet = TabletId(1);
    let meta = base_meta_with_ttl_table(table, tablet);

    let not_yet = item("future-one", SIM_WALL_EPOCH_SECS + 10_000);
    let mut store = TabletStore::new();
    store.insert(
        animus_dynamo::storage_key(&AttributeValue::S("future-one".to_owned()), None),
        wire::encode_stored_item(&not_yet),
    );
    let mut tablets = BTreeMap::new();
    tablets.insert(tablet, store);

    let host = FakeTtlHost::new(meta, tablets);
    env.clone().spawn_task(ttl_reaper_loop(
        env,
        host.clone(),
        Duration::from_millis(10),
    ));

    let mut sim = sim;
    sim.run_for(Duration::from_secs(2));

    assert_eq!(host.row_count(tablet), 1, "seed={seed}");
    assert!(
        host.delete_calls().is_empty(),
        "nothing expired — no delete should ever have been attempted (seed={seed})"
    );

    // Roadmap U-07: an idle sweep still reports honestly — the table is
    // counted, but nothing was seen expired or deleted.
    let progress = host.progress();
    assert_eq!(progress.deleted_total, 0, "seed={seed}");
    assert_eq!(progress.expired_seen_total, 0, "seed={seed}");
    assert_eq!(progress.tables_with_ttl, 1, "seed={seed}");
    assert_eq!(progress.last_error, None, "seed={seed}");
}
