//! Hinted handoff (ADR 0010 + 0005): availability + faster convergence for a
//! write/delete that could not reach one of a tablet's replicas.
//!
//! When a quorum write/delete is acknowledged by `W` replicas but a replica was
//! down (or did not ack within the timeout), the coordinator stores a **hint**
//! for that replica — the `(value/tombstone, version)` it missed — and a
//! background [`serve_hint_handoff`] loop replays the hint to the replica once it
//! is observed healthy again. Replay rides the existing [`DataMsg::Sync`] path,
//! so it is epoch-fenced and applied by per-key last-writer-wins (idempotent:
//! re-delivering a hint the replica already has, or has superseded, is a no-op).
//!
//! This is strictly an **availability + convergence-latency** optimization on top
//! of the repair/anti-entropy machinery (ADR 0010): even with no hints a missed
//! write eventually converges via background anti-entropy. Hinted handoff just
//! delivers it promptly the moment the replica returns, and lets a write still be
//! *recorded for* an unavailable replica rather than waiting for the next
//! anti-entropy round.
//!
//! **Residency (ADR 0005).** A hint is only ever stored for, and replayed to, a
//! node the tablet's placement *admits* — the same `allowed` set the residency
//! repair guard ([`serve_replica_with_residency`](crate::serve_replica_with_residency))
//! uses. So hinted handoff cannot move data across a residency boundary even to a
//! reachable node: a hint for an ineligible target is never recorded, and a
//! recorded hint is never replayed to an ineligible target.
//!
//! Determinism (ADR 0003): the store is a `BTreeMap` (sorted, no `HashMap`), and
//! the replay loop runs on the `Env` clock/network seam — no wall clock, no
//! unseeded randomness. The store's lock is never held across an `.await`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use animus_env::{Env, EnvExt, Metric, MetricsHandle, NodeId};
use animus_tablet::{Epoch, TabletId};

use crate::DataMsg;

/// Bounds on how many hints — and how old — a [`HintStore`] retains per target,
/// so a permanently-unavailable target cannot accumulate hints without limit
/// (the original store was unbounded, the deferred hardening in ADR 0010).
///
/// Eviction is **safe by construction**: a dropped hint costs only promptness,
/// never a write. The hint was buffered *after* a quorum of `W` replicas durably
/// acked the write, so the durable record always lives on those replicas and
/// background anti-entropy converges the laggard regardless (ADR 0010). The hint
/// is purely an accelerator; bounding it trades the convergence-latency win for a
/// down node away once it has been down "too long" (by count or by version age),
/// keeping the store's memory bounded.
///
/// Both bounds are per **target** (a stuck node is the failure mode); `None`
/// disables that bound. The default ([`HintLimits::unbounded`]) disables both,
/// preserving the original behavior.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct HintLimits {
    /// Max pending hints retained per target. When exceeded, the **lowest-version**
    /// hints for that target are evicted first (oldest writes — the most likely to
    /// have already converged via anti-entropy). `None` ⇒ no count cap.
    pub per_target_cap: Option<usize>,
    /// Version-age TTL: a hint is evicted once it is more than `ttl_versions`
    /// behind the **highest** hint version seen for its target (the target's most
    /// recent missed write). A small TTL keeps only the freshest tail of misses.
    /// `None` ⇒ no TTL.
    pub ttl_versions: Option<u64>,
}

impl HintLimits {
    /// No bounds — the original unbounded behavior.
    #[must_use]
    pub fn unbounded() -> Self {
        Self::default()
    }

    /// Cap each target at `cap` pending hints (evicting the lowest-version first).
    #[must_use]
    pub fn capped(cap: usize) -> Self {
        Self {
            per_target_cap: Some(cap),
            ttl_versions: None,
        }
    }

    /// Drop a hint once it is more than `ttl_versions` behind the freshest hint
    /// version for its target.
    #[must_use]
    pub fn ttl(ttl_versions: u64) -> Self {
        Self {
            per_target_cap: None,
            ttl_versions: Some(ttl_versions),
        }
    }

    /// Whether any bound is active.
    fn is_bounded(&self) -> bool {
        self.per_target_cap.is_some() || self.ttl_versions.is_some()
    }
}

/// Which targets a coordinator may store / replay hints for — the tablet's
/// residency-eligible placement (ADR 0005). `None` ⇒ no residency boundary (any
/// target); `Some(set)` ⇒ only members of the set. Mirrors the receive-side
/// `AllowedPeers` guard in [`replica`](crate::replica).
pub type AllowedTargets = Option<BTreeSet<NodeId>>;

/// One stored hint: a write (or delete) a target replica missed, to be replayed
/// when the target returns. `value == None` is a tombstone (a missed delete).
#[derive(Clone, Debug, PartialEq, Eq)]
struct Hint {
    epoch: Epoch,
    value: Option<Vec<u8>>,
    version: u64,
}

/// The key a hint is filed under: a specific `(target, tablet, key)`. Keying by
/// the data key (not by write) means a later write to the same key supersedes the
/// pending hint, so the store stays bounded by the number of distinct keys a
/// target is behind on — and the per-key LWW that governs the data plane governs
/// the hint queue too.
type HintKey = (NodeId, TabletId, Vec<u8>);

/// A coordinator-side store of pending hints for unavailable replicas.
///
/// Cheap to clone (shares one inner map). In-memory and per-coordinator: a hint
/// is a convergence accelerator, not a durability guarantee (the write was
/// already acked by `W` replicas; the durable record lives there and converges
/// via anti-entropy regardless). Losing the store on a coordinator restart just
/// falls back to background anti-entropy.
#[derive(Clone, Default)]
pub struct HintStore {
    inner: Arc<Mutex<BTreeMap<HintKey, Hint>>>,
    /// Per-target retention bounds. `unbounded` by default ⇒ the original
    /// behavior (no eviction).
    limits: HintLimits,
}

impl HintStore {
    /// An empty, **unbounded** store (the original behavior — no eviction).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// An empty store that retains at most `limits` hints per target, evicting
    /// the oldest (lowest-version / most-aged) hints beyond the bound. Eviction
    /// is safe: anti-entropy is the backstop for any dropped hint (ADR 0010).
    #[must_use]
    pub fn with_limits(limits: HintLimits) -> Self {
        Self {
            inner: Arc::new(Mutex::new(BTreeMap::new())),
            limits,
        }
    }

    /// Record a hint that `target` missed an `entry = (key, value, version)` for
    /// `tablet` at `epoch` (`value == None` ⇒ a missed delete). No-op if `allowed`
    /// forbids the target (residency, ADR 0005). Per-key LWW: a stored hint is
    /// overwritten only by a version at least as new for the same
    /// `(target, tablet, key)`, so a stale hint never clobbers a fresher one.
    ///
    /// Returns whether a hint was actually stored (residency-admitted *and* it
    /// superseded any prior hint for the key), so the caller can count it
    /// (ADR 0015) without re-deriving the residency/LWW decision.
    pub(crate) fn record(
        &self,
        allowed: &AllowedTargets,
        target: NodeId,
        tablet: TabletId,
        epoch: Epoch,
        entry: crate::SyncEntry,
    ) -> bool {
        if !target_allowed(allowed, target) {
            return false;
        }
        let (key, value, version) = entry;
        let mut map = self.inner.lock().expect("hint store poisoned");
        let hk = (target, tablet, key);
        let supersedes = map.get(&hk).is_none_or(|h| version >= h.version);
        if supersedes {
            map.insert(
                hk,
                Hint {
                    epoch,
                    value,
                    version,
                },
            );
            enforce_bounds(&mut map, target, &self.limits);
        }
        supersedes
    }

    /// The distinct targets that currently have at least one pending hint, sorted
    /// (BTreeMap key order ⇒ deterministic).
    fn targets(&self) -> Vec<NodeId> {
        let map = self.inner.lock().expect("hint store poisoned");
        let mut out: Vec<NodeId> = Vec::new();
        for (target, _, _) in map.keys() {
            if out.last() != Some(target) {
                out.push(*target);
            }
        }
        out
    }

    /// Take all pending hints for `target`, removing them from the store and
    /// returning them grouped per tablet as `Sync`-ready entry batches keyed by
    /// `(tablet, epoch)`. Called once the target is observed healthy.
    fn drain_target(&self, target: NodeId) -> BTreeMap<(TabletId, Epoch), Vec<crate::SyncEntry>> {
        let mut map = self.inner.lock().expect("hint store poisoned");
        let keys: Vec<HintKey> = map
            .range((target, TabletId(u64::MIN), Vec::new())..)
            .take_while(|((t, _, _), _)| *t == target)
            .map(|(k, _)| k.clone())
            .collect();
        let mut out: BTreeMap<(TabletId, Epoch), Vec<crate::SyncEntry>> = BTreeMap::new();
        for hk in keys {
            if let Some(hint) = map.remove(&hk) {
                let (_, tablet, key) = hk;
                out.entry((tablet, hint.epoch))
                    .or_default()
                    .push((key, hint.value, hint.version));
            }
        }
        out
    }

    /// Read all pending hints for `target` **without** removing them, grouped per
    /// `(tablet, epoch)` as `Sync`-ready batches. Used by the send-only
    /// [`serve_hint_replay`] loop, which re-sends each round rather than draining.
    fn snapshot_target(
        &self,
        target: NodeId,
    ) -> BTreeMap<(TabletId, Epoch), Vec<crate::SyncEntry>> {
        let map = self.inner.lock().expect("hint store poisoned");
        let mut out: BTreeMap<(TabletId, Epoch), Vec<crate::SyncEntry>> = BTreeMap::new();
        for ((t, tablet, key), hint) in map
            .range((target, TabletId(u64::MIN), Vec::new())..)
            .take_while(|((t, _, _), _)| *t == target)
        {
            debug_assert_eq!(*t, target);
            out.entry((*tablet, hint.epoch)).or_default().push((
                key.clone(),
                hint.value.clone(),
                hint.version,
            ));
        }
        out
    }

    /// Re-file a batch of hints whose replay could not be confirmed (e.g. the
    /// target went away again before acking the probe), per-key LWW. Keeps a hint
    /// pending across a flap so it is retried next round.
    fn restore(
        &self,
        target: NodeId,
        batches: &BTreeMap<(TabletId, Epoch), Vec<crate::SyncEntry>>,
    ) {
        let mut map = self.inner.lock().expect("hint store poisoned");
        for ((tablet, epoch), entries) in batches {
            for (key, value, version) in entries {
                let hk = (target, *tablet, key.clone());
                let supersedes = map.get(&hk).is_none_or(|h| *version >= h.version);
                if supersedes {
                    map.insert(
                        hk,
                        Hint {
                            epoch: *epoch,
                            value: value.clone(),
                            version: *version,
                        },
                    );
                }
            }
        }
        enforce_bounds(&mut map, target, &self.limits);
    }

    /// Number of pending hints (for tests/diagnostics).
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.lock().expect("hint store poisoned").len()
    }

    /// Whether the store has no pending hints.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl std::fmt::Debug for HintStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HintStore")
            .field("len", &self.len())
            .finish()
    }
}

/// Evict hints for `target` that fall outside `limits`, in place.
///
/// Applied after every insert for `target`, so the store stays bounded
/// incrementally rather than growing unbounded between sweeps. Eviction order
/// (lowest version first) drops the **oldest** missed writes — the ones most
/// likely already converged via anti-entropy — and keeps the freshest tail, the
/// hints whose prompt replay is still worth the most.
///
/// Operates only on the `target`'s slice of the map (`(target, _, _)` keys),
/// leaving other targets untouched. Safe per ADR 0010: every evicted hint's
/// write is already durable on `W` replicas, so anti-entropy is the backstop.
fn enforce_bounds(map: &mut BTreeMap<HintKey, Hint>, target: NodeId, limits: &HintLimits) {
    if !limits.is_bounded() {
        return;
    }
    // Collect this target's hints as (version, key) so we can rank by age.
    let mut owned: Vec<(u64, HintKey)> = map
        .range((target, TabletId(u64::MIN), Vec::new())..)
        .take_while(|((t, _, _), _)| *t == target)
        .map(|(hk, h)| (h.version, hk.clone()))
        .collect();
    if owned.is_empty() {
        return;
    }
    // TTL: drop anything more than `ttl_versions` behind the freshest version
    // seen for this target.
    if let Some(ttl) = limits.ttl_versions {
        let high_water = owned.iter().map(|(v, _)| *v).max().unwrap_or(0);
        let floor = high_water.saturating_sub(ttl);
        owned.retain(|(v, hk)| {
            if *v < floor {
                map.remove(hk);
                false
            } else {
                true
            }
        });
    }
    // Count cap: keep the highest-version `cap` hints; evict the rest (lowest
    // versions first). Sort ascending by (version, key) for a deterministic,
    // stable eviction order.
    if let Some(cap) = limits.per_target_cap {
        if owned.len() > cap {
            owned.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
            let evict = owned.len() - cap;
            for (_, hk) in owned.into_iter().take(evict) {
                map.remove(&hk);
            }
        }
    }
}

/// Whether `target` may receive hints under `allowed` (residency, ADR 0005).
fn target_allowed(allowed: &AllowedTargets, target: NodeId) -> bool {
    match allowed {
        None => true,
        Some(set) => set.contains(&target),
    }
}

/// Start the background **hint-handoff replay** loop on `env`.
///
/// Every `interval` of (virtual) time, for each target with pending hints, the
/// loop sends a [`DataMsg::Probe`] and waits up to `interval` for a
/// [`DataMsg::ProbeAck`]. A `ProbeAck` means the target is reachable, so the loop
/// drains its hints and replays them as a [`DataMsg::Sync`] per `(tablet, epoch)`
/// (epoch-fenced, per-key LWW, idempotent). No `ProbeAck` (the target is still
/// down/partitioned) leaves the hints pending for a later round.
///
/// Residency (ADR 0005): hints are only ever *recorded* for an `allowed` target
/// (see [`HintStore::record`]), so the loop never has a hint for an ineligible
/// node to replay; the same `allowed` set should bound the replica's receive-side
/// guard ([`serve_replica_with_residency`](crate::serve_replica_with_residency)).
///
/// The loop runs on the coordinator's `env`. It owns the `recv` for `ProbeAck`s
/// during a probe window; do not co-locate it with another protocol consuming the
/// same node's inbox (the single-consumer rule). In `animusd` the coordinator's
/// `DataClient` does not otherwise `recv` outside an in-flight client op, and the
/// client ops are serialized behind a lock, so the handoff loop shares the coord
/// env cleanly.
pub fn serve_hint_handoff<E>(env: E, store: HintStore, allowed: AllowedTargets, interval: Duration)
where
    E: Env,
{
    let metrics = env.metrics();
    serve_hint_handoff_with_metrics(env, store, allowed, interval, metrics);
}

/// Like [`serve_hint_handoff`], but records `data_hints_delivered` (ADR 0015)
/// into `metrics` rather than `env.metrics()`. Additive and observe-only — it
/// changes no handoff behavior. A sim test threads a recording handle here to
/// read the counter back (`SimEnv::metrics()` is the no-op default).
pub fn serve_hint_handoff_with_metrics<E>(
    env: E,
    store: HintStore,
    allowed: AllowedTargets,
    interval: Duration,
    metrics: MetricsHandle,
) where
    E: Env,
{
    env.clone().spawn_task(async move {
        loop {
            env.sleep(interval).await;
            let targets = store.targets();
            for target in targets {
                // Residency: never replay to a target the placement forbids
                // (defence in depth — `record` already refuses to store one).
                if !target_allowed(&allowed, target) {
                    let _ = store.drain_target(target);
                    continue;
                }
                if probe(&env, target, interval).await {
                    let batches = store.drain_target(target);
                    let delivered = replay(&env, target, &batches).await;
                    if delivered && !batches.is_empty() {
                        metrics.incr(Metric::DataHintsDelivered);
                    }
                    if !delivered {
                        store.restore(target, &batches);
                    }
                }
            }
        }
    });
}

/// Start a **send-only** hint-replay loop on `env` — the variant for a holder
/// that *shares* its node's inbox with another `recv` consumer (e.g. `animusd`'s
/// coordinator, where the `DataClient` already owns the coord inbox during a
/// client op). It cannot probe (probing needs to `recv` the `ProbeAck`, which
/// would violate the single-consumer rule against the co-located coordinator), so
/// each round it simply drains the pending hints and replays them as
/// fire-and-forget [`DataMsg::Sync`]s, exactly like read-repair/anti-entropy push
/// repair traffic.
///
/// The replay is **idempotent and epoch-fenced** (per-key LWW via `Sync`): a
/// replica that is up applies it (converging promptly); a replica still down
/// drops it on the floor, and the durable record on the `W` replicas that did ack
/// converges it via background anti-entropy regardless (ADR 0010) — so a missed
/// replay is a lost *accelerator*, never a lost write. Residency-bounded
/// (ADR 0005): hints are only recorded for `allowed` targets, and this loop
/// re-checks `allowed` before replaying (defence in depth).
///
/// It **re-sends** (does not drain) so a target that is still down this round
/// still gets the hint on a later round once it returns; a hint leaves the store
/// only when a newer write to the same key supersedes it (per-key LWW in
/// [`HintStore::record`]). For an `allowed` target it cannot reach, this drains
/// the hint (it can never be delivered there).
///
/// Use [`serve_hint_handoff`] instead when the holder has a **dedicated** node id
/// it can `recv` `ProbeAck`s on — that variant only replays to a target it has
/// just observed healthy (no wasted sends to a still-down replica, and the hint
/// is cleared on confirmed delivery).
pub fn serve_hint_replay<E>(env: E, store: HintStore, allowed: AllowedTargets, interval: Duration)
where
    E: Env,
{
    let metrics = env.metrics();
    serve_hint_replay_with_metrics(env, store, allowed, interval, metrics);
}

/// Like [`serve_hint_replay`], but records `data_hints_delivered` (ADR 0015) into
/// `metrics` rather than `env.metrics()`. Each round that actually re-sends a
/// non-empty batch to a target counts one delivery. Additive and observe-only.
///
/// Note this is a *send-only* loop (no `ProbeAck` to confirm receipt), so a
/// "delivery" here means the `Sync` was emitted on the wire — a still-down target
/// will be re-sent (and re-counted) next round. That is the honest meaning of the
/// counter for this variant: replay attempts emitted, not acks observed.
pub fn serve_hint_replay_with_metrics<E>(
    env: E,
    store: HintStore,
    allowed: AllowedTargets,
    interval: Duration,
    metrics: MetricsHandle,
) where
    E: Env,
{
    env.clone().spawn_task(async move {
        loop {
            env.sleep(interval).await;
            for target in store.targets() {
                if !target_allowed(&allowed, target) {
                    let _ = store.drain_target(target);
                    continue;
                }
                // Re-send without draining: a still-down target gets it on a
                // later round; a superseding write clears it via LWW.
                let batches = store.snapshot_target(target);
                if !batches.is_empty() {
                    replay(&env, target, &batches).await;
                    metrics.incr(Metric::DataHintsDelivered);
                }
            }
        }
    });
}

/// Send `target` a `Probe` and wait up to `timeout` for its matching `ProbeAck`.
/// Other inbound messages during the window are ignored. Returns whether the ack
/// arrived (the target is reachable).
async fn probe<E: Env>(env: &E, target: NodeId, timeout: Duration) -> bool {
    use futures::future::{Either, select};
    let req = env.next_u64();
    let msg = DataMsg::Probe { req };
    let bytes = serde_json::to_vec(&msg).expect("data message serializes");
    env.send(target, bytes).await;

    let deadline = env.now().0.saturating_add(dur_nanos(timeout));
    loop {
        let now = env.now().0;
        if now >= deadline {
            return false;
        }
        let remaining = Duration::from_nanos(deadline - now);
        match select(env.recv(), env.sleep(remaining)).await {
            Either::Left((envelope, _)) => {
                if let Ok(DataMsg::ProbeAck { req: r }) =
                    serde_json::from_slice::<DataMsg>(&envelope.payload)
                {
                    if r == req {
                        return true;
                    }
                }
            }
            Either::Right(((), _)) => return false,
        }
    }
}

/// Replay drained hint batches to `target` as fire-and-forget `Sync`s (one per
/// `(tablet, epoch)`). Returns whether anything was sent (so an empty drain is
/// treated as delivered, not re-filed).
async fn replay<E: Env>(
    env: &E,
    target: NodeId,
    batches: &BTreeMap<(TabletId, Epoch), Vec<crate::SyncEntry>>,
) -> bool {
    if batches.is_empty() {
        return true;
    }
    for ((tablet, epoch), entries) in batches {
        let msg = DataMsg::Sync {
            tablet: *tablet,
            epoch: *epoch,
            entries: entries.clone(),
        };
        let bytes = serde_json::to_vec(&msg).expect("data message serializes");
        env.send(target, bytes).await;
    }
    true
}

fn dur_nanos(d: Duration) -> u64 {
    d.as_nanos().min(u128::from(u64::MAX)) as u64
}
