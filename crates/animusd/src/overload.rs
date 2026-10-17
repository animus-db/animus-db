//! Resource bounds and overload shedding for the node's listeners (R-01
//! sub-track (d), ADR 0074 §2). Process-boundary code (real sockets, real
//! time), like `dynamo.rs`/`admin.rs` — never under `SimEnv`.
//!
//! Two primitives, both a pair of atomics with no lock and no wait:
//!
//! - [`CountGate`] — a bounded counter handed out as RAII [`Permit`]s. It never
//!   blocks and never queues: `try_acquire` either takes a slot or says no,
//!   so every use site turns "no" into a prompt refusal. Used for the
//!   per-listener connection caps, the node-wide in-flight request bound, and
//!   the cap on the refusal tasks themselves.
//! - [`shed_connection`] — what a listener does with a connection it will not
//!   serve: answer a minimal `503` (when the listener speaks plain HTTP) and
//!   close, on a *bounded* number of short-lived tasks, else close outright.
//!   The accept loop itself never waits on a refused peer.
//!
//! A connection accepted below the cap holds its [`Permit`] for its whole
//! life, so the cap bounds live connections, not accept rate. Memory is
//! therefore bounded per node by `max_connections × MAX_BODY` (one request
//! buffered per connection, `animus_node::http::MAX_BODY`) plus the in-flight
//! bound's own working set; see `docs/resource-bounds.md`.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use animus_env::MetricsHandle;

use crate::config::ResolvedLimits;

/// A bounded counter: at most `cap` [`Permit`]s live at once.
#[derive(Clone, Debug)]
pub(crate) struct CountGate {
    cap: usize,
    live: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
}

/// One slot of a [`CountGate`], returned to it on drop.
#[derive(Debug)]
pub(crate) struct Permit {
    live: Arc<AtomicUsize>,
}

impl Drop for Permit {
    fn drop(&mut self) {
        self.live.fetch_sub(1, Ordering::AcqRel);
    }
}

impl CountGate {
    pub(crate) fn new(cap: usize) -> Self {
        Self {
            cap,
            live: Arc::new(AtomicUsize::new(0)),
            peak: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Take a slot, or `None` when `cap` are already out. Never blocks.
    pub(crate) fn try_acquire(&self) -> Option<Permit> {
        self.try_acquire_capped(self.cap)
    }

    /// As [`try_acquire`](Self::try_acquire) against a caller-supplied cap
    /// (never above the gate's own): the lag-scaled node gate (ADR 0080 2a)
    /// passes its current effective size on every request.
    pub(crate) fn try_acquire_capped(&self, cap: usize) -> Option<Permit> {
        let cap = cap.min(self.cap);
        let mut cur = self.live.load(Ordering::Acquire);
        loop {
            if cur >= cap {
                return None;
            }
            match self
                .live
                .compare_exchange_weak(cur, cur + 1, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    self.peak.fetch_max(cur + 1, Ordering::Relaxed);
                    return Some(Permit {
                        live: self.live.clone(),
                    });
                }
                Err(seen) => cur = seen,
            }
        }
    }

    /// Slots currently out.
    #[allow(dead_code)] // read by the overload tests
    pub(crate) fn live(&self) -> usize {
        self.live.load(Ordering::Acquire)
    }

    /// The most slots ever out at once (a high-water mark, never reset).
    #[allow(dead_code)] // read by the overload tests
    pub(crate) fn peak(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }
}

/// Requests parked on each tablet from this node's edge (ADR 0080 2c), capped
/// at `cap` per tablet (`0` = off). An entry exists only while it is non-zero,
/// so the map is bounded by the tablets with live requests.
#[derive(Clone, Debug)]
pub(crate) struct GroupInflight {
    cap: usize,
    live: Arc<std::sync::Mutex<std::collections::BTreeMap<animus_tablet::TabletId, usize>>>,
}

/// One request's slot on a tablet; returned on drop.
#[derive(Debug)]
pub(crate) struct GroupPermit {
    tablet: animus_tablet::TabletId,
    live: Arc<std::sync::Mutex<std::collections::BTreeMap<animus_tablet::TabletId, usize>>>,
}

impl Drop for GroupPermit {
    fn drop(&mut self) {
        let mut m = self.live.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(n) = m.get_mut(&self.tablet) {
            *n = n.saturating_sub(1);
            if *n == 0 {
                m.remove(&self.tablet);
            }
        }
    }
}

impl GroupInflight {
    pub(crate) fn new(cap: usize) -> Self {
        Self {
            cap,
            live: Arc::default(),
        }
    }

    /// Take a slot on `tablet`, or `None` when it already has `cap` parked.
    /// With `cap == 0` (off) every request is admitted without a slot.
    pub(crate) fn try_acquire(
        &self,
        tablet: animus_tablet::TabletId,
    ) -> Result<Option<GroupPermit>, ()> {
        if self.cap == 0 {
            return Ok(None);
        }
        let mut m = self.live.lock().unwrap_or_else(|e| e.into_inner());
        let n = m.entry(tablet).or_insert(0);
        if *n >= self.cap {
            return Err(());
        }
        *n += 1;
        drop(m);
        Ok(Some(GroupPermit {
            tablet,
            live: Arc::clone(&self.live),
        }))
    }

    /// Requests parked on `tablet` now.
    #[allow(dead_code)] // read by the overload tests
    pub(crate) fn live(&self, tablet: animus_tablet::TabletId) -> usize {
        self.live
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&tablet)
            .copied()
            .unwrap_or(0)
    }
}

/// What a request for a tablet with no resolved leader should do (ADR 0080 2b).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum LeaderlessVerdict {
    /// Keep waiting (an election is normally shorter), until `until`.
    Wait { until: animus_env::Nanos },
    /// Refuse at once: the tablet has had no leader for `waited`.
    Refuse { waited: Duration },
}

/// Per-tablet "unresolved since" state (ADR 0080 2b). Pure: time is passed
/// in. An entry exists only while its tablet is unresolved, so the map is
/// bounded by the number of currently leaderless tablets.
#[derive(Clone, Debug)]
pub(crate) struct LeaderlessTracker {
    wait: Duration,
    since: Arc<
        std::sync::Mutex<std::collections::BTreeMap<animus_tablet::TabletId, animus_env::Nanos>>,
    >,
}

impl LeaderlessTracker {
    pub(crate) fn new(wait: Duration) -> Self {
        Self {
            wait,
            since: Arc::default(),
        }
    }

    /// `tablet`'s leader did not resolve at `now`: record the first miss and
    /// say wait or refuse.
    pub(crate) fn unresolved(
        &self,
        tablet: animus_tablet::TabletId,
        now: animus_env::Nanos,
    ) -> LeaderlessVerdict {
        let mut m = self.since.lock().unwrap_or_else(|e| e.into_inner());
        let since = *m.entry(tablet).or_insert(now);
        let waited = Duration::from_nanos(now.0.saturating_sub(since.0));
        if waited >= self.wait {
            LeaderlessVerdict::Refuse { waited }
        } else {
            LeaderlessVerdict::Wait {
                until: since.saturating_add(self.wait),
            }
        }
    }

    /// `tablet`'s leader resolved: clear it.
    pub(crate) fn resolved(&self, tablet: animus_tablet::TabletId) {
        let mut m = self.since.lock().unwrap_or_else(|e| e.into_inner());
        if !m.is_empty() {
            m.remove(&tablet);
        }
    }

    /// How long `tablet` has been leaderless at `now` (zero when it is not).
    pub(crate) fn waited(
        &self,
        tablet: animus_tablet::TabletId,
        now: animus_env::Nanos,
    ) -> Duration {
        let m = self.since.lock().unwrap_or_else(|e| e.into_inner());
        m.get(&tablet).map_or(Duration::ZERO, |s| {
            Duration::from_nanos(now.0.saturating_sub(s.0))
        })
    }

    /// Tablets currently tracked as leaderless.
    #[allow(dead_code)] // read by the overload tests
    pub(crate) fn len(&self) -> usize {
        self.since.lock().unwrap_or_else(|e| e.into_inner()).len()
    }
}

/// How many refusal tasks may be in flight at once. A connection refused
/// beyond this is closed without a response.
const SHED_TASK_SLOTS: usize = 64;
/// Longest a refusal task spends writing its response.
const SHED_WRITE_TIMEOUT: Duration = Duration::from_secs(1);
/// Longest a refusal task keeps draining the peer's unread request so the
/// close is a FIN, not an RST that could destroy the response in flight.
const SHED_DRAIN_TIMEOUT: Duration = Duration::from_millis(250);

/// The per-node overload state carried on `ClientCtx`: the DynamoDB
/// connection cap, the node-wide in-flight request bound, the resolved
/// limits the other listeners read, and the refusal-task budget.
#[derive(Clone, Debug)]
pub(crate) struct OverloadState {
    pub(crate) limits: ResolvedLimits,
    /// Live connections on the DynamoDB listener.
    pub(crate) dynamo_conns: CountGate,
    /// DynamoDB requests executing now (past auth, inside `dispatch`).
    pub(crate) inflight: CountGate,
    /// Refusal tasks in flight (shared by every listener of this node).
    pub(crate) shed_tasks: CountGate,
    /// Where the `overload_shed_*` counters are recorded (this node's own
    /// sink, the one `/metrics` exports).
    pub(crate) metrics: MetricsHandle,
    /// ADR 0080: this node's propose admission (the per-group backlog bound
    /// and the shared `QueuedBytes` budget). Handed to the tablet-host
    /// reconciler (via `ElectionBackpressure`), fed its lag by the
    /// `LoadMonitor`, and read by the edge for its lag-scaled gates.
    pub(crate) admission: animus_cp_data::admission::NodeAdmission,
    /// ADR 0080 2d: forwarded requests executing now on this node's intra
    /// listener, separate from `inflight` so client work cannot starve the
    /// forwards that complete a leader's writes (and the reverse).
    pub(crate) forwarded: CountGate,
    /// ADR 0080 2c: requests parked on one tablet from this node's edge.
    pub(crate) group_inflight: GroupInflight,
    /// ADR 0080 2b: per-tablet "no leader since" state for the fast-fail.
    pub(crate) leaderless: LeaderlessTracker,
    /// ADR 0080 section 5 (4.7): admin handlers executing now.
    pub(crate) admin_inflight: CountGate,
}

impl OverloadState {
    pub(crate) fn new(limits: ResolvedLimits, metrics: MetricsHandle) -> Self {
        let budget = animus_env::QueuedBytes::new(limits.queued_bytes_budget);
        Self {
            limits,
            admission: animus_cp_data::admission::NodeAdmission::new(
                limits.backlog_limits(),
                budget,
                metrics.clone(),
            ),
            metrics,
            dynamo_conns: CountGate::new(limits.max_connections),
            inflight: CountGate::new(limits.max_inflight_requests),
            shed_tasks: CountGate::new(SHED_TASK_SLOTS),
            forwarded: CountGate::new(limits.max_forwarded_inflight),
            group_inflight: GroupInflight::new(limits.group_inflight_max),
            leaderless: LeaderlessTracker::new(limits.leaderless_wait),
            admin_inflight: CountGate::new(limits.max_admin_inflight),
        }
    }
}

impl Default for OverloadState {
    fn default() -> Self {
        Self::new(ResolvedLimits::default(), MetricsHandle::noop())
    }
}

/// Refuse `stream`: write `response` (a complete HTTP response, `Connection:
/// close`) and close, on one of `slots`' bounded tasks; or, if `response` is
/// `None` (a TLS listener cannot speak plaintext HTTP before its handshake) or
/// no refusal slot is free, just drop the stream. Returns immediately — the
/// caller's accept loop never waits on a refused peer.
pub(crate) fn shed_connection(
    mut stream: TcpStream,
    response: Option<Arc<str>>,
    slots: &CountGate,
) {
    let (Some(response), Some(slot)) = (response, slots.try_acquire()) else {
        return; // dropped: closed without a response
    };
    tokio::spawn(async move {
        let _slot = slot;
        let _ = tokio::time::timeout(SHED_WRITE_TIMEOUT, async {
            stream.write_all(response.as_bytes()).await?;
            stream.flush().await
        })
        .await;
        // Half-close our side so the peer reads the response then EOF, then
        // swallow whatever request bytes it had already sent: closing with
        // unread data in the receive buffer turns the FIN into an RST, which
        // can discard the response before the peer reads it.
        let _ = stream.shutdown().await;
        let mut sink = [0u8; 4096];
        let _ = tokio::time::timeout(SHED_DRAIN_TIMEOUT, async {
            while let Ok(n) = stream.read(&mut sink).await {
                if n == 0 {
                    break;
                }
            }
        })
        .await;
    });
}

/// The complete `503` a plain-HTTP listener sheds with. `body` is the
/// listener's error body (the DynamoDB JSON error for the DynamoDB port).
pub(crate) fn shed_response(content_type: &str, body: &str) -> Arc<str> {
    animus_node::http::format_response(503, content_type, body, false, "").into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_gate_never_exceeds_its_cap_and_recovers_on_drop() {
        let g = CountGate::new(2);
        let a = g.try_acquire().expect("first");
        let b = g.try_acquire().expect("second");
        assert!(g.try_acquire().is_none(), "third must be refused");
        assert_eq!((g.live(), g.peak()), (2, 2));
        drop(a);
        let c = g.try_acquire().expect("a freed slot is reusable");
        assert!(g.try_acquire().is_none());
        drop((b, c));
        assert_eq!(g.live(), 0);
        assert_eq!(g.peak(), 2, "the high-water mark is never lowered");
    }

    #[test]
    fn a_gate_is_exact_under_contention() {
        let g = CountGate::new(8);
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let g = g.clone();
                std::thread::spawn(move || {
                    for _ in 0..20_000 {
                        if let Some(p) = g.try_acquire() {
                            assert!(g.live() <= 8);
                            drop(p);
                        }
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().expect("worker panicked");
        }
        assert_eq!(g.live(), 0, "every permit was returned");
        assert!(g.peak() <= 8, "peak {} exceeded the cap", g.peak());
    }

    #[test]
    fn a_capped_acquire_honours_a_smaller_cap_but_never_a_larger_one() {
        let g = CountGate::new(4);
        let a = g.try_acquire_capped(2).expect("1");
        let b = g.try_acquire_capped(2).expect("2");
        assert!(g.try_acquire_capped(2).is_none(), "the lag-scaled cap");
        let c = g.try_acquire_capped(100).expect("the gate's own cap is 4");
        let d = g.try_acquire_capped(100).expect("4th");
        assert!(
            g.try_acquire_capped(100).is_none(),
            "never above its own cap"
        );
        drop((a, b, c, d));
        assert_eq!(g.live(), 0);
    }

    #[test]
    fn group_inflight_is_per_tablet_and_cleans_up() {
        let g = GroupInflight::new(2);
        let t1 = animus_tablet::TabletId(1);
        let t2 = animus_tablet::TabletId(2);
        let a = g.try_acquire(t1).expect("room").expect("slot");
        let b = g.try_acquire(t1).expect("room").expect("slot");
        assert!(g.try_acquire(t1).is_err(), "t1 is full");
        assert!(
            g.try_acquire(t2).is_ok(),
            "a full tablet does not block another"
        );
        drop(a);
        assert!(g.try_acquire(t1).is_ok());
        drop(b);
        // Everything released: the map is empty again (bounded memory).
        assert_eq!(g.live(t1), 0);
        assert_eq!(g.live.lock().unwrap().len(), 0);
        assert!(
            GroupInflight::new(0)
                .try_acquire(t1)
                .expect("off")
                .is_none()
        );
    }

    #[test]
    fn leaderless_waits_then_refuses_and_recovers() {
        use animus_env::Nanos;
        let t = LeaderlessTracker::new(Duration::from_secs(2));
        let tab = animus_tablet::TabletId(7);
        let at = |ms: u64| Nanos(ms * 1_000_000);
        match t.unresolved(tab, at(1_000)) {
            LeaderlessVerdict::Wait { until } => assert_eq!(until, at(3_000)),
            v => panic!("{v:?}"),
        }
        assert!(matches!(
            t.unresolved(tab, at(2_999)),
            LeaderlessVerdict::Wait { .. }
        ));
        assert!(matches!(
            t.unresolved(tab, at(3_000)),
            LeaderlessVerdict::Refuse { waited } if waited == Duration::from_secs(2)
        ));
        assert_eq!(t.len(), 1);
        t.resolved(tab);
        assert_eq!(t.len(), 0, "recovers on its own");
        assert!(matches!(
            t.unresolved(tab, at(9_000)),
            LeaderlessVerdict::Wait { .. }
        ));
    }
}
