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
        let mut cur = self.live.load(Ordering::Acquire);
        loop {
            if cur >= self.cap {
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
}

impl OverloadState {
    pub(crate) fn new(limits: ResolvedLimits, metrics: MetricsHandle) -> Self {
        Self {
            limits,
            metrics,
            dynamo_conns: CountGate::new(limits.max_connections),
            inflight: CountGate::new(limits.max_inflight_requests),
            shed_tasks: CountGate::new(SHED_TASK_SLOTS),
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
}
