//! The crate's entire contact surface with real time, task spawning and
//! wall-clock calendar time — everything else (schedule, recorder,
//! distributions, op streams, report types) is pure and clock-free.
//!
//! `animus-bench` is a real process boundary (a load generator measuring a
//! real server over real sockets), so these few wrappers carry the crate's
//! only `disallowed_methods` allows, each individually justified. The rest of
//! the crate (including its tests) goes through them; no package-level
//! exemption exists.

use std::future::Future;
use std::time::Duration;

/// A run-scoped monotonic clock. All timestamps in the pure core are `u64`
/// nanoseconds since this clock's origin ("run time"), so the core never
/// sees an `Instant`.
#[derive(Clone, Copy, Debug)]
pub struct Clock {
    origin: std::time::Instant,
}

impl Clock {
    /// Start a clock; `now_ns()` is 0 at this instant.
    #[must_use]
    #[allow(
        clippy::disallowed_methods,
        reason = "load generator process boundary: measuring a real server needs real monotonic time; the pure core only ever sees the u64 nanos this returns"
    )]
    pub fn start() -> Self {
        Self {
            origin: std::time::Instant::now(),
        }
    }

    /// Nanoseconds since [`Clock::start`].
    #[must_use]
    pub fn now_ns(&self) -> u64 {
        u64::try_from(self.origin.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }
}

/// Real sleep.
#[allow(
    clippy::disallowed_methods,
    reason = "load generator process boundary: the open-loop dispatcher paces itself against real time"
)]
pub async fn sleep(d: Duration) {
    tokio::time::sleep(d).await;
}

/// Run `f` for at most `d`; `None` on timeout.
#[allow(
    clippy::disallowed_methods,
    reason = "load generator process boundary: a per-operation client timeout is a real wall-clock bound on a real socket"
)]
pub async fn timeout<F: Future>(d: Duration, f: F) -> Option<F::Output> {
    tokio::time::timeout(d, f).await.ok()
}

/// Spawn a task on the ambient tokio runtime.
#[allow(
    clippy::disallowed_methods,
    reason = "load generator process boundary: workers, the fault injector and the load fan-out are real tokio tasks (there is no SimEnv here)"
)]
pub fn spawn<F>(f: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(f)
}

/// Calendar time as seconds since the Unix epoch — used only for the SigV4
/// `X-Amz-Date` stamp and the report's `generated_at`; never for timing.
#[must_use]
#[allow(
    clippy::disallowed_methods,
    reason = "SigV4 requires a calendar timestamp and the report records when it was generated; never used for any latency or deadline"
)]
pub fn wall_epoch_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}
