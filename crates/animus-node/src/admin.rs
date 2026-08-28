//! The admin/debug HTTP-JSON endpoint's pure route table (ADR 0061 rung
//! C4d, ADR 0020) — the `(method, path)` match [`dispatch`] used to run
//! directly against `animusd::ClientCtx`; it is now generic over
//! [`crate::host::AdminHost`], so the routing decision itself (which route
//! exists, which HTTP verb it accepts, the two 404 shapes for an unknown
//! path vs. a known path with the wrong verb) is unit-testable with no
//! socket, no `ClientCtx`, and no `ProdEnv` in reach — only a trivial fake
//! `AdminHost`.
//!
//! Everything that actually computes a view or runs an action stays in
//! `animusd::admin` exactly as it was — `impl AdminHost for ClientCtx`
//! there is a thin, logic-free delegation to those unmoved functions (see
//! [`crate::host::AdminHost`]'s own doc for why the trait is drawn at
//! "one method per route" rather than decomposing further). `animusd`'s
//! own `dispatch` becomes a one-line wrapper calling straight into
//! [`dispatch`] here. The `OPTIONS`/CORS preflight, the dashboard's static
//! JS/CSS assets, and the dashboard shell HTML stay in `animusd`'s
//! `handle_conn` — they're checked *before* this dispatch table is ever
//! reached and read `crate::dashboard`-owned `include_str!` constants no
//! `AdminHost` method has any reason to carry.

use serde_json::Value;

use crate::host::AdminHost;

/// Route one already-parsed admin request to its [`AdminHost`] method,
/// returning `(http status, pretty-printed JSON body)` — the exact shape
/// `animusd::admin::dispatch` used to build directly.
pub async fn dispatch<H: AdminHost + ?Sized>(
    host: &H,
    method: &str,
    path: &str,
    query: &str,
    body: &[u8],
) -> (u16, String) {
    let (status, value): (u16, Value) = match (method, path) {
        ("GET", "/admin/config") => (200, host.config_view().await),
        ("GET", "/admin/peers") => (200, host.peers_view().await),
        ("GET", "/admin/status") => (200, host.status_json().await),
        ("GET", "/admin/raft") => (200, host.raft_view().await),
        ("GET", "/admin/raftkv") => (200, host.raftkv_view(query).await),
        ("GET", "/admin/txns") => (200, host.txns_view().await),
        ("GET", "/admin/storage/lsm") => host.storage_lsm(query).await,
        ("GET", "/admin/storage/control") => host.storage_control().await,
        ("GET", "/admin/storage/wal") => host.storage_wal(query).await,
        ("GET", "/admin/storage/wal/segment") => host.storage_wal_segment(query).await,
        ("GET", "/admin/storage/key") => host.storage_key(query).await,
        ("GET", "/admin/storage/scan") => host.storage_scan(query).await,
        ("GET", "/admin/system-table") => host.system_table(query).await,
        ("GET", "/admin/backups") => (200, host.backups_view().await),
        ("GET", "/admin/restores") => (200, host.restores_view().await),
        ("GET", "/admin/metrics") => (200, host.metrics_view().await),
        ("GET", "/admin/metrics/history") => (200, host.metrics_history_view().await),
        ("GET", "/admin/member/drain-status") => host.member_drain_status(query).await,
        ("GET", "/admin/health") => host.health().await,
        ("POST", "/admin/tablet/split") => host.action_split(body).await,
        ("POST", "/admin/stream/grow") => host.action_stream_grow(body).await,
        ("POST", "/admin/storage/flush") => host.action_flush(body).await,
        ("POST", "/admin/storage/compact") => host.action_compact(body).await,
        ("POST", "/admin/raftkv/reconfigure") => host.action_reconfigure(body).await,
        ("POST", "/admin/drain") => host.action_drain(body).await,
        ("POST", "/admin/member/add") => host.action_add_member(body).await,
        ("POST", "/admin/member/remove") => host.action_remove_member(body).await,
        ("GET", "/admin/control/members") => (200, host.control_members_view().await),
        ("POST", "/admin/control/member/add") => host.action_add_control_member(body).await,
        ("POST", "/admin/control/member/remove") => host.action_remove_control_member(body).await,
        ("POST", "/admin/data/dynamo") => host.action_data_dynamo(body).await,
        ("POST", "/admin/data/drop-table") => host.action_drop_table(body).await,
        ("POST", "/admin/data/seed") => host.action_data_seed(body).await,
        // A known admin path with the wrong verb vs. an unknown path.
        ("GET" | "POST", p) if p.starts_with("/admin/") => (
            404,
            serde_json::json!({"error": format!("unknown admin route {p}")}),
        ),
        _ => (
            404,
            serde_json::json!({"error": "not found; admin routes live under /admin/"}),
        ),
    };
    (
        status,
        serde_json::to_string_pretty(&value).unwrap_or_default(),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;

    use super::*;

    /// A fake [`AdminHost`] that records which method was called and hands
    /// back a fixed, distinguishable value — enough to prove the *routing*
    /// (which method fires for which `(method, path)`, and the two 404
    /// shapes) with no `ClientCtx`, no `ProdEnv`, no socket at all. Every
    /// method that isn't under test panics, so a wrong route lighting up an
    /// unexpected method fails loudly rather than silently returning a
    /// plausible-looking value.
    struct FakeHost {
        calls: AtomicUsize,
    }

    impl FakeHost {
        fn new() -> Self {
            Self {
                calls: AtomicUsize::new(0),
            }
        }
        fn record(&self) -> Value {
            self.calls.fetch_add(1, Ordering::SeqCst);
            serde_json::json!({"marker": "called"})
        }
    }

    /// Drive a future to completion with no async runtime — a plain
    /// busy-poll loop under `Waker::noop()`, entirely through the safe
    /// `std::task`/`std::pin::pin!` surface (no `unsafe`, matching this
    /// workspace's blanket `unsafe_code` lint). Sound here specifically
    /// because every [`AdminHost`] method this test file's `FakeHost`
    /// implements resolves on its very first poll (none of them ever
    /// actually `.await`s anything) — this crate has no `tokio` dependency
    /// at all (rung C0/C1's compiler-enforced invariant), so a real
    /// executor isn't an option even for tests.
    fn block_on<F: std::future::Future>(fut: F) -> F::Output {
        use std::task::{Context, Poll, Waker};

        let waker = Waker::noop();
        let mut cx = Context::from_waker(waker);
        let mut fut = std::pin::pin!(fut);
        loop {
            if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
                return v;
            }
        }
    }

    #[async_trait]
    impl AdminHost for FakeHost {
        async fn config_view(&self) -> Value {
            self.record()
        }
        async fn peers_view(&self) -> Value {
            unreachable!()
        }
        async fn status_json(&self) -> Value {
            unreachable!()
        }
        async fn raft_view(&self) -> Value {
            unreachable!()
        }
        async fn raftkv_view(&self, query: &str) -> Value {
            assert_eq!(query, "exact=1");
            self.record()
        }
        async fn txns_view(&self) -> Value {
            unreachable!()
        }
        async fn storage_lsm(&self, _query: &str) -> (u16, Value) {
            (200, self.record())
        }
        async fn storage_control(&self) -> (u16, Value) {
            unreachable!()
        }
        async fn storage_wal(&self, _query: &str) -> (u16, Value) {
            unreachable!()
        }
        async fn storage_wal_segment(&self, _query: &str) -> (u16, Value) {
            unreachable!()
        }
        async fn storage_key(&self, _query: &str) -> (u16, Value) {
            unreachable!()
        }
        async fn storage_scan(&self, _query: &str) -> (u16, Value) {
            unreachable!()
        }
        async fn system_table(&self, _query: &str) -> (u16, Value) {
            unreachable!()
        }
        async fn backups_view(&self) -> Value {
            unreachable!()
        }
        async fn restores_view(&self) -> Value {
            unreachable!()
        }
        async fn metrics_view(&self) -> Value {
            unreachable!()
        }
        async fn metrics_history_view(&self) -> Value {
            unreachable!()
        }
        async fn member_drain_status(&self, _query: &str) -> (u16, Value) {
            unreachable!()
        }
        async fn health(&self) -> (u16, Value) {
            unreachable!()
        }
        async fn action_split(&self, body: &[u8]) -> (u16, Value) {
            assert_eq!(body, b"the-body");
            (200, self.record())
        }
        async fn action_stream_grow(&self, _body: &[u8]) -> (u16, Value) {
            unreachable!()
        }
        async fn action_flush(&self, _body: &[u8]) -> (u16, Value) {
            unreachable!()
        }
        async fn action_compact(&self, _body: &[u8]) -> (u16, Value) {
            unreachable!()
        }
        async fn action_reconfigure(&self, _body: &[u8]) -> (u16, Value) {
            unreachable!()
        }
        async fn action_drain(&self, _body: &[u8]) -> (u16, Value) {
            unreachable!()
        }
        async fn action_add_member(&self, _body: &[u8]) -> (u16, Value) {
            unreachable!()
        }
        async fn action_remove_member(&self, _body: &[u8]) -> (u16, Value) {
            unreachable!()
        }
        async fn control_members_view(&self) -> Value {
            unreachable!()
        }
        async fn action_add_control_member(&self, _body: &[u8]) -> (u16, Value) {
            unreachable!()
        }
        async fn action_remove_control_member(&self, _body: &[u8]) -> (u16, Value) {
            unreachable!()
        }
        async fn action_data_dynamo(&self, _body: &[u8]) -> (u16, Value) {
            unreachable!()
        }
        async fn action_drop_table(&self, _body: &[u8]) -> (u16, Value) {
            unreachable!()
        }
        async fn action_data_seed(&self, _body: &[u8]) -> (u16, Value) {
            unreachable!()
        }
    }

    #[test]
    fn get_admin_config_routes_to_config_view() {
        let host = FakeHost::new();
        let (status, body) = block_on(dispatch(&host, "GET", "/admin/config", "", b""));
        assert_eq!(status, 200);
        assert!(body.contains("\"marker\""));
        assert_eq!(host.calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn query_string_reaches_the_handler_untouched() {
        let host = FakeHost::new();
        let (status, _) = block_on(dispatch(&host, "GET", "/admin/raftkv", "exact=1", b""));
        assert_eq!(status, 200);
    }

    #[test]
    fn post_body_reaches_the_handler_untouched() {
        let host = FakeHost::new();
        let (status, _) = block_on(dispatch(
            &host,
            "POST",
            "/admin/tablet/split",
            "",
            b"the-body",
        ));
        assert_eq!(status, 200);
    }

    #[test]
    fn a_known_route_with_the_wrong_verb_is_404_not_405() {
        let host = FakeHost::new();
        // `/admin/config` only exists as GET.
        let (status, body) = block_on(dispatch(&host, "POST", "/admin/config", "", b""));
        assert_eq!(status, 404);
        assert!(body.contains("unknown admin route"));
        assert_eq!(
            host.calls.load(Ordering::SeqCst),
            0,
            "the wrong verb must not reach any AdminHost method"
        );
    }

    #[test]
    fn an_unknown_path_under_admin_is_the_generic_unknown_route_404() {
        let host = FakeHost::new();
        let (status, body) = block_on(dispatch(&host, "GET", "/admin/no-such-route", "", b""));
        assert_eq!(status, 404);
        assert!(body.contains("unknown admin route /admin/no-such-route"));
    }

    #[test]
    fn a_path_entirely_outside_admin_is_the_catch_all_404() {
        let host = FakeHost::new();
        let (status, body) = block_on(dispatch(&host, "GET", "/not-admin-at-all", "", b""));
        assert_eq!(status, 404);
        assert!(body.contains("admin routes live under /admin/"));
    }

    #[test]
    fn the_body_is_pretty_printed_json() {
        let host = FakeHost::new();
        let (_, body) = block_on(dispatch(&host, "GET", "/admin/config", "", b""));
        // Pretty-printing puts each object key on its own indented line.
        assert!(body.contains("\n  "));
    }
}
