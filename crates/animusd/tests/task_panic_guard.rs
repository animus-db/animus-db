//! Regression test for issue #939: a panic inside a task spawned through
//! `env.spawn_task` (the seam every background driver/apply loop uses) was
//! invisible to the test harness. The issue's Run-6 panic
//! (`raftkv apply split-fork seal marker: Backend("wal group-commit sync
//! failed")`, inside `animus_cp_data::apply_and_compact`) killed a replica's
//! apply task while `streams_e2e::
//! cascade_split_walks_the_grandparent_chain_with_closed_shard_shape` still
//! reported ok — `ProdEnv::spawn` kept only an `AbortHandle` (never a
//! `JoinHandle`), so nobody ever observed the task's `JoinError`.
//!
//! This suite proves the fix end to end against a real `ProdEnv` node:
//! `ProdEnv::spawn` now counts a spawned task's panic on the env itself
//! (`crates/animus-env/src/prod.rs`'s own unit tests prove that half in
//! isolation), and `support::TaskPanicGuard` fails the *test* — not just the
//! env's own counter — when a watched node counted one.
//!
//! **This test genuinely depends on the env-side counting added for issue
//! #939**: with the `fetch_add` in `ProdEnv::spawn`'s panic path
//! temporarily commented out, `guard_drop_panics_when_a_watched_node_
//! counted_a_task_panic` goes red — `node.spawned_task_panics()` stays 0
//! forever, the bounded wait loop below times out, and the test fails on
//! that assertion instead of on the guard's own panic. (Verified manually;
//! see this crate's PR/commit description for the captured red-run output
//! tail — not left in the tree as a toggle, since a real regression here
//! should always be caught by the very code path this test exercises.)
use std::panic::{self, AssertUnwindSafe};
use std::time::Duration;

use animus_env::EnvExt;
use animusd::StorageBackend;

mod support;

/// A task spawned via `env.spawn_task` on one of a node's role envs panics;
/// once the node's own `spawned_task_panics()` counter observes it (bounded
/// poll — `spawn` keeps no `JoinHandle` to await directly), dropping a
/// `TaskPanicGuard` watching that node must panic, naming the injected
/// message.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn guard_drop_panics_when_a_watched_node_counted_a_task_panic() {
    let dir = support::panic_safe_tempdir();
    let (node, _config) = support::start_single_node(dir.path(), StorageBackend::Memory).await;

    let env = node
        .envs_for_test()
        .first()
        .expect("a node has at least one role env")
        .clone();
    env.spawn_task(async {
        panic!("issue-939 task_panic_guard injected panic");
    });

    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while node.spawned_task_panics() == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "injected task panic was never counted on the node"
        );
        tokio::task::yield_now().await;
    }

    let caught = panic::catch_unwind(AssertUnwindSafe(|| {
        let _guard = support::watch_task_panics(&[&node]);
        // The guard drops at the end of this closure — that drop must panic.
    }));
    let err = caught.expect_err(
        "dropping a TaskPanicGuard watching a node that counted a task panic must panic",
    );
    let msg = err
        .downcast_ref::<String>()
        .cloned()
        .or_else(|| err.downcast_ref::<&str>().map(|s| (*s).to_string()))
        .unwrap_or_default();
    assert!(
        msg.contains("issue-939 task_panic_guard injected panic"),
        "guard's panic message must name the injected panic text, got: {msg}"
    );

    node.shutdown_and_wait().await;
}

/// A clean node's `TaskPanicGuard` drops silently — this fix never changes
/// a passing test's outcome.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn guard_drop_is_silent_for_a_clean_node() {
    let dir = support::panic_safe_tempdir();
    let (node, _config) = support::start_single_node(dir.path(), StorageBackend::Memory).await;

    let caught = panic::catch_unwind(AssertUnwindSafe(|| {
        let _guard = support::watch_task_panics(&[&node]);
    }));
    assert!(
        caught.is_ok(),
        "a clean node's TaskPanicGuard must drop silently"
    );

    node.shutdown_and_wait().await;
}
