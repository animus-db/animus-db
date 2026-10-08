//! Regression for issue #1042: `animusd join`-shaped tests intermittently
//! failing under a full `cargo test --workspace` (or any concurrent test
//! process) with `could not join node N within 30s: Address already in use
//! (os error 98)`.
//!
//! **Root cause (fixed in `run_node_join_with_settings`/
//! `run_node_data_join_with_settings`, `crates/animusd/src/lib.rs`)**: for
//! an explicit `--id`, the join path used to durably register `--id`'s
//! `NodeAddrs` (a `MetaCommand::RegisterNode` CAS, ADR 0040 Decision C) via
//! `claim_join_identity` BEFORE it ever called `Node::bind`. A `Node::bind`
//! failure — the ordinary, transient port-TOCTOU issue #278/#627 already
//! documents (another process momentarily holding a just-probed-and-
//! released port) — therefore left a durable claim on file for addresses
//! this attempt never actually bound. A caller retrying the SAME `--id`
//! against a *different* address set (the only safe response once the
//! first set's own port turns out to be held by something else) then
//! re-registered a *different* `NodeAddrs` for an id that had already
//! durably claimed a different one moments earlier — a genuine
//! `MetaCommand::RegisterNode` CAS self-collision
//! (`RegisterOutcome::Collision`, surfacing as "node id already claimed by
//! a different registration") that no amount of retrying at yet another
//! fresh address set can ever resolve, wedging the join for its full
//! deadline. `join_fresh_deadline`'s own pre-#1042 mitigation (freezing one
//! address set across every retry) only prevented THIS self-collision — it
//! did nothing for the case this test drives, where the SAME frozen
//! address genuinely stays held by something else for a while: with the
//! address frozen, a bind failure there just keeps retrying against the
//! identical doomed port until the deadline.
//!
//! The fix reorders `run_node_join_with_settings`'s explicit-`--id` path to
//! bind every listener FIRST (so a `:0` port is resolved and held
//! atomically, the same bind-and-hold discipline issue #627 already
//! established for fresh-cluster bring-up) and only claim the identity with
//! the addresses actually bound — a bind failure can therefore never have
//! registered anything, so a caller is always free to retry (even at a
//! completely different address set) with no risk of colliding with its
//! own earlier, bind-failed attempt.
//!
//! This drives that exact sequence end to end against the real production
//! entry point (`animusd::run_node_join`, not just the test-support
//! wrapper): hold one of a would-be joiner's own ports for a bounded
//! window (a stand-in for "another process/test grabbed it" — the ordinary
//! transient contention this whole mechanism exists to survive), confirm
//! the first attempt fails to bind and durably claims nothing, then retry
//! the SAME explicit `--id` at a fresh address set and confirm the join
//! completes promptly — never wedged behind a self-collision, and nowhere
//! near [`support::JOIN_DEADLINE`].

use std::collections::BTreeMap;
use std::net::{SocketAddr, TcpListener};
use std::time::Duration;

use animusd::config::NodeRole;
use animusd::{RoleAddrs, StorageBackend};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::{sleep, timeout};

mod support;

/// One HTTP/1.0 GET to the admin endpoint; returns the parsed JSON body —
/// mirrors `join_data_seed_settings_reach.rs::admin_get`.
async fn admin_get(addr: SocketAddr, path: &str) -> serde_json::Value {
    let mut stream = TcpStream::connect(addr).await.expect("connect to admin");
    let request = format!("GET {path} HTTP/1.0\r\nHost: animus\r\nConnection: close\r\n\r\n");
    stream
        .write_all(request.as_bytes())
        .await
        .expect("send request");
    stream.flush().await.expect("flush");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.expect("read response");
    let text = String::from_utf8(raw).expect("utf8 response");
    let (_head, payload) = text.split_once("\r\n\r\n").expect("response has a body");
    serde_json::from_str(payload).expect("admin body is JSON")
}

/// A fresh, fully-`:0` combined-mode [`RoleAddrs`] for join id `id`.
fn fresh_addrs(id: animus_env::NodeId) -> RoleAddrs {
    let ephemeral = SocketAddr::from(([127, 0, 0, 1], 0));
    RoleAddrs {
        id,
        role: NodeRole::Both,
        internal: ephemeral,
        client: ephemeral,
        dynamo: ephemeral,
        admin: ephemeral,
        intra: ephemeral,
        console: ephemeral,
        advertise_host: None,
        tls: None,
        encryption_key_path: None,
        labels: Default::default(),
        overload: None,
    }
}

/// A fresh, fully **fixed** (probe-then-release, [`support::free_addrs`])
/// combined-mode [`RoleAddrs`] for join id `id` — deliberately the
/// pre-#1042 test-helper shape (a real, already-resolved port for every
/// field), so this test can hold exactly one of them itself.
fn fresh_fixed_addrs(id: animus_env::NodeId) -> RoleAddrs {
    let raw = support::free_addrs(6);
    RoleAddrs {
        id,
        role: NodeRole::Both,
        internal: raw[0],
        client: raw[1],
        dynamo: raw[2],
        admin: raw[3],
        intra: raw[4],
        console: raw[5],
        advertise_host: None,
        tls: None,
        encryption_key_path: None,
        labels: Default::default(),
        overload: None,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn join_with_explicit_id_survives_a_held_port_and_never_self_collides() {
    let dir = support::panic_safe_tempdir();
    let (base_nodes, base_config) =
        support::bring_up_deadline(1, dir.path(), support::JOIN_DEADLINE).await;
    let seeds: Vec<String> = base_config
        .nodes
        .iter()
        .map(|n| n.intra.to_string())
        .collect();
    let base_admin = base_nodes[0].admin_addr();

    let join_id = animusd::config::node_id(1);

    // Attempt 1: a real, already-resolved address set (the pre-#1042
    // test-helper's own shape) whose `internal` port is held by a foreign
    // listener for a bounded window — a stand-in for another process
    // momentarily owning a just-probed-and-released port (issue #278/#627).
    let attempt1_addrs = fresh_fixed_addrs(join_id.clone());
    let held = TcpListener::bind(attempt1_addrs.internal).unwrap_or_else(|e| {
        panic!(
            "could not hold the joiner's own internal port {}: {e}",
            attempt1_addrs.internal
        )
    });
    let hold_for = Duration::from_millis(500);
    let releaser = tokio::spawn(async move {
        sleep(hold_for).await;
        drop(held);
    });

    let attempt1_dir = dir.path().join("join-attempt-1");
    let err = match animusd::run_node_join(
        seeds.clone(),
        Some(join_id.clone()),
        attempt1_addrs.clone(),
        &attempt1_dir,
        StorageBackend::Memory,
        BTreeMap::new(),
    )
    .await
    {
        Ok(_) => panic!(
            "attempt 1 unexpectedly succeeded — it should have failed to bind \
             {} while it was held",
            attempt1_addrs.internal
        ),
        Err(e) => e,
    };
    let msg = err.to_string();
    assert!(
        !msg.contains("already claimed by a different registration"),
        "attempt 1's own bind failure must never have registered anything — a genuine \
         AlreadyExists collision here means bind-before-claim regressed: {msg}"
    );

    // Bind-before-claim means a failed bind never proposed
    // `MetaCommand::RegisterNode` at all — confirm directly rather than
    // only inferring it from attempt 2 succeeding below.
    let status = admin_get(base_admin, "/admin/status").await;
    assert!(
        !status["node_addrs"]
            .as_object()
            .expect("node_addrs is an object")
            .contains_key(join_id.as_str()),
        "join id {join_id} must not appear in node_addrs after a bind-failed attempt \
         (bind-before-claim regressed — something registered before bind ran): {status}"
    );

    // Attempt 2: the SAME explicit `--id`, a genuinely DIFFERENT (fresh
    // `:0`) address set — the only safe response once attempt 1's own
    // frozen port turns out to be held by something else. Bounded well
    // under `support::JOIN_DEADLINE` (30s): under the pre-#1042 ordering
    // this would self-collide and spin for the whole deadline instead of
    // ever completing.
    let overall_deadline = Duration::from_secs(15);
    let join = async {
        let mut attempt: u32 = 0;
        loop {
            let addrs = fresh_addrs(join_id.clone());
            let attempt_dir = dir.path().join(format!("join-attempt-2-{attempt}"));
            attempt += 1;
            match animusd::run_node_join(
                seeds.clone(),
                Some(join_id.clone()),
                addrs,
                &attempt_dir,
                StorageBackend::Memory,
                BTreeMap::new(),
            )
            .await
            {
                Ok(node) => return node,
                Err(e) => {
                    let msg = e.to_string();
                    assert!(
                        !msg.contains("already claimed by a different registration"),
                        "join self-collided against attempt 1's own bind-failed claim \
                         (issue #1042 regression): {msg}"
                    );
                    sleep(Duration::from_millis(50)).await;
                }
            }
        }
    };
    let joined = timeout(overall_deadline, join).await.unwrap_or_else(|_| {
        panic!(
            "join never completed within {overall_deadline:?} — wedged, the exact \
             issue #1042 symptom"
        )
    });

    releaser.await.expect("releaser task panicked");

    joined.shutdown_graceful().await;
    for n in base_nodes {
        n.shutdown_graceful().await;
    }
}
