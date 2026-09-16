//! Real-time `ProdEnv` **driver-liveness** smoke test for the CP data plane
//! (issue #900), mirroring `animus-control`'s own `tests/prod_liveness.rs::
//! wiped_voter_refuses_and_the_rest_of_the_cluster_keeps_serving` byte-for-byte
//! in shape, over a real `RaftKvNode` tablet group instead of the control
//! plane's `RaftNode`.
//!
//! `crates/animus-cp-data/tests/wiped_tablet_voter_boot_check.rs` already
//! proves the mechanism deterministically under `SimEnv`. This file exists
//! for the same reason `prod_liveness.rs`'s own copy does: `SimEnv` proves
//! logic and ordering, never real-thread liveness (root `CLAUDE.md`'s
//! standing lesson) — a real bind/accept/multi-threaded-tokio bring-up, with
//! a genuine process-restart-shaped wipe (a real directory deleted and
//! recreated, a real socket rebound), is the only way to catch a boot-path
//! deadlock or a wall-clock-scale stall `SimEnv`'s virtual clock cannot
//! observe. See ADR 0017's 2026-09-15 amendment (issue #900) for the full
//! mechanism.

// ADR 0003 / ADR 0061 Decision 4 (rung B5): a real-thread ProdEnv driver-
// liveness smoke test (see the module doc above) — the whole point is
// observing real time/threads, which SimEnv's virtual clock structurally
// cannot do.
#![allow(
    clippy::disallowed_methods,
    reason = "real-thread ProdEnv driver-liveness smoke test (the class SimEnv's virtual clock cannot observe, see module doc); ADR 0061 Decision 4"
)]

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use animus_control::ProposeResult;
use animus_cp_data::RaftKvNode;
use animus_env::{Env, NodeId, ProdEnv, nid};
use animus_storage::MemoryEngine;
use tokio::time::{sleep, timeout};

type KvNode = RaftKvNode<ProdEnv, MemoryEngine>;

fn unique_tmp_dir() -> std::path::PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("animus-cp-live-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    dir
}

async fn leader_of(nodes: &[KvNode]) -> Option<usize> {
    for _ in 0..200 {
        for (i, n) in nodes.iter().enumerate() {
            if n.is_leader() {
                return Some(i);
            }
        }
        sleep(Duration::from_millis(50)).await;
    }
    None
}

/// A real 3-node tablet group, one non-leader voter's whole data directory
/// (WAL and anything else in it) wiped and restarted fresh on the same id
/// and the same static bootstrap config — exactly `storage.ephemeral:
/// true`'s real `EmptyDir` pod-recreate shape, the same root cause issue
/// #667 closed for the control plane. Expect: the wiped voter resolves to a
/// permanent refusal, never becomes leader, and the rest of the group keeps
/// serving throughout and after — it already formed a majority of the
/// group's static 3-voter config on its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn wiped_tablet_voter_refuses_and_the_rest_of_the_group_keeps_serving() {
    timeout(Duration::from_secs(60), async {
        let group: Vec<NodeId> = (0..3).map(nid).collect();
        let mut dirs: Vec<_> = (0..3).map(|_| unique_tmp_dir()).collect();
        let loop0 = || "127.0.0.1:0".parse::<SocketAddr>().unwrap();

        let mut envs = Vec::new();
        for (i, dir) in dirs.iter().enumerate() {
            let (env, _addr) = ProdEnv::bind(nid(i as u64), loop0(), dir)
                .await
                .expect("bind");
            envs.push(env);
        }
        let mut book: BTreeMap<NodeId, String> = envs
            .iter()
            .map(|e| (e.node_id(), e.local_addr().to_string()))
            .collect();
        for e in &envs {
            e.set_peers(book.clone());
        }

        let mut nodes: Vec<KvNode> = envs
            .iter()
            .map(|e| RaftKvNode::start(e.clone(), group.clone(), MemoryEngine::new()))
            .collect();

        let leader_idx = leader_of(&nodes)
            .await
            .expect("no leader elected at genesis");
        assert!(
            matches!(
                nodes[leader_idx].put(b"k0".to_vec(), b"v0".to_vec()),
                ProposeResult::Accepted { .. }
            ),
            "pre-wipe write should commit"
        );
        sleep(Duration::from_millis(500)).await;

        // Wipe a NON-leader voter's whole data directory (WAL *and* engine —
        // a fresh `MemoryEngine`, not the old one) and restart it fresh on
        // the same id/config.
        let victim = (0..3)
            .find(|&i| i != leader_idx)
            .expect("a non-leader exists");
        envs[victim].shutdown_and_wait().await;
        std::fs::remove_dir_all(&dirs[victim]).expect("wipe victim data dir");
        dirs[victim] = unique_tmp_dir();
        let (victim_env, _addr) = ProdEnv::bind(nid(victim as u64), loop0(), &dirs[victim])
            .await
            .expect("rebind victim on a fresh port");
        book.insert(victim_env.node_id(), victim_env.local_addr().to_string());
        envs[victim] = victim_env.clone();
        for e in &envs {
            e.set_peers(book.clone());
        }
        nodes[victim] = RaftKvNode::start(victim_env, group.clone(), MemoryEngine::new());

        // The wiped voter must resolve to a permanent refusal — it already
        // was an established voter (the leader's own committed config names
        // it), and at least one other voter is reachable and has real
        // history to prove it.
        let mut refused = false;
        for _ in 0..400 {
            if nodes[victim].refused_as_voter() {
                refused = true;
                break;
            }
            sleep(Duration::from_millis(50)).await;
        }
        assert!(
            refused,
            "the wiped tablet voter never resolved to a permanent refusal within budget"
        );
        assert!(
            !nodes[victim].is_leader(),
            "a refused voter must never become leader"
        );

        // The rest of the group must keep serving throughout and after —
        // never waiting on the wiped node.
        let live: Vec<usize> = (0..3).filter(|&i| i != victim).collect();
        let still_leading = leader_of(&[nodes[live[0]].clone(), nodes[live[1]].clone()])
            .await
            .is_some();
        assert!(
            still_leading,
            "the other two voters must still elect/keep a leader while the wiped voter is refused"
        );
        let cur_leader = if nodes[live[0]].is_leader() {
            live[0]
        } else {
            live[1]
        };
        assert!(
            matches!(
                nodes[cur_leader].put(b"k1".to_vec(), b"v1".to_vec()),
                ProposeResult::Accepted { .. }
            ),
            "a write must still commit after the victim's refusal"
        );

        for env in &envs {
            env.shutdown_and_wait().await;
        }
        for dir in &dirs {
            let _ = std::fs::remove_dir_all(dir);
        }
    })
    .await
    .expect("wiped tablet voter liveness scenario timed out");
}
