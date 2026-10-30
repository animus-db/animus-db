//! Real-thread `ProdEnv` regression for the free-space canary
//! ([`probe_free_space`], issue #1331): under the shared-WAL layout every
//! tablet group on a node passes the **same** wal name, so concurrent probes
//! must not share scratch files. `ProdEnv::replace` writes `{file}.tmp` and
//! renames it; with a shared `{wal}.probe.tmp` one probe's cleanup deleted
//! another's in-flight tmp and the rename failed with ENOENT -- a non-ENOSPC
//! error that `recover_suspect_wal`'s caller turns into a node panic.
//! `SimEnv::replace` is atomic (no `.tmp`), so only real threads can see it.

#![allow(
    clippy::disallowed_methods,
    reason = "real-thread ProdEnv race regression (SimEnv's atomic replace hides the tmp-file race); ADR 0061 Decision 4"
)]

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use animus_control::persist_round::probe_free_space;
use animus_env::{ProdEnv, nid};
use tokio::time::timeout;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_probes_on_one_wal_name_never_collide() {
    let dir = std::env::temp_dir().join(format!("animus-probe-race-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp dir");
    let (env, _addr) = ProdEnv::bind(nid(0), "127.0.0.1:0".parse::<SocketAddr>().unwrap(), &dir)
        .await
        .expect("bind");
    let env = Arc::new(env);

    timeout(Duration::from_secs(120), async {
        for round in 0..300 {
            let mut tasks = Vec::new();
            for _ in 0..16 {
                let env = Arc::clone(&env);
                tasks.push(tokio::spawn(async move {
                    probe_free_space(&*env, "shared.wal").await
                }));
            }
            for t in tasks {
                if let Err(e) = t.await.expect("join") {
                    panic!("probe failed in round {round}: {e}");
                }
            }
        }
    })
    .await
    .expect("probe rounds timed out");

    // Every probe cleaned up only its own files, and all of them.
    let left: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert!(left.is_empty(), "probe left scratch files behind: {left:?}");
    let _ = std::fs::remove_dir_all(&dir);
}
