//! ADR 0050 (Train B rung 1) — the per-tablet-engine **idle-cost gating
//! measurement**: with one private `LsmEngine` per hosted tablet, a node
//! hosting many mostly-cold tablets must not pay a standing per-engine
//! cost that undermines ADR 0048's quiescence story from the storage side.
//!
//! Two halves:
//!
//! - **Zero background work, asserted structurally**: `LsmEngine::open`
//!   spawns no task, arms no timer, and — under the production default
//!   `LsmOptions` (`background_maintenance: false`, what `animusd` uses) —
//!   even maintenance runs inline inside a write call. An idle engine is a
//!   plain heap structure (empty memtable + manifest + SSTable
//!   footers/indexes); an unwritten one creates **no files at all**
//!   (asserted below: open is passive).
//! - **Memory footprint, measured**: the `#[ignore]`d measurement opens
//!   N=100 idle engines on one `ProdEnv` and reports the per-engine RSS
//!   delta (Linux `/proc/self/statm`). Run it with
//!   `cargo test -p animus-storage --test idle_engine_cost -- --ignored --nocapture`.

use animus_env::{Disk, ProdEnv, nid};
use animus_storage::{LsmEngine, StorageEngine};

const N: usize = 100;

fn rss_bytes() -> u64 {
    // /proc/self/statm: `size resident shared ...` in pages.
    let statm = std::fs::read_to_string("/proc/self/statm").expect("read /proc/self/statm");
    let resident_pages: u64 = statm
        .split_whitespace()
        .nth(1)
        .expect("statm resident field")
        .parse()
        .expect("resident is a number");
    resident_pages * 4096
}

/// An unwritten engine's `open` is passive: no files, no tasks, no timers —
/// the structural half of the idle-cost claim, cheap enough to run always.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn idle_engine_open_is_passive_no_files_no_tasks() {
    let dir = std::env::temp_dir().join(format!("animus-idle-passive-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let addr = "127.0.0.1:0".parse().unwrap();
    let (env, _bound) = ProdEnv::bind(nid(0), addr, &dir)
        .await
        .expect("bind ProdEnv");

    let engines: Vec<LsmEngine<ProdEnv>> = {
        let mut v = Vec::with_capacity(8);
        for t in 0..8u64 {
            v.push(
                LsmEngine::open(env.clone(), format!("db-t{t}-"))
                    .await
                    .expect("open idle engine"),
            );
        }
        v
    };

    // Opening created nothing on disk — an idle engine costs no I/O and
    // leaves nothing to recover.
    let files = env.list().await.expect("list files");
    assert!(
        files.is_empty(),
        "opening unwritten engines must create no files, found: {files:?}"
    );

    // A write to ONE engine creates only that engine's files.
    engines[3]
        .put(b"k", b"v", 1)
        .await
        .expect("write to one engine");
    let files = env.list().await.expect("list files");
    assert!(
        !files.is_empty() && files.iter().all(|f| f.starts_with("db-t3-")),
        "only the written engine may own files, found: {files:?}"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// The N=100 idle-engine RSS measurement (the ADR 0050 rung-1 gating
/// number). `#[ignore]`d: a measurement, not a regression gate — the
/// passive-open test above is the always-on structural assertion.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "measurement, not a gate — run with --ignored --nocapture for the ADR numbers"]
async fn idle_engine_rss_per_instance_measurement() {
    let dir = std::env::temp_dir().join(format!("animus-idle-rss-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let addr = "127.0.0.1:0".parse().unwrap();
    let (env, _bound) = ProdEnv::bind(nid(0), addr, &dir)
        .await
        .expect("bind ProdEnv");

    let before = rss_bytes();
    let mut engines = Vec::with_capacity(N);
    for t in 0..N as u64 {
        engines.push(
            LsmEngine::open(env.clone(), format!("db-t{t}-"))
                .await
                .expect("open idle engine"),
        );
    }
    let after = rss_bytes();
    let delta = after.saturating_sub(before);
    let per_engine = delta / N as u64;
    println!("idle LsmEngine RSS: {N} engines -> {delta} bytes total, ~{per_engine} bytes/engine");

    // Generous sanity ceiling — an idle engine is an empty memtable + a
    // manifest struct; if this ever trips, something grew a real standing
    // footprint and the ADR 0050 rung-1 gating claim needs re-review.
    assert!(
        per_engine < 2 * 1024 * 1024,
        "an idle engine must cost well under 2 MiB, measured ~{per_engine} bytes"
    );

    drop(engines);
    let _ = std::fs::remove_dir_all(&dir);
}
