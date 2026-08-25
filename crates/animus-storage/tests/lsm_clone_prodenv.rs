//! Real-multithreading, real-filesystem regression for `LsmEngine::clone_to`
//! (ADR 0058 rung 2): `SimEnv` proves the clone's logic and crash-safety
//! ordering (`lsm_clone.rs`), but only a real `ProdEnv` over a real disk can
//! prove the clone is an actual hard link — not merely a model of one — and
//! that the whole flush+link+manifest sequence works over genuine async I/O.
//! Timeout-guarded so a regression fails loudly instead of hanging (mirrors
//! `lsm_concurrent.rs`'s convention).

use std::os::unix::fs::MetadataExt;
use std::time::Duration;

use animus_env::{ProdEnv, nid};
use animus_storage::{LsmEngine, StorageEngine};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn clone_to_hard_links_real_sstables_and_is_independently_readable() {
    let dir = std::env::temp_dir().join(format!("animus-lsm-clone-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let addr = "127.0.0.1:0".parse().unwrap();
    let (env, _bound) = ProdEnv::bind(nid(0), addr, &dir)
        .await
        .expect("bind ProdEnv");

    let work = async {
        let src = LsmEngine::open(env.clone(), "src-")
            .await
            .expect("open src");
        for i in 0..200u64 {
            src.put(
                format!("k{i:04}").as_bytes(),
                format!("v{i}").as_bytes(),
                i + 1,
            )
            .await
            .unwrap();
        }
        for i in (0..200u64).step_by(7) {
            src.delete(format!("k{i:04}").as_bytes(), 1000 + i)
                .await
                .unwrap();
        }

        let clone = src.clone_to("dst-").await.expect("clone_to");

        let mut src_view = src.entries_with_tombstones().await.unwrap();
        src_view.sort();
        let mut clone_view = clone.entries_with_tombstones().await.unwrap();
        clone_view.sort();
        assert_eq!(
            src_view, clone_view,
            "a real-filesystem clone must match the source's full record set"
        );

        // A completely independent reopen at the same prefix (a fresh
        // `LsmEngine` handle, same underlying `ProdEnv`) sees the same
        // durable state — proving the manifest + linked files are real,
        // durable disk state, not an in-memory view riding the live handle.
        let reopened = LsmEngine::open(env.clone(), "dst-")
            .await
            .expect("reopen clone");
        let mut reopened_view = reopened.entries_with_tombstones().await.unwrap();
        reopened_view.sort();
        assert_eq!(
            reopened_view, src_view,
            "clone survives an independent reopen"
        );

        // The whole point of `clone_to`: it is a **hard link**, not a byte
        // copy. `clone_to` flushes exactly the memtable's single pending
        // batch (nothing had crossed the default flush threshold yet), so
        // `src-sst-000001` is the one SSTable file the clone linked as
        // `dst-sst-000001` — same inode, and the source file's link count
        // reflects the extra name.
        let src_meta =
            std::fs::metadata(dir.join("src-sst-000001")).expect("source sstable file exists");
        let dst_meta = std::fs::metadata(dir.join("dst-sst-000001"))
            .expect("cloned sstable file exists at the target prefix");
        assert_eq!(
            src_meta.ino(),
            dst_meta.ino(),
            "clone_to must hard-link the SSTable file, not copy its bytes"
        );
        assert!(
            src_meta.nlink() >= 2,
            "the source file's link count must reflect the new hard link"
        );

        // Isolation: writes after cloning never cross between the two engines.
        src.put(b"only-src", b"x", 5000).await.unwrap();
        clone.put(b"only-clone", b"x", 5000).await.unwrap();
        assert!(src.get(b"only-clone").await.unwrap().is_none());
        assert!(clone.get(b"only-src").await.unwrap().is_none());
    };
    tokio::time::timeout(Duration::from_secs(30), work)
        .await
        .expect("clone_to (real filesystem) timed out");

    let _ = std::fs::remove_dir_all(&dir);
}
