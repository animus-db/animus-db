//! Issue #1303: hosting N per-tablet engines on one flat data directory must
//! not cost a whole-directory listing per tablet.
//!
//! Counts `Disk::list` calls and the entries they return (the work a listing
//! costs) through a counting `Disk`, deterministic under `SimEnv`: the cost
//! per hosted tablet must be flat as N grows 10x, while the pre-fix shape
//! (one listing per probe, kept here as a negative control) grows with N.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use animus_cp_data::tablet_files::TabletFileIndex;
use animus_env::{Disk, nid};
use animus_sim::{SimEnv, Simulator};
use animus_tablet::TabletId;
use futures::executor::block_on;

#[derive(Clone)]
struct CountingDisk {
    inner: SimEnv,
    lists: Arc<AtomicU64>,
    listed_entries: Arc<AtomicU64>,
    fail_list: Arc<AtomicBool>,
}

impl CountingDisk {
    fn new(seed: u64) -> Self {
        Self {
            inner: Simulator::new(seed).env(nid(0)),
            lists: Arc::default(),
            listed_entries: Arc::default(),
            fail_list: Arc::default(),
        }
    }
}

#[async_trait::async_trait]
impl Disk for CountingDisk {
    async fn append(&self, file: &str, bytes: &[u8]) -> std::io::Result<()> {
        self.inner.append(file, bytes).await
    }
    async fn sync(&self, file: &str) -> std::io::Result<()> {
        self.inner.sync(file).await
    }
    async fn read(&self, file: &str) -> std::io::Result<Vec<u8>> {
        self.inner.read(file).await
    }
    async fn read_at(&self, file: &str, offset: u64, len: usize) -> std::io::Result<Vec<u8>> {
        self.inner.read_at(file, offset, len).await
    }
    async fn size(&self, file: &str) -> std::io::Result<u64> {
        self.inner.size(file).await
    }
    async fn remove(&self, file: &str) -> std::io::Result<()> {
        self.inner.remove(file).await
    }
    async fn replace(&self, file: &str, bytes: &[u8]) -> std::io::Result<()> {
        self.inner.replace(file, bytes).await
    }
    async fn list(&self) -> std::io::Result<Vec<String>> {
        self.lists.fetch_add(1, Ordering::SeqCst);
        if self.fail_list.load(Ordering::SeqCst) {
            return Err(std::io::Error::other("injected list failure"));
        }
        let names = self.inner.list().await?;
        self.listed_entries
            .fetch_add(names.len() as u64, Ordering::SeqCst);
        Ok(names)
    }
    async fn link(&self, src: &str, dst: &str) -> std::io::Result<()> {
        self.inner.link(src, dst).await
    }
}

fn prefix(t: TabletId) -> String {
    format!("db-t{}-", t.0)
}

fn parse(f: &str) -> Option<TabletId> {
    let rest = f.strip_prefix("db-t")?;
    let end = rest.find('-')?;
    rest[..end].parse().ok().map(TabletId)
}

fn index(d: &CountingDisk) -> TabletFileIndex<CountingDisk> {
    TabletFileIndex::new(d.clone(), prefix, parse)
}

/// What an engine open does to the disk: three files (manifest, wal, sst).
async fn write_engine(d: &CountingDisk, t: TabletId) {
    for kind in ["MANIFEST", "wal-000000", "sst-000001"] {
        let f = format!("{}{kind}", prefix(t));
        d.append(&f, b"x").await.unwrap();
        d.sync(&f).await.unwrap();
    }
}

/// The host path the reconciler runs for each fresh tablet: probe (is there
/// data already?), then open. Returns (list calls, entries listed).
fn host_fresh(n: u64, control: bool) -> (u64, u64) {
    let d = CountingDisk::new(0x1303_0000 + n);
    let idx = index(&d);
    block_on(async {
        for i in 0..n {
            let t = TabletId(i);
            let had = if control {
                // The pre-fix probe: list the whole directory per tablet.
                d.list()
                    .await
                    .unwrap()
                    .iter()
                    .any(|f| f.starts_with(&prefix(t)))
            } else {
                idx.probe(t).await
            };
            assert!(!had, "a fresh tablet {i} must probe false");
            idx.note_open(t);
            write_engine(&d, t).await;
        }
    });
    (
        d.lists.load(Ordering::SeqCst),
        d.listed_entries.load(Ordering::SeqCst),
    )
}

#[test]
fn hosting_fresh_tablets_lists_the_directory_a_constant_number_of_times() {
    let (l100, e100) = host_fresh(100, false);
    let (l1000, e1000) = host_fresh(1000, false);
    assert_eq!(l100, 1, "one initial listing at N=100, got {l100}");
    assert_eq!(l1000, 1, "one initial listing at N=1000, got {l1000}");
    // Entries listed per hosted tablet must not grow with N (here: 0 files at
    // the one listing, as the directory is empty when the node starts).
    assert!(
        e1000 * 100 <= e100 * 1000 + 1,
        "entries listed per tablet grew: {e100} at 100 vs {e1000} at 1000"
    );
}

#[test]
fn negative_control_the_per_probe_listing_grows_with_n() {
    let (l100, e100) = host_fresh(100, true);
    let (l1000, e1000) = host_fresh(1000, true);
    assert_eq!((l100, l1000), (100, 1000));
    // Quadratic: 10x the tablets, ~100x the entries scanned.
    assert!(
        e1000 > 50 * e100,
        "pre-fix control should scan quadratically: {e100} vs {e1000}"
    );
}

#[test]
fn restart_over_n_existing_tablets_lists_once_and_answers_exactly() {
    for n in [100u64, 1000] {
        let d = CountingDisk::new(0x1303_1000 + n);
        block_on(async {
            for i in 0..n {
                write_engine(&d, TabletId(i)).await;
            }
        });
        let idx = index(&d); // a fresh process over the populated disk
        block_on(async {
            for i in 0..n {
                assert!(idx.probe(TabletId(i)).await, "tablet {i} has files");
            }
            assert!(!idx.probe(TabletId(n + 7)).await);
            assert!(
                !idx.probe(TabletId(n * 10)).await,
                "t1 must not match t10.."
            );
        });
        assert_eq!(d.lists.load(Ordering::SeqCst), 1, "N={n}");
        let all: BTreeSet<TabletId> = (0..n).map(TabletId).collect();
        assert_eq!(block_on(idx.local_tablets()), all);
    }
}

#[test]
fn probe_is_exact_for_tablets_opened_after_the_first_listing() {
    let d = CountingDisk::new(0x1303_2000);
    let idx = index(&d);
    block_on(async {
        assert!(!idx.probe(TabletId(1)).await); // loads the cache
        idx.note_open(TabletId(1)); // opened, nothing written yet
        assert!(!idx.probe(TabletId(1)).await, "opened but never written");
        write_engine(&d, TabletId(1)).await; // first write creates files
        assert!(idx.probe(TabletId(1)).await, "now has files");
        assert!(idx.probe(TabletId(1)).await);
        // 5 must not be confused with 51.
        write_engine(&d, TabletId(51)).await;
        idx.note_open(TabletId(51));
        assert!(!idx.probe(TabletId(5)).await);
        assert!(idx.probe(TabletId(51)).await);
    });
}

#[test]
fn destroy_removes_exactly_the_tablets_files_and_is_idempotent() {
    let d = CountingDisk::new(0x1303_3000);
    let idx = index(&d);
    block_on(async {
        for i in [5u64, 51, 6] {
            idx.note_open(TabletId(i));
            write_engine(&d, TabletId(i)).await;
        }
        assert!(idx.destroy(TabletId(5)).await.is_empty());
        assert!(!idx.probe(TabletId(5)).await);
        assert!(idx.probe(TabletId(51)).await && idx.probe(TabletId(6)).await);
        let files = d.inner.list().await.unwrap();
        assert!(files.iter().all(|f| !f.starts_with("db-t5-")), "{files:?}");
        assert_eq!(files.len(), 6);
        // Destroying an unknown / already-destroyed tablet is a no-op and
        // costs no listing.
        let before = d.lists.load(Ordering::SeqCst);
        assert!(idx.destroy(TabletId(5)).await.is_empty());
        assert!(idx.destroy(TabletId(999)).await.is_empty());
        assert_eq!(d.lists.load(Ordering::SeqCst), before);
        // A re-hosted tablet is visible again.
        idx.note_open(TabletId(5));
        write_engine(&d, TabletId(5)).await;
        assert!(idx.probe(TabletId(5)).await);
    });
}

#[test]
fn a_failed_listing_never_poisons_the_cache() {
    let d = CountingDisk::new(0x1303_4000);
    block_on(async {
        write_engine(&d, TabletId(3)).await;
    });
    let idx = index(&d);
    d.fail_list.store(true, Ordering::SeqCst);
    block_on(async {
        // As before the cache: a failed listing reads as "nothing found"...
        assert!(!idx.probe(TabletId(3)).await);
        assert!(idx.local_tablets().await.is_empty());
    });
    d.fail_list.store(false, Ordering::SeqCst);
    block_on(async {
        // ...but is not remembered: the next call lists again and finds it.
        assert!(idx.probe(TabletId(3)).await);
    });
}
