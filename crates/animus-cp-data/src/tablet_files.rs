//! A node-local index of which tablets have durable engine files on this
//! node's flat data directory (issue #1303).
//!
//! Per-tablet engines (ADR 0050) are identified purely by filename prefix on
//! one flat [`Disk`] namespace (`db-t<id>-MANIFEST`, `-wal-*`, `-sst-*`), and
//! the on-disk layout is durable (ADR 0073), so a prefix factory can only ask
//! "does tablet T have files?" with a whole-directory [`Disk::list`]. Doing
//! that once per hosted tablet makes hosting N tablets cost O(N^2) directory
//! work. [`TabletFileIndex`] answers the same question from one cached
//! listing instead, and stays *exact* by tracking the only way a tablet's
//! files can appear after that listing: an engine being opened or cloned
//! through the factory.
//!
//! State, per node:
//! - `present`: tablets seen with at least one file in the last listing;
//! - `unsure`: tablets whose engine was opened (or cloned into existence)
//!   through [`note_open`](TabletFileIndex::note_open) and that no listing
//!   has yet shown files for. Their files may or may not exist, so a probe of
//!   one of them re-lists (and every listing resolves all of them at once).
//!
//! A tablet in neither set has had no engine opened through this index since
//! the first listing, so it has exactly the files that listing showed: none.
//! The common hosting probe (a fresh tablet, never opened) is therefore O(1)
//! after the one initial listing. Callers must route every engine creation
//! (`open`, the target of a clone) through `note_open` *before* the engine
//! touches the disk.
//!
//! Crash safety is unchanged: this is a cache of a directory listing, rebuilt
//! from the disk on every process start; nothing here is persisted.

use std::collections::BTreeSet;
use std::sync::Mutex;

use animus_env::Disk;
use animus_tablet::TabletId;

#[derive(Default)]
struct State {
    /// Has the initial listing run?
    loaded: bool,
    /// Tablets with files in the last listing (minus later destroys).
    present: BTreeSet<TabletId>,
    /// Tablets opened since, with no file seen yet.
    unsure: BTreeSet<TabletId>,
}

/// See the module doc. One per node, shared by every factory call.
///
/// Concurrency: all state sits behind a short std mutex never held across an
/// `.await`. Overlapping `destroy`/listing calls on the *same* tablet are not
/// supported (the tablet-host reconciler, the only caller, is sequential per
/// node).
pub struct TabletFileIndex<D: Disk> {
    disk: D,
    prefix: fn(TabletId) -> String,
    parse: fn(&str) -> Option<TabletId>,
    state: Mutex<State>,
}

impl<D: Disk> TabletFileIndex<D> {
    /// `prefix` maps a tablet to its engine's filename prefix (trailing
    /// separator included, so `t5-` never matches `t51-`); `parse` is its
    /// inverse over one filename.
    pub fn new(
        disk: D,
        prefix: fn(TabletId) -> String,
        parse: fn(&str) -> Option<TabletId>,
    ) -> Self {
        Self {
            disk,
            prefix,
            parse,
            state: Mutex::new(State::default()),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().expect("tablet file index poisoned")
    }

    /// Record that `tablet`'s engine is about to be opened or cloned into
    /// existence. Call before the engine first touches the disk.
    pub fn note_open(&self, tablet: TabletId) {
        let mut s = self.lock();
        if !s.present.contains(&tablet) {
            s.unsure.insert(tablet);
        }
    }

    /// Re-list the directory and resolve the cache against it. Returns the
    /// listing's file names (so `destroy` can reuse them).
    ///
    /// A failed listing leaves the cache untouched (and unloaded if it was):
    /// callers then answer as the pre-cache code did for a failed listing
    /// (nothing found), but the *next* call lists again rather than
    /// remembering "no tablets" for good.
    async fn refresh(&self) -> Option<Vec<String>> {
        let files = self.disk.list().await.ok()?;
        let seen: BTreeSet<TabletId> = files.iter().filter_map(|f| (self.parse)(f)).collect();
        let mut s = self.lock();
        s.unsure.retain(|t| !seen.contains(t));
        s.present = seen;
        s.loaded = true;
        Some(files)
    }

    /// Does durable engine state exist for `tablet`? Same answer as scanning
    /// a fresh listing for the tablet's prefix, without the scan unless the
    /// tablet's state is genuinely unresolved.
    pub async fn probe(&self, tablet: TabletId) -> bool {
        let need_listing = {
            let s = self.lock();
            if !s.loaded {
                true
            } else if s.present.contains(&tablet) {
                return true;
            } else {
                s.unsure.contains(&tablet)
            }
        };
        if need_listing && self.refresh().await.is_none() {
            return false;
        }
        self.lock().present.contains(&tablet)
    }

    /// Every tablet with durable engine files (one listing, which also
    /// refreshes the cache).
    pub async fn local_tablets(&self) -> BTreeSet<TabletId> {
        if self.refresh().await.is_none() {
            return BTreeSet::new();
        }
        self.lock().present.clone()
    }

    /// Delete every file of `tablet`'s engine (closed by the caller). A
    /// tablet the index knows has no files costs nothing. Returns each file
    /// that failed to delete with its error.
    pub async fn destroy(&self, tablet: TabletId) -> Vec<(String, std::io::Error)> {
        {
            let s = self.lock();
            if s.loaded && !s.present.contains(&tablet) && !s.unsure.contains(&tablet) {
                return Vec::new();
            }
        }
        let prefix = (self.prefix)(tablet);
        let Some(files) = self.refresh().await else {
            return Vec::new();
        };
        let mut failed = Vec::new();
        for f in files.iter().filter(|f| f.starts_with(&prefix)) {
            if let Err(e) = self.disk.remove(f).await {
                failed.push((f.clone(), e));
            }
        }
        if failed.is_empty() {
            let mut s = self.lock();
            s.present.remove(&tablet);
            s.unsure.remove(&tablet);
        }
        failed
    }
}
