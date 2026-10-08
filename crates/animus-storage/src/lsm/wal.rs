//! WAL **group commit** over **rotating numbered segments**: many concurrent
//! writes share one `fsync`, and the log is split into bounded segment files so a
//! flush can drop whole covered segments instead of rewriting one growing file.
//!
//! ## Why
//!
//! The naive WAL path is `append(record)` + `sync()` per write, so every write
//! pays a full `fsync` before it returns. Under `ProdEnv` (a real `fsync`) that
//! dominates the write cost. **Group commit** amortizes it: concurrent writers
//! each append their record to a shared pending buffer, then exactly one of them
//! (the *leader*) performs a single `append` of the whole batch followed by one
//! `sync`, and wakes every writer whose record the sync covered. An ack still
//! means durable — a writer's [`commit`](GroupCommit::commit) returns only after
//! a `sync` that included its record has completed.
//!
//! ## Segment rotation
//!
//! The WAL is a sequence of numbered files `<prefix>wal-NNNNNN`. The leader
//! appends each batch to the **active** segment; once that segment exceeds a byte
//! threshold the next batch opens a **fresh** segment (a new file) and the old
//! one is *sealed*. Every WAL record carries a strictly increasing `wal_seq`, and
//! each sealed segment records the highest `wal_seq` it contains. On a memtable
//! flush the engine learns which segments are **fully covered** by the flush (all
//! their records folded into the new SSTable) via
//! [`segments_covered_by`](GroupCommit::segments_covered_by) and simply `remove`s
//! those files — bounding total WAL size and avoiding any whole-file rewrite. The
//! active (partially covered) segment is always retained. The live segment set is
//! recorded in the durable MANIFEST so recovery knows which files to replay.
//!
//! ## Determinism (ADR 0003)
//!
//! All disk I/O flows through the `Env` [`Disk`] seam. The coordination state is a
//! plain `std::sync::Mutex<Inner>` whose guard is **never held across an
//! `.await`**: the I/O (`append`/`sync`) happens lock-free, and the lock is taken
//! only for brief synchronous buffer mutations / waker bookkeeping. Ordering is a
//! deterministic function of the scheduler: writers are assigned a strictly
//! increasing `wal_seq` under the lock in call order, the leader is whichever
//! writer first observes no flush in progress, segment rotation is decided by the
//! leader under the lock, and waiters / their wakers / the sealed-segment map live
//! in `BTreeMap`s (no `HashMap`). Under the cooperative single-threaded `SimEnv`
//! executor a writer yields once after enqueueing, which lets every other writer
//! that is *already ready in the same drain cycle* enqueue into the same batch
//! before the leader flushes — so batching is observable and reproducible from the
//! seed.
//!
//! ## Crash safety
//!
//! The durability boundary is unchanged: a record is durable iff a `sync` that
//! covered it has returned. The memtable is mutated by the caller **only after**
//! [`commit`](GroupCommit::commit) resolves, so an un-synced batch tail dropped by
//! a crash is exactly the set of writes whose `commit` had not yet returned — they
//! were never acked, never made visible to reads, and recovery (replaying the live
//! segments) sees only the synced prefix. A leader that crashes mid-flush syncs
//! nothing past the prior durable point, so the whole in-flight batch is lost
//! together; no waiter is woken, so no such write is ever reported committed.
//! Segment GC is crash-safe at the manifest swap: a segment file is `remove`d only
//! **after** the manifest that no longer names it is durable, so a crash mid-GC
//! recovers a manifest that still lists the segment (whose bytes are intact) and
//! replay is correct — replaying an already-flushed record just re-inserts an
//! identical `(key, version)` slot (idempotent).
//!
//! ## Sync markers (WAL v2, issue #1142)
//!
//! A group-commit batch is *many* writers' frames in one `append` + one `sync`,
//! so a crash can leave several un-synced frames, and `corrupt_on_crash` can
//! flip a byte in any of them while a later one survives intact. "A valid
//! frame follows the bad one, so it is not a torn tail" (the v1 rule) then
//! refuses a correct writer's file (76 of 300 seeds in the `lsm_wal_coalesced_
//! tear_probe` regression). v2 gives the reader a durable boundary: a
//! CRC-checked marker frame `[len=9][crc][tag 5][offset u64]` whose `offset` is
//! **its own file offset** and which claims "every byte before me is fsynced".
//! The decoder (`decode_wal_v2`) treats a bad frame that starts *before* a
//! valid, correctly-placed marker as corruption (hard error) and anything
//! after the last marker as a tolerated torn tail.
//!
//! Mirrors the control-WAL's CWL1 v2 (#1140): the marker is **piggybacked on
//! the next batch's own `append`** rather than written/synced on its own (a
//! second append+fsync per round would double the commit cost). It is only
//! prepended when the *previous* batch's `sync` on this segment returned `Ok`
//! (`Inner::marker_ready`, cleared when the batch is claimed and set again only
//! on success), and its offset is read from `env.size` at flush time (the
//! leader is the only appender, so it is exactly where the marker lands). So a
//! marker is never written over bytes whose sync is unknown, and it is itself
//! durable only with the next sync — the newest batch has no durable marker
//! until the next one, which merely shrinks the provable region. A disk that
//! lies about `fsync` and then loses acked bytes can leave a marker claiming
//! them; the decoder then fails loudly, which is correct. A segment with no
//! marker is lenient exactly like v1 (a recovered v1 segment, or one with no
//! marker yet).
//!
//! ## File-level format header (ADR 0073 Phase 0, Workstream A)
//!
//! Every segment file's first bytes are a **file-level header**: magic
//! `LWL1` + `u8` version, written exactly **once**, before any
//! [`WalRecord`] frame — not a header on every individual record. The ADR's
//! Phase 0 conventions table names this "LSM WAL record header", but a
//! file-level header is the safer and cheaper shape for what is, in this
//! codebase, a **rotating-segment-file** format rather than one growing
//! file with an ambiguous start: it costs 5 bytes once per segment instead
//! of once per record (segments already rotate on a byte threshold, ADR
//! 0008, so the per-record cost would be paid forever on the hot write
//! path for no ongoing benefit — every record in a given file is already
//! known to share one version the instant the file's own header is read);
//! and it needs no new per-record framing change at all — `encode_wal`/
//! `decode_wal_record`'s frame (`len | crc32 | payload`) and every record's
//! own tag byte are untouched, so nothing about record-level decoding
//! (including the positional torn-tail-vs-corruption proof, see `lsm.rs`'s
//! `decode_wal` module docs) needs to change to accommodate it.
//!
//! **Crash safety** follows the same durable-before-visible argument the
//! rest of this file already relies on. The header is prepended to a
//! segment's **first-ever** batch (whether that's the very first write to a
//! brand-new engine's segment 0, or the first write to a segment a rotation
//! just created) but is appended and **`sync`ed on its own, before any
//! record is appended** ([`GroupCommit::flush_batch`]: one extra `fsync` per
//! segment creation). An earlier shape shared one `sync` between the header
//! and the first records; a crash that tore that un-synced write (and, with
//! corruption, flipped a byte in the kept prefix) could then damage the
//! *header* of a segment that held no acked data, leaving a node that could
//! never restart (`UnsupportedFormatVersion { found: 254 }` /
//! `PreBaselineFormat`; `lsm_crash.rs`'s
//! `crash_during_segment_header_creation`). With the header synced first,
//! every file longer than [`WAL_HEADER_LEN`] has a durable header, so a bad
//! header on such a file is real corruption and stays a loud error; a file
//! *shorter* than the header never had it synced, provably holds no acked
//! data, and `decode_wal` recovers it as empty whatever its bytes. The
//! on-disk encoding is unchanged (no version bump, no fixture change).
//!
//! **When to (re-)write it**: [`Inner::active_seg_needs_header`] tracks,
//! for the *currently* active segment only (older, sealed segments are
//! never appended to again, so they need no ongoing tracking), whether its
//! next batch must be header-prefixed. It starts `true` for a segment this
//! `GroupCommit` has never durably touched — a brand-new engine's segment
//! 0, or any segment reached via [`commit`](GroupCommit::commit)'s own
//! rotation — and `false` for a segment recovered with existing bytes on
//! disk (`GroupCommit::new`'s `active_seg_len` parameter; a non-zero
//! recovered length can only exist if a prior header-carrying `append`
//! already landed it — see `LsmEngine::open_with_metrics`, which derives
//! this from the *post-repair* length so a torn-header segment that got
//! truncated back to empty correctly reads as "needs a header again").
//!
//! It is flipped to `false` **the instant this coordinator's own `append`
//! call (carrying the header) returns `Ok`** — deliberately neither earlier
//! nor later:
//! - **Not earlier** (e.g. alongside `active_seg_bytes`'s own speculative,
//!   pre-outcome bump above): `SimEnv`'s fault model guarantees a *failed*
//!   `append` changes nothing at all (no partial write), so flipping the
//!   flag before knowing the call succeeded would leak a segment that
//!   never received a header if that exact call happened to fail —
//!   unlike the harmless `active_seg_bytes` heuristic, a missing header is
//!   a real, later-fatal format error on reopen.
//! - **Not later** (only after `sync` succeeds): a failed `sync` does
//!   **not** roll back an already-`append`ed buffer (it stays buffered,
//!   to be flushed by whatever unrelated `sync` eventually succeeds on
//!   this file); waiting for `sync` before flipping the flag would make a
//!   retried batch prepend a **second** header ahead of a buffer that
//!   already starts with one the moment that first, sync-failed `append`
//!   is ever followed by a later successful `sync` — corrupting the file
//!   with no crash involved at all.
//!
//! See [`GroupCommit::flush_batch`] for where this is implemented.
//!
//! [`Disk`]: animus_env::Disk

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll, Waker};

use animus_env::Env;

use crate::{Result, StorageError};

/// WAL segment file-level format magic: "LWL1" (Lsm Write-ahead Log, format
/// family 1 — ADR 0073 Phase 0, Workstream A). See the module docs above for
/// why this is a once-per-file header rather than a per-record tag.
pub(super) const WAL_MAGIC: [u8; 4] = *b"LWL1";
/// Current WAL segment file-header version (within the `LWL1` magic family).
///
/// **v2** (issue #1142) adds *sync-boundary marker* frames — see the module
/// docs' "Sync markers" section. v1 files stay readable (`lsm.rs`'s
/// `legacy::v1`); a recovered v1 *active* segment keeps being appended to
/// without markers until it rotates.
pub(super) const WAL_VERSION: u8 = 2;
/// The first WAL version (no sync markers).
pub(super) const WAL_VERSION_V1: u8 = 1;
/// Payload tag byte of a sync-marker frame (record tags are `0..=4`).
pub(super) const WAL_MARKER_TAG: u8 = 5;
/// Bytes of one encoded sync-marker frame (`len | crc` header + tag + `u64`).
pub(super) const WAL_MARKER_FRAME_LEN: usize = 8 + 1 + 8;
/// Bytes in the WAL segment file-level header (`WAL_MAGIC` + one version byte).
pub(super) const WAL_HEADER_LEN: usize = WAL_MAGIC.len() + 1;

/// Encode the WAL segment file-level header: `WAL_MAGIC` followed by
/// `WAL_VERSION`.
pub(super) fn encode_wal_header() -> [u8; WAL_HEADER_LEN] {
    encode_wal_header_version(WAL_VERSION)
}

/// The header for an explicit `version` (test-only legacy encoders write v1).
pub(super) fn encode_wal_header_version(version: u8) -> [u8; WAL_HEADER_LEN] {
    let mut out = [0u8; WAL_HEADER_LEN];
    out[..WAL_MAGIC.len()].copy_from_slice(&WAL_MAGIC);
    out[WAL_MAGIC.len()] = version;
    out
}

/// Coordinates group-committed appends to a rotating set of WAL segment files
/// named `<prefix>wal-NNNNNN`.
pub(super) struct GroupCommit {
    /// Filename prefix shared by every segment (`{prefix}wal-{seg:06}`).
    prefix: String,
    /// Byte threshold: once the active segment's appended bytes reach this, the
    /// next batch rotates to a fresh segment.
    seg_threshold: u64,
    inner: Mutex<Inner>,
    /// Count of batch `fsync`s performed (one per group commit). Introspection:
    /// fewer than the number of writes proves coalescing happened.
    batch_syncs: AtomicU64,
    /// Count of WAL segment rotations (the active segment crossed its byte budget
    /// and a fresh segment file was opened). Monotonic for the engine's life;
    /// observability (ADR 0015) reads its delta. Lock-free so the leader can bump
    /// it under the `Inner` lock without extra contention on read-back.
    rotations: AtomicU64,
}

struct Inner {
    /// Next WAL sequence number to hand out. Strictly increasing; assigned to a
    /// writer's record under the lock in call order. **Monotonic for the life of
    /// the engine** (segment rotation/GC never resets it).
    next_seq: u64,
    /// Highest sequence number made durable (an enclosing `sync` returned).
    durable_seq: u64,
    /// Records appended but not yet flushed, oldest first: `(seq, bytes)`.
    pending: Vec<(u64, Vec<u8>)>,
    /// Whether a leader is currently performing the batch `append` + `sync`.
    flushing: bool,
    /// Writers parked waiting for their sequence to become durable, keyed by the
    /// sequence they are waiting on (`BTreeMap` for deterministic iteration).
    waiters: BTreeMap<u64, Vec<Waker>>,
    /// Set when a leader's batch `append`/`sync` failed: every writer whose record
    /// was in that lost batch must surface the failure rather than claim durability.
    failed_through: u64,
    /// The leader's own `flush_batch` error text for the *first* failure
    /// (`failed_through`'s companion) — kept so every waiter riding that lost
    /// batch, not only the leader, can see what actually failed on disk instead
    /// of a generic "sync failed" message. Never overwritten by a later failure:
    /// the first failure is what every already-parked waiter is surfacing, and a
    /// second, unrelated failure on a later batch gets its own `failed_through`
    /// bump but does not need its own text (the generic prefix already names the
    /// batch as failed; the point of this field is the *first* underlying cause).
    failed_error: Option<String>,
    /// Whether the **latest** failed batch was out-of-space (ENOSPC/EDQUOT):
    /// surfaced to its waiters as [`StorageError::StorageFull`] (recoverable,
    /// retryable) rather than [`StorageError::Backend`].
    failed_full: bool,
    /// After an **ENOSPC** batch failure: `(segment, length)` the active
    /// segment must be cut back to before the next batch rides it. A failed
    /// `append` can leave a short write (a torn frame) at the file's tail; a
    /// later, acked batch appended behind that garbage would make recovery
    /// either refuse the file or drop the acked record. `length` is the
    /// segment's byte count before the failed batch (every byte of which was
    /// already durable). Consumed by the next leader, which repairs *first*
    /// (and re-arms this if the repair itself fails). Never set by a
    /// non-ENOSPC failure, whose handling is unchanged.
    repair_to: Option<(u64, u64)>,
    /// The segment number currently being appended to.
    active_seg: u64,
    /// Bytes in the active segment so far (drives rotation): its post-repair
    /// on-disk length at open (see [`GroupCommit::new`]) plus every byte the
    /// leader has since handed to `append`, so it advances as batches are
    /// written, not only once they sync. Resets to 0 on rotation.
    active_seg_bytes: u64,
    /// Sealed (no-longer-active) segments: segment number → the highest `wal_seq`
    /// that segment contains. `BTreeMap` for deterministic iteration. The active
    /// segment is never in this map (its max seq is `durable_seq`).
    sealed: BTreeMap<u64, u64>,
    /// Whether the active segment's **next** batch must be prefixed with the
    /// WAL file-level header (ADR 0073 Phase 0) before its records. See the
    /// module docs' "File-level format header" section for the full
    /// crash-safety argument and exactly when this flips.
    active_seg_needs_header: bool,
    /// Whether the active segment is a v2 file (markers allowed). `false` only
    /// for a recovered v1 segment, until it rotates.
    markers_enabled: bool,
    /// The previous batch's append+`sync` on the active segment both returned
    /// `Ok` and nothing has been appended since, so every byte now in the file
    /// is durable and the next batch may open with a marker claiming that.
    /// Cleared the moment a leader claims a batch; re-set only on success.
    marker_ready: bool,
}

/// Encode a sync-marker frame claiming "every byte before file offset `offset`
/// is fsynced", where `offset` is this frame's own start.
pub(super) fn encode_wal_marker(offset: u64) -> Vec<u8> {
    let mut payload = Vec::with_capacity(9);
    payload.push(WAL_MARKER_TAG);
    payload.extend_from_slice(&offset.to_be_bytes());
    let crc = crc32fast::hash(&payload);
    let mut out = Vec::with_capacity(WAL_MARKER_FRAME_LEN);
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(&crc.to_be_bytes());
    out.extend_from_slice(&payload);
    out
}

impl GroupCommit {
    /// A fresh coordinator writing segments under `prefix`, with `live_segments`
    /// being the segments the recovered manifest names (in ascending order),
    /// `seg_threshold` the per-segment byte budget, and `active_seg_len` the
    /// **post-recovery-repair** byte length of the active (highest-numbered)
    /// segment on disk — `0` for a brand-new engine with no segments at all.
    ///
    /// The sequence space resumes after recovery: recovered records already live
    /// in the memtable, so the next durable record is the first *new* write. The
    /// highest live segment is reopened as the active segment (further appends ride
    /// it until it crosses `seg_threshold`); the rest are sealed. `live_segments`
    /// empty means a fresh engine — the first write opens segment 0.
    ///
    /// `active_seg_len` is the reopened active segment's byte length on disk
    /// **after** recovery's torn-tail/torn-header repair (0 for a fresh
    /// engine). It drives two things:
    ///
    /// - **The rotation counter.** It seeds `active_seg_bytes`, so a segment
    ///   that already holds bytes rotates at `seg_threshold` in total, not
    ///   `seg_threshold` more bytes after each reopen.
    /// - **The file header.** `0` means the active segment has no durable bytes
    ///   at all yet (a brand-new engine, or a recovered segment whose header was
    ///   itself torn and got repaired back to empty — see `LsmEngine::
    ///   open_with_metrics`), so its first batch here must carry a fresh
    ///   header; any nonzero length can only exist because a prior
    ///   header-carrying `append` already landed on this exact file (every
    ///   segment ever discovered by recovery got that way through this same
    ///   coordinator's own writes), so the header must not be written again.
    ///
    /// `active_seg_is_v2` says whether the recovered (non-empty) active segment
    /// carries a v2 header; a v1 one keeps being appended to without sync
    /// markers. Ignored when `active_seg_len == 0` (a fresh header is v2).
    pub(super) fn new(
        prefix: String,
        live_segments: &[u64],
        seg_threshold: u64,
        active_seg_len: u64,
        active_seg_is_v2: bool,
    ) -> Self {
        // All recovered records are folded into the memtable already, so the
        // resumed sequence space starts at 0; the active segment is the highest
        // live one (or 0 for a fresh engine). Older live segments are sealed with
        // max_seq 0 — they hold only recovered (pre-resume) records, so no *new*
        // record can ever be "covered" by their seq, and they survive until a
        // flush captures the whole memtable and GCs them by membership.
        let active_seg = live_segments.last().copied().unwrap_or(0);
        let mut sealed = BTreeMap::new();
        for &seg in live_segments {
            if seg != active_seg {
                sealed.insert(seg, 0);
            }
        }
        Self {
            prefix,
            seg_threshold: seg_threshold.max(1),
            batch_syncs: AtomicU64::new(0),
            rotations: AtomicU64::new(0),
            inner: Mutex::new(Inner {
                next_seq: 0,
                durable_seq: 0,
                pending: Vec::new(),
                flushing: false,
                waiters: BTreeMap::new(),
                failed_through: 0,
                failed_error: None,
                failed_full: false,
                repair_to: None,
                active_seg,
                active_seg_bytes: if live_segments.is_empty() {
                    0
                } else {
                    active_seg_len
                },
                sealed,
                active_seg_needs_header: active_seg_len == 0,
                markers_enabled: active_seg_len == 0 || active_seg_is_v2,
                // Nothing is known synced at open (also after a tail repair).
                marker_ready: false,
            }),
        }
    }

    /// The on-disk file name for segment `seg`.
    pub(super) fn segment_file(&self, seg: u64) -> String {
        format!("{}wal-{seg:06}", self.prefix)
    }

    /// The highest WAL sequence currently durable. A flush samples this when it
    /// snapshots the memtable: it is the watermark up to which every WAL record is
    /// reflected in the flushed SSTable.
    pub(super) fn durable_seq(&self) -> u64 {
        self.lock().durable_seq
    }

    /// The live segment set (ascending): every sealed segment plus the active one.
    /// Recorded in the durable manifest so recovery replays exactly these files.
    pub(super) fn live_segments(&self) -> Vec<u64> {
        let inner = self.lock();
        let mut segs: Vec<u64> = inner.sealed.keys().copied().collect();
        segs.push(inner.active_seg);
        segs
    }

    /// Given a flush watermark (`durable_seq` at memtable-snapshot time), return
    /// the **sealed** segments fully covered by the flush — every record they hold
    /// has `wal_seq <= watermark`, so it is now in the SSTable and the segment file
    /// can be removed. The active segment is never returned (it may carry records
    /// beyond the watermark, and is where new writes land). The caller removes the
    /// files **after** a manifest no longer naming them is durable, then calls
    /// [`forget_segments`](Self::forget_segments).
    pub(super) fn segments_covered_by(&self, watermark: u64) -> Vec<u64> {
        let inner = self.lock();
        inner
            .sealed
            .iter()
            .filter(|&(_, &max_seq)| max_seq <= watermark)
            .map(|(&seg, _)| seg)
            .collect()
    }

    /// Drop the given sealed segments from the live set, after their files have
    /// been removed and the new manifest is durable. Idempotent; only ever removes
    /// sealed segments (never the active one).
    pub(super) fn forget_segments(&self, segs: &[u64]) {
        let mut inner = self.lock();
        for seg in segs {
            inner.sealed.remove(seg);
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().expect("wal group-commit poisoned")
    }

    /// Durably append `bytes` (one encoded WAL record) as part of a shared group
    /// commit, returning only once a `sync` covering this record has completed.
    ///
    /// Multiple tasks calling this concurrently coalesce: their records ride one
    /// `append` and one `sync`. The caller must not mutate the memtable until this
    /// resolves (durability precedes visibility).
    ///
    /// # Errors
    /// Returns [`StorageError::Backend`] if the underlying `append`/`sync` failed
    /// for the batch carrying this record (nothing past the prior durable point
    /// became durable).
    pub(super) async fn commit<E: Env>(&self, env: &E, bytes: Vec<u8>) -> Result<()> {
        // Enqueue under the lock, taking a strictly increasing sequence number.
        let my_seq = {
            let mut inner = self.lock();
            inner.next_seq += 1;
            let seq = inner.next_seq;
            inner.pending.push((seq, bytes));
            seq
        };

        // Yield once so any writer already ready in this scheduler drain cycle can
        // enqueue into the same batch before we (potentially) become the leader.
        // Under SimEnv this is a single cooperative turn; under ProdEnv it lets a
        // sibling task on the runtime make progress. Cheap and deterministic.
        YieldOnce::default().await;

        loop {
            let action = {
                let mut inner = self.lock();
                if inner.failed_through >= my_seq {
                    Action::Failed
                } else if inner.durable_seq >= my_seq {
                    Action::Done
                } else if inner.flushing {
                    Action::Wait
                } else {
                    // Become the leader: claim the whole pending buffer.
                    inner.flushing = true;
                    let mut batch = Vec::new();
                    let mut up_to = inner.durable_seq;
                    for (seq, rec) in inner.pending.drain(..) {
                        batch.extend_from_slice(&rec);
                        up_to = up_to.max(seq);
                    }
                    // A pending ENOSPC tail repair (see `Inner::repair_to`) runs
                    // before this batch: re-baseline the segment's accounting to
                    // the known-durable length, and do NOT rotate in this lead
                    // (sealing a segment that still carries a torn tail, or an
                    // empty one that would leave a gap in the numbering, is
                    // exactly what the repair avoids).
                    let repair = inner.repair_to.take();
                    if let Some((rseg, rlen)) = repair {
                        inner.active_seg = rseg;
                        inner.active_seg_bytes = rlen;
                        // A segment cut back to nothing needs its header again
                        // (the failed batch may have been the one carrying it).
                        inner.active_seg_needs_header = rlen == 0;
                    }
                    // Decide the target segment: rotate to a fresh one if the
                    // active segment is over threshold. Rotation only happens
                    // between batches (no flush is in progress here), so every
                    // record already in the active segment is durable: seal it at
                    // the current durable seq.
                    if repair.is_none() && inner.active_seg_bytes >= self.seg_threshold {
                        let sealed_seg = inner.active_seg;
                        let sealed_max = inner.durable_seq;
                        inner.sealed.insert(sealed_seg, sealed_max);
                        inner.active_seg += 1;
                        inner.active_seg_bytes = 0;
                        // A freshly rotated-to segment is a brand-new file that
                        // this coordinator has never written a byte to: its
                        // first batch must carry the file header (see the
                        // module docs' "File-level format header" section).
                        inner.active_seg_needs_header = true;
                        inner.markers_enabled = true;
                        inner.marker_ready = false;
                        // Observability (ADR 0015): a rotation actually happened.
                        self.rotations.fetch_add(1, Ordering::Relaxed);
                    }
                    let seg = inner.active_seg;
                    let needs_header = inner.active_seg_needs_header;
                    // The segment's known-durable length before this batch: the
                    // length an ENOSPC failure of this batch must cut it back to.
                    let len_before = inner.active_seg_bytes;
                    // Claim the marker flag: cleared now, re-set only if this
                    // batch's append + sync both succeed (see module docs).
                    let with_marker = inner.marker_ready && inner.markers_enabled && !needs_header;
                    inner.marker_ready = false;
                    // Account the bytes now so the *next* batch's rotation decision
                    // sees this batch's contribution — including the header's own
                    // bytes when this batch will carry one, since those bytes are
                    // genuinely appended to the segment file too.
                    inner.active_seg_bytes += batch.len() as u64
                        + if needs_header {
                            WAL_HEADER_LEN as u64
                        } else {
                            0
                        }
                        + if with_marker {
                            WAL_MARKER_FRAME_LEN as u64
                        } else {
                            0
                        };
                    Action::Lead {
                        batch,
                        up_to,
                        seg,
                        needs_header,
                        with_marker,
                        len_before,
                        repair,
                    }
                }
            };

            match action {
                Action::Done => return Ok(()),
                Action::Failed => {
                    let leader_err = {
                        let inner = self.lock();
                        inner
                            .failed_error
                            .clone()
                            .unwrap_or_else(|| "unknown error".to_string())
                    };
                    let full = self.lock().failed_full;
                    let msg = format!("wal group-commit sync failed: {leader_err}");
                    return Err(if full {
                        StorageError::StorageFull(msg)
                    } else {
                        StorageError::Backend(msg)
                    });
                }
                Action::Wait => {
                    DurableUpTo {
                        gc: self,
                        seq: my_seq,
                    }
                    .await;
                }
                Action::Lead {
                    batch,
                    up_to,
                    seg,
                    needs_header,
                    with_marker,
                    len_before,
                    repair,
                } => {
                    // Perform the single batched append + sync, lock-free, to the
                    // chosen segment file — after cutting back any torn tail a
                    // previous ENOSPC failure may have left on it.
                    let batch_len = batch.len();
                    let repaired = match repair {
                        Some((rseg, rlen)) => self.repair_tail(env, rseg, rlen).await,
                        None => Ok(()),
                    };
                    let repair_failed = repaired.is_err();
                    let res = match repaired {
                        Ok(()) => {
                            self.flush_batch(env, seg, needs_header, with_marker, &batch)
                                .await
                        }
                        Err(e) => Err(e),
                    };
                    let woken = {
                        let mut inner = self.lock();
                        inner.flushing = false;
                        match &res {
                            Ok(()) => {
                                inner.durable_seq = inner.durable_seq.max(up_to);
                                // Everything now in the segment is synced: the
                                // next batch may open with a marker.
                                inner.marker_ready = inner.markers_enabled;
                            }
                            // The append/sync failed: nothing past the prior durable
                            // point is durable, and the claimed records are gone from
                            // `pending`. Mark the whole lost batch failed so every
                            // writer it carried surfaces the error rather than waiting
                            // forever or falsely claiming durability. Keep the
                            // *first* failure's text (see `failed_error`'s own doc) and
                            // log it once here, at the leader, with the detail a waiter
                            // has no way to reconstruct on its own.
                            Err(e) => {
                                inner.failed_through = inner.failed_through.max(up_to);
                                inner.failed_full = e.is_storage_full();
                                if e.is_storage_full() || repair_failed {
                                    // The tail is unknown (a short write, or
                                    // bytes a failed fsync may drop): cut the
                                    // segment back to its pre-batch length
                                    // before anything else rides it, and
                                    // re-baseline the byte accounting the
                                    // claim already advanced past the lost
                                    // batch. (A failed *repair* keeps its own
                                    // target — `len_before` is that same
                                    // length then, see the claim block.)
                                    inner.repair_to = Some((seg, len_before));
                                    inner.active_seg_bytes = len_before;
                                }
                                if inner.failed_error.is_none() {
                                    inner.failed_error = Some(e.to_string());
                                }
                                tracing::error!(
                                    segment = seg,
                                    batch_bytes = batch_len,
                                    up_to,
                                    error = %e,
                                    "wal group-commit sync failed"
                                );
                            }
                        }
                        // Wake **all** parked writers, not only the ones this batch
                        // made durable: a writer whose record arrived *after* we
                        // claimed the batch is now durable-or-not but still parked,
                        // and one of them must re-poll to lead the next batch. They
                        // re-register if they still must wait, so this cannot lose a
                        // wakeup (the alternative — waking only `<= durable_seq` —
                        // would strand a later record with no leader: a deadlock).
                        inner.take_all_wakers()
                    };
                    for w in woken {
                        w.wake();
                    }
                    // Loop: re-evaluate (we now observe Done / Failed, or, if our
                    // record was not in this batch, re-lead or wait).
                }
            }
        }
    }

    /// Cut segment `seg` back to `len` bytes (its last known-durable length) if
    /// a failed ENOSPC batch left anything longer — see [`Inner::repair_to`].
    /// `replace` is the atomic temp-file-and-rename primitive, so a failure here
    /// leaves the file exactly as it was and the repair simply re-arms.
    async fn repair_tail<E: Env>(&self, env: &E, seg: u64, len: u64) -> Result<()> {
        let file = self.segment_file(seg);
        let bytes = env
            .read(&file)
            .await
            .map_err(|e| StorageError::from_io(&e))?;
        if bytes.len() as u64 > len {
            env.replace(&file, &bytes[..len as usize])
                .await
                .map_err(|e| StorageError::from_io(&e))?;
        }
        Ok(())
    }

    /// Append the whole batch — prefixed with the WAL file-level header when
    /// `needs_header` (this segment's first-ever batch) — to segment `seg`'s
    /// file, then `sync` it once. The lock is **not** held across the I/O;
    /// it is retaken briefly, only to clear `active_seg_needs_header`, and
    /// only once the `append` that carried the header has itself returned
    /// `Ok` — see the module docs' "File-level format header" section for
    /// exactly why that timing (neither earlier nor later) is load-bearing.
    async fn flush_batch<E: Env>(
        &self,
        env: &E,
        seg: u64,
        needs_header: bool,
        with_marker: bool,
        batch: &[u8],
    ) -> Result<()> {
        let file = self.segment_file(seg);
        if needs_header {
            // Header first, and **synced on its own** before any record is
            // appended (see the module docs' "File-level format header"
            // section): a file longer than the header therefore always has a
            // durable header, so a bad header on such a file is real
            // corruption, never a crash-torn write.
            env.append(&file, &encode_wal_header())
                .await
                .map_err(|e| StorageError::from_io(&e))?;
            self.lock().active_seg_needs_header = false;
            env.sync(&file)
                .await
                .map_err(|e| StorageError::from_io(&e))?;
        }
        if with_marker {
            // The marker's offset is where it lands: the file's live length
            // (buffered bytes included; the leader is the only appender). If
            // the size is unreadable, skip the marker — it only shrinks the
            // provable region.
            match env.size(&file).await {
                Ok(len) => {
                    let mut buf = encode_wal_marker(len);
                    buf.extend_from_slice(batch);
                    env.append(&file, &buf)
                        .await
                        .map_err(|e| StorageError::from_io(&e))?;
                }
                Err(_) => {
                    if !batch.is_empty() {
                        env.append(&file, batch)
                            .await
                            .map_err(|e| StorageError::from_io(&e))?;
                    }
                }
            }
        } else if !batch.is_empty() {
            env.append(&file, batch)
                .await
                .map_err(|e| StorageError::from_io(&e))?;
        }
        env.sync(&file)
            .await
            .map_err(|e| StorageError::from_io(&e))?;
        self.batch_syncs.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Number of batch `fsync`s performed since open (introspection / tests).
    pub(super) fn batch_sync_count(&self) -> u64 {
        self.batch_syncs.load(Ordering::Relaxed)
    }

    /// Number of WAL segment rotations performed since open (monotonic).
    /// Observability (ADR 0015) records the delta after each `commit`.
    pub(super) fn rotation_count(&self) -> u64 {
        self.rotations.load(Ordering::Relaxed)
    }

    /// Number of live WAL segments (sealed + active). Introspection / tests.
    pub(super) fn segment_count(&self) -> usize {
        let inner = self.lock();
        inner.sealed.len() + 1
    }
}

/// What a `commit` poll iteration should do, decided under the lock.
enum Action {
    /// Our record is already durable.
    Done,
    /// Our record's batch failed to sync; surface the error.
    Failed,
    /// Lead the flush of this claimed `batch` to segment `seg`, which makes records
    /// `<= up_to` durable on success. `needs_header` says whether this is
    /// segment `seg`'s first-ever batch, so `flush_batch` must prepend the
    /// WAL file-level header.
    Lead {
        batch: Vec<u8>,
        up_to: u64,
        seg: u64,
        needs_header: bool,
        /// Prepend a sync marker to the batch's append (see module docs).
        with_marker: bool,
        /// Segment `seg`'s known-durable byte length before this batch — what an
        /// ENOSPC failure of the batch cuts it back to (`Inner::repair_to`).
        len_before: u64,
        /// A pending tail repair from an earlier ENOSPC failure, performed
        /// before this batch's append.
        repair: Option<(u64, u64)>,
    },
    /// Another writer is leading; park until our sequence is durable.
    Wait,
}

impl Inner {
    /// Remove and return every parked waker. Called after a batch flush so each
    /// waiter re-polls: durable ones complete, the rest re-park or one leads the
    /// next batch. Draining all of them is what prevents a stranded record (a
    /// record enqueued after the leader claimed its batch) from deadlocking.
    fn take_all_wakers(&mut self) -> Vec<Waker> {
        std::mem::take(&mut self.waiters)
            .into_values()
            .flatten()
            .collect()
    }
}

/// Parks a non-leading writer **only while a flush is actually in progress**,
/// then resolves so the `commit` loop re-decides its action. Registers its waker
/// under the lock so the leader's post-`sync` wake re-readies it.
///
/// Resolving as soon as `!flushing` (rather than only on `durable_seq >= seq`) is
/// load-bearing for liveness under real multithreading: a writer whose record was
/// enqueued *after* the current leader claimed its batch is not covered by that
/// flush, so once the leader finishes this must return to the loop and become the
/// **next** leader for its own record. Parking until `durable_seq >= seq` here
/// stranded it — nothing else would ever flush its record — which deadlocked the
/// multi-threaded `ProdEnv` path (the single-threaded `SimEnv` cannot produce that
/// interleaving). The `commit` loop re-checks `durable`/`failed`/`flushing` under
/// the lock, so an early resolve just causes a re-decision (it re-parks if a flush
/// is still running, or leads otherwise).
struct DurableUpTo<'a> {
    gc: &'a GroupCommit,
    seq: u64,
}

impl Future for DurableUpTo<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut inner = self.gc.lock();
        if inner.durable_seq >= self.seq || inner.failed_through >= self.seq || !inner.flushing {
            Poll::Ready(())
        } else {
            inner
                .waiters
                .entry(self.seq)
                .or_default()
                .push(cx.waker().clone());
            Poll::Pending
        }
    }
}

/// Yields control exactly once: the first poll re-readies the task and returns
/// `Pending`; the second poll returns `Ready`. Lets a sibling task that is already
/// ready run before we proceed (the group-commit accumulation window).
#[derive(Default)]
struct YieldOnce {
    yielded: bool,
}

impl Future for YieldOnce {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.yielded {
            Poll::Ready(())
        } else {
            self.yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}
