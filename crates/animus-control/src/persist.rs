//! Raft durable state (ADR 0009 follow-up).
//!
//! [`RaftCore`](crate::raft::RaftCore) is pure and does no I/O; instead it emits
//! [`WalRecord`]s describing changes to its durable state, which the node driver
//! appends to a write-ahead log on the `Env` disk and `fsync`s **before** acting
//! on them (granting a vote, acknowledging an append). On startup the driver
//! replays the log into a [`PersistedState`] and recovers the core.
//!
//! The state machine is snapshotted as a full [`Metadata`] image at a committed
//! `(last_index, last_term)`; the log keeps only entries *after* that index.
//! Recovery restores the snapshot, then re-applies the log tail as the leader
//! re-advances commit — so a committed command is applied exactly once relative
//! to the snapshot base (no double-applied compare-and-swap), while the log
//! prefix the snapshot covers is discarded.
//!
//! **Versioned WAL format (ADR 0073 Phase 0 workstream B).** Every line
//! [`PersistedState::encode_record`]/[`PersistedState::decode`] handle is
//! tagged with [`CONTROL_WAL`] (magic `CWL1`, currently version 1) via
//! [`crate::format::encode_line`]/[`crate::format::decode_lines`] — see
//! that module's doc for the framing shape and error semantics in full,
//! and [`PersistedState::decode`]'s own doc below for how this WAL
//! specifically distinguishes a torn tail (tolerated, `Ok` with a shorter
//! prefix) from real corruption or an unrecognized/future tag (loud,
//! `Err`, never silently misdecoded). **`animus-cp-data`'s own per-group
//! WAL inherits `CONTROL_WAL` for free**: its non-`SharedWal` persist path
//! (`PersistedState::encode_record`'s fallback branch, when
//! `--no-shared-wal` is set) is the *same* generic `PersistedState<C, S>`
//! defined here, just instantiated with `C = KvCommand`/`S = KvState`
//! instead of `MetaCommand`/`Metadata` — there is no second WAL-line codec
//! to keep in sync. This is the last permitted incompatible reset of this
//! format (`docs/adr/0073-upgrade-compatibility.md`): once the Phase 0
//! baseline lands, a future change to this shape is a new version, a
//! decoder that still accepts every older post-baseline version, and a new
//! golden fixture — never an edit to an existing one.
//!
//! **The tagged/multiplexed `SharedWal` envelope (`encode_tagged_record`/
//! `decode_tagged`, below) is [`SHARED_WAL_TAG`]** (magic `SWL1`, ADR 0073 Phase
//! 0 workstream C): the same [`crate::format::encode_line`]/
//! [`crate::format::decode_lines`] line shape as [`CONTROL_WAL`], wrapping a
//! `{"tablet":..,"record":..}` JSON payload whose inner `record` is the very
//! same [`WalRecord`] `serde_json` shape `CWL1` carries (not a separate
//! codec). `shared_wal.rs` lives in this crate, hence this conversion too.
//! The pre-baseline untagged `<crc32>:<json>` line is refused by name
//! (`PreBaselineFormat`), never silently misread.

use std::collections::{BTreeMap, BTreeSet};

use animus_env::{Env, NodeId};
/// Version dispatch for the three control-plane formats (ADR 0073 Phase 1,
/// workstream P1-C): `format::unwrap`/`decode_lines` hand back the version
/// byte and each body decoder here `match`es on it, so a future v2 adds an
/// arm (and moves the v1 arm's body into a frozen `legacy::v1`, with a
/// `From` translation into the current type) instead of editing one shared
/// decoder in place. Every format is still v1, whose shape *is* the current
/// in-memory type, so the v1 arm decodes directly and there is no `legacy`
/// module yet. An unknown version is [`format::unsupported_version`], never
/// a panic.
pub(crate) mod dispatch {
    use super::{CONTROL_SNAPSHOT, CONTROL_WAL, SHARED_WAL_TAG};
    use crate::format::{self, FormatError};
    use serde::de::DeserializeOwned;

    fn malformed(name: &'static str, e: &serde_json::Error) -> FormatError {
        FormatError::Malformed {
            format: name,
            detail: e.to_string(),
        }
    }

    /// Frozen decoders for retired versions (ADR 0073 Phase 1). Never edited
    /// to change behaviour.
    pub(crate) mod legacy {
        /// `CWL1` version 1: the record payload shape is the same
        /// `serde_json` `WalRecord` as v2 (v2 only added sync-marker lines,
        /// which the line decoder consumes before dispatch), and a v1 file
        /// has no markers, so the line decoder keeps its lenient
        /// torn-tail-anywhere behaviour for it.
        pub(crate) mod v1 {
            use crate::format::FormatError;
            use crate::persist::CONTROL_WAL;
            use serde::de::DeserializeOwned;

            pub(crate) fn wal_record<T: DeserializeOwned>(
                payload: &[u8],
            ) -> Result<T, FormatError> {
                serde_json::from_slice(payload)
                    .map_err(|e| super::super::malformed(CONTROL_WAL.name, &e))
            }

            /// Test-only legacy encoder (ADR 0073 checklist step 7): the
            /// exact bytes a v1 writer produced for one record, anchored to
            /// `tests/fixtures/formats/control-wal/v1.bin` by a byte-equality
            /// test in `persist::tests`.
            #[cfg(test)]
            pub(crate) fn encode_record<C, S>(record: &crate::persist::WalRecord<C, S>) -> Vec<u8>
            where
                C: serde::Serialize,
                S: serde::Serialize,
            {
                use crate::format::{self, FormatTag};
                const V1: FormatTag = FormatTag {
                    magic: *b"CWL1",
                    version: 1,
                    name: "control-wal",
                };
                format::encode_line(&V1, &serde_json::to_vec(record).expect("serializes"))
            }
        }
    }

    /// Body of one [`CONTROL_WAL`] line (a `WalRecord<C, S>`).
    pub(crate) fn wal_record<T: DeserializeOwned>(
        version: u8,
        payload: &[u8],
    ) -> Result<T, FormatError> {
        match version {
            1 => legacy::v1::wal_record(payload),
            2 => serde_json::from_slice(payload).map_err(|e| malformed(CONTROL_WAL.name, &e)),
            found => Err(format::unsupported_version(&CONTROL_WAL, found)),
        }
    }

    /// Frozen `SWL1` version-1 decoder (the `SharedWal` sibling of `legacy`): same
    /// `{tablet, record}` payload shape as v2; a v1 file has no sync markers.
    pub(crate) mod legacy_shared {
        pub(crate) mod v1 {
            use crate::format::FormatError;
            use crate::persist::SHARED_WAL_TAG;
            use serde::de::DeserializeOwned;

            pub(crate) fn shared_wal_line<T: DeserializeOwned>(
                payload: &[u8],
            ) -> Result<T, FormatError> {
                serde_json::from_slice(payload)
                    .map_err(|e| super::super::malformed(SHARED_WAL_TAG.name, &e))
            }

            /// Test-only legacy encoder (ADR 0073 checklist step 7), anchored
            /// to `tests/fixtures/formats/shared-wal/v1.bin` in `persist::tests`.
            #[cfg(test)]
            pub(crate) fn encode_tagged_record<C, S>(
                tablet: animus_tablet::TabletId,
                record: &crate::persist::WalRecord<C, S>,
            ) -> Vec<u8>
            where
                C: serde::Serialize,
                S: serde::Serialize,
            {
                use crate::format::{self, FormatTag};
                #[derive(serde::Serialize)]
                struct Line<'a, C, S> {
                    tablet: animus_tablet::TabletId,
                    record: &'a crate::persist::WalRecord<C, S>,
                }
                const V1: FormatTag = FormatTag {
                    magic: *b"SWL1",
                    version: 1,
                    name: "shared-wal",
                };
                format::encode_line(
                    &V1,
                    &serde_json::to_vec(&Line { tablet, record }).expect("serializes"),
                )
            }
        }
    }

    /// Body of one [`SHARED_WAL_TAG`] line (`{tablet, record}`).
    pub(crate) fn shared_wal_line<T: DeserializeOwned>(
        version: u8,
        payload: &[u8],
    ) -> Result<T, FormatError> {
        match version {
            1 => legacy_shared::v1::shared_wal_line(payload),
            2 => serde_json::from_slice(payload).map_err(|e| malformed(SHARED_WAL_TAG.name, &e)),
            found => Err(format::unsupported_version(&SHARED_WAL_TAG, found)),
        }
    }

    /// Body of a [`CONTROL_SNAPSHOT`] envelope (the control state `S`, or the
    /// system-keyspace image entries).
    pub(crate) fn snapshot_body<T: DeserializeOwned>(
        version: u8,
        payload: &[u8],
    ) -> Result<T, FormatError> {
        match version {
            1 => serde_json::from_slice(payload).map_err(|e| malformed(CONTROL_SNAPSHOT.name, &e)),
            found => Err(format::unsupported_version(&CONTROL_SNAPSHOT, found)),
        }
    }
}

#[cfg(test)]
use animus_env::nid;
use animus_tablet::TabletId;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::format::{self, FormatError, FormatTag};
use crate::meta::{MetaCommand, Metadata};
use crate::raft::LogEntry;

/// The control-plane Raft WAL's line envelope (ADR 0073 Phase 0 workstream
/// B): magic `CWL1`, currently version 1. See this module's own doc for how
/// broadly this tag applies (both planes' non-`SharedWal` WAL) and
/// `crate::format`'s doc for the line shape/error semantics.
pub const CONTROL_WAL: FormatTag = FormatTag {
    magic: *b"CWL1",
    version: 2,
    name: "control-wal",
};

/// The control-plane snapshot / `InstallSnapshot` payload envelope (ADR 0073
/// Phase 0 workstream B): magic `CSN1`, currently version 1 — [`format::wrap`]/
/// [`format::unwrap`]'s binary shape (this payload has no line framing of its
/// own to protect). Covers **both** shapes this crate's `RaftCore<C, S>`
/// puts inside an `InstallSnapshot` chunk stream:
///
/// - The real, `DRIVER_APPLIED` control plane's transfer payload — the
///   system-keyspace image `crate::node`'s `syskv_image`/`install_syskv_image`
///   build/consume (a `serde_json`-encoded `Vec<(key, value-or-tombstone,
///   version)>`), never a serialized [`Metadata`] blob (see `crate::node`'s
///   own doc for why: `Metadata` is `DRIVER_APPLIED`, so `RaftCore::metadata`
///   is a meaningless placeholder and the real image is built lazily from the
///   engine).
/// - [`RaftCore`](crate::raft::RaftCore)'s own generic `!S::DRIVER_APPLIED`
///   fallback (`raft.rs`'s `snapshot_upto`/`recovered`/`handle_install_snapshot`),
///   which wraps `serde_json::to_vec(&self.metadata)`/`serde_json::from_slice::<S>`
///   directly — exercised in this workspace only by the toy test state
///   machine (`generic_state_machine.rs`), since every real `S` in this
///   codebase is `DRIVER_APPLIED`.
///
/// One shared tag for both, since both are, physically, "the bytes an
/// `InstallSnapshot` transfer carries for this plane" — not two independent
/// formats that happen to look similar. **`animus-cp-data`'s own `KvState`
/// snapshot image is a wholly separate binary codec** (`codec::encode_image`,
/// workstream C) and does not use this tag at all.
pub const CONTROL_SNAPSHOT: FormatTag = FormatTag {
    magic: *b"CSN1",
    version: 1,
    name: "control-snapshot",
};

/// The flat `(tablet, record)` sequence [`PersistedState::decode_tagged`]
/// returns, in file order.
pub type TaggedRecords<C, S> = Vec<(TabletId, WalRecord<C, S>)>;

/// The `SharedWal` outer line envelope (ADR 0073 Phase 0 workstream C):
/// magic `SWL1`, currently version 1. One [`crate::format::encode_line`] line
/// per record, payload `{"tablet":<id>,"record":<WalRecord>}` as
/// `serde_json`. See this module's own doc.
pub const SHARED_WAL_TAG: FormatTag = FormatTag {
    magic: *b"SWL1",
    version: 2,
    name: "shared-wal",
};

/// Why [`PersistedState::recover`] refused a WAL file.
#[derive(Debug)]
pub enum WalRecoverError {
    /// The file failed to decode (named, typed: see [`FormatError`]).
    Format(FormatError),
    /// The file decoded, but cutting its torn tail back could not be made
    /// durable.
    Repair(std::io::Error),
}

impl std::fmt::Display for WalRecoverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WalRecoverError::Format(e) => write!(f, "{e}"),
            WalRecoverError::Repair(e) => write!(f, "tail repair failed: {e}"),
        }
    }
}

impl std::error::Error for WalRecoverError {}

/// Cut a torn/corrupt tail off `file` (see [`format::repaired_image`]) so the
/// next append starts on a clean line. A no-op for an already-clean file.
pub async fn repair_tail<E: Env>(
    env: &E,
    file: &str,
    bytes: &[u8],
    valid_len: usize,
) -> std::io::Result<()> {
    match format::repaired_image(bytes, valid_len) {
        Some(clean) => env.replace(file, &clean).await,
        None => Ok(()),
    }
}

/// Piggybacked sync-marker state for one WAL file (issue #1132, CWL1/SWL1 v2).
///
/// A sync marker `!sync:<N>` claims "every byte before offset `N` is fsynced"
/// (see [`crate::format::decode_lines_extent`]). It is **never** appended on its
/// own: a standalone marker after every fsync is a second `append` under the
/// WAL lock, which a slow disk charges a full extra latency per round (it
/// starved a slow learner's catch-up — see the lesson under
/// `docs/lessons/testing/`). Instead the writer remembers that its previous
/// round's `fsync` succeeded and **prepends the marker to the next round's own
/// single `append`**: the claim is still true when written (that fsync
/// completed before this append starts), and the marker's own durability is
/// the same as a standalone one's (it only becomes durable at the next sync).
/// The residual is unchanged and documented: the latest round has no durable
/// marker until the next round syncs.
///
/// Protocol, all under the file's writer lock (the state lives beside it so
/// the two cannot be separated):
/// 1. [`take_marker`](Self::take_marker) — returns the marker bytes to prepend
///    (empty unless the previous sync is known good) and **clears** the flag;
///    the offset `N` is the file's live length, i.e. exactly where the
///    prepended marker will start.
/// 2. `append(marker ++ records)`, then `sync`.
/// 3. [`mark_synced`](Self::mark_synced) only if both returned `Ok`.
///
/// Anything that rewrites the file (compaction's `replace`) calls
/// [`invalidate`](Self::invalidate); a failure anywhere leaves the flag clear
/// (step 1 cleared it), so a marker is never claimed over bytes whose sync is
/// unknown. Opening a file starts clear (also after a tail repair).
#[derive(Debug, Default)]
pub struct SyncMarkerState {
    synced: std::sync::atomic::AtomicBool,
}

impl SyncMarkerState {
    /// The marker line to prepend to this round's append, or empty. Clears
    /// the flag (see the type doc).
    pub async fn take_marker<E: Env>(&self, env: &E, tag: &FormatTag, file: &str) -> Vec<u8> {
        if !self.synced.swap(false, std::sync::atomic::Ordering::SeqCst) {
            return Vec::new();
        }
        match env.size(file).await {
            Ok(len) => format::encode_sync_marker(tag, len),
            Err(_) => Vec::new(),
        }
    }

    /// Record that the append + `fsync` after a [`take_marker`](Self::take_marker)
    /// both succeeded: everything now in the file is durable.
    pub fn mark_synced(&self) {
        self.synced.store(true, std::sync::atomic::Ordering::SeqCst);
    }

    /// Forget any pending marker (the file was rewritten or its sync state is
    /// unknown).
    pub fn invalidate(&self) {
        self.synced
            .store(false, std::sync::atomic::Ordering::SeqCst);
    }
}

/// One durable change, appended to the write-ahead log. Generic over the command
/// type `C` and snapshot-image type `S` (defaults: the control plane's
/// [`MetaCommand`] / [`Metadata`]), so the same WAL machinery serves any
/// `RaftCore<C, S>`. The generic is erased in the JSON form, so the on-disk
/// encoding for the control plane is unchanged.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WalRecord<C = MetaCommand, S = Metadata> {
    /// Persisted hard state: current term and vote (must be durable before the
    /// vote/term is acted on).
    Hard {
        term: u64,
        voted_for: Option<NodeId>,
    },
    /// A log entry was appended.
    Append(LogEntry<C>),
    /// The log was truncated to `keep` entries (conflict resolution).
    Truncate { keep: usize },
    /// A state-machine snapshot: the applied state covering all entries through
    /// `last_index` (whose term is `last_term`). The log keeps only entries
    /// after `last_index`.
    Snapshot {
        metadata: S,
        last_index: u64,
        last_term: u64,
        /// The Raft voter configuration effective at `last_index` (ADR 0017 C):
        /// membership lives in the log, so a snapshot that truncates the log must
        /// carry the config or it is lost. `None` (the default for older records /
        /// the never-reconfigured control plane) means "the node's initial set".
        #[serde(default)]
        config: Option<BTreeSet<NodeId>>,
        /// The **learner** configuration effective at `last_index` (ADR 0058
        /// Train 1), mirroring `config` above. `None` means "no learners".
        #[serde(default)]
        learners: Option<BTreeSet<NodeId>>,
    },
}

/// Durable Raft state reconstructed by replaying the write-ahead log. Generic over
/// the command / snapshot-image types (defaults: [`MetaCommand`] / [`Metadata`]).
#[derive(Clone, Debug)]
pub struct PersistedState<C = MetaCommand, S = Metadata> {
    /// Persisted current term.
    pub term: u64,
    /// Persisted vote for the current term.
    pub voted_for: Option<NodeId>,
    /// The reconstructed log (entries after the snapshot's `last_index`).
    pub log: Vec<LogEntry<C>>,
    /// The latest snapshot: `(state, last_index, last_term)`.
    pub snapshot: Option<(S, u64, u64)>,
    /// The voter configuration recorded by the latest snapshot, if any (ADR 0017
    /// C). `None` means the snapshot predates membership changes / there is none.
    pub snapshot_config: Option<BTreeSet<NodeId>>,
    /// The learner configuration recorded by the latest snapshot, if any (ADR
    /// 0058 Train 1). `None` means no learners (or the snapshot predates this
    /// field).
    pub snapshot_learners: Option<BTreeSet<NodeId>>,
}

// Manual `Default` (not derived): the derive would demand `C: Default` + `S:
// Default`, but an empty `PersistedState` needs neither (the log/snapshot default
// to empty/`None`), and `MetaCommand` is not `Default`.
impl<C, S> Default for PersistedState<C, S> {
    fn default() -> Self {
        Self {
            term: 0,
            voted_for: None,
            log: Vec::new(),
            snapshot: None,
            snapshot_config: None,
            snapshot_learners: None,
        }
    }
}

impl<C, S> PersistedState<C, S>
where
    C: Serialize + DeserializeOwned,
    S: Serialize + DeserializeOwned,
{
    /// Whether the log was empty (a never-before-run node).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.term == 0 && self.voted_for.is_none() && self.log.is_empty() && self.snapshot.is_none()
    }

    /// Reconstruct durable state by folding the WAL records in order.
    pub fn replay(records: impl IntoIterator<Item = WalRecord<C, S>>) -> Self {
        let mut state = Self::default();
        for record in records {
            match record {
                WalRecord::Hard { term, voted_for } => {
                    state.term = term;
                    state.voted_for = voted_for;
                }
                WalRecord::Append(entry) => state.log.push(entry),
                WalRecord::Truncate { keep } => state.log.truncate(keep),
                WalRecord::Snapshot {
                    metadata,
                    last_index,
                    last_term,
                    config,
                    learners,
                } => {
                    state.snapshot = Some((metadata, last_index, last_term));
                    state.snapshot_config = config;
                    state.snapshot_learners = learners;
                }
            }
        }
        state
    }

    /// Encode a single record as one [`CONTROL_WAL`]-tagged, checksummed
    /// line via [`crate::format::encode_line`] (ADR 0073 Phase 0 workstream
    /// B — see this module's own doc). See [`decode`](Self::decode)'s doc
    /// for why a framing failure is treated exactly like a torn tail, while
    /// an unrecognized/future tag or a malformed payload is a loud `Err`.
    #[must_use]
    pub fn encode_record(record: &WalRecord<C, S>) -> Vec<u8> {
        let payload = serde_json::to_vec(record).expect("wal record serializes");
        format::encode_line(&CONTROL_WAL, &payload)
    }

    /// Decode the WAL bytes back into records.
    ///
    /// - A **bad line** (a trailing partial line, a write torn by a crash,
    ///   or a **checksum mismatch**, issue #495) stops decoding at the
    ///   first such line and returns everything before it as `Ok` — never
    ///   applied, never a panic — **provided no later valid sync marker
    ///   proves the bad line was already durable**. A version-2 writer
    ///   records every successful `fsync` and prepends a `!sync:<offset>` marker
    ///   (piggybacked on the next round's append, see [`SyncMarkerState`]); a bad line that starts before the
    ///   greatest valid marker is [`FormatError::MidFileCorruption`], never
    ///   a silent truncation (issue #1132: the old "stop at the first bad
    ///   line, anywhere" rule silently dropped acked term/vote/log history
    ///   when an early line rotted). The exact algorithm and why a plain
    ///   "valid line after a bad one" scan is unsound here (the un-synced
    ///   tail holds several lines, so `corrupt_on_crash` legitimately
    ///   produces that shape) is [`crate::format::decode_lines_extent`]'s
    ///   doc. A version-1 file has no markers and keeps the lenient rule.
    /// - A CRC-valid line with no recognized [`CONTROL_WAL`] tag (a
    ///   pre-baseline WAL file, written before the ADR 0073 Phase 0 reset)
    ///   is `Err(FormatError::PreBaselineFormat)`.
    /// - A CRC-valid, correctly-tagged line whose version this build
    ///   doesn't know is `Err(FormatError::UnsupportedFormatVersion)`.
    /// - A CRC-valid, correctly-tagged, supported-version line whose JSON
    ///   payload doesn't decode is `Err(FormatError::Malformed)` — real
    ///   corruption inside an otherwise well-framed record, always loud,
    ///   never silently dropped like a torn tail.
    ///
    /// Nothing here ever panics on corrupt input.
    pub fn decode(bytes: &[u8]) -> Result<Vec<WalRecord<C, S>>, FormatError> {
        Self::decode_with_extent(bytes).map(|(records, _)| records)
    }

    /// [`decode`](Self::decode) plus the length of the file's clean prefix
    /// (see [`crate::format::DecodedLines::valid_len`]) — what a writer
    /// reopening the file needs to repair a torn tail before appending.
    pub fn decode_with_extent(bytes: &[u8]) -> Result<(Vec<WalRecord<C, S>>, usize), FormatError> {
        let decoded = format::decode_lines_extent(&CONTROL_WAL, bytes)?;
        let mut records = Vec::with_capacity(decoded.lines.len());
        for (version, payload) in decoded.lines {
            records.push(dispatch::wal_record(version, payload)?);
        }
        Ok((records, decoded.valid_len))
    }

    /// Recover a line-framed WAL file for a node about to append to it:
    /// read `file` (missing reads as empty), decode it, and **repair the
    /// tail on disk** (`Disk::replace`, atomic) so later appends never sit
    /// after torn bytes — otherwise the second recovery would see a bad line
    /// before durable markers and refuse the file. A decode failure or a
    /// failed repair is an `Err` the caller must treat as "halt, never
    /// recover as empty".
    pub async fn recover<E: Env>(
        env: &E,
        file: &str,
    ) -> Result<Vec<WalRecord<C, S>>, WalRecoverError> {
        let bytes = env.read(file).await.unwrap_or_default();
        let (records, valid_len) =
            Self::decode_with_extent(&bytes).map_err(WalRecoverError::Format)?;
        repair_tail(env, file, &bytes, valid_len)
            .await
            .map_err(WalRecoverError::Repair)?;
        Ok(records)
    }

    /// Encode one record tagged with the tablet it belongs to, for a **shared**,
    /// multi-tenant WAL file holding several tablets' `RaftCore` records
    /// interleaved (the single-command-split redesign, `docs/adr/0028-*.md`).
    /// One [`SHARED_WAL_TAG`]-tagged, checksummed line via
    /// [`crate::format::encode_line`], same framing discipline as
    /// [`encode_record`](Self::encode_record) (issue #495, ADR 0073).
    #[must_use]
    pub fn encode_tagged_record(tablet: TabletId, record: &WalRecord<C, S>) -> Vec<u8> {
        #[derive(Serialize)]
        struct Line<'a, C, S> {
            tablet: TabletId,
            record: &'a WalRecord<C, S>,
        }
        let payload =
            serde_json::to_vec(&Line { tablet, record }).expect("tagged wal record serializes");
        format::encode_line(&SHARED_WAL_TAG, &payload)
    }

    /// Decode a shared WAL's bytes into `(tablet, record)` pairs, in file
    /// order, stopping at the first line whose framing fails — a trailing
    /// partial line **or** a checksum mismatch (issue #495), per
    /// [`decode`](Self::decode)'s doc (`Ok` with the valid prefix) — **unless
    /// a later `SWL1` v2 sync marker proves that line was already durable**,
    /// in which case it is [`FormatError::MidFileCorruption`] (issue #1132;
    /// `SharedWal` appends a marker after each successful `fsync`, see
    /// `SharedWal::flush`).
    ///
    /// Loud errors, exactly as [`decode`](Self::decode): a CRC-valid line
    /// with no [`SHARED_WAL_TAG`] magic (a pre-baseline untagged
    /// `<crc32>:<json>` line) is `Err(FormatError::PreBaselineFormat)`; an
    /// unknown version is `Err(UnsupportedFormatVersion)`; and a CRC-valid,
    /// correctly tagged line whose JSON doesn't parse is
    /// `Err(FormatError::Malformed)` — a checksum-valid record that won't
    /// decode is a decoder/encoder bug or version skew, not a torn write, so
    /// it must not be silently truncated away (the old untagged decoder
    /// broke out of the loop here; `CWL1`'s `decode` never did). Unlike `decode`, a bad line here stops
    /// the **whole file**, not just one tablet's own stream: this method
    /// returns a flat, not-yet-demultiplexed sequence, so there is no
    /// per-tablet boundary to truncate at independently. This is
    /// deliberately conservative rather than a per-tablet skip-and-continue
    /// (which would risk re-admitting the exact silently-wrong-value gap
    /// this issue closes, now scoped to one tablet's own fold instead of the
    /// whole file) — and it is **safe, not merely conservative, now that
    /// this path is wired into production (C-05 PR 2)**: a torn/corrupted
    /// region can only ever be the file's physical TAIL (every write is an
    /// append, and a whole-file rewrite via `SharedWal::compact_group`/
    /// `forget` is an atomic `Disk::replace`), so every record physically
    /// BEFORE the tear — for every tablet, not just the one whose write was
    /// torn — is already fully valid and decodes correctly; "stop the whole
    /// file at the first bad line" therefore never discards a different
    /// tablet's own already-durable data. See `animus_control::shared_wal`'s
    /// own module doc ("Crash safety") and `docs/adr/0028-*.md`'s C-05 PR 2
    /// amendment for the full argument.
    pub fn decode_tagged(bytes: &[u8]) -> Result<TaggedRecords<C, S>, FormatError> {
        Self::decode_tagged_with_extent(bytes).map(|(lines, _)| lines)
    }

    /// [`decode_tagged`](Self::decode_tagged) plus the clean-prefix length,
    /// for `SharedWal::open`'s tail repair.
    pub fn decode_tagged_with_extent(
        bytes: &[u8],
    ) -> Result<(TaggedRecords<C, S>, usize), FormatError> {
        #[derive(Deserialize)]
        struct Line<C, S> {
            tablet: TabletId,
            record: WalRecord<C, S>,
        }
        let raw = format::decode_lines_extent(&SHARED_WAL_TAG, bytes)?;
        let mut lines = Vec::with_capacity(raw.lines.len());
        for (version, payload) in raw.lines {
            let line: Line<C, S> = dispatch::shared_wal_line(version, payload)?;
            lines.push((line.tablet, line.record));
        }
        Ok((lines, raw.valid_len))
    }

    /// Demultiplex a shared WAL's bytes into one [`PersistedState`] per tablet —
    /// each tablet's records are folded **in the order they appear in the
    /// file**, independently of every other tablet's, exactly as
    /// [`replay`](Self::replay) folds a single tablet's own dedicated file
    /// today. A tablet with no records in the file is simply absent from the
    /// result (never a spurious empty entry).
    ///
    /// Errors are [`decode_tagged`](Self::decode_tagged)'s (a torn tail is
    /// still `Ok`).
    pub fn replay_multiplexed(bytes: &[u8]) -> Result<BTreeMap<TabletId, Self>, FormatError> {
        let mut grouped: BTreeMap<TabletId, Vec<WalRecord<C, S>>> = BTreeMap::new();
        for (tablet, record) in Self::decode_tagged(bytes)? {
            grouped.entry(tablet).or_default().push(record);
        }
        Ok(grouped
            .into_iter()
            .map(|(tablet, records)| (tablet, Self::replay(records)))
            .collect())
    }

    /// Build a shared WAL's full compaction image by concatenating each
    /// locally-hosted tablet's own minimal record set (its `wal_image()` —
    /// snapshot + hard state + log tail), tagged with that tablet's id.
    /// Iteration order of `per_tablet` becomes the file's tablet ordering (it
    /// doesn't matter for correctness — [`replay_multiplexed`](Self::replay_multiplexed)
    /// demuxes by tag — but a stable caller-supplied order, e.g. a `BTreeMap`
    /// iterator, keeps the image byte-reproducible for a given state).
    #[must_use]
    pub fn encode_multiplexed_image<'a>(
        per_tablet: impl IntoIterator<Item = (TabletId, &'a [WalRecord<C, S>])>,
    ) -> Vec<u8>
    where
        C: 'a,
        S: 'a,
    {
        let mut bytes = Vec::new();
        for (tablet, records) in per_tablet {
            for record in records {
                bytes.extend_from_slice(&Self::encode_tagged_record(tablet, record));
            }
        }
        bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::{MetaCommand, NodeStatus};
    use animus_env::Disk;

    // --- tagged / multiplexed WAL (PR1 of the single-command-split redesign) ---

    fn upsert(node: NodeId) -> MetaCommand {
        MetaCommand::UpsertMember {
            node,
            labels: std::collections::BTreeMap::new(),
            status: NodeStatus::Active,
        }
    }

    fn entry(index: u64, term: u64, command: MetaCommand) -> LogEntry<MetaCommand> {
        LogEntry {
            index,
            term,
            command,
            config: None,
            learners: None,
        }
    }

    /// Two tablets' records interleaved in one shared file demux back into two
    /// independent, correctly-ordered `PersistedState`s — the core correctness
    /// property a multi-tenant WAL needs: one tablet's records must never leak
    /// into another's replay, and each tablet's own order must survive
    /// interleaving with everyone else's.
    #[test]
    fn tagged_records_demux_by_tablet_independent_of_interleaving() {
        let t1 = TabletId(1);
        let t2 = TabletId(2);

        let mut bytes = Vec::new();
        bytes.extend(
            PersistedState::<MetaCommand, Metadata>::encode_tagged_record(
                t1,
                &WalRecord::Hard {
                    term: 1,
                    voted_for: Some(nid(300)),
                },
            ),
        );
        bytes.extend(
            PersistedState::<MetaCommand, Metadata>::encode_tagged_record(
                t2,
                &WalRecord::Hard {
                    term: 5,
                    voted_for: Some(nid(301)),
                },
            ),
        );
        bytes.extend(
            PersistedState::<MetaCommand, Metadata>::encode_tagged_record(
                t1,
                &WalRecord::Append(entry(1, 1, upsert(nid(300)))),
            ),
        );
        bytes.extend(
            PersistedState::<MetaCommand, Metadata>::encode_tagged_record(
                t2,
                &WalRecord::Append(entry(1, 5, upsert(nid(301)))),
            ),
        );
        bytes.extend(
            PersistedState::<MetaCommand, Metadata>::encode_tagged_record(
                t1,
                &WalRecord::Append(entry(2, 1, upsert(nid(302)))),
            ),
        );

        let demuxed = PersistedState::<MetaCommand, Metadata>::replay_multiplexed(&bytes).unwrap();

        assert_eq!(demuxed.len(), 2);
        let s1 = &demuxed[&t1];
        assert_eq!(s1.term, 1);
        assert_eq!(s1.voted_for, Some(nid(300)));
        assert_eq!(s1.log.len(), 2);
        assert_eq!(s1.log[0].index, 1);
        assert_eq!(s1.log[1].index, 2);

        let s2 = &demuxed[&t2];
        assert_eq!(s2.term, 5);
        assert_eq!(s2.voted_for, Some(nid(301)));
        assert_eq!(s2.log.len(), 1);
    }

    /// A trailing torn line (crash mid-append) is dropped, exactly like the
    /// single-tablet `decode`'s existing contract — and it must not corrupt any
    /// *other* tablet's already-complete records earlier in the same file.
    #[test]
    fn tagged_replay_tolerates_a_torn_trailing_line() {
        let t1 = TabletId(7);
        let mut bytes = PersistedState::<MetaCommand, Metadata>::encode_tagged_record(
            t1,
            &WalRecord::Hard {
                term: 2,
                voted_for: None,
            },
        );
        bytes.extend(
            PersistedState::<MetaCommand, Metadata>::encode_tagged_record(
                t1,
                &WalRecord::Append(entry(1, 2, upsert(nid(300)))),
            ),
        );
        // Simulate a crash mid-write of a second tablet's record: a truncated
        // trailing line with no newline.
        bytes.extend_from_slice(br#"{"tablet":8,"record":{"Hard":{"term":9"#);

        let demuxed = PersistedState::<MetaCommand, Metadata>::replay_multiplexed(&bytes).unwrap();
        assert_eq!(demuxed.len(), 1, "the torn record must not appear at all");
        let s1 = &demuxed[&t1];
        assert_eq!(s1.term, 2);
        assert_eq!(s1.log.len(), 1);
    }

    /// [`PersistedState::encode_multiplexed_image`] round-trips through
    /// [`PersistedState::replay_multiplexed`] back to each tablet's original
    /// `PersistedState` — the shape a shared-WAL compaction rewrite will use
    /// (concatenate every locally-hosted tablet's own minimal record set).
    #[test]
    fn multiplexed_image_round_trips_per_tablet() {
        let t1 = TabletId(3);
        let t2 = TabletId(4);
        let t1_records = vec![
            WalRecord::Hard {
                term: 4,
                voted_for: Some(nid(300)),
            },
            WalRecord::Append(entry(10, 4, upsert(nid(300)))),
        ];
        let t2_records = vec![WalRecord::Hard {
            term: 1,
            voted_for: None,
        }];

        let image = PersistedState::<MetaCommand, Metadata>::encode_multiplexed_image([
            (t1, t1_records.as_slice()),
            (t2, t2_records.as_slice()),
        ]);

        let demuxed = PersistedState::<MetaCommand, Metadata>::replay_multiplexed(&image).unwrap();
        assert_eq!(demuxed.len(), 2);
        assert_eq!(demuxed[&t1].term, 4);
        assert_eq!(demuxed[&t1].log.len(), 1);
        assert_eq!(demuxed[&t2].term, 1);
        assert!(demuxed[&t2].log.is_empty());
    }

    // --- per-record checksum (issue #495) ---

    /// A correctly round-tripped WAL decodes byte-identically to the records
    /// that produced it — the checksum framing changes nothing about the
    /// happy path.
    #[test]
    fn checksummed_records_round_trip() {
        let records = vec![
            WalRecord::Hard {
                term: 3,
                voted_for: Some(nid(300)),
            },
            WalRecord::Append(entry(1, 3, upsert(nid(301)))),
            WalRecord::Append(entry(2, 3, upsert(nid(302)))),
        ];
        let mut bytes = Vec::new();
        for r in &records {
            bytes.extend(PersistedState::<MetaCommand, Metadata>::encode_record(r));
        }
        let decoded = PersistedState::<MetaCommand, Metadata>::decode(&bytes)
            .expect("no format error expected here");
        assert_eq!(decoded.len(), records.len());
        let state = PersistedState::<MetaCommand, Metadata>::replay(decoded);
        assert_eq!(state.term, 3);
        assert_eq!(state.voted_for, Some(nid(300)));
        assert_eq!(state.log.len(), 2);
    }

    /// A trailing torn line (crash mid-append, no newline, no valid checksum
    /// prefix at all) is still tolerated exactly as before the checksum was
    /// added — a genuinely torn write's effect was never acted on, so
    /// dropping it silently is correct, not merely tolerated.
    #[test]
    fn checksummed_decode_still_tolerates_a_torn_trailing_line() {
        let good = WalRecord::<MetaCommand, Metadata>::Hard {
            term: 7,
            voted_for: None,
        };
        let mut bytes = PersistedState::<MetaCommand, Metadata>::encode_record(&good);
        // A crash mid-write of a second record: a truncated line with no
        // trailing newline and (since it was cut off before the encoder ever
        // got to emit one) no complete checksum-hex prefix either.
        bytes.extend_from_slice(b"deadbeef:{\"Hard\":{\"term\":9");

        let decoded = PersistedState::<MetaCommand, Metadata>::decode(&bytes)
            .expect("no format error expected here");
        assert_eq!(decoded.len(), 1, "the torn record must not appear at all");
        let state = PersistedState::<MetaCommand, Metadata>::replay(decoded);
        assert_eq!(state.term, 7, "must reflect only the one good record");
    }

    /// The heart of issue #495: a single bit-flip inside an already-fsynced
    /// record's payload — landing on a digit, so the line is still perfectly
    /// valid JSON — must be caught by the checksum and dropped, never
    /// silently decoded into a different, wrong-but-plausible value. And
    /// because recovery has no way to tell "this one record is corrupt" from
    /// "the log genuinely ends here" once a checksum has failed, every
    /// record physically after the corrupted one is dropped too, even though
    /// it is itself perfectly intact — matching the same "recovery stops at
    /// the last good record" contract a torn tail already has.
    #[test]
    fn corrupted_middle_record_is_dropped_along_with_everything_after_it() {
        let r1 = WalRecord::<MetaCommand, Metadata>::Hard {
            term: 1,
            voted_for: Some(nid(300)),
        };
        // `term: 5` is the field the flipped digit will land on.
        let r2 = WalRecord::<MetaCommand, Metadata>::Append(entry(1, 5, upsert(nid(301))));
        let r3 = WalRecord::<MetaCommand, Metadata>::Append(entry(2, 5, upsert(nid(302))));

        let line1 = PersistedState::<MetaCommand, Metadata>::encode_record(&r1);
        let mut line2 = PersistedState::<MetaCommand, Metadata>::encode_record(&r2);
        let line3 = PersistedState::<MetaCommand, Metadata>::encode_record(&r3);

        // Flip the digit `5` (the entry's term) to `6` inside line2's JSON
        // payload, past its checksum-hex prefix — still syntactically valid
        // JSON, just a different, wrong value, exactly the corruption issue
        // #495 describes hitting a real numeric field (a packed
        // `HlcTimestamp` in `animus-cp-data`'s own sibling WAL).
        let payload_start = line2.iter().position(|&b| b == b':').unwrap() + 1;
        let five_pos = payload_start
            + line2[payload_start..]
                .iter()
                .position(|&b| b == b'5')
                .expect("the term digit is present in the payload");
        assert_eq!(line2[five_pos], b'5');
        line2[five_pos] = b'6';

        let mut bytes = line1;
        bytes.extend(&line2);
        bytes.extend(&line3);

        let decoded = PersistedState::<MetaCommand, Metadata>::decode(&bytes)
            .expect("no format error expected here");
        assert_eq!(
            decoded.len(),
            1,
            "only the record before the corrupted one may survive"
        );
        let state = PersistedState::<MetaCommand, Metadata>::replay(decoded);
        assert_eq!(state.term, 1);
        assert_eq!(state.voted_for, Some(nid(300)));
        assert!(
            state.log.is_empty(),
            "neither the corrupted entry nor the intact one after it may be applied"
        );
    }

    /// The tagged/multiplexed WAL variant gets the identical checksum
    /// protection: a corrupted-but-JSON-valid tagged line must never decode
    /// into a wrong value either.
    #[test]
    fn corrupted_tagged_record_is_rejected_not_silently_misdecoded() {
        let t1 = TabletId(9);
        let good = WalRecord::<MetaCommand, Metadata>::Hard {
            term: 4,
            voted_for: None,
        };
        let mut line = PersistedState::<MetaCommand, Metadata>::encode_tagged_record(t1, &good);
        let payload_start = line.iter().position(|&b| b == b':').unwrap() + 1;
        let four_pos = payload_start
            + line[payload_start..]
                .iter()
                .position(|&b| b == b'4')
                .expect("the term digit is present in the payload");
        line[four_pos] = b'9';

        let demuxed = PersistedState::<MetaCommand, Metadata>::replay_multiplexed(&line).unwrap();
        assert!(
            demuxed.is_empty(),
            "a corrupted tagged record must never surface, wrong-valued or otherwise"
        );
    }

    // --- CONTROL_WAL tagging (ADR 0073 Phase 0 workstream B) ---

    /// A pre-baseline WAL line (the old, untagged `<crc32>:<json>` shape) is refused by name through `PersistedState::decode`, never
    /// silently misread as an empty or partial log.
    #[test]
    fn decode_rejects_a_pre_baseline_untagged_line() {
        let good = WalRecord::<MetaCommand, Metadata>::Hard {
            term: 1,
            voted_for: None,
        };
        let payload = serde_json::to_vec(&good).unwrap();
        let line = old_untagged_line(&payload);
        let err = PersistedState::<MetaCommand, Metadata>::decode(&line).unwrap_err();
        assert_eq!(
            err,
            FormatError::PreBaselineFormat {
                format: CONTROL_WAL.name
            }
        );
    }

    /// A correctly CRC-framed, correctly `CWL1`-tagged line whose version
    /// this build doesn't know is a loud, named `Err`, not a silent
    /// misdecode or an empty result.
    #[test]
    fn decode_rejects_an_unsupported_future_version() {
        let good = WalRecord::<MetaCommand, Metadata>::Hard {
            term: 1,
            voted_for: None,
        };
        let payload = serde_json::to_vec(&good).unwrap();
        let future_tag = FormatTag {
            magic: CONTROL_WAL.magic,
            version: CONTROL_WAL.version + 1,
            name: CONTROL_WAL.name,
        };
        let line = format::encode_line(&future_tag, &payload);
        let err = PersistedState::<MetaCommand, Metadata>::decode(&line).unwrap_err();
        assert_eq!(
            err,
            FormatError::UnsupportedFormatVersion {
                format: CONTROL_WAL.name,
                found: CONTROL_WAL.version + 1,
                max_supported: CONTROL_WAL.version,
            }
        );
    }

    /// A correctly framed, correctly tagged, supported-version line whose
    /// JSON payload is garbage is `Err(Malformed)` — loud, not a silent
    /// torn-tail-style drop.
    #[test]
    fn decode_reports_a_malformed_payload_loudly() {
        let line = format::encode_line(&CONTROL_WAL, b"not valid json");
        let err = PersistedState::<MetaCommand, Metadata>::decode(&line).unwrap_err();
        match err {
            FormatError::Malformed { format, .. } => assert_eq!(format, CONTROL_WAL.name),
            other => panic!("expected Malformed, got {other:?}"),
        }
    }

    // --- SHARED_WAL_TAG (SWL1) tagging (ADR 0073 Phase 0 workstream C) ---

    /// The pre-baseline untagged framing, hand-built (the private helper
    /// that used to produce it is gone): `<crc32 8 hex>:<payload>\n`.
    fn old_untagged_line(payload: &[u8]) -> Vec<u8> {
        let mut line = format!("{:08x}:", crc32fast::hash(payload)).into_bytes();
        line.extend_from_slice(payload);
        line.push(b'\n');
        line
    }

    fn tagged_payload() -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "tablet": 1,
            "record": WalRecord::<MetaCommand, Metadata>::Hard { term: 1, voted_for: None },
        }))
        .unwrap()
    }

    #[test]
    fn decode_tagged_rejects_a_pre_baseline_untagged_line() {
        let line = old_untagged_line(&tagged_payload());
        let err = PersistedState::<MetaCommand, Metadata>::decode_tagged(&line).unwrap_err();
        assert_eq!(
            err,
            FormatError::PreBaselineFormat {
                format: SHARED_WAL_TAG.name
            }
        );
        assert!(PersistedState::<MetaCommand, Metadata>::replay_multiplexed(&line).is_err());
    }

    #[test]
    fn decode_tagged_rejects_an_unsupported_future_version() {
        let future = FormatTag {
            version: SHARED_WAL_TAG.version + 1,
            ..SHARED_WAL_TAG
        };
        let line = format::encode_line(&future, &tagged_payload());
        let err = PersistedState::<MetaCommand, Metadata>::decode_tagged(&line).unwrap_err();
        assert_eq!(
            err,
            FormatError::UnsupportedFormatVersion {
                format: SHARED_WAL_TAG.name,
                found: SHARED_WAL_TAG.version + 1,
                max_supported: SHARED_WAL_TAG.version,
            }
        );
    }

    /// A different format's tag (a `CWL1` line fed to the shared-WAL
    /// decoder) is refused by name too.
    #[test]
    fn decode_tagged_rejects_another_formats_magic() {
        let line = format::encode_line(&CONTROL_WAL, &tagged_payload());
        assert!(matches!(
            PersistedState::<MetaCommand, Metadata>::decode_tagged(&line),
            Err(FormatError::PreBaselineFormat { .. })
        ));
    }

    /// A CRC-valid, correctly tagged line with unparsable JSON is loud.
    #[test]
    fn decode_tagged_reports_a_malformed_payload_loudly() {
        let mut bytes = PersistedState::<MetaCommand, Metadata>::encode_tagged_record(
            TabletId(1),
            &WalRecord::Hard {
                term: 1,
                voted_for: None,
            },
        );
        bytes.extend(format::encode_line(&SHARED_WAL_TAG, b"not valid json"));
        let err = PersistedState::<MetaCommand, Metadata>::decode_tagged(&bytes).unwrap_err();
        match err {
            FormatError::Malformed { format, .. } => assert_eq!(format, SHARED_WAL_TAG.name),
            other => panic!("expected Malformed, got {other:?}"),
        }
    }

    /// Torn tails of every shape stay a silent stop returning the prefix.
    #[test]
    fn decode_tagged_torn_tail_is_a_silent_stop() {
        let good = PersistedState::<MetaCommand, Metadata>::encode_tagged_record(
            TabletId(1),
            &WalRecord::Hard {
                term: 2,
                voted_for: None,
            },
        );
        let next = PersistedState::<MetaCommand, Metadata>::encode_tagged_record(
            TabletId(2),
            &WalRecord::Hard {
                term: 3,
                voted_for: None,
            },
        );
        for cut in [1, 5, 9, 12, next.len() / 2, next.len() - 2] {
            let mut bytes = good.clone();
            bytes.extend_from_slice(&next[..cut]);
            let decoded = PersistedState::<MetaCommand, Metadata>::decode_tagged(&bytes)
                .unwrap_or_else(|e| panic!("cut {cut}: torn tail must not be an Err: {e}"));
            assert_eq!(decoded.len(), 1, "cut {cut}");
        }
    }

    #[test]
    fn dispatchers_reject_an_unknown_version_by_name() {
        for v in [0u8, 3, 255] {
            assert_eq!(
                dispatch::wal_record::<serde_json::Value>(v, b"{}").unwrap_err(),
                FormatError::UnsupportedFormatVersion {
                    format: CONTROL_WAL.name,
                    found: v,
                    max_supported: CONTROL_WAL.version
                }
            );
            assert_eq!(
                dispatch::shared_wal_line::<serde_json::Value>(v, b"{}").unwrap_err(),
                FormatError::UnsupportedFormatVersion {
                    format: SHARED_WAL_TAG.name,
                    found: v,
                    max_supported: SHARED_WAL_TAG.version
                }
            );
            assert_eq!(
                dispatch::snapshot_body::<serde_json::Value>(v, b"{}").unwrap_err(),
                FormatError::UnsupportedFormatVersion {
                    format: CONTROL_SNAPSHOT.name,
                    found: v,
                    max_supported: CONTROL_SNAPSHOT.version
                }
            );
        }
    }

    #[test]
    fn dispatchers_decode_v1_and_report_malformed_bodies() {
        let ok: serde_json::Value = dispatch::wal_record(1, br#"{"a":1}"#).unwrap();
        assert_eq!(ok["a"], 1);
        assert!(matches!(
            dispatch::snapshot_body::<serde_json::Value>(1, b"nope"),
            Err(FormatError::Malformed { format, .. }) if format == CONTROL_SNAPSHOT.name
        ));
    }

    // --- issue #1132: mid-file corruption vs torn tail (CWL1 v2) -----------

    fn hard(term: u64) -> WalRecord<MetaCommand, Metadata> {
        WalRecord::Hard {
            term,
            voted_for: None,
        }
    }

    fn lines_of(records: &[WalRecord<MetaCommand, Metadata>]) -> Vec<Vec<u8>> {
        records
            .iter()
            .map(PersistedState::<MetaCommand, Metadata>::encode_record)
            .collect()
    }

    /// Two synced rounds (`[1,2]` then `[3]`), each followed by its marker,
    /// as the real writer lays them out. Returns the bytes and the start
    /// offset of each record line / marker line, in file order.
    fn two_round_file() -> (Vec<u8>, Vec<usize>) {
        let mut bytes = Vec::new();
        let mut starts = Vec::new();
        for round in [vec![hard(1), hard(2)], vec![hard(3)]] {
            for line in lines_of(&round) {
                starts.push(bytes.len());
                bytes.extend(line);
            }
            starts.push(bytes.len());
            bytes.extend(format::encode_sync_marker(&CONTROL_WAL, bytes.len() as u64));
        }
        (bytes, starts)
    }

    #[test]
    fn a_flipped_byte_before_a_durable_marker_is_a_named_error_even_on_the_first_line() {
        let (clean, starts) = two_round_file();
        // starts = [r1, r2, marker1, r3, marker2]; flip inside r1, r2, r3.
        for (victim, durable_to) in [(0usize, starts[4]), (1, starts[4]), (3, starts[4])] {
            let mut bytes = clean.clone();
            bytes[starts[victim] + 12] ^= 0xFF;
            assert_eq!(
                PersistedState::<MetaCommand, Metadata>::decode(&bytes).unwrap_err(),
                FormatError::MidFileCorruption {
                    format: "control-wal",
                    offset: starts[victim] as u64,
                    durable_to: durable_to as u64,
                },
                "victim line {victim}"
            );
        }
    }

    #[test]
    fn damage_after_the_last_marker_is_a_tolerated_torn_tail() {
        let (mut bytes, _) = two_round_file();
        let unsynced = lines_of(&[hard(4), hard(5), hard(6)]);
        let tail_start = bytes.len();
        for l in &unsynced {
            bytes.extend(l);
        }
        // Flip a byte in the FIRST un-synced line while later un-synced lines
        // stay valid: exactly what `corrupt_on_crash` makes, and not an error.
        let mut flipped = bytes.clone();
        flipped[tail_start + 12] ^= 0xFF;
        let (records, valid_len) =
            PersistedState::<MetaCommand, Metadata>::decode_with_extent(&flipped).unwrap();
        assert_eq!(records, vec![hard(1), hard(2), hard(3)]);
        assert_eq!(valid_len, tail_start);
        // And a torn (cut) final marker: its proof is lost, nothing invented.
        let mut cut = two_round_file().0;
        cut.truncate(cut.len() - 5);
        let records = PersistedState::<MetaCommand, Metadata>::decode(&cut).unwrap();
        assert_eq!(records, vec![hard(1), hard(2), hard(3)]);
    }

    #[test]
    fn partial_final_line_and_blank_final_line_decode_as_before() {
        let (mut bytes, _) = two_round_file();
        bytes.extend_from_slice(b"\n\n");
        assert_eq!(
            PersistedState::<MetaCommand, Metadata>::decode(&bytes)
                .unwrap()
                .len(),
            3
        );
        bytes.extend_from_slice(b"deadbeef:CWL1");
        let (records, valid_len) =
            PersistedState::<MetaCommand, Metadata>::decode_with_extent(&bytes).unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(valid_len, bytes.len() - b"deadbeef:CWL1".len());
    }

    #[test]
    fn a_marker_that_is_not_where_it_claims_is_a_named_error() {
        let (mut bytes, _) = two_round_file();
        let at = bytes.len() as u64;
        bytes.extend(format::encode_sync_marker(&CONTROL_WAL, 7));
        assert_eq!(
            PersistedState::<MetaCommand, Metadata>::decode(&bytes).unwrap_err(),
            FormatError::BadSyncMarker {
                format: "control-wal",
                offset: at,
                claimed: 7,
            }
        );
    }

    #[test]
    fn a_forged_future_version_line_keeps_its_existing_error() {
        // CRC-valid CWL1 line, version 03, first in the file.
        let body = b"CWL103{}";
        let mut line = format!("{:08x}:", crc32fast::hash(body)).into_bytes();
        line.extend_from_slice(body);
        line.push(b'\n');
        let expect = FormatError::UnsupportedFormatVersion {
            format: "control-wal",
            found: 3,
            max_supported: 2,
        };
        assert_eq!(
            PersistedState::<MetaCommand, Metadata>::decode(&line).unwrap_err(),
            expect
        );
        // Still the same error with durable-looking history behind it.
        let (mut bytes, _) = two_round_file();
        bytes.extend(&line);
        assert_eq!(
            PersistedState::<MetaCommand, Metadata>::decode(&bytes).unwrap_err(),
            expect
        );
    }

    #[test]
    fn v1_files_keep_the_lenient_torn_tail_anywhere_rule() {
        let v1 = include_bytes!("../tests/fixtures/formats/control-wal/v1.bin");
        let clean = PersistedState::<MetaCommand, Metadata>::decode(v1).unwrap();
        assert_eq!(clean.len(), 4);
        // Corrupting line 0 of a marker-less v1 file is still a (silent) tail.
        let mut bad = v1.to_vec();
        bad[12] ^= 0xFF;
        assert!(
            PersistedState::<MetaCommand, Metadata>::decode(&bad)
                .unwrap()
                .is_empty()
        );
    }

    /// Checklist step 7. The fixture's `Snapshot` line embeds a `Metadata`
    /// serialized before `Metadata` grew its `"v"` field (P1-C kept that field
    /// optional on read precisely so this frozen fixture still decodes), so
    /// that one line cannot be byte-equal to what *today's* `Metadata` serde
    /// emits; every other line is byte-for-byte, and the snapshot line must
    /// round-trip to the identical record.
    #[test]
    fn legacy_v1_encoder_reproduces_the_v1_fixture_byte_for_byte() {
        let v1 = include_bytes!("../tests/fixtures/formats/control-wal/v1.bin");
        let records = PersistedState::<MetaCommand, Metadata>::decode(v1).unwrap();
        let fixture_lines: Vec<&[u8]> = v1.split_inclusive(|&b| b == b'\n').collect();
        assert_eq!(fixture_lines.len(), records.len());
        for (i, (record, fixture)) in records.iter().zip(&fixture_lines).enumerate() {
            let re = dispatch::legacy::v1::encode_record(record);
            if matches!(record, WalRecord::Snapshot { .. }) {
                let again = PersistedState::<MetaCommand, Metadata>::decode(&re).unwrap();
                assert_eq!(again, vec![record.clone()], "line {i}");
            } else {
                assert_eq!(&re[..], *fixture, "line {i}");
            }
        }
    }

    /// A v1 file reopened by this build: new appends are v2 lines + markers
    /// (version is per line), the mix decodes, and a marker's proof covers
    /// the v1 lines before it.
    #[test]
    fn a_v1_file_extended_with_v2_rounds_decodes_and_protects_its_v1_prefix() {
        let v1 = include_bytes!("../tests/fixtures/formats/control-wal/v1.bin");
        let mut bytes = v1.to_vec();
        for line in lines_of(&[hard(9)]) {
            bytes.extend(line);
        }
        bytes.extend(format::encode_sync_marker(&CONTROL_WAL, bytes.len() as u64));
        let records = PersistedState::<MetaCommand, Metadata>::decode(&bytes).unwrap();
        assert_eq!(records.len(), 5);
        assert_eq!(records[4], hard(9));
        let mut rotted = bytes.clone();
        rotted[12] ^= 0xFF;
        assert!(matches!(
            PersistedState::<MetaCommand, Metadata>::decode(&rotted),
            Err(FormatError::MidFileCorruption { offset: 0, .. })
        ));
    }

    /// `SWL1` mirror of the CWL1 checks: the legacy v1 encoder reproduces every
    /// non-snapshot line of the v1 fixture byte for byte (the `Snapshot` line
    /// embeds a `Metadata` serialized before its `"v"` field existed, so it
    /// is checked by round trip instead), a marker-less v1 file keeps the
    /// lenient rule, and a forged future-version line keeps its error.
    #[test]
    fn shared_wal_v1_fixture_legacy_encoder_leniency_and_forged_version() {
        let v1 = include_bytes!("../tests/fixtures/formats/shared-wal/v1.bin");
        let lines = PersistedState::<MetaCommand, Metadata>::decode_tagged(v1).unwrap();
        let fixture_lines: Vec<&[u8]> = v1.split_inclusive(|&b| b == b'\n').collect();
        assert_eq!(fixture_lines.len(), lines.len());
        for (i, ((tablet, record), fixture)) in lines.iter().zip(&fixture_lines).enumerate() {
            let re = dispatch::legacy_shared::v1::encode_tagged_record(*tablet, record);
            if matches!(record, WalRecord::Snapshot { .. }) {
                let again = PersistedState::<MetaCommand, Metadata>::decode_tagged(&re).unwrap();
                assert_eq!(again, vec![(*tablet, record.clone())], "line {i}");
            } else {
                assert_eq!(&re[..], *fixture, "line {i}");
            }
        }
        let mut bad = v1.to_vec();
        bad[12] ^= 0xFF;
        assert!(
            PersistedState::<MetaCommand, Metadata>::decode_tagged(&bad)
                .unwrap()
                .is_empty()
        );
        let body = b"SWL103{}";
        let mut forged = format!("{:08x}:", crc32fast::hash(body)).into_bytes();
        forged.extend_from_slice(body);
        forged.push(b'\n');
        assert_eq!(
            PersistedState::<MetaCommand, Metadata>::decode_tagged(&forged).unwrap_err(),
            FormatError::UnsupportedFormatVersion {
                format: "shared-wal",
                found: 3,
                max_supported: 2,
            }
        );
    }

    // --- issue #1132: piggybacked sync markers (`SyncMarkerState`) ---------

    /// Drive the writer protocol against a `SimEnv` disk exactly as
    /// `persist_wal` does: take_marker, one append, sync, mark_synced.
    async fn round<E: Env>(
        env: &E,
        st: &SyncMarkerState,
        records: &[WalRecord<MetaCommand, Metadata>],
    ) {
        let mut buf = st.take_marker(env, &CONTROL_WAL, "w").await;
        for r in records {
            buf.extend(PersistedState::<MetaCommand, Metadata>::encode_record(r));
        }
        env.append("w", &buf).await.unwrap();
        env.sync("w").await.unwrap();
        st.mark_synced();
    }

    fn sim_env() -> animus_sim::SimEnv {
        animus_sim::Simulator::new(7).env(animus_env::nid(0))
    }

    #[test]
    fn a_marker_rides_at_the_start_of_the_next_rounds_append_with_its_own_offset() {
        futures::executor::block_on(async {
            let env = sim_env();
            let st = SyncMarkerState::default();
            round(&env, &st, &[hard(1), hard(2)]).await;
            let after_first = env.size("w").await.unwrap();
            // Round 1 wrote no marker (nothing was proven yet).
            let bytes = env.read("w").await.unwrap();
            assert_eq!(bytes.len() as u64, after_first);
            assert!(!bytes.windows(6).any(|w| w == b"!sync:"));
            round(&env, &st, &[hard(3)]).await;
            let bytes = env.read("w").await.unwrap();
            let expect = format::encode_sync_marker(&CONTROL_WAL, after_first);
            assert_eq!(
                &bytes[after_first as usize..after_first as usize + expect.len()],
                &expect[..],
                "the marker sits at offset N == its own start, before round 2's records"
            );
            // And the file decodes cleanly to all three records.
            let (records, valid) =
                PersistedState::<MetaCommand, Metadata>::decode_with_extent(&bytes).unwrap();
            assert_eq!(records, vec![hard(1), hard(2), hard(3)]);
            assert_eq!(valid, bytes.len());
        });
    }

    #[test]
    fn a_flip_before_a_piggybacked_marker_is_mid_file_corruption() {
        futures::executor::block_on(async {
            let env = sim_env();
            let st = SyncMarkerState::default();
            round(&env, &st, &[hard(1), hard(2)]).await;
            let n = env.size("w").await.unwrap();
            round(&env, &st, &[hard(3)]).await;
            let mut bytes = env.read("w").await.unwrap();
            bytes[12] ^= 0xFF; // inside record 1, before the marker at `n`
            assert_eq!(
                PersistedState::<MetaCommand, Metadata>::decode(&bytes).unwrap_err(),
                FormatError::MidFileCorruption {
                    format: "control-wal",
                    offset: 0,
                    durable_to: n,
                }
            );
        });
    }

    #[test]
    fn invalidate_and_a_failed_round_clear_the_pending_marker() {
        futures::executor::block_on(async {
            let env = sim_env();
            let st = SyncMarkerState::default();
            round(&env, &st, &[hard(1)]).await;
            st.invalidate(); // e.g. a compaction rewrite
            assert!(st.take_marker(&env, &CONTROL_WAL, "w").await.is_empty());
            // take_marker itself clears: a round that then fails (never calls
            // mark_synced) leaves nothing pending.
            round(&env, &st, &[hard(2)]).await;
            let _ = st.take_marker(&env, &CONTROL_WAL, "w").await;
            assert!(st.take_marker(&env, &CONTROL_WAL, "w").await.is_empty());
        });
    }
}
