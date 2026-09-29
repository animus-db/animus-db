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

use animus_env::NodeId;
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
    version: 1,
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
    version: 1,
    name: "shared-wal",
};

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
    /// - A **trailing partial line** (a write torn by a crash — its effect
    ///   was never acted on) or a **checksum mismatch** (issue #495:
    ///   at-rest corruption of an already-fsynced record) stops decoding at
    ///   the first such line and returns everything collected before it as
    ///   `Ok` — never applied, never a panic, never an `Err`: a dropped
    ///   tail-of-log is always safe here (see [`PersistedState::replay`]'s
    ///   doc), so silently returning the valid prefix is the correct
    ///   behavior, not merely tolerated.
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
        let lines = format::decode_lines(&CONTROL_WAL, bytes)?;
        let mut records = Vec::with_capacity(lines.len());
        for (_version, payload) in lines {
            let record = serde_json::from_slice(payload).map_err(|e| FormatError::Malformed {
                format: CONTROL_WAL.name,
                detail: e.to_string(),
            })?;
            records.push(record);
        }
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
    /// order, stopping silently at the first line whose framing fails — a
    /// trailing partial line **or** a checksum mismatch (issue #495), per
    /// [`decode`](Self::decode)'s doc (`Ok` with the valid prefix).
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
        #[derive(Deserialize)]
        struct Line<C, S> {
            tablet: TabletId,
            record: WalRecord<C, S>,
        }
        let raw = format::decode_lines(&SHARED_WAL_TAG, bytes)?;
        let mut lines = Vec::with_capacity(raw.len());
        for (_version, payload) in raw {
            let line: Line<C, S> =
                serde_json::from_slice(payload).map_err(|e| FormatError::Malformed {
                    format: SHARED_WAL_TAG.name,
                    detail: e.to_string(),
                })?;
            lines.push((line.tablet, line.record));
        }
        Ok(lines)
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
}
