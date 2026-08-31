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

use std::collections::{BTreeMap, BTreeSet};

use animus_env::NodeId;
#[cfg(test)]
use animus_env::nid;
use animus_tablet::TabletId;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::meta::{MetaCommand, Metadata};
use crate::raft::LogEntry;

// ---------------------------------------------------------------------------
// Per-record WAL checksum framing (issue #495).
//
// The pre-existing format was plain newline-terminated `serde_json` with no
// way to tell a wrong-but-still-valid value apart from a correct one: a
// bit-flip landing inside a byte that kept the JSON syntactically valid (e.g.
// a digit inside a packed numeric field) decoded successfully into a
// different, silently-corrupt record instead of a decode error — confirmed
// to reach a hard panic once such a record applied past
// `animus_cp_data::assert_ts_monotonic` (see `docs/engineering-lessons.md`'s
// issue #495 entry for the full account). Every line is now framed
// `<crc32 as 8 lowercase hex chars>:<json>\n` — `crc32fast::hash`, the same
// crate/impl `animus-storage`'s own CRC-checked SSTable/WAL framing uses,
// reused here rather than a second dependency for the same job. This is a
// text-based checksum wrapper around the existing JSON line, not a switch to
// `animus-storage`'s binary length-prefixed frame — this WAL stays
// `serde_json`, shared generic over any `RaftCore<C, S>` (the module doc
// above), where a binary frame would still need a self-describing payload
// codec underneath it and would buy nothing extra for the control plane's
// non-hot-path WAL. No back-compat: this repo carries no WAL format
// compatibility guarantee between revisions (root `CLAUDE.md`), so there is
// no migration for a pre-existing unchecksummed WAL file — a node upgraded
// in place would need a fresh WAL, exactly like any other format change here.

/// Frame one already-serialized payload as a checksummed WAL line. Shared by
/// [`PersistedState::encode_record`] and
/// [`PersistedState::encode_tagged_record`].
fn encode_checksummed_line(payload: &[u8]) -> Vec<u8> {
    let crc = crc32fast::hash(payload);
    let mut line = Vec::with_capacity(payload.len() + 10);
    line.extend_from_slice(format!("{crc:08x}:").as_bytes());
    line.extend_from_slice(payload);
    line.push(b'\n');
    line
}

/// Validate one non-empty WAL line's checksum prefix, returning the payload
/// bytes (still to be JSON-decoded by the caller) on a match. `None` on any
/// malformed framing — no `:` separator, a non-8-hex-digit prefix, or a CRC
/// mismatch — which the caller (`decode`/`decode_tagged`) treats identically
/// to a torn trailing line: everything from here on is dropped, never
/// applied, never a panic.
fn verify_checksummed_line(line: &[u8]) -> Option<&[u8]> {
    let colon = line.iter().position(|&b| b == b':')?;
    let (hex, rest) = line.split_at(colon);
    if hex.len() != 8 {
        return None;
    }
    let expected = u32::from_str_radix(std::str::from_utf8(hex).ok()?, 16).ok()?;
    let payload = &rest[1..];
    (crc32fast::hash(payload) == expected).then_some(payload)
}

/// One durable change, appended to the write-ahead log. Generic over the command
/// type `C` and snapshot-image type `S` (defaults: the control plane's
/// [`MetaCommand`] / [`Metadata`]), so the same WAL machinery serves any
/// `RaftCore<C, S>`. The generic is erased in the JSON form, so the on-disk
/// encoding for the control plane is unchanged.
#[derive(Clone, Debug, Serialize, Deserialize)]
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

    /// Encode a single record as one newline-terminated, CRC32-checksummed
    /// JSON line for the WAL (issue #495): `<crc32 as 8 lowercase hex
    /// chars>:<json>\n`. `serde_json` never emits raw newlines or `:` inside
    /// its own line-terminal position ambiguously — the checksum's fixed
    /// 8-hex-digit-plus-colon prefix is unambiguous framing, same principle
    /// as `animus-storage`'s length-prefixed + `crc32fast`-checksummed binary
    /// WAL frames, just kept text-based here since this format is shared
    /// generic JSON, not a hand-rolled binary codec. See [`decode`](Self::decode)'s
    /// doc for why a checksum failure is treated exactly like a torn tail.
    #[must_use]
    pub fn encode_record(record: &WalRecord<C, S>) -> Vec<u8> {
        let payload = serde_json::to_vec(record).expect("wal record serializes");
        encode_checksummed_line(&payload)
    }

    /// Decode the WAL bytes back into records, stopping at the first record
    /// that fails to decode — whether because it is a **trailing partial
    /// line** (a write torn by a crash — its effect was never acted on) or
    /// because its **checksum doesn't match** (issue #495: at-rest
    /// corruption of an already-fsynced record, which the pre-checksum
    /// newline-JSON framing could not distinguish from a legitimate value —
    /// a bit-flip landing inside a byte that kept the JSON syntactically
    /// valid used to decode successfully into a wrong-but-plausible value
    /// instead of failing). Both cases are handled identically: everything
    /// from the first bad record onward is dropped, never applied, and
    /// nothing here ever panics on corrupt input — deliberately simpler than
    /// `animus-storage`'s own WAL framing, which additionally distinguishes
    /// a real mid-file corruption (hard error) from a torn tail (tolerated)
    /// by checking whether any valid record follows; this WAL has no
    /// invariant that needs that finer distinction (a dropped tail-of-log is
    /// always safe here — see `PersistedState::replay`'s doc), so "stop at
    /// the first bad record" is the simplest sufficient rule that closes the
    /// gap.
    pub fn decode(bytes: &[u8]) -> Vec<WalRecord<C, S>> {
        let mut records = Vec::new();
        for line in bytes.split(|&b| b == b'\n') {
            if line.is_empty() {
                continue;
            }
            let Some(payload) = verify_checksummed_line(line) else {
                break;
            };
            let Ok(record) = serde_json::from_slice(payload) else {
                break;
            };
            records.push(record);
        }
        records
    }

    /// Encode one record tagged with the tablet it belongs to, for a **shared**,
    /// multi-tenant WAL file holding several tablets' `RaftCore` records
    /// interleaved (the single-command-split redesign, `docs/adr/0028-*.md`).
    /// One newline-terminated, checksummed JSON line, same framing discipline
    /// as [`encode_record`](Self::encode_record) (issue #495).
    #[must_use]
    pub fn encode_tagged_record(tablet: TabletId, record: &WalRecord<C, S>) -> Vec<u8> {
        #[derive(Serialize)]
        struct Line<'a, C, S> {
            tablet: TabletId,
            record: &'a WalRecord<C, S>,
        }
        let payload =
            serde_json::to_vec(&Line { tablet, record }).expect("tagged wal record serializes");
        encode_checksummed_line(&payload)
    }

    /// Decode a shared WAL's bytes into `(tablet, record)` pairs, in file
    /// order, stopping at the first record that fails to decode — a trailing
    /// partial line **or** a checksum mismatch (issue #495), per
    /// [`decode`](Self::decode)'s doc. Unlike `decode`, a bad line here stops
    /// the **whole file**, not just one tablet's own stream: this method
    /// returns a flat, not-yet-demultiplexed sequence, so there is no
    /// per-tablet boundary to truncate at independently. This is
    /// deliberately conservative rather than a per-tablet skip-and-continue
    /// (which would risk re-admitting the exact silently-wrong-value gap
    /// this issue closes, now scoped to one tablet's own fold instead of the
    /// whole file) — acceptable since this shared-WAL path is currently
    /// unwired (`shared_wal.rs`'s own doc), so no production replica loses
    /// interleaved siblings' records to this today.
    pub fn decode_tagged(bytes: &[u8]) -> Vec<(TabletId, WalRecord<C, S>)> {
        #[derive(Deserialize)]
        struct Line<C, S> {
            tablet: TabletId,
            record: WalRecord<C, S>,
        }
        let mut lines = Vec::new();
        for line in bytes.split(|&b| b == b'\n') {
            if line.is_empty() {
                continue;
            }
            let Some(payload) = verify_checksummed_line(line) else {
                break;
            };
            let Ok(line) = serde_json::from_slice::<Line<C, S>>(payload) else {
                break;
            };
            lines.push((line.tablet, line.record));
        }
        lines
    }

    /// Demultiplex a shared WAL's bytes into one [`PersistedState`] per tablet —
    /// each tablet's records are folded **in the order they appear in the
    /// file**, independently of every other tablet's, exactly as
    /// [`replay`](Self::replay) folds a single tablet's own dedicated file
    /// today. A tablet with no records in the file is simply absent from the
    /// result (never a spurious empty entry).
    pub fn replay_multiplexed(bytes: &[u8]) -> BTreeMap<TabletId, Self> {
        let mut grouped: BTreeMap<TabletId, Vec<WalRecord<C, S>>> = BTreeMap::new();
        for (tablet, record) in Self::decode_tagged(bytes) {
            grouped.entry(tablet).or_default().push(record);
        }
        grouped
            .into_iter()
            .map(|(tablet, records)| (tablet, Self::replay(records)))
            .collect()
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

        let demuxed = PersistedState::<MetaCommand, Metadata>::replay_multiplexed(&bytes);

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

        let demuxed = PersistedState::<MetaCommand, Metadata>::replay_multiplexed(&bytes);
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

        let demuxed = PersistedState::<MetaCommand, Metadata>::replay_multiplexed(&image);
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
        let decoded = PersistedState::<MetaCommand, Metadata>::decode(&bytes);
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

        let decoded = PersistedState::<MetaCommand, Metadata>::decode(&bytes);
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

        let decoded = PersistedState::<MetaCommand, Metadata>::decode(&bytes);
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

        let demuxed = PersistedState::<MetaCommand, Metadata>::replay_multiplexed(&line);
        assert!(
            demuxed.is_empty(),
            "a corrupted tagged record must never surface, wrong-valued or otherwise"
        );
    }
}
