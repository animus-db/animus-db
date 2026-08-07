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
use animus_tablet::TabletId;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::meta::{MetaCommand, Metadata};
use crate::raft::LogEntry;

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
                } => {
                    state.snapshot = Some((metadata, last_index, last_term));
                    state.snapshot_config = config;
                }
            }
        }
        state
    }

    /// Encode a single record as one newline-terminated JSON line for the WAL.
    /// (`serde_json` never emits raw newlines, so the framing is unambiguous.)
    #[must_use]
    pub fn encode_record(record: &WalRecord<C, S>) -> Vec<u8> {
        let mut bytes = serde_json::to_vec(record).expect("wal record serializes");
        bytes.push(b'\n');
        bytes
    }

    /// Encode a [`WalRecord::Snapshot`] line **reusing `metadata_json`** — the
    /// already-serialized state image (the core's cached `snapshot_blob`) — for the
    /// large `metadata` field, so the state is serialized **once** per compaction
    /// rather than twice (once to cache/ship, once for the WAL). The bytes are
    /// identical to `encode_record(&WalRecord::Snapshot { .. })` (a `RawValue`
    /// embeds `metadata_json` verbatim, and JSON object field order is immaterial to
    /// the decoder); the round-trip is guarded by
    /// `snapshot_record_blob_reuse_round_trips`. `metadata_json` MUST be valid JSON
    /// for `S` — it always is, since it comes from `serde_json::to_vec::<S>`.
    #[must_use]
    pub fn encode_snapshot_record_from_blob(
        metadata_json: &[u8],
        last_index: u64,
        last_term: u64,
        config: &Option<BTreeSet<NodeId>>,
    ) -> Vec<u8> {
        use serde_json::value::RawValue;

        // Mirror of `WalRecord::Snapshot`'s fields, with `metadata` as a pre-serialized
        // `RawValue`. A newtype variant wrapping a struct serializes to the identical
        // externally-tagged `{"Snapshot":{..}}` form as the struct variant does.
        #[derive(serde::Serialize)]
        struct SnapshotFields<'a> {
            metadata: &'a RawValue,
            last_index: u64,
            last_term: u64,
            config: &'a Option<BTreeSet<NodeId>>,
        }
        #[derive(serde::Serialize)]
        enum SnapshotRecord<'a> {
            Snapshot(SnapshotFields<'a>),
        }

        let metadata = RawValue::from_string(
            String::from_utf8(metadata_json.to_vec()).expect("snapshot blob is UTF-8 JSON"),
        )
        .expect("snapshot blob is valid JSON");
        let record = SnapshotRecord::Snapshot(SnapshotFields {
            metadata: &metadata,
            last_index,
            last_term,
            config,
        });
        let mut bytes = serde_json::to_vec(&record).expect("snapshot record serializes");
        bytes.push(b'\n');
        bytes
    }

    /// Decode the WAL bytes back into records, ignoring a trailing partial line
    /// (a write torn by a crash — its effect was never acted on).
    pub fn decode(bytes: &[u8]) -> Vec<WalRecord<C, S>> {
        bytes
            .split(|&b| b == b'\n')
            .filter(|line| !line.is_empty())
            .filter_map(|line| serde_json::from_slice(line).ok())
            .collect()
    }

    /// Encode one record tagged with the tablet it belongs to, for a **shared**,
    /// multi-tenant WAL file holding several tablets' `RaftCore` records
    /// interleaved (the single-command-split redesign, `docs/adr/0028-*.md`).
    /// One newline-terminated JSON line, same framing discipline as
    /// [`encode_record`](Self::encode_record).
    #[must_use]
    pub fn encode_tagged_record(tablet: TabletId, record: &WalRecord<C, S>) -> Vec<u8> {
        #[derive(Serialize)]
        struct Line<'a, C, S> {
            tablet: TabletId,
            record: &'a WalRecord<C, S>,
        }
        let mut bytes =
            serde_json::to_vec(&Line { tablet, record }).expect("tagged wal record serializes");
        bytes.push(b'\n');
        bytes
    }

    /// Decode a shared WAL's bytes into `(tablet, record)` pairs, in file order,
    /// ignoring a trailing partial line (a crash-torn write, per [`decode`](Self::decode)).
    pub fn decode_tagged(bytes: &[u8]) -> Vec<(TabletId, WalRecord<C, S>)> {
        #[derive(Deserialize)]
        struct Line<C, S> {
            tablet: TabletId,
            record: WalRecord<C, S>,
        }
        bytes
            .split(|&b| b == b'\n')
            .filter(|line| !line.is_empty())
            .filter_map(|line| serde_json::from_slice::<Line<C, S>>(line).ok())
            .map(|line| (line.tablet, line.record))
            .collect()
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

    /// The blob-reuse snapshot encoder must produce **byte-identical** output to the
    /// normal `encode_record(&WalRecord::Snapshot { .. })`, so compaction can reuse
    /// the cached serialized state (`snapshot_blob`) for the WAL and serialize the
    /// (large) `Metadata` only once per compaction. This guards against `WalRecord::
    /// Snapshot` field drift silently corrupting the WAL: if a field is added/renamed/
    /// reordered, the hand-mirrored encoder diverges and this fails.
    #[test]
    fn snapshot_record_blob_reuse_round_trips() {
        let mut meta = Metadata::default();
        for i in 0..50u64 {
            meta.apply(&MetaCommand::UpsertMember {
                node: i,
                labels: std::collections::BTreeMap::from([("rack".to_string(), format!("r{i}"))]),
                status: NodeStatus::Active,
            });
        }
        let last_index = 123u64;
        let last_term = 7u64;
        let config: Option<BTreeSet<NodeId>> = Some(BTreeSet::from([0, 1, 2]));

        // The cached `snapshot_blob` is `serde_json::to_vec(&metadata)`.
        let blob = serde_json::to_vec(&meta).unwrap();

        let via_reuse = PersistedState::<MetaCommand, Metadata>::encode_snapshot_record_from_blob(
            &blob, last_index, last_term, &config,
        );
        let via_serialize =
            PersistedState::<MetaCommand, Metadata>::encode_record(&WalRecord::Snapshot {
                metadata: meta.clone(),
                last_index,
                last_term,
                config: config.clone(),
            });

        assert_eq!(
            via_reuse, via_serialize,
            "blob-reuse snapshot encoding diverged from the serialized WalRecord — \
             WalRecord::Snapshot fields likely changed; update encode_snapshot_record_from_blob"
        );

        // And it decodes back to the original state (a real round trip).
        let replayed =
            PersistedState::<MetaCommand, Metadata>::replay(
                PersistedState::<MetaCommand, Metadata>::decode(&via_reuse),
            );
        let (state, idx, term) = replayed.snapshot.expect("snapshot present");
        assert_eq!(state, meta);
        assert_eq!((idx, term), (last_index, last_term));
        assert_eq!(replayed.snapshot_config, config);
    }

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
                    voted_for: Some(300),
                },
            ),
        );
        bytes.extend(
            PersistedState::<MetaCommand, Metadata>::encode_tagged_record(
                t2,
                &WalRecord::Hard {
                    term: 5,
                    voted_for: Some(301),
                },
            ),
        );
        bytes.extend(
            PersistedState::<MetaCommand, Metadata>::encode_tagged_record(
                t1,
                &WalRecord::Append(entry(1, 1, upsert(300))),
            ),
        );
        bytes.extend(
            PersistedState::<MetaCommand, Metadata>::encode_tagged_record(
                t2,
                &WalRecord::Append(entry(1, 5, upsert(301))),
            ),
        );
        bytes.extend(
            PersistedState::<MetaCommand, Metadata>::encode_tagged_record(
                t1,
                &WalRecord::Append(entry(2, 1, upsert(302))),
            ),
        );

        let demuxed = PersistedState::<MetaCommand, Metadata>::replay_multiplexed(&bytes);

        assert_eq!(demuxed.len(), 2);
        let s1 = &demuxed[&t1];
        assert_eq!(s1.term, 1);
        assert_eq!(s1.voted_for, Some(300));
        assert_eq!(s1.log.len(), 2);
        assert_eq!(s1.log[0].index, 1);
        assert_eq!(s1.log[1].index, 2);

        let s2 = &demuxed[&t2];
        assert_eq!(s2.term, 5);
        assert_eq!(s2.voted_for, Some(301));
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
                &WalRecord::Append(entry(1, 2, upsert(300))),
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
                voted_for: Some(300),
            },
            WalRecord::Append(entry(10, 4, upsert(300))),
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
}
