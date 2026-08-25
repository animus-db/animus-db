//! The **in-place split fork marker** (ADR 0058 Train 2 rung 3, Stage 3):
//! the durable record of a parent group's own [`crate::KvCommand::SplitTablet`]
//! apply — mirrors `seal.rs`'s marker discipline exactly (an engine-global key
//! outside every kind scope, surviving log compaction), but carries the
//! **payload** a fork needs to be rediscovered after a restart or by the host
//! reconciler: the split key, both children's ids/final replica sets, and the
//! bootstrap voter set BOTH children's local Raft groups start with.
//!
//! Unlike `seal.rs`'s seal marker (keyed by `(tablet, range)`, since one
//! tablet can seal more than one range over its lifetime), a tablet forks **at
//! most once** — a parent that forks retires for good (ADR 0058 Train 2 Stage
//! 4) — so this marker is keyed by `tablet` alone, and a point [`get`] suffices
//! to read it back; no scan is needed the way `seal.rs`'s does.
//!
//! **`bootstrap_voters` vs. `SplitChild::replicas`.** A child's own
//! `replicas` field is its placement-chosen **final** homes — what
//! `MetaCommand::CutoverSplit` (Stage 4) records as the tablet's `replicas`
//! in `Metadata`, driving the ordinary reconciler's trim (Stage 5).
//! `bootstrap_voters` is a DIFFERENT, larger set: the parent's own full
//! voter-plus-learner config at the exact moment `SplitTablet` applies,
//! captured once by the apply arm (`RaftCore::config() ∪ RaftCore::
//! learners()`, read while holding the core lock) — deterministic because
//! every replica that reaches this apply has, by Raft log order, already
//! applied every earlier config-change entry, so this read produces the
//! IDENTICAL set on every replica. Both children bootstrap their own local
//! `RaftKvNode` with `bootstrap_voters` as the initial **voter** config —
//! every node that has the parent's fully-replicated data (original voter
//! or caught-up learner) can safely vote for both new groups immediately,
//! which is what makes Stage 5's "each child is over-replicated relative to
//! its own final RF" true by construction: a node in `bootstrap_voters` but
//! NOT in a given child's own `replicas` is exactly what the ordinary
//! reconciler's `reconfigure_step` trims off afterward, no new mechanism.
//!
//! [`get`]: animus_storage::StorageEngine::get

use std::collections::BTreeSet;

use animus_control::syskv::RESERVED_NAMESPACE;
use animus_env::NodeId;
use animus_tablet::{SplitChild, TabletId, escape};

use crate::hlc::HlcTimestamp;

/// The segment distinguishing this crate's fork-marker keys from every other
/// reserved-namespace user — chosen not to collide with `seal.rs`'s
/// `SEAL_TAG`/`ceiling.rs`'s own tag or any `syskv::EntityKind` segment.
const SPLIT_TAG: &[u8] = b"cp_split";

/// The physical, engine-global key for `tablet`'s own fork marker. Disjoint
/// from every table's physical keys and from every other kind scope by the
/// identical argument `seal.rs::seal_marker_key`'s doc gives (this crate's
/// reserved-namespace lead byte, `0x5F`, never collides with a kind byte, all
/// of which top out at `0x04`); disjoint from `seal.rs`'s own marker keys and
/// `ceiling.rs`'s by the distinct tag segment.
pub(crate) fn split_marker_key(tablet: u64) -> Vec<u8> {
    let mut out = escape(RESERVED_NAMESPACE.as_bytes());
    out.extend_from_slice(&escape(SPLIT_TAG));
    out.extend_from_slice(&tablet.to_be_bytes());
    out
}

/// One decoded fork marker — see the module doc for what each field means
/// and why `bootstrap_voters` is distinct from either child's own
/// `replicas`.
pub(crate) struct DecodedSplit {
    pub(crate) split_key: Vec<u8>,
    pub(crate) children: [SplitChild; 2],
    pub(crate) bootstrap_voters: BTreeSet<NodeId>,
    pub(crate) ts: HlcTimestamp,
}

/// The value stored at a fork marker's key. See the module doc.
pub(crate) fn encode_split_value(
    split_key: &[u8],
    children: &[SplitChild; 2],
    bootstrap_voters: &BTreeSet<NodeId>,
    ts: HlcTimestamp,
) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(split_key.len() as u32).to_be_bytes());
    out.extend_from_slice(split_key);
    for child in children {
        out.extend_from_slice(&child.id.0.to_be_bytes());
        put_node_list(&mut out, &child.replicas);
    }
    put_node_list(
        &mut out,
        &bootstrap_voters.iter().cloned().collect::<Vec<_>>(),
    );
    out.extend_from_slice(&ts.wall_ms.to_be_bytes());
    out.extend_from_slice(&ts.logical.to_be_bytes());
    out
}

fn put_node_list(out: &mut Vec<u8>, nodes: &[NodeId]) {
    out.extend_from_slice(&(nodes.len() as u32).to_be_bytes());
    for r in nodes {
        let bytes = r.as_str().as_bytes();
        out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
        out.extend_from_slice(bytes);
    }
}

/// The exact inverse of [`encode_split_value`]. `None` on malformed input —
/// an engine-internal marker this crate itself wrote should never be
/// malformed; a decode failure indicates real corruption.
pub(crate) fn decode_split_value(bytes: &[u8]) -> Option<DecodedSplit> {
    struct Cursor<'a> {
        bytes: &'a [u8],
        pos: usize,
    }
    impl Cursor<'_> {
        fn take(&mut self, n: usize) -> Option<Vec<u8>> {
            let s = self.bytes.get(self.pos..self.pos + n)?;
            self.pos += n;
            Some(s.to_vec())
        }
        fn u32(&mut self) -> Option<u32> {
            Some(u32::from_be_bytes(self.take(4)?.try_into().ok()?))
        }
        fn u64(&mut self) -> Option<u64> {
            Some(u64::from_be_bytes(self.take(8)?.try_into().ok()?))
        }
        fn node_list(&mut self) -> Option<Vec<NodeId>> {
            let n = self.u32()? as usize;
            let mut out = Vec::with_capacity(n);
            for _ in 0..n {
                let len = self.u32()? as usize;
                let raw = self.take(len)?;
                let s = String::from_utf8(raw).ok()?;
                out.push(NodeId::new_unchecked(s));
            }
            Some(out)
        }
    }
    let mut c = Cursor { bytes, pos: 0 };
    let split_key_len = c.u32()? as usize;
    let split_key = c.take(split_key_len)?;
    let mut children = Vec::with_capacity(2);
    for _ in 0..2 {
        let id = TabletId(c.u64()?);
        let replicas = c.node_list()?;
        children.push(SplitChild { id, replicas });
    }
    let bootstrap_voters: BTreeSet<NodeId> = c.node_list()?.into_iter().collect();
    let wall_ms = c.u64()?;
    let logical = u32::from_be_bytes(c.take(4)?.try_into().ok()?);
    let children: [SplitChild; 2] = children.try_into().ok()?;
    Some(DecodedSplit {
        split_key,
        children,
        bootstrap_voters,
        ts: HlcTimestamp { wall_ms, logical },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use animus_env::nid;

    #[test]
    fn key_disjoint_from_every_kind_scope_and_from_the_seal_marker() {
        let marker = split_marker_key(7);
        assert_eq!(marker[0], 0x5F, "marker keys lead with escape('__…')");
        assert!(crate::ALL_KINDS.iter().all(|&k| k < 0x5F));
        let seal = crate::seal::seal_marker_key(7, &animus_tablet::KeyRange::whole());
        assert_ne!(
            marker, seal,
            "split and seal markers must use distinct tags"
        );
    }

    #[test]
    fn split_value_round_trips() {
        let children = [
            SplitChild {
                id: TabletId(2),
                replicas: vec![nid(1), nid(2), nid(3)],
            },
            SplitChild {
                id: TabletId(3),
                replicas: vec![nid(4), nid(5)],
            },
        ];
        let bootstrap_voters: BTreeSet<NodeId> = [nid(0), nid(1), nid(2), nid(3), nid(4), nid(5)]
            .into_iter()
            .collect();
        let ts = HlcTimestamp {
            wall_ms: 12_345,
            logical: 7,
        };
        let bytes = encode_split_value(b"split-key", &children, &bootstrap_voters, ts);
        let decoded = decode_split_value(&bytes).expect("decodes");
        assert_eq!(decoded.split_key, b"split-key");
        assert_eq!(decoded.children, children);
        assert_eq!(decoded.bootstrap_voters, bootstrap_voters);
        assert_eq!(decoded.ts, ts);
    }

    #[test]
    fn distinct_tablets_get_distinct_keys() {
        assert_ne!(split_marker_key(1), split_marker_key(2));
    }
}
