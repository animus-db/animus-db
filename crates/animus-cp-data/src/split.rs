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
//! **`bootstrap_voters` vs. `SplitChild::replicas` (ADR 0062 rung 4:
//! "fork first, always local").** Both fields carry the SAME set now — the
//! parent's own current replicas, verbatim — but they still mean different
//! things and neither is derived from the other: `SplitChild::replicas` is
//! `trigger_split`'s own proposer-side computation (`meta.tablets[parent]
//! .replicas.clone()`, ADR 0062 §1), captured at `BeginSplitInPlace`
//! propose time and carried unchanged through to this apply; `bootstrap_
//! voters` is captured once HERE, independently, from the parent's own
//! live `RaftCore::config()` at the exact moment `SplitTablet` applies
//! (deterministic because every replica that reaches this apply has, by
//! Raft log order, already applied every earlier config-change entry, so
//! this read produces the IDENTICAL set on every replica). The two values
//! coincide in the common case (nothing reconfigured the parent's voters
//! between propose and apply) and can legitimately diverge if an ordinary
//! rebalance/repair committed a membership change on the parent in that
//! window — `bootstrap_voters`, not `SplitChild::replicas`, is what both
//! children's local `RaftKvNode`s actually bootstrap their **voter** config
//! from, since it is the freshest agreed answer to "who has the parent's
//! data right now."
//!
//! **No more learner union (ADR 0062 rung 4, fork D — accepted residual).**
//! `bootstrap_voters` used to be `RaftCore::config() ∪ RaftCore::
//! learners()` — a strict superset of the parent's own voters, engineered
//! so both children could start over-replicated relative to their
//! (placement-chosen, possibly off-parent) final homes, with the ordinary
//! reconciler's `reconfigure_step` trimming the rest afterward (ADR 0058's
//! Stage 5). Since a child's replicas are now the parent's own replicas —
//! never a disjoint placement-chosen home — there is nothing left to
//! recruit or trim: `bootstrap_voters` is the parent's voter set ONLY, both
//! children bootstrap directly onto it, and Stage 5's trim step never has
//! anything to do at fork time. The accepted cost: an unrelated,
//! in-flight, ordinary rebalance's own learner on the parent at this exact
//! apply is no longer inherited by either child (it used to ride along in
//! the union as a bonus, half-caught-up member) — it is simply absent from
//! both, and a fresh Placing/reconcile pass re-adds it to whichever child
//! it still belongs on via the ordinary add-learner → catch-up → promote
//! sequence, from scratch, self-healing at the cost of at most one extra
//! `reconfigure_step` hop.
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
            // `n` is a length prefix read before its elements are
            // validated against the remaining buffer. This marker is
            // normally only ever this crate's own write (see the doc
            // above), but — like `txn.rs`'s envelope decoder — a
            // corrupted/adversarial `InstallSnapshot` image could smuggle
            // a crafted value straight into the engine, bypassing the
            // `KvCommand` decode path entirely. Cap the requested
            // capacity so a hostile `n` near `u32::MAX` can't demand a
            // many-GB allocation and trigger an allocator abort.
            let mut out = Vec::with_capacity(n.min(1 << 20));
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
