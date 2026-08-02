//! Globally-comparable transaction timestamps.
//!
//! Accord gives every transaction a unique, monotonic, totally-ordered
//! timestamp. We use a Lamport-style `(logical, node)` pair: the logical
//! component is a monotonically advanced counter, and the node id breaks ties so
//! two transactions can never share a timestamp. The total order on
//! `(logical, node)` is the order transactions execute in.

use animus_env::NodeId;
use serde::{Deserialize, Serialize};

/// A transaction timestamp: a logical clock value tagged with the node that
/// minted it. Ordered first by `logical`, then by `node`, so the order is total
/// and every distinct `(logical, node)` is unique.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
pub struct Timestamp {
    /// The logical-clock component. Advanced past every timestamp this node has
    /// observed, so it never goes backwards.
    pub logical: u64,
    /// The minting node id, used purely as a tiebreaker for a total order.
    pub node: NodeId,
}

impl Timestamp {
    /// The zero timestamp (precedes every minted timestamp on a given node).
    pub const ZERO: Timestamp = Timestamp {
        logical: 0,
        node: 0,
    };

    /// Construct a timestamp from its parts.
    #[must_use]
    pub fn new(logical: u64, node: NodeId) -> Timestamp {
        Timestamp { logical, node }
    }
}

/// A **recovery ballot** (ADR 0011): the proposal number a recovery coordinator
/// runs under, so duelling recoverers converge deterministically. Ordered first
/// by `round`, then by the recovering `node` (tiebreak), so the order is total
/// and two recoverers can never share a ballot.
///
/// The implicit [`Ballot::ZERO`] is the *original* coordinator's ballot — every
/// recoverer mints `round >= 1`, so a recoverer always outranks the original
/// coordinator's steady-state `Accept`. A replica promises the highest ballot it
/// has seen for a transaction and rejects any `Recover`/`Accept` carrying a lower
/// one, reporting the promised ballot so a superseded recoverer can retry higher.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
pub struct Ballot {
    /// The proposal round. The original coordinator runs at round 0; recoverers
    /// mint strictly-increasing rounds (`>= 1`), bumping past any promised ballot.
    pub round: u64,
    /// The recovering node id, a tiebreaker so two recoverers at the same round
    /// hold distinct, totally-ordered ballots.
    pub node: NodeId,
}

impl Ballot {
    /// The original coordinator's implicit ballot (lower than every recoverer's).
    pub const ZERO: Ballot = Ballot { round: 0, node: 0 };

    /// A recovery ballot for `node` at `round`.
    #[must_use]
    pub fn new(round: u64, node: NodeId) -> Ballot {
        Ballot { round, node }
    }

    /// This node's recovery ballot one round above `highest` — the ballot a
    /// recoverer adopts to supersede every ballot promised so far (`highest` is
    /// the maximum promised ballot it has learned of, [`Ballot::ZERO`] if none).
    #[must_use]
    pub fn next_above(highest: Ballot, node: NodeId) -> Ballot {
        Ballot {
            round: highest.round + 1,
            node,
        }
    }
}

/// A per-node logical clock. Minting always returns a value strictly greater
/// than any timestamp this clock has seen, keeping minted timestamps monotonic
/// even after observing a peer's higher timestamp (`witness`).
#[derive(Clone, Debug)]
pub struct LogicalClock {
    node: NodeId,
    highest: u64,
}

impl LogicalClock {
    /// A fresh clock for `node`, starting at logical 0.
    #[must_use]
    pub fn new(node: NodeId) -> LogicalClock {
        LogicalClock { node, highest: 0 }
    }

    /// Advance past `ts` so a later mint outranks it. Idempotent and monotonic.
    pub fn witness(&mut self, ts: Timestamp) {
        self.highest = self.highest.max(ts.logical);
    }

    /// Mint a fresh timestamp strictly greater than everything seen so far,
    /// tagged with this node id.
    pub fn mint(&mut self) -> Timestamp {
        self.highest += 1;
        Timestamp {
            logical: self.highest,
            node: self.node,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_is_logical_then_node() {
        assert!(Timestamp::new(1, 9) < Timestamp::new(2, 0));
        assert!(Timestamp::new(2, 0) < Timestamp::new(2, 1));
        assert_eq!(Timestamp::new(3, 4), Timestamp::new(3, 4));
    }

    #[test]
    fn ballot_order_is_round_then_node() {
        assert!(Ballot::ZERO < Ballot::new(1, 0));
        assert!(Ballot::new(1, 9) < Ballot::new(2, 0));
        assert!(Ballot::new(2, 0) < Ballot::new(2, 1));
        assert_eq!(Ballot::new(3, 4), Ballot::new(3, 4));
    }

    #[test]
    fn ballot_next_above_supersedes() {
        // A recoverer always outranks the highest promised ballot, regardless of
        // which node held it.
        let highest = Ballot::new(5, 2);
        let mine = Ballot::next_above(highest, 0);
        assert!(mine > highest, "next_above must supersede the highest seen");
        // From the zero (original-coordinator) ballot, the first recoverer is
        // round 1.
        assert_eq!(Ballot::next_above(Ballot::ZERO, 1), Ballot::new(1, 1));
    }

    #[test]
    fn mint_is_monotonic_across_witness() {
        let mut clock = LogicalClock::new(7);
        let a = clock.mint();
        clock.witness(Timestamp::new(100, 2));
        let b = clock.mint();
        assert!(b > a);
        assert!(b.logical > 100, "mint must outrank a witnessed peer");
        assert_eq!(b.node, 7);
    }
}
