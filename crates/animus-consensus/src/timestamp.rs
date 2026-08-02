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
