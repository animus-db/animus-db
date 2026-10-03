//! The one authoritative allocation table of reserved ADR 0026 network
//! stream ids, plus a compile-time proof that they are pairwise distinct
//! (issue #1055).
//!
//! `(node, stream)` is single-consumer (ADR 0026): two serve loops bound to
//! the same stream on the same node silently steal each other's frames.
//! This crate is the lowest one that depends on every crate minting a
//! reserved stream (`animus-env`, `animus-cp-data`, itself), so the table
//! and the assertion live here. Every other doc points at this one.
//!
//! | Stream | Owner | Value |
//! |---|---|---|
//! | [`animus_env::PRIMARY_STREAM`] | every pre-multiplexing protocol (control-plane Raft group; a non-split CP tablet's Raft group) | `0` |
//! | a CP data-plane tablet's own Raft group | `animus-cp-data`'s host reconciler, `animusd`'s `RaftKvNode` wiring | `tablet.0` (small, sequential, never near `u64::MAX`) |
//! | [`animus_cp_data::cluster_segment_store::SEGMENT_STREAM`] | the DynamoDB Streams segment store's `ClusterSegmentStore` | `u64::MAX` |
//! | [`animus_cp_data::backup::BACKUP_SEGMENT_STREAM`] | the on-demand backup store's `ClusterSegmentStore` | `u64::MAX - 1` |
//! | [`animus_cp_data::heartbeat_batch::HEARTBEAT_BATCH_STREAM`] | the per-node `HeartbeatBatcher` | `u64::MAX - 2` |
//! | [`crate::sim_relay::RELAY_STREAM`] | `SimRelayClient`'s request/reply traffic | `u64::MAX - 3` |
//!
//! All ids above `0` sit inside `animus_env::is_reserved_stream`'s block
//! (the 16 ids below `u64::MAX`), which exempts them from `InboxCap`. A new
//! reserved stream takes the next free value (`u64::MAX - 4`, ...), is added
//! to this table and to [`ALL_RESERVED`], and the `const` assertion below
//! then proves it collides with nothing.

use animus_cp_data::backup::BACKUP_SEGMENT_STREAM;
use animus_cp_data::cluster_segment_store::SEGMENT_STREAM;
use animus_cp_data::heartbeat_batch::HEARTBEAT_BATCH_STREAM;
use animus_env::PRIMARY_STREAM;

use crate::sim_relay::RELAY_STREAM;

/// Every reserved stream id in the workspace (see the module doc's table).
pub const ALL_RESERVED: [u64; 5] = [
    SEGMENT_STREAM,
    BACKUP_SEGMENT_STREAM,
    HEARTBEAT_BATCH_STREAM,
    RELAY_STREAM,
    PRIMARY_STREAM,
];

const fn pairwise_distinct(ids: &[u64]) -> bool {
    let mut i = 0;
    while i < ids.len() {
        let mut j = i + 1;
        while j < ids.len() {
            if ids[i] == ids[j] {
                return false;
            }
            j += 1;
        }
        i += 1;
    }
    true
}

const _: () = assert!(
    pairwise_distinct(&ALL_RESERVED),
    "two reserved ADR 0026 stream ids collide (see reserved_streams module doc)"
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reserved_streams_are_distinct_and_in_the_exempt_block() {
        assert!(pairwise_distinct(&ALL_RESERVED));
        for s in ALL_RESERVED {
            assert!(animus_env::is_reserved_stream(s), "{s} outside block");
        }
    }
}
