# Parallel per-peer maps keyed by the same id belong in one record per peer

`RaftCore` grew five `BTreeMap<NodeId, _>` for the snapshot-send state of a
peer (`snapshot_offset`, `_offset_regressions`, `_chunk_sent`, `_offer_base`,
`_heartbeat_attempts`) and three satellites of `departing`
(`departing_since`, `departing_quiet`, `removal_sched`). Every one needed
"cleared at the identical points" by hand: seven sites for the snapshot maps,
three plus a defensive `contains_key` filter for the departing ones. Two of the
snapshot maps had "absent == default" semantics, and one (`offer_base`) was
correct only because "it is only read while another map still has the peer".
None of that is checkable by a reader or the compiler.

Fix: one map of a per-peer struct (`SnapshotSend`, `DepartingPeer`). Removing
the key drops everything, a wholesale reset is one `clear()`, partial resets
are a small method on the struct (`SnapshotSend::restart_from`), and "absent ==
default" becomes `Default`. Write down the record's existence invariant (here:
a `SnapshotSend` exists iff an offset was acked or a chunk sent) so predicates
like "any transfer in flight" collapse to `is_empty()`.

Checklist when doing this refactor:
- Establish that every satellite write is for a key already in the primary map
  (no stale satellite can survive into a later episode); otherwise merging
  changes behaviour.
- Create the record only where the old code created its first entry; read
  paths and early returns must use `get`, not `entry().or_default()`.
- Preserve any existing predicate asymmetry between sites (do not "fix" it in a
  refactor): here the transfer-completion branch drops the cached image when no
  peer has an *acked* offset, while `forget_snapshot_transfer` requires no
  record at all.
