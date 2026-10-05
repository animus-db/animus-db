# A TxnId must be unique per tablet group, not per node

**What happened.** chaos_disk_full with 2PC ops lost one half of a
transaction. Trace: txn `9180/18 n0` appeared as two different transactions
(anchors on tablets 2 and 3, same leader node). `TxnId = (ts, node)` where `ts`
is minted by the *group's own* `Hlc`; the doc claimed the node tiebreak
separated groups, but one node leads many groups. A's participant resolve
applied on tablet 2 right after B's anchor stage and committed B's intent early.

**Rule.** An id minted from per-group state must be qualified by the group
(`RaftKvNode::txn_id_node`: `n0#<stream>`, primary stream unchanged so formats
and fixtures stay identical). Never rely on node identity to disambiguate
per-group counters.

**Two sibling findings from the same hunt** (all pre-existing, none from the
disk-full commits): a split key inside a token separates a txn record from its
anchor item (`decide::align_split_key`, see the split-inside-a-token lesson);
and a txn group staged against a stale tablet snapshot must be refused at
propose time when any key is outside the leader's range.

**How it was found.** Temporary `txndbg` tracing at the four apply sites, then a
deterministic two-groups-one-node repro. Dead-end worth skipping: bisecting the
four PR commits (failure rate ~1/12, none introduced it).
