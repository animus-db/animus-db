# A newly-built bounded primitive over a shared resource doesn't retroactively fix every existing unbounded call site that has the same shape — grep for siblings, don't assume the one call site you built it for was the only one.

**A newly-built bounded primitive over a shared resource doesn't retroactively
fix every existing unbounded call site that has the same shape — grep for
siblings, don't assume the one call site you built it for was the only one.**
ADR 0034 added `StorageScope::physical_bounds()` specifically so the
byte-estimate auto-split gate wouldn't degrade into a whole-engine scan on an
unbounded-above tablet (a node's tablets share one `StorageEngine`, ADR
0028). But `RaftKvNode::local_scan`'s own unbounded-above branch (`end:
None`) — used by `/admin/raftkv`'s `raft_view` (every hosted tablet, every
request) and by `erase_scope()` (every `Release`/`Reclaim` teardown) — kept
falling through to `storage.entries()`, a genuine **whole node engine** scan,
not just this tablet's own data. Invisible on a lightly-loaded dev cluster;
live-observed as `/admin/raftkv` hanging indefinitely (20s+, no response) on
every node of a cluster that had grown 3→5 and auto-split down to a 20KB
threshold (many tablets sharing each node's engine, actively rebalancing) —
every request paying O(hosted tablets × whole node engine) instead of
O(each tablet's own data). Fixed by routing the unbounded branch through the
same `physical_bounds()` primitive `approx_bytes()` already used, falling
back to `entries()` only for the one case with no finite bound at all
(`StorageScope::whole()`, no prefix). This also transparently fixed
`linearizable_scan`'s unbounded case (the real DynamoDB `Scan`/CQL
full-table-`SELECT` path), which called `local_scan` and had the identical
gap — a production-facing improvement the debug-endpoint symptom didn't
even hint at directly. **When a fix like this lands for one call site, grep
every other call site with the same shape (any `entries()`/unbounded scan
over a scoped resource) before considering the class of bug closed.**
(`animus-cp-data::RaftKvNode::local_scan`.)
