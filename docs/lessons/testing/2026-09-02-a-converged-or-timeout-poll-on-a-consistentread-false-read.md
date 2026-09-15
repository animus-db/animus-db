# A converged-or-timeout poll on a `ConsistentRead: false` read proves one replica converged, not the group (2026-09-02, issue #559, ADR 0055).

**A converged-or-timeout poll on a `ConsistentRead: false` read proves one
replica converged, not the group (2026-09-02, issue #559, ADR 0055).**
`dynamo_index_scan.rs`'s `gsi_scan_paginates_and_drains_all_rows` polled a
GSI `Scan` on one node until it reported all 5 rows, then immediately
Limit-paginated the *same* index with no `ConsistentRead` and asserted
every row appeared on exactly one page. It flaked in CI (once absorbed
silently by the `prod-liveness` retry) with a page short one row and no
`LastEvaluatedKey` — the server had genuinely decided the index was
exhausted, one row early. This is not the already-documented
"multi-endpoint read loop" shape (the walk here never changed which node
it talked to) — it is a strictly narrower trap: ADR 0055's freshness gate
(`RaftKvNode::stale_read_ready`) only asks whether a replica has applied
everything *it itself* currently believes committed, never whether that
belief matches the group's true frontier, so *even one fixed node* can
answer a `ConsistentRead: false` request from its own replica one moment
and, the next, silently take ADR 0055 §4's one-hop-forward path to a
sibling replica that hasn't caught up — the connecting endpoint is
constant, the serving replica is not. A prior poll converging against
that same endpoint proves nothing about which replica answers the next
request. Reproducing it required forcing the mechanism deterministically
(pointing the convergence poll and the paginated walk at two different
nodes in a scratch copy of the test) — the natural flake rate here was
too low to catch reliably in 100 unmodified loop iterations even under
added CPU load, which does not mean the bug is rare in the wild, only
that this repro loop's window is narrow. Fixed the way ADR 0055's own
Consequences section prescribes for a walk that cannot ask for the strong
read (a GSI `Scan` rejects `ConsistentRead: true`, ADR 0041 §5):
converge the poll against *every* replica (every node's own address)
before trusting the data is stable enough to paginate, mirroring
`dynamo_query_pagination.rs`'s existing `await_gsi_query_everywhere`.
**Generalized**: a test asserting stability across more than one
eventually-consistent request — paginated or not, same endpoint or
rotating — must either read linearizably or converge every replica
first; "the endpoint didn't change" is not the same guarantee as "the
replica didn't change" once a node can answer its own eventual reads
from more than one place.
