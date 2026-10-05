# A soft (best-effort) placement constraint is never repaired later, so the initial pick must already satisfy it — and "no command can fix this row" means fix the apply, not the caller.

**Found in G-01 stage G-a (zone-spread default).** Two generalizable points:

1. A best-effort `SpreadPolicy` imposes no *hard* constraint (`set_satisfies`), so neither `replan_repair` (violation-driven) nor `rebalance_step` (only "never worsen") ever fixes a replica set that merely *started* skewed. The first tablet's initial replicas therefore have to be chosen with the same policy (`select_replicas_balanced`), not the legacy first-N-by-id pick; the `SimCluster` corpus caught it at once when the zone-aware pick was disabled (replicas `n3,n4,n5` — two zones).
2. A spread policy *drops* candidates lacking the domain label, so a policy decision must also require every member to carry it, or a partially labelled cluster silently shrinks its candidate pool.
3. Labels reach `Metadata` only through the first command that creates the member row, and `bootstrap`'s unlabelled `UpsertMember` can win that race; `UpsertMember` is relayable only as `Down`, so no caller-side retry can repair it. The fix lives in the apply (`RegisterNode` fills labels into an *empty* row), not in more caller logic.
