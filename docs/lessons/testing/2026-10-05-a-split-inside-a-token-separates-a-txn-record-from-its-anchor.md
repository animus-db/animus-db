# A split key inside a token separates a txn record from its anchor's item

**Context.** R-01 F-2 (`chaos_disk_full` with 2PC ops on) and an intermittent
`chaos-smoke` red (`lost acknowledged append`, `txn-atomicity` half-applied)
looked like regressions of a disk-full PR. They were neither disk-related nor
new: bisecting a ~1-in-12 real-process flake by re-running is hopeless, but the
instrumented failing run named the cause in one log line.

**What happened.** A transaction's record key is `token(anchor) || ...` and sorts
*below* every item of that token. Auto-split picked the byte-weighted median of
the live rows, an item's own key; only a **streamed** table's key was rounded to a
token boundary. With one item per partition key that item is the first row of its
token, so the boundary fell between the token's record and its item. The anchor
stage applied on the right child; `TxnCommit` and recovery were routed by record
key to the left child, which had no record: no-op commits, an orphan-abort
tombstone, a never-resolved intent. It needs a txn anchored on exactly the one
partition key on the boundary, hence rare.

**Lessons.**

- When a real-process chaos run fails rarely, log the *routing facts* (which
  tablet owns the record key vs where the anchor stage applied, plus the tablet
  ranges) at the moment of the anomaly; one failing run then gives the cause.
  Do not try to bisect a low-rate flake with re-runs.
- An invariant that a design doc calls a "documented residual" (here
  `txn.rs`: a split may fall inside a token) is a bug waiting for a workload that
  reaches it. Close it at the single choke point (`decide::align_split_key`).
- A sim cannot find it with default tablet layouts: the repro needs a split whose
  boundary *is* an item key and a txn anchored on that item
  (`sim_cluster_auto_split::h_a_split_never_separates_an_item_from_its_txn_record`).
- A finding's "probable mechanism" is a hypothesis: the earlier tombstone-GC
  attribution (chaos.md Finding 1) was real but did not explain every red.
