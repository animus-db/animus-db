# An unbounded per-stream queue is invisible until it has its own observability — and once it does, the growth is a *mix* of causes, not one

Investigating the `ProdEnv` `Demux` inbox-growth symptom (ADR 0026: a
stream nobody polls accumulates frames forever) by adding per-stream
frame/byte/last-pop bookkeeping (`ProdEnv::inbox_stats`,
`GET /admin/debug/inboxes`) and running a real `--cluster-control 3
--cluster-data 5 --auto-split-bytes 1000000` cluster under sustained
bulk-seed + concurrent `PutItem` load found the growth is real (~160
MB/min RSS on a workload whose entire on-disk footprint stayed under
150 MB — i.e. the growth was provably not legitimate written data) and
is a **mix of at least two structurally different senders**, not the one
mechanism a first guess would reach for:

1. **Retired split-parent residue** — small (single-digit-KB), one-time
   per retired tablet: a parent's replicas tear down asynchronously
   (`animus-cp-data::host::Reconciler::teardown`, `RECLAIM_STOP_GRACE`
   500ms / `RECLAIM_STOP_TIMEOUT` 10s), so a replica whose own reconciler
   ticks first can keep heartbeating a sibling that already tore its own
   listener down. Bounded per occurrence, but the auto-split cadence
   under heavy write load (here: ~25 splits in under 10 minutes on one
   table) makes the *count* of these residues grow without bound even
   though each one is small.
2. **Replication addressed to a peer that never established a
   consumer at all** — large (tens of MB in a single burst), and the
   dangerous one: a stream whose `ever_polled` was `false` — meaning
   `recv_stream` was never even called for it — already held 26 MB /
   590 frames and 12.9 MB / 343 frames for two tablets this node was not
   a current replica of. The likely mechanism (inferred from `animus-
   cp-data`'s own reconfigure/fork code, not directly instrumented in
   this investigation): a placement rebalance or fork briefly names a
   node as a learner/bootstrap-voter, a leader starts replicating (full
   AppendEntries/InstallSnapshot payload, not bare heartbeats) before
   that node's reconciler tick ever calls `start_hosted`, and a fast
   subsequent rebalance reassigns the replica away before the reconciler
   catches up — leaving a stream with real payload and **no driver that
   will ever exist to poll it**, unlike case 1 where a driver existed and
   tore down.

**The generalizable lesson**: when a resource-growth symptom has an
architectural "known gap" (here, ADR 0026/PR2's own documented "nothing
tears down a stream"), don't stop at confirming the gap exists — a live
measurement with per-stream attribution routinely finds the dominant
contributor is a *different, more specific* mechanism than the one named
in the ticket (here: not just "retired tablets", but "a peer added to a
group's replication set and then removed before its own consumer ever
started" — a live-lagging-replica shape, not a teardown-timing shape).
Sizing a fix (teardown vs. a byte/frame cap vs. both) from the ticket's
own framing alone would size it for the wrong case. A "measure first"
PR earns its keep exactly by surfacing this kind of split before the
"now cap/tear down" PR picks a mechanism.

**A cheap corroborating signal, worth checking before deeper
instrumentation**: comparing total process RSS against `du -sh` of the
node's own on-disk data directory is a fast sanity check that RSS
growth is not simply "more real data" — in this investigation the
on-disk footprint (~146 MB) was two orders of magnitude below the RSS
growth (~1.9 GB over 12 minutes), ruling out the LSM engine/cache as the
explanation before ever looking at the demux.
