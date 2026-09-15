# Getting a torn/corrupted WAL tail ahead of a *fresh-process* restart needs `crash` → `restart` → `stop`, not `stop` alone (`backup_fault_corpus.rs`, ADR 0061 Decision 3)

Wiring `DiskConfig::torn_tail_on_crash`/`corrupt_on_crash` into a
`RaftKvNode`-level corpus scenario that also wants a genuine fresh-process
restart (the `sim.stop` + a brand-new `RaftKvNode::start_hosted` on the same
id/engine idiom `capture_driver_node_crash_restart`/
`raftkv_linearizable.rs`'s `StopRestart` nemesis both use) runs into a real
mismatch between the two primitives: the tear/corrupt logic lives **only**
inside `Simulator::crash` — `Simulator::stop`'s own disk handling
unconditionally clears any buffered-but-unsynced bytes with no tear
consideration at all (see both methods' doc comments), so a scenario that
only ever calls `stop` can set `torn_tail_on_crash: true` all it wants and
never actually exercise it. Reaching first is not enough either: `crash`
also inserts the node into `Simulator`'s `crashed` set, which both the
send and deliver paths check permanently — a node left in that set (nothing
but `restart` ever removes it) has every future send and delivery silently
dropped, including everything a freshly-constructed `RaftKvNode` on that
same id would try to do, which reads as the fresh node being inexplicably
unable to participate in Raft at all rather than as a leftover crashed-flag
bug. The composed fix, safe because nothing drives the executor between
these three synchronous calls (so `restart`'s re-armed old tasks are
removed by the immediately-following `stop` before they're ever re-polled):
`sim.crash(node)` (applies the configured tear/corruption to whatever was
genuinely buffered at that instant), `sim.restart(node)` (clears the
`crashed` flag — its task re-arming is a harmless no-op here), `sim.stop
(node)` (drops those tasks, keeps the now-torn durable state), *then*
construct the fresh node. Generalizes beyond this one corpus: any scenario
combining a `DiskConfig` crash-only fault with a `stop`-based fresh-restart
idiom needs this three-call sequence, not a straight substitution of
`crash` for `stop`.
