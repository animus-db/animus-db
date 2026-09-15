# `Simulator::stop` does not clear the `crashed` flag `Simulator::crash` set — composing them silently mutes the reconstructed node forever (quiescence corpus fault-primitives wiring)

Writing a crash-based test cell for `animus-cp-data/tests/quiescence.rs`
(ADR 0061 Decision 3, giving `DiskConfig::torn_tail_on_crash`/
`corrupt_on_crash` real teeth — see the next entry) needed the genuine
process-restart shape the sibling `raftkv` corpus's `StopRestart` nemesis
already uses: `Simulator::stop` (kills tasks + volatile state, keeps
durable disk) followed by a fresh `RaftKvNode::start` on the same node id,
which recovers from the durable WAL. But `torn_tail_on_crash`/
`corrupt_on_crash` only fire inside `Simulator::crash`, not `stop` (see the
"a crash-only fault has zero test teeth" pattern this composition exists to
avoid) — so the natural-looking sequence is `crash` (to tear/corrupt the
un-synced tail) → `stop` (to kill the task so a fresh one can be
constructed) → reconstruct. That sequence silently breaks: `crash` inserts
the node into `Simulator`'s shared `crashed: BTreeSet<NodeId>`, and `stop`'s
own doc says outright "Unlike `crash`, this does not mute or set the node
`crashed`" — meaning it also doesn't **clear** it. Every message to or from
a node still in `crashed` is dropped (`DROP ... (crashed)` /
`DROP ... (sender-crashed)` in the trace), so the freshly reconstructed
node — despite having brand-new tasks and a live env — never sends or
receives a single message for the rest of the run. This is genuinely quiet:
no panic, no error, just a replica that sits at its pre-crash term with
`engine_applied_index() == 0` forever, which reads exactly like "the
recovered WAL was empty" rather than "the network is silently muted" — the
first draft of this test's own failure looked like a WAL-recovery bug for
several debugging passes before an `eprintln!` of `is_leader`/`term`/
`engine_applied_index` plus the trace tail made the all-drops pattern
obvious. The fix is one extra call: `sim.crash(id); sim.stop(id);
sim.restart(id);` (clears `crashed`; `restart`'s own re-arm step finds
nothing to re-arm, since `stop` already removed every task it owned) —
*then* construct the fresh node. **General rule**: two fault primitives
that individually look composable (each has its own clear, narrow doc)
should still be traced through each other's state machine before combining
them in a new way no existing test does — `crash`+`stop` is exactly the
kind of pairing where each method's doc is accurate in isolation but their
combined effect on a third piece of shared state (`crashed`) is only
obvious from reading both source bodies side by side, not from either doc
comment alone.
