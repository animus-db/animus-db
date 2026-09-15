# A crash-only disk fault has zero test teeth unless something later reads the crashed node's own post-crash state back (`pitr_fault_corpus.rs`, ADR 0061 Decision 3 wiring)

Wiring `DiskConfig::torn_tail_on_crash`/`corrupt_on_crash` into a new
`scenario_wal_torn_on_crash_kill_sealing_leader` cell surfaced a design trap
worth naming on its own, separate from the already-documented `crash` →
`restart` → `stop` sequencing gotcha (see the entries this generalizes from,
found by the sibling backup-corpus PR): in this corpus,
`current_open_pitr_epoch` — the very value the new cell exists to protect —
is derived **only** from the hand-scripted `Metadata` struct the test drives
directly with `.apply()` calls, never from anything read off the crashing
node's own Raft WAL or storage engine. Copying `scenario_kill_sealing_leader`
verbatim (crash a node, fail over to the two survivors, never look at the
crashed node again) and simply turning on the torn-tail disk config would
have compiled, run green, and proven **nothing**: the tear sits on a file
nothing in the scenario ever reads again. The fault only gets real teeth once
the crashed node is brought all the way back — `crash` → `restart` → `stop` →
a fresh `RaftKvNode::start_hosted` on the same id/engine — and made to
participate in a *further* round of writes and a second seal, so its own
recovery from the torn/corrupted tail can actually influence what the next
`pending_changes()` scan (and therefore the next epoch number) sees. The
general check this generalizes to: before wiring a disk- or network-fault
primitive into an existing scenario as a "near-copy plus one fault call",
trace where the property-under-test's own inputs actually come from — a
fault that lands on state nothing downstream ever reads is a no-op with a
green checkmark, not a stronger test.
