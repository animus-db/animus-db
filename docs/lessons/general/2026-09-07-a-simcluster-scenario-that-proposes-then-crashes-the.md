# A `SimCluster` scenario that proposes-then-crashes-the-proposer must let the entry replicate first, or it is stranded, not merely delayed (ADR 0061 rung D4 PR 5)

`sim_cluster_backup_janitor.rs`'s scenario (d) (the control-plane leader
crashes right after `MarkBackupDeleted` commits, restarted only once the
survivors have already reclaimed the backup on their own) first called
`SimCluster::propose_meta(MetaCommand::MarkBackupDeleted{..})` and then
`SimCluster::crash(victim)` immediately after, with zero intervening
`run_for` — and hung forever waiting for the row to disappear on the two
survivors. `RaftNode::propose` only appends to the **leader's own local
log** and returns `ProposeResult::Accepted` the instant that append
happens — never "committed to a majority," the exact distinction root
`CLAUDE.md`'s durable-before-visible entry states for every proposer in
this codebase. With no virtual time advanced between the propose and the
crash, the entry had not yet replicated to either follower: `crash` mutes
the leader's outbound sends, so the entry was permanently stranded on a
now-silent log — the two survivors' own `Metadata` never saw the backup
marked `Expired` at all, so their own `backup_janitor_loop` had nothing to
reclaim, and the poll spun to its own 20s budget every time. **Fix**: a
short `run_for` (well under the janitor's own tick interval, so the
scenario still proves the *survivors* — not the about-to-crash leader
itself — do the reclaim) between the propose and the crash, giving the
entry time to replicate and commit to a real majority before the leader
goes silent. **General lesson, not specific to this scenario**: any
`SimCluster` (or `RaftNode<SimEnv>`-driven) test that proposes something
and then immediately crashes/stops/partitions the node it proposed on must
let at least one round of replication happen first — `ProposeResult::
Accepted` is a promise about the *proposer's own log*, never about what
the rest of the cluster has seen, and a fault injected in the same
instant as the propose can turn a durable write into one that was never
really there at all from every other replica's point of view.
