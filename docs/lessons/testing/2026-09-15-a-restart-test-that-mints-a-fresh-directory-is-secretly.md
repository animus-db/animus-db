# A restart test that mints a fresh directory for the "restarted" node is secretly testing a wiped-voter restart, not an ordinary one.

`advertise_host.rs::same_identity_restart_on_a_different_bind_ip_keeps_
the_same_advertised_identity` shut node 1 down and "restarted" it at
`dir.path().join("moved")` — a directory distinct from the one it
originally bound (`dir.join(format!("core-{attempt}-1"))`). Before issue
#667's boot-time cluster check existed, this didn't matter: a node with
an empty WAL just replayed nothing and rejoined as an ordinary follower,
indistinguishable (to this test) from a real preserved-disk restart. Once
the cluster check landed, this test became a 100%-reproducible instance
of the exact hazard the check exists to catch — an already-established
voter (real committed history, named in its peers' config, actually
voted before) restarting with a wiped store into the still-running
cluster — and was correctly refused, deadlocking cluster bring-up
("cluster did not bootstrap within 30s").

The fix was not to weaken the safety check; it was to fix the test to
express what its own name and doc comment already claimed to test: an
`advertise_host` change on a node whose **disk survives** the move (the
real production scenario — a rescheduled pod keeps its PVC, only its IP
changes). `bring_up_with_config` was changed to return each node's own
data directory, and the "restart" reuses `node_dirs[1]` instead of
minting `"moved"`. The fixed test also runs far *faster* (sub-second vs.
the original's real bring-up time) — a genuine WAL-recovering restart
skips the whole empty-WAL cluster-check code path entirely and rejoins
via ordinary catch-up.

**The general lesson**: before writing (or fixing) a test that "restarts"
a node, check whether it reuses the *same* data directory or mints a
fresh one. A fresh directory is not a cheap approximation of a restart —
it is a genuine simulated disk wipe, and once any safety mechanism keys
on "empty store rejoining an established cluster" (this one, or any
future one), a fresh-directory "restart" test either becomes a false
positive for that mechanism (as here) or silently stops testing the
restart property it claims to, whichever the mechanism's absence used to
paper over. If the scenario under test is about network/address identity
across a restart, not about storage loss, the directory must be
preserved.
