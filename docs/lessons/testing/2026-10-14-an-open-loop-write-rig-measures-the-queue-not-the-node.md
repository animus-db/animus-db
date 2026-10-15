# An open-loop write rig grows without bound past capacity; add a window before reading its RSS

The ADR 0079 density rig proposes at a fixed rate straight into the leader's
Raft group (`ProposeResult::Accepted` means "appended locally"). Nothing on
that path pushes back, so any rate above what the node can apply grows the
committed-but-unapplied backlog (and with it RSS) linearly forever. The
DynamoDB wire does not behave like that: a request waits for its write to
apply and `max_inflight_requests` (2048) bounds the node.

`ANIMUS_BP_LIVE_WRITE_WINDOW=N` closes the loop (sheds a put while the
leader's commit-minus-engine-applied backlog exceeds N entries) and the hold
lines carry `log_len_sum` / `apply_lag_sum` / `apply_lag_max` / `writes_shed`.
Read a memory figure from a run with a window (or at a rate you have shown
the node keeps up with), and read the open-loop figure only as "what an
overloaded node queues". Issue #1304's 2.7 GB was both at once: a real
replication amplification (fixed) on top of an open-loop overload (a rig
property).
