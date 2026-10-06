# Diagnose a chaos failure from the per-group dump, not from the hypothesis

Issue #1228 (chaos finding F-1) said "a full follower acks nothing, so a full
leader loses quorum contact, so ReadIndex cannot complete". Reproducing it as a
`SimEnv` cell showed that story was wrong on every count: the leader kept
leading, ReadIndex probes are a separate message that is never gated, and the
eventual read worked. What actually broke in the simulator was a linearizable
read needing a *committed* `ReadCeiling` that a full group cannot commit. The
real-process run then showed a *third* cause (a step-down that handed leadership
to a voter whose fullness the leader could not see, deposing the only node that
could still serve), visible only in the per-group `/admin/raftkv` dump taken
while the disks were still full (`tablet 2: term 3, n2 Candidate, n0/n1 leader
None`).

Rules:

1. Before designing the fix for a documented finding, build the smallest
   reproduction and read **what each component reports**, not the finding's
   explanation. Three independent mechanisms hid behind one symptom ("reads
   time out").
2. A chaos scenario must be able to dump the evidence it needs at the moment of
   failure: `ANIMUS_CHAOS_KEEP=1` now writes every node's `/admin/raftkv` and
   `/admin/metrics` counters (and the node logs) at the end of the all-full
   phase, before recovery erases the state.
3. A failure that is a race against leadership loss ("some probe saw the 503")
   is a product bug to fix, not an assertion to soften: the F-3 downgrade hid
   the very defect that made reads fail.
