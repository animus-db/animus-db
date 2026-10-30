# Every grant path must consult the same safety gate as the real vote

A node that must not vote (issue #667: a wiped voter whose boot-time cluster
check is pending) was gated on the real `RequestVote` grant but not on the
pre-vote grant. Pre-vote is "only advisory", so the second path was never
audited, yet its majority is what sends a live follower into a real higher-term
election (which a leader lease then ignores, losing a write in the gap).

When a node-local "must not vote / must not campaign" condition exists, grep
every place a grant, vote or campaign decision is made (pre-vote, real vote,
timeout-now, election start) and give each the same predicate. Test it at the
core level by firing a healthy follower's election timer while the gated node
has not yet heard the leader, and assert no real `RequestVote` is sent. A
one-off CI flake in a wiped-node `ProdEnv` test is a candidate for exactly this
class of ungated side path; make the test's failure message dump per-node role,
term, leader and the campaign counters so the next one is diagnosable.
