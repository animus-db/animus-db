# "The replicated catalog is identical everywhere" is a converged fact, not an instantaneous one — comparing it across nodes right after a write still needs its own poll (docs/roadmap.md U-07, `GET /admin/segment-store`)

`GET /admin/segment-store`'s own regression test creates a streamed table,
writes an item, waits for the write to seal into a `Metadata::
stream_shards` row, then asserts every node's own `/admin/segment-store`
response carries the identical `shards` array — true in the converged
state, since `Metadata` is Raft-replicated (ADR 0038) and every combined
node in the test runs its own full local control `RaftCore`. The first
draft of this assertion read every node's view **once**, in a single pass,
immediately after confirming (on the LEADER's own metadata) that the row
existed — and failed intermittently: one follower's own `effective_
metadata()` (`control.metadata_cached()`, a purely local read with no
network round trip) still showed `stream_shards` empty, because that
replica's own log hadn't yet applied the `SealStreamShard` entry the
leader had already committed and applied moments earlier. The node in
question had, in the same instant, already received and stored the
segment's physical *bytes* (`ClusterSegmentStore::put_replicated` pushes
data directly to its chosen replicas over the network, independently of
the metadata proposal that records the placement) — so `local_objects.
count` was already `1` there while `shards` was still `[]`: two different
facts about the same event, propagating on two different channels
(direct replication vs. Raft-committed metadata), at two different
speeds.

The fix was not to change what the route reports (both facts are
correctly, independently true at the instant each was read) but to change
how the TEST compares them: a `timeout(..)` loop re-fetching every node's
view on each iteration and only returning once every node's `shards`
array is simultaneously non-empty AND pairwise equal — the same
converged-or-timeout idiom this codebase already uses for the leader's
own commit visibility, applied here to a cross-node **comparison**
instead of a single node's own state.

The generalizable rule: "every node mirrors the identical replicated
`Metadata`" is a true statement about the CONVERGED cluster, never a
promise that two nodes read at the same wall-clock instant already agree
— especially when a test's own setup step confirms a fact on one node
(the leader) and then immediately fans out to read every node including
followers whose local apply has its own independent lag. Any assertion of
the form "node A's view equals node B's view" needs the identical
converged-or-timeout discipline this codebase already applies to "did my
own write show up yet" — a bare one-shot snapshot comparison across
nodes is exactly as flaky as a bare one-shot snapshot of a single node's
own eventual state, for the same underlying reason.
