# A forwarded RPC's serve arm must run the SAME confirm implementation as the caller's own local arm — two implementations for one RPC diverge the moment a new payload shape arrives, and the failure is leader-placement-bimodal.

**A forwarded RPC's serve arm must run the SAME confirm implementation as
the caller's own local arm — two implementations for one RPC diverge the
moment a new payload shape arrives, and the failure is
leader-placement-bimodal.** `cp_kind_write_raw`'s local arm confirmed a
raw kind batch on its *last* write, tolerating a tombstone (`None`)
probe; `cp_serve_forwarded`'s `KindWrite` arm confirmed the identical
batch via `cp_kind_local`, whose confirm *requires* a `Some`-valued base
write. The two agreed for every payload shape that existed when they were
written (the GSI drain's cursor/footprint puts) and disagreed on the
first new shape (ADR 0049 Train A rung 2's whole-partition CQL DELETE — a
batch whose base write is a tombstone): the delete succeeded iff the
serving node happened to lead the tablet, an election-dependent bimodal
failure that one pre-existing e2e (`cql_clustering`) only caught by luck
of leader placement. Two lessons: (1) when a request can be served
locally or forwarded, extract the serve body into ONE function called
from both arms (`ClientCtx::cp_kind_raw_local`) — the local/forward split
is transport, never semantics; (2) the existing "every internal RPC needs
at least one non-leader-issued call in its suite" rule applies per
*payload shape*, not per RPC — a new shape through an old RPC needs its
own follower-connected regression
(`cql::cql_kind_write_tests::cql_whole_partition_delete_serves_from_every_node`,
red on the two-implementation code with exactly the diagnosed refusal).
