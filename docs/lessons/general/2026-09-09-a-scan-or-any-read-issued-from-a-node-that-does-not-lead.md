# A `Scan` (or any read) issued from a node that does not lead the tablet, right after a write, needs `ConsistentRead: true` — even in a from-scratch deterministic `SimEnv` test, and even when the pattern was copied from an existing test that had the identical gap (ADR 0021 amendment, `sim_cluster_seed_latency.rs`, 2026-09-09)

Rewriting `sim_cluster_seed_latency.rs` for the new client-side-shaped
seeder kept its pre-existing verification shape verbatim: seed N rows from
a node that does not lead the table's tablet (`non_leader_of_table`), then
`Scan` the same node with `{"Select":"COUNT"}` and assert the count equals
N. 1 of 5 `_over_seeds` seeds failed this assertion deterministically (199
of 200 rows), even though the seed call's own `written` field correctly
reported 200 and every one of its writes had already durably committed
before the call returned.

**Root cause: the `Scan` carried no `ConsistentRead`, so it defaulted to
`false`** — ADR 0055's eventually-consistent read path, served from
whichever replica answers, with no read barrier against the leader's own
latest commit. `non_leader_of_table`'s whole point is to prove the seed's
own forwarding path works, which means the node serving the verification
`Scan` is, by construction, not the node that just committed every write —
so the `Scan`'s own local replica state can legitimately lag the write
it's trying to observe. This is precisely the gotcha `animusd/CLAUDE.md`'s
ADR 0055 section already names in so many words ("a read that verifies a
write must ask for `ConsistentRead: true`... the failure is a race, so one
green run of a binary proves nothing") — but it was missed on first read
because the **pre-existing, pre-rewrite version of this exact test had the
identical gap** (its own `Scan` call also omitted `ConsistentRead`, also
against a non-leader node) and had apparently never been observed to fail
across its prior lifetime. Copying a working-looking pattern from an
existing test file is not the same as that pattern being correct — a
latent race can sit unfired for a long time and then fire on the very next
seed a rewrite happens to add coverage at.

**Fixed** by adding `"ConsistentRead":true` to the verification `Scan` —
reproduced first with `ANIMUS_SEED=<seed>` against the single-seed test
(this crate's own standard replay convention) to confirm the exact failure
deterministically, then confirmed fixed the same way before re-running the
full `_over_seeds` sweep (three clean repeats). **The generalizable rule,
restated once more because this is not the first time it's been the actual
cause of a `SimCluster` test finding a "bug" that was really a missing
`ConsistentRead`**: whenever a test verifies a write by reading it back,
check whether the read is `ConsistentRead: true` and whether the node
serving it is the tablet's own leader — if either is no, the read can race
replication, and it will eventually be caught, not prevented, by "it
always passed before."
