# The "sweep for the pattern" advice above was followed for two known sites and still missed the actual common root: the shared helper both of them (and most other schema proposals) sit on top of.

**The "sweep for the pattern" advice above was followed for two known sites
and still missed the actual common root: the shared helper both of them (and
most other schema proposals) sit on top of.** `ClientCtx::propose_and_await`
— the generic "propose a `MetaCommand`, poll `Metadata` for its commit"
helper backing `propose_split_metadata`, `register_node_addrs` (formerly
`register_cp_addr`, superseded by ADR 0032 PR1),
`create_table_schema`, `replace_table_schema`, `drop_table`, and
`drop_table_schema`'s own hand-rolled copy of the same loop — called
`propose_schema` unconditionally on **every** `SCHEMA_POLL_INTERVAL` (50ms)
tick regardless of whether the previous call had already reached a leader's
log, for up to `SCHEMA_COMMIT_TIMEOUT` (10s) ⇒ up to ~200 duplicate proposals
per call. `SplitTablet` apply's `new_id`-exists guard makes a duplicate
harmless (cleanly rejected), so this was pure wasted WAL/replication work —
but under `--cluster N`'s auto-split loop running on every node concurrently
(see the sibling cross-node-contention entry), that waste compounds directly
into the 10-minute-long "split metadata did not commit in time" stalls seen
live: three nodes' independent retry storms flooding the control-plane log
fast enough that nothing drains within any single 10s window. Fixed by
having `propose_schema` report whether it has reason to believe the command
reached a leader's log (a local `Accepted`, or a relay that didn't visibly
error) and having `propose_and_await` only resubmit immediately when it
knows the prior attempt went nowhere, otherwise backing off
`SCHEMA_PROPOSE_PATIENCE` (1s) before trying again — mirroring
`propose_and_confirm_split`'s confirm-before-resubmit shape one level up the
call graph. **Corollary: "sweep for the pattern" means grep the shared
primitives a retry loop calls into, not just the two sites a bug report
named** — the pattern's most common instance was hiding one layer below
where it had already been fixed twice.
