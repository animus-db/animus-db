# A retry loop that reuses a fixed identity across attempts must reclaim its own abandoned claim before retrying — never assume a torn-down attempt left nothing durable behind.

**A retry loop that reuses a fixed identity across attempts must reclaim its
own abandoned claim before retrying — never assume a torn-down attempt left
nothing durable behind.** Issue #951: `split_placing_two_replica_diff_e2e.rs`'s
`join_extra` retries a failed `animusd::run_node_join` by tearing down
whatever local nodes it managed to start and picking a **fresh** address book
(`free_addrs`) — but keeps the **same** node id (`m0`/`m1`) across every
attempt. `run_node_join`'s identity claim (`MetaCommand::RegisterNode`, ADR
0040 Decision C) becomes durable and observably confirmed — via
`claim_join_identity` → `register_node_over_wire`'s `Registered` outcome,
`crates/animusd/src/lib.rs:15515`/`:15697` — **before** the rest of the join
(`Node::bind`'s TCP listener binds, `crates/animusd/src/lib.rs:5859`; the
shared `LsmEngine::open` and local Raft bring-up inside
`start_with_growth`/`finish_combined_join`, `:5242`/`:15522`) has run at all.
Any failure in that remaining setup — a port-bind TOCTOU race under
contention (the exact hazard `animusd/CLAUDE.md`'s "every in-crate bring-up
retries the port-TOCTOU race" note already names), a disk error opening the
storage engine, anything — leaves the claim on file with the *old* addresses
while the process that made it is gone. A next attempt proposing the same id
with the *new* `free_addrs` is then, correctly, rejected by
`RegisterNode`'s CAS (`crates/animus-control/src/meta.rs:4841`, keyed on
`node_addrs` alone) as "already claimed by a different registration" — the
CAS cannot tell "abandoned" from "still starting up," by design (a live node
must never be silently superseded by an impostor sharing its id).

This is **not** a product gap: the intended recovery already exists and is
already tested (`orphan_sweep_loop`, ADR 0040 PR6,
`crates/animus-control/tests/orphan_sweep.rs::crash_mid_join_orphan_swept_
after_ttl`) — but its grace period (`DEFAULT_ORPHAN_SWEEP_AFTER`, 600s) is
ten times longer than a 60s test retry budget, so a fixed-id test retry loop
will never outlast it. **The fix belongs in the retry loop itself**: before
retrying with the same id, reclaim that id's own possibly-stale claim
directly (propose `MetaCommand::RemoveMember { node: id }` through the
seed/admin surface — safe unconditionally here, since a claim this retry
loop itself minted and never saw go `Active` is never referenced by a
tablet and never the live process it's about to replace) rather than relying
on the id being free. See `same_id_retry_with_fresh_addrs_is_rejected_until_
the_stale_claim_is_reclaimed` in `orphan_sweep.rs` for the state-machine-level
proof of both halves (rejected while unclaimed-but-stale; freely reclaimable
once the stale claim is removed). **General rule**: a retry loop that pins
an identity across attempts but discards everything else about a failed
attempt must explicitly account for whatever that attempt made durable
under that identity — "I tore down my local process" is not the same claim
as "nothing this identity claimed survives."
