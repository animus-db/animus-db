# Freezing a picked address set across retries only relocates a port TOCTOU — the fix is bind-and-hold before claim (issue #1042)

`crates/animusd/tests/support/mod.rs::join_fresh_deadline` (and the other
explicit-`--id` join helpers built the same way) used to pick a port set
via `free_addrs` **once**, before its retry loop, and reuse that exact
`RoleAddrs` value on every attempt — a deliberate mitigation for issues
#406/#450 (documented on the function itself): `run_node_join`'s
explicit-`--id` path durably registers `--id`'s `NodeAddrs` (a
`MetaCommand::RegisterNode` CAS) **before** it ever calls `Node::bind`, so
if the retry loop had instead re-picked fresh ports on every attempt, a
bind failure on attempt *N* would leave a durable claim on file for
addresses attempt *N+1*'s new pick would never match — a `RegisterOutcome::
Collision` against the id's own earlier, different claim, unresolvable no
matter how many further attempts followed.

Freezing the address set genuinely fixed *that* specific self-collision.
It did **not** fix the underlying port-TOCTOU `free_addrs` itself
documents (probe `:0`, read the address, release the listener — free the
instant it returns, for **any** other process to grab). It relocated the
hazard instead: under `cargo test --workspace`-scale contention, if the one
frozen port this helper picked was grabbed by something else for anything
longer than a brief moment, the retry loop kept retrying `Node::bind` at
that **exact same, now-durably-owned-by-someone-else** address for the
whole of `JOIN_DEADLINE` (30s) — it never picked a different port, because
picking a different port was precisely what the freeze was there to
prevent. The visible symptom (`could not join node N within 30s: Address
already in use (os error 98)`) looked identical to the original #406/#450
bug report, but the two are different mechanisms wearing the same error
message: one is a self-inflicted CAS collision, the other is a genuinely
contended port that the fixture's own address-freezing discipline had
committed to for the full deadline.

**The generalizable rule**: when a retry loop must claim an identity tied
to a set of addresses, freezing that address set across retries is not
itself a fix for a claim-before-bind ordering — it only trades a
self-collision hazard for a can-never-recover-from-a-genuinely-contended-
port hazard. The real fix is the same one issue #627 already established
for fresh-cluster bring-up (`docs/lessons/testing/2026-09-20-allocate-
test-ports-by-binding-and-holding-never-probe-and-release.md`): **bind
first, atomically, with the kernel handing out a genuinely free `:0` port
at the moment of the real bind — then claim the identity using the
addresses actually bound.** Once the claim only ever proposes addresses
this process has *already* bound and is *already holding*, a bind failure
can never leave a stray durable claim behind, so a caller is free to
retry with a **completely fresh** `:0` bind on every attempt — no freezing
needed at all, and no window in which another process can steal the exact
port a frozen retry is committed to.

This is why the two structural fixes have to travel together: freezing the
address set alone (the pre-#1042 shape) is a genuine, real mitigation for
one half of the hazard, but claiming what you're not yet holding is the
other half, and neither alone is sufficient. `run_node_join_with_settings`/
`run_node_data_join_with_settings`'s explicit-`--id` path now binds before
it claims (`crates/animusd/src/lib.rs`); `support::join_fresh_deadline`/
`join_data_fresh_deadline` (and `join_data_seed_settings_reach.rs`'s
`join_bare`/`join_with_settings`) no longer freeze anything — every attempt
mints a fresh `:0` request, exactly like every other fresh-cluster bring-up
in this module.

**One case where this doesn't apply, worth naming so it isn't "fixed" by
analogy without thinking it through**: a **self-minted** `--id` join
(`NodeId::mint`, ADR 0040 Decision B) is drawn fresh on every retry
attempt regardless, so the #406/#450 self-collision this whole class of
bug is about can never arise for it in the first place — there is no
earlier durable claim under a self-minted attempt's own (different) id for
a later attempt to collide with. That path's own production ordering
(claim, using the caller-supplied pre-bind addresses, then bind) was
deliberately left unchanged, and its own test helpers
(`join_allocated_fresh_deadline`/`join_data_allocated_fresh_deadline`)
correspondingly still need `free_addrs` to hand them a real,
already-resolvable address up front — passing a literal `:0` into that
unchanged branch would register a bogus, unreachable `"host:0"` NodeAddrs,
a strictly worse bug than the one being fixed. Before moving a helper off
`free_addrs`, check which branch of the production function it actually
exercises, not just whether the port picker in front of it looks the same
shape as one that was already fixed.
