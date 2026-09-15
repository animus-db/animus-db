# A per-role internal `Env` peer address book (`ProdEnv::set_peers`) that is only ever installed once, at process bring-up, from static config has no path for a peer added *after* bring-up to become reachable — even once a higher-level replicated membership change (e.g. `RaftCore:: change_membership`) accepts it.

**A per-role internal `Env` peer address book (`ProdEnv::set_peers`) that
is only ever installed once, at process bring-up, from static config has no
path for a peer added *after* bring-up to become reachable — even once a
higher-level replicated membership change (e.g. `RaftCore::
change_membership`) accepts it.** The `raftkv` role already has this solved
generically (`animusd::peer_sync_loop`, a periodic static-base ∪
replicated-`Metadata.node_addrs[*].raftkv`-overlay rebuild); the **control**
role never needed it before ADR 0037 because the control group was static
(ADR 0030's scope decision) — so implementing "add a control voter at
runtime" (PR3 of the ADR 0037 stack) rediscovered the same class of gap
`ProdEnv::send`'s own doc already anticipates ("an unknown peer is just
another way the message is dropped... Raft retries once the address
lands" — but only if *something* makes the address land). Scoped down for
PR3 to a narrower, still-correct fix rather than porting the full
`peer_sync_loop` pattern to the control role: a new `ProdEnv::merge_peer`
(add one entry without replacing the whole book) called by the admin
action on the **local leader's own** env only, immediately before
`change_membership` — sufficient for the leader to replicate to a freshly
added voter, but *not* for a different, later leader (after a subsequent
transfer/crash) to independently rediscover that voter's address; that
gap is named, not silently left for a future maintainer to rediscover the
hard way. See `crates/animus-env/src/prod.rs::ProdEnv::merge_peer`'s doc
and `ClientCtx::admin_add_control_member`'s doc (`animusd`).
**Update (ADR 0037 PR4): this gap is now closed**, by finishing the port
the paragraph above predicted — `animus-control::NodeAddrs` gained a
`control: Option<SocketAddr>` field (replicated via the existing
`RegisterNodeAddrs`, `None` for every statically-configured voter) and
`animusd` gained `control_peer_sync_loop`, a genuine per-tick
static-∪-replicated overlay for the control role (mirroring
`peer_sync_loop`, but `merge_peer`-incremental rather than
`set_peers`-rebuilding, since there is no separate static control peer
book parameter to layer under here — each node's static book was already
installed once, directly, at `RaftNode::start`). Regression:
`crates/animusd/tests/control_membership_admin.rs::
runtime_added_voter_survives_leadership_change_to_a_different_original_voter`
(self-removes the adder to force a transfer to a *different* original
voter, then proves a fresh proposal still reaches the runtime-added
voter). Building that regression test surfaced a second, unrelated race
worth its own note (below): a non-voter's self-registration can never
observe its own commit, so its bounded retry keeps re-proposing (and can
clobber a concurrent admin action's write) for the full
`SCHEMA_COMMIT_TIMEOUT`.
