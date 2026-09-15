# A closing rung's own deferred-work pointer names the right file list but can undersell how much of the real mechanism is already exercised — re-derive the precise gap from source, not the pointer's own headline sentence (ADR 0061 rung L→M handoff, C-13 opener)

Rung L's (C-12) own close-out deferred four seed/join files with a
one-line reason: `SimCluster::grow`/`seed_members` "bypass the ADR
0030/0032 join dance." Taken at face value, that reads as "none of the
join mechanism is exercised under `SimCluster` yet." Reading the
production join sequence directly (`crates/animusd/src/lib.rs:15192-
15813`, `crates/animus-control/src/node.rs:1753-1861`) instead of relying
on that headline sentence found the opposite: almost every step past the
initial wire discovery/claim round trip — `ClientCtx::register_node`/
`admin_add_member`, `animus_control::node::heartbeat_loop`/`detect_loop`/
`liveness_transitions`, `ClientCtx::propose_schema`'s relay fallback — is
already `<E, R>`-generic and already exercised correctly by the existing
bypass, just reached via a shortcut instead of the real discovery path.
The actual gap is much narrower and more mechanical than the headline
suggested: two pre-bind raw-socket wire calls with no `Env` seam at all,
plus one missing `ClientRequest::JoinInfo` arm on a relay dispatcher's
allowlist — found only by reading the dispatcher's own match arms
directly, since no prior rung's doc named it. The general rule this
confirms for the second time in this crate's own history (the first
being `control_membership_admin.rs`'s 12th test, misdescribed across four
rungs before rung L's own close-out re-read it): a prior rung's own
deferred-work pointer is a starting hypothesis for where to look, never a
substitute for re-deriving the precise mechanism boundary from the
current production source. The narrower a rung's opener can state the
actual gap, the less its own fixture-design phase has to guess at what
"the join dance" even means.
