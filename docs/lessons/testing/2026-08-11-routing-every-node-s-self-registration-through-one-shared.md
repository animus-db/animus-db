# Routing every node's self-registration through one shared command can silently make a role that must never be placement-eligible show up in the placement-eligible set — a second, unrelated bug the same "unify the claim path" change can introduce even after the first one (above) is fixed.

**Routing every node's self-registration through one shared command can
silently make a role that must never be placement-eligible show up in the
placement-eligible set — a second, unrelated bug the same "unify the
claim path" change can introduce even after the first one (above) is
fixed.** Once `RegisterNode` became the sole path that inserts a `members`
row, a **control-only** node's own self-registration inserted one too
(labels + `Down` status, the same as every other node) — and the existing
`Down → Active` promotion chain (ADR 0030 §1, unchanged) promoted it the
moment it started heartbeating, same as any data-capable node. Nothing
about `RegisterNode`'s own apply logic was wrong in isolation; the bug was
purely in *scope* — a control-only node has no `raftkv` role and can never
host a tablet, so its mere presence in `members` silently makes it a
placement candidate the moment the reconciler considers `Active` members,
corrupting replica-set assignment with no error anywhere in the write
path. Caught by `animusd/tests/control_only.rs` going bimodal ("put via
control node did not forward... Elapsed") — a downstream symptom several
layers removed from the actual cause, not a direct assertion on
membership. **Fix**: gate the `members`-row insert on the registering
node's own declared role (`NodeAddrs.role == "control"` skips it
entirely) — the address book claim (`node_addrs`) still succeeds for every
role; only the placement-eligibility side effect is role-gated. **General
rule: when unifying several roles' registration/bootstrap paths onto one
shared command, explicitly enumerate which side effects that command
produces are safe for *every* role versus which are only safe for a
subset — a command that "just inserts a row" can smuggle in an implicit
eligibility grant that was previously only reachable from a role-specific
code path.** (`animus-control::meta.rs::
register_node_never_claims_membership_for_a_control_role_registration`,
`animusd/tests/control_only.rs`; ADR 0040 PR4.)
