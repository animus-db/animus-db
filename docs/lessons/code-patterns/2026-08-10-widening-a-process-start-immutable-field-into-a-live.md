# Widening a process-start-immutable field into a live, periodically re-synced one: change the field's *type* first, then let the compiler enumerate every consumer — don't grep for call sites by hand.

**Widening a process-start-immutable field into a live, periodically
re-synced one: change the field's *type* first, then let the compiler
enumerate every consumer — don't grep for call sites by hand.** Making
`ClientCtx.client_route` (a plain `BTreeMap`, filled once at node start)
live (ADR 0032 PR1, closing ADR 0030's `client_route`-staleness gap) meant
wrapping it in `Arc<Mutex<_>>` and adding a `route_sync_loop` sibling to the
already-proven `peer_sync_loop` (same static-seed-∪-replicated-overlay
shape, same cadence). Every direct `.get()`/`.values()` access to the old
plain-map field (`cp_forward_target`, `propose_schema`'s relay + broadcast
fallback, a routing fallback search, the growth-node
`remote_metadata_sync_loop` seed computation) became a **type error** the
moment the field's type changed, so the compiler itself produced the exact
call-site list — a mechanical, self-auditing sweep, unlike the many
documented gaps in this codebase that are *silent* to the compiler (a
missing `is_relayable_command`/`cp_serve_forwarded` match arm, a stale
cached invariant). Route every such access through small
lock-scoped accessor methods (`route_addr`/`route_snapshot`, cloning out
under the lock) so no caller can end up holding the guard across an
`.await`. The *test* fallout from the same change was not compiler-caught,
though: a test asserting directly on the **superseded** state
(`cp_member_addrs`, no longer populated by `animusd`'s own startup path
once `RegisterNodeAddrs` replaced `RegisterCpAddr` as the self-registration
command) failed at runtime, not compile time — when retiring a producer in
favor of a superset command that keeps the old command only for WAL
back-compat, grep tests for direct field/assertion checks on the
old-producer's output, not just callers of the old propose function.
(`animus-control::meta::NodeAddrs`/`RegisterNodeAddrs`; `animusd`
`route_sync_loop`; `tests/cp_plane.rs::cp_member_addresses_register_and_replicate`.)
