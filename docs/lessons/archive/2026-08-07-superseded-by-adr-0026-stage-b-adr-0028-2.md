# Superseded by ADR 0026 Stage B / ADR 0028

**Superseded by ADR 0026 Stage B / ADR 0028**: `cp_member_id`/`cp_base_id`
are deleted; `cp_forward_target` no longer translates at all, since a
tablet's member id is always its base id. Retained for historical record. **An id-translation seam must be applied in *both* directions — and the identity
case masks the missing one.** The tablet map speaks stable **base** node ids and a
tablet group speaks **derived member** ids (`cp_member_id`); `cp_forward_target`
consumed a group's leader *hint* (a member id) as a `client_route` key (base ids) —
and worked anyway for the bootstrap tablet, where member == base. It also worked
for the **first** provisioned table, which wins the tablet-id race with bootstrap
and rides the bootstrap group; only a **second** table (or split child) gets
derived ids, so the miss surfaced as a bimodal flake ("no CP group leader
reachable" on a *healthy, led* group — the follower had the hint but couldn't map
it, and having a local replica suppressed the forward-anywhere fallback, so it
waited out `CLIENT_TIMEOUT`). Fixes and morals: add the inverse (`cp_base_id`) at
the same seam as the forward map; when debugging "no leader", first dump the
group state (`/admin/raftkv`) — *formed-but-unroutable* looks identical to
*never-formed* from the client; and regression-test derived-id paths with a
**second** provisioned table, per-process, reading via **every** node (≥2 forced
forwards, deterministic teeth wherever the leader lands).
(`animusd` `cp_forward_target`; `cp_cross_process.rs::second_table_…`.)
