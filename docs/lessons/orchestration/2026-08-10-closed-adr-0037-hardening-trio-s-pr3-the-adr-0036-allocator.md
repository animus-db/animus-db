# Closed (ADR 0037 hardening trio's PR3): the ADR 0036 allocator is now wired into `control-add`

**Closed (ADR 0037 hardening trio's PR3): the ADR 0036 allocator is now
wired into `control-add`**, closing the last of ADR 0037's three deferrals
(heartbeat-liveness/hardening PR1, quorum-guard/hardening PR2, this one).
Two lessons worth keeping from wiring it:
- **A function whose only prior nonce source was a documented, narrowly-
  scoped `Env`-seam exception is not license to reuse that exception
  elsewhere.** ADR 0036's `generate_join_nonce` deliberately draws real
  (non-`Env`) randomness, but its own doc scopes that exception tightly to
  one real-process, pre-bind CLI boundary no `SimEnv` test ever drives.
  `admin_add_control_member`'s new minted-id branch runs in-process on a
  live control leader instead — exactly the kind of place a `SimEnv` test
  *does* drive (and this PR's own tests do) — so its nonce comes from
  `leader.env().next_u64()` instead, keeping the `Env`-seam rule (ADR 0003)
  intact with no exception invoked. When extending a helper that has a
  documented seam exception, re-read the *scope* of that exception before
  assuming a new call site inherits it — most don't.
- **"Wire an allocator into an existing add-member action" is not
  automatically "make the action mint-then-bind-ready for a real new
  process."** The obvious design — mint an id, then have the operator's
  already-running new node discovered via the same `GET /admin/config`
  liveness check the operator-supplied form uses — doesn't compose: a
  physical `RaftCore`'s own self-id is fixed at bind time, so a server-
  minted id (unknown until the admin call returns) can only ever match a
  process that binds *after* learning it, not one already running. The
  shipped design accepts this: the omitted-node form takes the raw
  control-Raft address directly (no `/admin/config` resolution, no
  post-call convergence poll), mints, registers, and adds the voter in one
  call, and tells the operator to bring the physical process up
  *afterward* with `--node <minted-id>` — deliberately not solving "start
  a not-yet-`--node`-known process and have it discover its own minted id"
  (that would need its own join-allocated-style bootstrap entry point,
  scoped out as future work, same as ADR 0037's own coordination note
  anticipated). Test coverage follows the same shape as the existing
  concurrent-add regression (`concurrent_control_add_surfaces_in_flight_
  as_a_clean_retryable_error`): a fake, never-connected addr is enough to
  prove the admin-plane mint + register + `change_membership` mechanics
  and the `GET /admin/control/members` convergence signal, without needing
  a real second process — real Raft catch-up for a runtime-added voter is
  already covered elsewhere (`grow_control_group_converges_everywhere`).
  When a design reverses an existing action's "resolve-then-add" order
  into "add-then-instructs-resolve," check every step that assumed the
  old order (liveness pre-checks, convergence polls keyed off information
  that no longer exists yet) rather than threading the new `Option` through
  unchanged.
