# A branch-selection existence check needs the same read-your-writes barrier as a self-confirm — not just the proposer's own poll (issues #406/#450, `animusd::admin_add_control_member`)

The general "confirm a just-proposed effect via `engine_applied_index()`,
never a raw cache re-read" lesson is already logged several times over in
this file (grep the term) — always framed as *a proposer confirming its own
write*. `admin_add_control_member`'s bug is the same staleness hazard
wearing a different hat: it isn't confirming anything it just proposed —
it reads `metadata_cached()` once to decide **which branch to take**
("is `node` already registered, or genuinely unclaimed?"), where the fact
it's checking for is *someone else's* concurrent commit (the target
control-only node's own self-registration, landing on this same leader
just beforehand). Because a control-only registration never claims
`Metadata::members` by design (ADR 0040 PR4's own carve-out — see
`animus-control::meta.rs`'s `RegisterNode` doc), the old gate
(`meta.members.contains_key`) was *structurally* always false for this
node shape, so the "genuinely unclaimed" branch ran on **every** call and
re-derived a fresh `NodeAddrs` from the same stale `metadata_cached()` —
empty `client`/`intra`/`admin` fields whenever this leader's own ADR 0038
apply task hadn't yet caught up to the real registration's own commit,
producing a permanent CAS collision (or, worse, a durable blank address
book if the malformed proposal won the race). Two independent fixes were
needed, not one: (1) the gate itself must consult `node_addrs`, the
command's *actual* CAS key, not `members` alone; (2) before evaluating
either, the call must bound-wait for `engine_applied_index() >=
commit_index()` **on this exact leader** — a local read-your-writes
barrier against *this leader's own* Raft log, not the proposer's own
command, closing the dominant race (a self-registration relayed to/
proposed on this same leader moments earlier) outright, and narrowing what
remains to the genuinely irreducible "hasn't reached this leader's log at
all yet" case. Fix (1) alone measurably narrows the bug but does **not**
close it: in the reproduction built to prove this, checking `node_addrs`
without also adding the wait still collided reliably, because the leader's
own cache had **no trace under either key** at read time — the commit
itself, not just the label it's filed under, was the missing fact.
**General lesson: any local snapshot read used to gate a decision about
"has some other actor already done X" needs the identical staleness
audit a self-confirm poll gets, even when the caller never proposed
anything itself and has no `propose_and_await`-shaped call site to remind
it** — the caller-doesn't-know-to-wait failure mode is easy to miss
because nothing in the code *looks* like a write path. Reproducing this
deterministically in a real `ProdEnv` test needed the same technique as
the `InstallSnapshot`-window entry just above: not a fixed sleep or
artificial load, but a condition-based search polling `GET /admin/raft`'s
`commit_index`/`engine_applied_index` fields directly for the instant
where commit has advanced but apply hasn't, then firing the racing call at
exactly that reading.

**Amendment (issue #712, 2026-09-07): the bound-wait alone still measurably
collides under real CI contention — a single stale read at one checkpoint
should never be allowed to lock in a doomed, deterministic failure.** The
regression test above (`control_membership_split.rs::
admin_add_control_member_races_a_control_only_self_registration_and_still_
converges`) started failing intermittently in CI (`prod-liveness-animusd`
shard, ~1 run in 2-3 under real nextest-partition contention; reproduced
locally at 2/30-2/70 under a 4-core host loaded with `yes`/other `animusd`
integration binaries, 0 flake with `cargo test --workspace` off) with the
exact `409`/"already claimed by a different registration" body the test
guards against — proof the fix above narrows but does not close the
window. Root cause: the fix's own read-your-writes barrier
(`engine_applied_index() >= commit_index()`, bound-waited once, for up to
`SCHEMA_COMMIT_TIMEOUT`) is a **single checkpoint** — if the apply task
hasn't caught up by the time that one bound-wait's own timeout expires
(plausible, not exotic, under real host contention: the apply task is an
ordinary spawned task competing for the same CPU/disk every other
concurrently-running test binary is hammering), the call falls through to
read the still-stale cache exactly once, decides "genuinely unclaimed," and
proposes a malformed `RegisterNode` (empty `client`/`admin`/`intra`) that
is now doomed: Raft log order guarantees the target's own real
self-registration (already committed before this call ever started
waiting) applies *before* this malformed entry does, so the malformed
entry's own apply-time CAS check is a **deterministic** collision, not a
transient one — no amount of extra waiting inside that one proposal fixes
it, because the wrong bytes were already sent. This is case (a) from the
prompt (the wait can time out) compounding into case (c) (the racing
self-registration legitimately, permanently owns the slot the malformed
guess collides against) — widening `SCHEMA_COMMIT_TIMEOUT` would only
raise the contention bar at which this reappears, not remove the
determinism trap, so it was rejected as the fix.

**The actual fix doesn't try to make the *first* read reliably fresh —
it makes a stale first read recoverable.** `ClientCtx::register_node`'s own
`Collision` verdict is *itself* backed by a `metadata_fresh()` read (see
that method's own doc) — so the instant a caller observes `Collision`, its
local cache is *guaranteed* to already hold whatever entry caused it
(`ControlHandle::Local`'s `metadata_fresh()` and `metadata_cached()` are
the same read). `admin_add_control_member`'s "genuinely unclaimed" branch
used to retry-with-a-different-id only for a **minted** id and fail
immediately on the very first `Collision` for an **operator-supplied** one
— exactly the shape this admin action exists to serve (promoting an
already-self-registering growth node by its own known id). Fixed by also
retrying an operator-supplied id, **at the same id**, up to a small bound
(`MAX_CLAIM_REFRESH_ATTEMPTS = 3`, `animusd::lib`) — each retry re-derives
`addrs` from a fresh `metadata_cached()` read (the loop already did this
per-iteration; the only change is *letting it loop* for this id shape). A
stale-merge collision against the target's own now-applied self-
registration re-merges to an *identical* `addrs` on the very next
iteration, so the retry's own `register_node` call resolves as an
idempotent no-op (`Registered`), typically within one extra attempt,
independent of how long the apply task took to catch up. A **genuine**,
permanent conflict (a real different registration holding the id) re-
derives the identical conflicting `addrs` every time and still fails
loudly — just after up to 3 bounded retries instead of the first one, a
deliberate small latency cost on a rare admin path, never a correctness
compromise (verified: `add_control_member_collision_shapes` and the wider
`control_membership_admin.rs`/`admin_endpoint.rs` suites stay green).

**General lesson, sharpened**: a read-your-writes barrier gated on a
single bound-wait is a *timing* fix, not a *correctness* fix, whenever the
decision it unblocks is otherwise irrevocable (here: proposing a
byte-committed guess that will deterministically collide once racing state
lands) — the barrier reduces how *often* the stale-read window is hit, it
does not change what happens when it's hit anyway. Pair a bound-wait with
a **retry that re-derives from a still-fresher signal the failure itself
proves is now available** (a `Collision`/`Rejected` outcome from a
`metadata_fresh()`-backed call is exactly such a signal) so a caller that
loses the timing race the first time gets a second, now-unstale attempt
instead of a deterministic failure. Reproduction discipline worth naming
too: this bug needed *real* multi-process CPU contention to surface at any
useful rate (a bare `yes`-loop on an otherwise idle host barely reproduces
it; running several other heavy `animusd` `ProdEnv` integration binaries
concurrently roughly doubled the observed rate on the same host) —
matching CI's own nextest-partition shape (many real test processes
sharing a runner) is what a flaky-under-load `ProdEnv` test's own repro
loop should mirror, not just added CPU spin.
