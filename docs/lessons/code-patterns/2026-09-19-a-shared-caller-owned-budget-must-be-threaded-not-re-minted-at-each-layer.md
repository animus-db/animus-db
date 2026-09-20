# A caller's own retry-loop deadline must be threaded into the helpers it calls, not re-minted independently by each one — otherwise a "bounded" call composes into a multiple of its own nominal budget

**A caller's own retry-loop deadline must be threaded into the helpers it
calls, not re-minted independently by each one — otherwise a "bounded"
call composes into a multiple of its own nominal budget.** Issue #961:
`ClientCtx::cp_route` and `ClientCtx::forward_to_tablet_leader`
(`crates/animusd/src/forwarding.rs`) each independently computed `self.
env.now().saturating_add(CLIENT_TIMEOUT)` the instant they were called —
so a caller like `cp_kind_write_item` (`write_path.rs`), which already
minted its own `CLIENT_TIMEOUT`-sized `deadline` at the top of its retry
loop specifically so ONE logical attempt would be bounded by it, got no
such bound in practice: `cp_route` could spend most of a fresh 10s budget
resolving via its own cross-replica fan-out, then hand off to `forward_
to_tablet_leader`, which minted a SECOND fresh 10s budget of its own for
the hop chase that followed. The outer loop's deadline check
(`self.env.now() >= deadline`) only ever ran *after* both inner calls had
already returned — by which point up to ~2x `CLIENT_TIMEOUT` had already
elapsed. The outer loop's own budget was real code, correctly written,
and completely inert: nothing it called ever consulted it.

**The generalizable rule**: when function A mints a deadline to bound its
own retry loop, and that loop's body calls B, and B's body calls C — the
budget A believes it is enforcing is a fiction unless the SAME deadline
value flows from A into B and from B into C as an explicit parameter. A
function that internally computes `now + SOME_TIMEOUT` is only safe to
call from a bounded loop if either (a) it is the loop's *only* per-
iteration call and its own timeout equals the loop's own budget exactly
(no composition), or (b) it takes the caller's remaining budget as an
argument instead of assuming its own fixed one. Two or more independently-
timed-out calls composed inside one nominally-bounded attempt is the
tell — grep for `saturating_add(CLIENT_TIMEOUT)`-shaped mints (or the
equivalent in another codebase) at more than one layer of the same call
chain before assuming a "this is bounded by X" doc comment is actually
true of the composed path, not just of each layer in isolation.

**The fix, and what it deliberately left alone**: `cp_route`/`cp_forward`/
`forward_to_tablet_leader` all now take a `deadline: Nanos` parameter;
every composing call site already had a local `deadline` binding (the
loop's own budget) and needed only to pass that existing value through
instead of letting the callee re-derive a fresh one — a signature change,
not a redesign. The **per-hop caps** one layer down
(`FORWARD_HOP_TIMEOUT`, `HINTED_FORWARD_HOP_TIMEOUT`,
`CP_ROUTE_FANOUT_PROBE_TIMEOUT`) are a different, orthogonal concern —
they bound one sub-step's own share of whatever budget remains
(`min(cap, deadline - now)`), and stayed completely unchanged; only the
`deadline - now` term's source changed, from a locally re-derived value
to the one the top-level caller actually owns. Threading a shared
deadline down through N layers is a mechanical, low-risk change exactly
because it never touches the per-layer caps that already exist — it only
stops those caps from being measured against the wrong clock.

**Why this one was easy to miss in review**: every individual function's
own doc comment, read in isolation, was correct — `cp_route` genuinely is
bounded by `CLIENT_TIMEOUT`; `forward_to_tablet_leader` genuinely is too.
The bug lived entirely in the *composition*, which no single function's
own doc or test ever exercised — `cp_route`'s own regression tested it
alone (`SimCluster::cp_route_timed`, bypassing the forward step entirely
by design, per its own doc's stated reason), and `forward_to_tablet_
leader`'s own regressions each started the chase already holding a
`CpRoute::Forward` — none of them ever let `cp_route`'s own fan-out
genuinely burn wall/virtual time immediately before handing off to a
forward chase that also stalls. A regression for this class of bug has to
deliberately engineer BOTH halves stalling in the same attempt, not just
each one in isolation — see `crates/animusd/src/sim_cluster_cp_route_
fanout.rs`'s `deadline_budget_tests` module (issue #961) for the shape:
partition the calling node from every other replica so the fan-out finds
nothing for several rounds, then heal exactly one of those links partway
through the call so the fan-out resolves to a hint whose OWN target
(a different link) never heals — reproducing the doubled-budget
compounding deterministically rather than merely plausibly.
