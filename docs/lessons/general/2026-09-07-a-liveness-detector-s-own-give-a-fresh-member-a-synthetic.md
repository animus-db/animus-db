# A liveness detector's own "give a fresh member a synthetic first heartbeat" hardening means "was directly marked `Active`" is not "will stay `Active`" — a design assumption that needed empirical verification, not just re-stating (ADR 0061 rung D3 PR 2a)

Populating `Metadata::members` for a `SimCluster` fixture (`RegisterNode`
then `UpsertMember{status: Active}`) was planned on the stated assumption
that "`UpsertMember{Active}` also sets `has_activated`, so the orphan
sweep never reclaims the id, and `liveness_transitions` only visits ids
the detector has heartbeats for, so a directly-activated member cannot
flip back to `Down`." The first half is correct (`has_activated` really is
sticky and really does protect against the *orphan sweep*, a genuinely
different mechanism). The second half is **wrong**, and would have stayed
wrong if taken on faith: `animus-control::node::detect_loop` has its own
"phantom-member hardening" (ADR 0030) that gives any member found
`Active`-but-**untracked** by the failure detector exactly one *synthetic*
`FailureDetector::observe` the very first tick it notices one — so a
member this fixture marks `Active` directly (never having sent a real
heartbeat) gets exactly one fabricated liveness timestamp, which then ages
out after `DETECT_TIMEOUT` (500ms) precisely like a real one would, and
the detector proposes `UpsertMember{Down}` for it. Confirmed live: a
member seeded this way flipped to `Down` well before a second wire
`CreateTable` call (each burning that fixture's own `OP_BUDGET` = 12
virtual seconds) ever reached `ClientCtx::provision_tablet`, which then
found zero `Active` replica candidates and spun uselessly until its own
commit-wait deadline — a real, load-bearing production behavior this
fixture had never previously exercised (every earlier `SimCluster` test
either used the hand-hosted DDL bypass, which never reads `Metadata::
members` at all, or never provisioned a second table after enough virtual
time had elapsed for the window to matter).

**The lesson, restated for reuse**: a design-pass claim about what a
*different* subsystem's mechanism does or doesn't do is a hypothesis, not
a fact, until traced against that subsystem's own source — "X only visits
Y" is exactly the kind of claim that a hardening/edge-case branch (here,
"give a cold-tracked member some initial grace") can quietly falsify,
because the hardening's own author was solving a *different* problem
(protecting a genuinely fresh member from a false-negative liveness
verdict) that happens to interact badly with a *synthetic* activation that
was never going to be followed by a real heartbeat. When a task brief
hands you a claim like this framed as already-established, grep the
actual mechanism it's a claim about before building on it — in this case,
`animus_control::node::detect_loop`'s own phantom-member-hardening block,
a maybe 10-line span, would have shown the gap in under a minute. The fix
itself was equally small once found: spawn the real `animus_control::
node::heartbeat_loop` (the exact loop a production deployment already
runs) on every fixture node, keeping every member's liveness genuinely
current rather than working around the detector's own correct behavior.
