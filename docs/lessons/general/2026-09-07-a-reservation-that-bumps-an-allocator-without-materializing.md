# A reservation that bumps an allocator without materializing a row means EVERY other minting command must check the floor, not just existence — and a comment claiming parity across sibling arms is not a test (issue #684, `CreateTablet`'s missing monotonic-allocator floor guard)

`Metadata::next_tablet_id` (`crates/animus-control/src/meta.rs`) is a
single, shared monotonic counter several different `MetaCommand` apply
arms mint tablet ids from: `CreateTablet`, `BeginSplitInPlace`,
`BeginRestore`, `BeginImport`. Three of the four correctly reject an id
below `next_free_tablet_id()` — the "same monotonic-allocator floor" gate,
each with a comment claiming parity with its siblings. `CreateTablet`'s
own apply arm was the one that had actually drifted: it checked only
`self.tablets.contains_key(tablet)` (existence) and the ADR 0023
one-tablet-per-table rule, never the floor. That gap was invisible by
inspection precisely because the *other* arms' comments asserted the
parity that `CreateTablet` didn't have — reading any ONE of those arms in
isolation looked correct and consistent with its neighbors.

**Why existence-only checking is not enough**: `BeginSplitInPlace`
reserves its two child ids by bumping `next_tablet_id` immediately at its
own apply, but mints NO tablet-map row for either child until
`CutoverSplit` runs later — by design, an in-place fork's children are
materialized directly from the intent at cutover, not at `BeginSplitInPlace`
time (ADR 0058). Between those two points, a reserved id exists in the
allocator's own counter but has no row in `self.tablets` — `contains_key`
returns `false` for it. A `CreateTablet` proposed for a wholly different
table, computed from a `next_free_tablet_id()` read that predates the
reservation, could therefore compute that exact same id, pass
`CreateTablet`'s existence-only check, and land — its row then sat at the
reserved id until the in-flight split's own `CutoverSplit` inserted its
child there with an **unconditional** `self.tablets.insert(child.id, t)`,
silently overwriting the other table's only tablet. Confirmed in CI on
`animusd/tests/auto_split_min_tablets.rs`: table A's ADR 0067
min-tablets-triggered split forked and reserved child ids while table B's
`provision_tablet` (`crates/animusd/src/schema.rs`) raced a `CreateTablet`
off a metadata read that predated the reservation — permanent, silent
data loss of table B's only tablet.

**Fix, two layers**: (1) the missing floor guard, added to `CreateTablet`'s
apply arm, identical in shape and message (`"tablet id below the monotonic
allocator"`) to its three siblings — this closes the actual production
gap. (2) Defense in depth in `CutoverSplit` itself: its own child-insertion
loop now rejects outright (`"child tablet id already occupied — allocator
invariant violated"`, plus a `tracing::error!`) if either child slot is
already occupied, rather than the previous unconditional overwrite — never
trust that an upstream guard is the only thing standing between "reserved"
and "safely materializable." A rejected `CutoverSplit` leaves the parent
`Splitting` forever (nothing else can clear an occupied slot), but this is
safe rather than a wedge: the `animusd` driver that proposes it
(`index_drain::inplace_split_driver_tick`) is itself idempotent/stateless
and already re-issues `CutoverSplit` every ~200ms tick "until the parent
vanishes from the map," so a rejection here only ever strands the one
already-corrupt split — loudly — never anything else in the cluster.

**General form**: when several apply arms share one allocator-shaped
counter, the floor check belongs on **every** command that can mint an id
from it, not just the ones an author happened to think of at the time —
"existence" and "not-below-the-floor" are two different, both-necessary
guards, because a reservation-without-materialization design (bump the
counter now, insert the row later) makes a window where an id is neither
free nor yet present. And a comment on arm B asserting "same discipline as
arm A" is not itself proof that arm A (or any other sibling) actually has
that discipline — it's a claim that goes stale the instant a new sibling
arm is added without updating every existing comment that named it. The
regression test for this class of bug is not "does the guarded arm reject
a duplicate/expired id" (that was already tested) but "does an UNRELATED
concurrent minting command, racing the exact reservation window, get
rejected too" — cheap to construct directly (mint the reservation, then
attempt the unrelated command at the reserved id) and it is precisely the
kind of cross-command interaction a single arm's own unit tests, however
thorough, cannot catch by construction.
- **A handoff route's success criterion must be the positive end state, not
  the old holder letting go — "it stepped down" and "the new holder has it"
  are different facts, and the second one is the only one a caller can act
  on (issue #688, 2026-09-07).** `POST /admin/control/transfer` (ADR 0037,
  ADR 0020) armed `RaftCore::transfer_leadership(target)` and, until this
  fix, reported `200` the instant its own node was no longer the control
  leader — treating "this node stepped down" as proof "the named target
  now leads." Those are not the same fact: once armed, the old leader
  keeps heartbeating every peer and steps down on **any** higher-term Raft
  message it receives, not only a vote triggered by the target's own
  `TimeoutNow` (`RaftCore::handle`'s ordinary higher-term step-down is
  generic — it has no notion of "the vote I'm stepping down for is the one
  I meant to arm"). Under real scheduling jitter — the reproducing case was
  `admin_endpoint.rs` running 3 real OS threads on a CI runner starved
  enough that the leader's heartbeats to *every* peer went missing
  together — more than one follower's election timer can lapse on the same
  gap, and a **third** voter this call never named can win the resulting
  pre-vote/vote round before or instead of the target. The route's own
  `admin_remove_control_member` self-removal sibling had already been hit
  by a *related* but distinct bug (issue #405/#671, this file's own entry
  above and below): "an arm attempt can fail outright with no retry of its
  own." Issue #688 is the second, independent failure mode #671's own fix
  left standing — even a **successfully armed** transfer can still resolve
  to the wrong winner, and nothing about retrying the arm call touches
  that, because the arm itself genuinely succeeded; what failed was the
  route's own belief about who won afterward.

  **Fix**: read the leader's own **live** `RaftCore::leader()` belief after
  arming, not just its own `is_leader()` flag — `leader()` keeps updating
  after this node steps down (cleared to `None` the instant a higher term
  is observed, set to `Some(winner)` only once a genuine `AppendEntries`/
  `InstallSnapshot` from that term's real leader arrives — never a guess),
  so it can name the actual election winner even once this node is no
  longer leader itself. `200` only once it names the requested target
  specifically. If it names a **different**, stable voter instead, the
  route returns a distinct, retryable `409` naming that voter, rather than
  folding it into the same "did not complete; retry" message an arm/
  timeout refusal uses — a caller needs to know it must retry the whole
  `POST` against a *different* admin port (this node's own `RaftCore` can
  arm nothing further once it isn't the leader), not just wait longer here.

  **General rule, generalizing past this one route**: whenever a route's
  job is to hand something off from A to B (leadership, a lease, a lock,
  ownership of a resource), "A no longer holds it" is necessary but never
  sufficient proof that "B now holds it" — something else could have taken
  it in the gap. The success check must read the **new** holder's own
  identity from a live, continuously-updated source, not infer it from the
  old holder's absence; and the "wrong new holder" case deserves its own
  distinguishable error from "no new holder yet," since a caller's retry
  strategy differs (retry the same target vs. redirect to whoever actually
  has it now).

  **Test-design note**: the deterministic regression
  (`crates/animus-control/tests/transfer_third_voter_wins.rs`) proves the
  race exists in `RaftCore::transfer_leadership` itself, at the `SimEnv`
  level, with no `animusd` route anywhere in the loop — it uses
  `Simulator::pause` (freeze a node fully, deferring every timer/send/
  delivery to the resume instant — the real CI flake's own root cause,
  "every peer's heartbeats go missing together," not a one-sided
  partition) rather than `Simulator::partition`, since a partition would
  only isolate the leader from *one* other voter, not model "both
  followers' election timers can lapse at once." A brute-force scan over
  seeds 0..3000 with no other fault applied found plenty of seeds where the
  transfer's own un-named third voter wins outright — this is not a rare
  edge case requiring exotic fault injection, just an ordinary two-follower
  election race the transfer route's old contract never accounted for.
  (`crates/animusd/src/lib.rs::ClientCtx::
  admin_transfer_control_leadership`, `crates/animusd/src/admin.rs::
  action_transfer_control_leadership`, `crates/animus-control/src/node.rs::
  RaftNode::leader`, `crates/animus-control/tests/
  transfer_third_voter_wins.rs`.)
