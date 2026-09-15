# ADR 0040 PR6 (orphan-member auto-reclaim sweep). Two generalizable lessons from adding a "was this member ever real" sticky flag next to an existing status field:

**ADR 0040 PR6 (orphan-member auto-reclaim sweep). Two generalizable
lessons from adding a "was this member ever real" sticky flag next to an
existing status field:**
- **A flag meant to distinguish "never happened" from "happened, then
  reverted" must be set wherever the state can be *directly* reached, not
  just at the one call site you're thinking about when you add it.** The
  plan's own framing ("set `has_activated` true the first time the
  detector promotes Down→Active") was almost a structural hole: a
  bootstrap-declared member is inserted `Active` **directly**, never
  passing through a `Down`→`Active` transition at all, so gating the flag
  on that one transition alone would leave every founding cluster member
  permanently `has_activated: false` — indistinguishable from a genuine
  orphan the instant it later legitimately crashes to `Down`, and thus
  wrongly swept. The fix was to compute the flag inside `Metadata::apply`
  itself, keyed purely on "does this command's desired status equal
  `Active`," so it's set correctly regardless of *which* caller (detector
  promotion, bootstrap, a future admin action) produces that state —
  catching every current and future path structurally instead of
  special-casing the one path a plan happened to name. When a new field
  is meant to answer "has X ever been true," audit every place the target
  state can be reached directly, not just the one transition path that
  prompted the field.
- **A removal command's existing "member absent → no-op" branch can be
  silently wrong once a *second* claim shape without a member row
  exists.** `RemoveMember` predates `RegisterNode`'s claim-without-member
  shape (a control-role registration that, by design, never claims a
  `members` row) by several PRs; its own "already absent, idempotent
  retry" branch quietly assumed "no `members` row" meant "nothing to
  clean up," which was true until a command that can create an
  address-only claim existed. The fix (checking `node_addrs` inside that
  same branch and pruning it if present) was small, but finding it
  required explicitly asking "what does this cleanup command actually
  clean up, for *every* shape a claim can now take" rather than trusting
  a comment written before the second shape existed. Any command whose
  contract is "remove everything this id ever claimed" needs re-auditing
  every time a new claim-creating command ships, not just when it was
  first written.
- **A structural safety property (an interleaving that must never
  corrupt state) is proven more rigorously as a pure unit test driving
  the exact functions production uses than as an approximated race under
  `SimEnv`'s timer-driven scheduling.** The catastrophic case here — a
  sweep proposal computed from a stale (pre-activation) view must never
  actually remove an already-`Active` member — depends on which of two
  independently-timed async loops' `propose()` call executes first,
  which `SimEnv`'s deterministic-but-still-concurrent-task scheduling
  cannot be reliably steered into either order from a black-box test
  (unlike a fixed heartbeat-vs-detector cadence race, there's no clean
  knob to force the ordering). Testing it as two direct, hand-ordered
  `Metadata::apply` calls (both orders) instead proves the guarding
  precondition exhaustively and is easier to read as a specification of
  the invariant than any timing-dependent integration test could be — and
  the complementary "no resurrection" half (a stray late heartbeat must
  never resurrect a removed claim) is provable the same way by calling
  the actual crate-private decision function (`liveness_transitions`)
  directly with the member already absent, rather than trying to force a
  heartbeat to arrive at exactly the wrong instant. When a plan asks you
  to prove "X never happens under this race," check whether the safety
  property is actually a precondition/postcondition of a pure function
  first — if so, a direct unit test of that function is the rigorous
  proof, and a `SimEnv` integration test (if written at all) is corroborating
  color, not the thing that actually establishes the property.
