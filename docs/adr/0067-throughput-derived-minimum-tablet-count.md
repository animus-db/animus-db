# ADR 0067 — Throughput-derived minimum tablet count (W-08b)

- **Status:** Accepted
- **Date:** 2026-09-05
- **Origin:** a direct follow-up to ADR 0065 (per-table throttling, W-08),
  flagged in that ADR's own §8 ("Out of scope, stated") as a gap this ADR
  closes.
- **Amends:** ADR 0065 §8 (adds a pointer), ADR 0034 (a fourth auto-split
  trigger).

## Context

Every table starts life as exactly **one** tablet — ADR 0023's `CreateTablet`
allows a single tablet per table at creation — and grows only through
`animusd::auto_split_loop`'s existing triggers (ADR 0034: bytes; ADR 0042
§14: change-rate, streamed tables only; W-09/ADR 0034's amendment: ops-rate,
any table). All three are **reactive**: they fire only after a tablet has
already accumulated enough bytes, change-log churn, or write load to cross a
configured threshold. A table that declares a large `ProvisionedThroughput`
at `CreateTable` time (ADR 0065) gets none of that lead time — ADR 0065
divides the table's provisioned RCU/WCU evenly across its **current** tablet
count, but nothing in this codebase ensures that count is large enough for a
single Raft group's single leader to plausibly serve the declared share. A
table provisioned at 5,000 WCU funnels every one of those writes through one
tablet's one leader until bytes (or ops-rate, if configured) happen to
accumulate enough to trigger a split — a real availability/throughput gap
between what the client declared and what one tablet's leader can serve,
with no signal anywhere that closes it proactively.

Real DynamoDB does not have this gap: it sizes a table's initial partition
count from its provisioned throughput **up front**, using the documented
formula `partitions = ceil(RCU/3000 + WCU/1000)` — 3000 RCU and 1000 WCU are
DynamoDB's own published per-partition ceilings. This ADR gives AnimusDB the
equivalent proactive mechanism, built as a fourth arm of the existing
auto-split loop rather than a new subsystem, reusing every primitive ADR
0034/0042/W-09 already established (the loop's per-tick cadence, per-tablet
cooldown, `ClientCtx::trigger_split`'s single choke point, the in-place fork
mechanism of ADR 0058/0062).

## Decision

### 1. Per-tablet capacity ceilings — cluster-wide settings

Two new cluster-wide knobs, plumbed exactly the way ADR 0065 §5(a)'s
`--throttle-read-units`/`--throttle-write-units` are plumbed (CLI flags,
`animusd::config::ClusterSettings` fields, the `animus-operator` mirror):

- `--tablet-max-read-units N` (default **3000**)
- `--tablet-max-write-units N` (default **1000**)

These are DynamoDB's own documented per-partition ceilings. **Unlike the
throttle knobs, these are not opt-in** — they are always in effect, at their
production default, for any table that declares its own
`ProvisionedThroughput`; there is no "unset means off" state the way
`PAY_PER_REQUEST` is throttling's default. A value of `0` in either
dimension means "no ceiling in that dimension" — that dimension contributes
nothing to the derived minimum (an operator can disable the read half, the
write half, or both, the last being equivalent to disabling this whole
trigger). This mirrors `--quiesce-after`'s own "default ON, `0` disables"
shape, not `--throttle-read-units`' "default off" one.

### 2. The derivation — a pure function

```
min_tablets_for(throughput, ceilings) = max(1, ceil(RCU/max_rcu + WCU/max_wcu))
```

DynamoDB's own formula, verbatim. **Reads and writes are summed, not
maxed** — a tablet's Raft group has a single leader serving both its read
and write load, so a table's read and write pressure genuinely compete for
the same per-tablet budget rather than being two independent ceilings a
tablet could separately max out against. See Alternatives-considered for
the `max()` shape this rejects.

Implemented as `animusd::min_tablets::min_tablets_for`, deliberately **not**
in `animus-control` — it needs nothing from that crate beyond the already-
public `ProvisionedThroughput` type, and keeping it in `animusd` avoids
adding build-graph churn to the control-plane crate for a function with
exactly one caller. Integer-exact: `ceil(a/b + c/d)` is computed over a
**common denominator** (`(a·d + c·b) / (b·d)`) before ever rounding — summing
each term's own `ceil` first is a different, wrong answer in general (e.g.
`1500/3000 + 500/1000` is each exactly `0.5`, summing to `1.0` — `ceil` of
the sum is `1`, but `ceil(0.5) + ceil(0.5) = 2`). Computed in `u128`
intermediates, saturating on the way back to `u64` — no floating point, and
no panic/wrap for a pathologically large `ProvisionedThroughput`.

### 3. Mechanism — a fourth `auto_split_loop` arm, per table

`AutoSplitThresholds` gains `tablet_capacity_ceilings:
min_tablets::TabletCapacityCeilings` (the two ceilings above, resolved at
node start — `None` from the CLI/config layer resolves to the production
default, an explicit `0` stays `0`). Each tick, **after** the existing
per-tablet byte/change-rate/ops-rate pass, a **separate, per-table** pass:

1. Group this node's currently-known `Active` tablets by table.
2. For every table with `throughput` set (via `Metadata::table_throughput`),
   compute `min = min_tablets_for(throughput, ceilings)`. A table with no
   `throughput` is skipped outright — **tables without provisioned
   throughput are never touched by this trigger**, so an existing
   deployment sees no behavior change.
3. If the table's current `Active` tablet count is already `>= min`, skip.
4. Otherwise, pick the **widest** (by approximate token range) `Active`
   tablet of that table that this node currently **leads** and is not in
   the existing per-tablet cooldown (`AUTO_SPLIT_COOLDOWN`, shared with the
   other three arms via the same `last_triggered` map) — widest so each
   fork roughly halves the ring, converging toward `min` in
   `ceil(log2(min))` splits rather than lopsidedly. If this node leads none
   of the table's tablets, it does nothing this tick; whichever node(s) do
   lead one will independently reach the same conclusion.
5. Split that one tablet via the identical `ClientCtx::trigger_split` path
   every other trigger uses — the ADR 0058/0062 in-place atomic fork.

**At most one split per table per tick, per node** — each node's own tick
picks and forks at most one tablet of a given under-provisioned table that
*it* leads, mirroring the "one balance-improving move per call" discipline
`animus-placement`'s rebalancer already uses for the same reason (bounded
churn, converges over several ticks rather than trying to reach the
target in one jump).

**Convergence**: on a single-node deployment (one leader for every
tablet), this trigger adds exactly one tablet per `AUTO_SPLIT_INTERVAL`
tick, so reaching a minimum of `N` tablets from 1 takes `N - 1` ticks —
linear, not the halving `ceil(log2(N))` a naive read of "the widest
tablet splits" might suggest, precisely because only one split happens per
table per tick regardless of how many tablets already exist. When a
table's tablets are spread across **several** leader nodes, convergence is
faster: each node's own tick independently evaluates and can split
whichever tablet *it* leads, so up to one additional tablet per
currently-distinct leader can be minted in the same tick round. Raising throughput via `UpdateTable` simply
raises `min` and the loop mints more tablets on subsequent ticks; **lowering
throughput never merges tablets back down** — tablets are split-only (ADR
0044), and this ADR does not reopen that. A table that was provisioned
large and then de-provisioned keeps however many tablets it already grew;
quiescence (ADR 0048) is what keeps an over-provisioned table's now-idle
extra tablets cheap, not a shrink mechanism.

### 4. The empty-tablet problem, and how this arm gets past it

The three existing triggers never see an empty tablet: a byte/change-rate/
ops-rate threshold, by construction, cannot be crossed by a tablet with no
data and no writes. This trigger's condition is different — it is driven by
a **configured** value, not observed activity — so it must handle the
common case of a table that declares `ProvisionedThroughput` at `CreateTable`
time, before a single row is ever written, and correctly still wants to
reach its derived minimum tablet count immediately.

`KeyRange::split_at` (`animus-tablet`) itself imposes no data requirement —
it only checks that the proposed split key satisfies `start < at < end`
against the tablet's own **range**, never against its **content**. What
blocks an empty tablet from splitting today is `auto_split_loop`'s own
`key_count < 2` guard (needed because `decide::byte_weighted_median`, the
split-point function every existing trigger uses, has no meaningful answer
with fewer than two real keys to bisect between).

This arm sidesteps that guard rather than tripping over it: when the
candidate tablet's materialized pairs number two or more, it uses
`byte_weighted_median` exactly like every other trigger (a table with data
still gets an evenly-balanced split). When the tablet has **fewer than two**
pairs — the empty-table-just-provisioned case — it instead synthesizes a
split key as the **token midpoint** of the tablet's own key range
(`min_tablets::midpoint_split_key`): a plain 8-byte big-endian value exactly
halfway between the range's start and end tokens (ADR 0022's leading token,
treating an unbounded-above range as ending at the top of token space).
This key is inherently token-aligned (no F11 rounding needed) and
mechanically satisfies `KeyRange::split_at`'s strict-interior requirement
whenever the range spans more than one token — which is exactly the
existing, accepted Fork E single-token hot-partition limit (ADR 0042 §14):
if the widest candidate tablet's own range has collapsed to a single token,
`midpoint_split_key` returns `None`, this arm skips that tick rather than
proposing a doomed split, and the loop tries again next tick (a different
tablet may be wider by then, or nothing changes and it keeps skipping,
identically to how the byte-driven triggers already handle a genuine
single-token hot partition). **No silent split-then-fail path exists**:
either a valid key is computed and `trigger_split` is called, or nothing is
proposed at all.

Deliberately, this arm does **not** skip a quiesced candidate tablet the way
the byte/change-rate/ops-rate arms do — their skip is an optimization
premised on "no activity since quiescing means the observed value can't
have changed," which does not apply to a trigger driven by a static,
operator-configured value rather than observed activity. Reading a
quiesced group's local pairs is itself a safe, wake-free local scan (ADR
0048 fork F); the propose this arm makes when a split is warranted lets the
CP-data host reconciler un-quiesce the tablet exactly as any other
split would.

### 5. Loop-spawn cost discipline

`auto_split_loop` is now spawned **unconditionally** on every node shape
that used to gate it on `auto_split_bytes_threshold.is_some() ||
auto_split_change_rate.is_some() || auto_split_ops_rate.is_some()` — this
arm's ceilings are on by default, so the loop can always have something to
do. To keep an unprovisioned cluster's cost at effectively zero, the very
first statement of each tick checks a cheap, lock-free condition: if none
of the three opt-in triggers is configured **and** `ClientCtx::
any_table_throughput` (the lock-free flag ADR 0065 §5(b) step 5 built for
exactly this "is anything configured at all" fast path) is `false`, the
tick does nothing beyond the check — no `effective_metadata()` call (a
`Mutex` lock plus a deep clone of the whole tablet map/schema/backup
catalog), no per-tablet work. A cluster with no provisioned table anywhere
pays one atomic load per `AUTO_SPLIT_INTERVAL`, same as before this ADR.

### 6. No cap on the derived minimum

A deliberately accepted consequence, stated plainly: nothing here caps how
large `min_tablets_for` may compute. A genuinely huge `ProvisionedThroughput`
legitimately implies a genuinely large tablet count — that is the whole
point of the mechanism, not a runaway to guard against. What keeps this
economical is quiescence (ADR 0048): a tablet minted by this trigger that
then sees little or no traffic quiesces like any other idle group, costing
essentially nothing at rest. An operator who provisions far beyond real
need pays only in cheap, quiesced tablet count, not in ongoing Raft/CPU
cost.

### 7. Observability

One new counter, `Metric::AutoSplitMinTablets` (`animus-env`), incremented
each time this arm's `trigger_split` call succeeds — the identical
"increment on a successful, this-trigger-caused split" shape none of the
other three arms currently has as a dedicated metric (they're covered by
the general split machinery's own admin surface), added here specifically
because this trigger's own convergence behavior (how many ticks a
newly-provisioned table takes to reach its minimum) is worth being able to
see directly. Surfaced on `/metrics`/`/admin/metrics` like every other
`Metric` variant.

The admin `/admin/tables`-shaped surfacing of "current tablet count vs.
derived minimum" next to a table's throughput was considered and
deliberately **deferred**: no single existing admin/dashboard view lists
every table's tablet count alongside its throughput in one place today
(the closest, the Console's per-table Config tab, is a read-only fact
strip with no aggregate-tablet-count field to extend), so adding it cleanly
is a real, separate UI change rather than the "small additive field" this
ADR's own scope was bounded to. `/admin/status`'s existing `schemas[*]`
(throughput) and `tablets[*]` (per-tablet, including `table`) already carry
every fact needed to compute it externally in the meantime.

## Consequences

- A provisioned table now gets tablet count that tracks its declared
  throughput proactively, closing the gap ADR 0065 §8 named: no more
  funneling a large declared budget through a single tablet's single
  leader until bytes/ops-rate happen to catch up.
- **Existing deployments are unaffected**: a table with no
  `ProvisionedThroughput` (the default, `PAY_PER_REQUEST`) is never touched
  by this trigger regardless of the ceilings' own default-on state.
- The auto-split loop is now always running (previously conditional on an
  opt-in flag) on every data-hosting node, but the added idle cost is one
  atomic load per tick when nothing is configured (§5).
- **No cap on the result** (§6) is an accepted, stated trade — an
  operator who provisions well beyond real need pays in quiesced tablet
  count, not runaway Raft/CPU cost.
- Lowering throughput never shrinks tablet count back down (tablets are
  split-only, ADR 0044) — a documented, pre-existing limitation this ADR
  inherits rather than introduces.
- A hot tablet within this trigger's own derived count still divides the
  table's throughput evenly (ADR 0065 Decision 1's static, non-adaptive
  division) — this ADR changes tablet *count*, not the even-division
  policy that count feeds.

## Testing

- Unit tests of `min_tablets_for` (`animusd::min_tablets`): the DynamoDB
  worked examples, the "sum-then-round vs. round-then-sum" distinction, the
  `0`-ceiling "disable this dimension" case (both dimensions independently
  and together), saturation under a pathological input, and the production
  defaults.
- Unit tests of `midpoint_split_key`/`token_range_width` (the empty-tablet
  split-key synthesis and the widest-candidate ranking): a bounded range's
  exact midpoint, the whole-keyspace case, the single-token "no room"
  `None` case, and a zero-width range.
- An end-to-end `animusd` integration test: `CreateTable` with a
  `ProvisionedThroughput` whose derived minimum is 4 under small test-sized
  ceilings (100/100, so the test needs no large numbers), a
  converged-or-timeout poll to >= 4 `Active` tablets, then `UpdateTable`
  raising throughput and polling to >= 8 — proving both the initial mint
  and a later raise.
- A table with no `throughput` never splits under this arm (a negative
  regression, run alongside the positive one).
- The pre-existing ADR 0065 wire-shape test suite
  (`tests/dynamo_throttling.rs`) explicitly sets `tablet_max_read_units`/
  `tablet_max_write_units` to `0` in its own cluster bring-up — several of
  its tests declare very large `ProvisionedThroughput` values (up to
  1,000,000 units) purely to exercise the *throttle bucket's* arithmetic,
  and would otherwise now also trigger this ADR's own trigger, splitting
  those tables hundreds of times mid-test for no reason relevant to what
  those tests check. Disabling this trigger there is the correct scoping,
  not a workaround: those tests were never about tablet count.

## Alternatives considered

- **`max(RCU/max_rcu, WCU/max_wcu)` instead of summing.** Rejected: a
  tablet's single Raft leader serves both a tablet's reads and its writes
  from the same process, so read and write pressure are not independent
  ceilings a tablet could separately max out against — a table provisioned
  at exactly `max_rcu` reads and exactly `max_wcu` writes simultaneously
  would, under `max()`, still compute a minimum of 1, understating the real
  combined load on that one leader. Summing (DynamoDB's own choice) is the
  conservative, correct shape.
- **A cap on the derived minimum.** Rejected (Decision 6) — a cap would
  silently under-provision a genuinely huge table, defeating the point of
  deriving from the declared value at all; quiescence is the intended
  cost-control lever instead, already built and already applied uniformly
  to every idle tablet regardless of how it came to exist.
- **Placing this in `animus-control` as a pure `Metadata` function.**
  Rejected for this iteration: it needs nothing from that crate beyond the
  already-`pub` `ProvisionedThroughput` type, and the one caller
  (`animusd::auto_split_loop`) already lives in `animusd`; adding it to
  `animus-control` would be a needless cross-crate hop with no present
  benefit, and was explicitly scoped out to keep this change's surface
  area — and build-graph churn for a crate other in-flight work also
  touches — minimal.
- **Merging tablets back down when throughput drops.** Rejected — tablets
  are split-only (ADR 0044), and reopening that decision is out of this
  ADR's scope; a de-provisioned table's excess tablets simply quiesce.
