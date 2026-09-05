# ADR 0034 — Byte-based auto-split trigger (amends 0002, 0028's auto-split loop)

- **Status:** Accepted — implemented in `animus-storage`, `animus-cp-data`,
  `animusd`.
- **2026-08-16 note:** [ADR 0050](0050-per-tablet-storage-copy-based-splits.md)
  (implemented 2026-08-17) keeps this ADR's trigger, byte-weighted median,
  cooldown, and F11 token alignment — but what they *trigger* becomes a
  copy-based background workflow, so a split is no longer "cheap to have
  made"; the "Mitigating context" paragraph below stops applying once 0050
  lands.
- **Date:** 2026-08-07

## Context

`animusd::auto_split_loop` (Phase 2.4, `--auto-split K`) is the only
production trigger that decides *when* a tablet splits (ADR 0002 explicitly
left "choosing split points automatically" as future work; ADR 0028 gave
split its current single-command shape but didn't revisit the trigger
metric). It gates on `CpGroup::approx_key_count` — the memtable's key count
plus an SSTable-bytes-divided-by-an-assumed-entry-size estimate — against a
plain **key count** threshold, and once confirmed by materializing the
tablet, splits at the **positional median** key (the key at
`pairs.len() / 2`).

Key count is a poor proxy for what splitting actually exists to bound. A
tablet of 2,000 rows at 30 bytes each and a tablet of 2,000 rows at 1MB each
(e.g. large document/blob values) look *identical* to this trigger, but
their real costs are nowhere close: every operation this codebase's own docs
already name as scaling with tablet size —

- **snapshot build + ship** (`animus-cp-data`'s `InstallSnapshot`, the
  crate's own docs on the control-plane's analogous large-metadata
  ship-storm hazard),
- **compaction rewrite cost** (`animus-storage`'s leveled compaction, whose
  WAL-rewrite stall is already measured in the root `CLAUDE.md` as scaling
  with serialized state size, not row count),
- **replica-move cost** (ADR 0029's rebalancer relocates whole tablets —
  moving a tablet is moving its bytes across the network and onto a new
  node's disk),
- **recovery window** (a crashed/restarted replica's catch-up time is
  bounded by how much it must replay/receive, which is bytes on the wire and
  on disk, not a key count),

—scales with **bytes**, not key count. A cluster of large-value tablets under
a key-count threshold would never split until each tablet is enormous by
every one of these real cost measures, while a cluster of tiny-value tablets
would split far more eagerly than any of these costs actually require. Two
widely-deployed systems this project already looks to for shape confirm the
industry answer: CockroachDB splits ranges at ~512MiB, TiKV at ~96MiB region
size — both byte thresholds, not row/key counts. HBase's default region
split policy is likewise byte-based (with a growth-factor variant, but never
a bare key count).

There is also a **codebase-specific** argument for why the split metric
matters beyond the split trigger itself: ADR 0029's rebalancer balances
per-node tablet-replica **counts** (`max − min ≤ 1` across `Active` members).
That is only a meaningful proxy for actual disk/IO balance across nodes if
tablets are themselves roughly **byte-uniform** — if one node's tablets
happen to be 100x bigger than another's, an evenly-distributed replica
*count* can still mean a wildly uneven distribution of actual bytes and I/O.
Byte-based splitting is what keeps tablets byte-uniform in the first place,
so the split metric quietly underwrites the rebalancer's own premise; a
key-count trigger does not.

**Mitigating context, stated honestly**: this was a reasonable v1 bootstrap
choice, not an oversight to walk back apologetically. Since ADR 0028, a
split is a single, cheap, metadata-only control-plane command — no data
moves, so a mis-timed or badly-positioned split under the old key-count
trigger was inexpensive to have made and is not a stranded mistake to
"repair" retroactively. Tablet **merge** (ADR 0033) briefly made an
over-eager split *reversible* — since removed entirely (ADR 0044, tablets
are split-only) — so getting the trigger byte-based now is about making
new splits track the real cost going forward; a mis-timed split is a
permanent, if cheap, cost, not a remediable one.

## Decision

Keep the auto-split loop's **mechanism** exactly as it is today — same
`AUTO_SPLIT_INTERVAL` sampling cadence, same cheap-estimate-then-materialize
shape, same per-tablet `AUTO_SPLIT_COOLDOWN`, same single-atomic-command
`ClientCtx::trigger_split` — and change only the **metric** it gates on and
the **split point** it computes:

### 1. A range-scoped byte estimator, additive on `StorageEngine`

`StorageEngine::approx_bytes_in_range(start, end) -> Result<u64>` is a new
trait method (`animus-storage`) with a **default implementation that is
exact**: it scans `[start, end)` (or filters `entries()` by `start` when
`end` is `None`, unbounded above) and sums `key.len() + value.len()`. This is
correct for any backend for free — including `MemoryEngine`, where
materializing the range costs nothing extra — exactly like
`StorageEngine::merge_batch`'s per-op default that any new implementor
inherits without writing a line of code.

`LsmEngine` **overrides** it with a cheap, non-materializing estimate built
from metadata it already holds for other purposes, mirroring
`CpGroup::approx_key_count`'s existing shape:

- the **memtable's** contribution is an exact, range-scoped `BTreeMap::range`
  sum (cheap — it's in memory already);
- the **SSTable** contribution sums the `file_size` of every table whose own
  `[min_key, max_key]` **overlaps** `[start, end)` at all.

This deliberately **over-estimates**, in the same spirit
`approx_key_count`'s doc already states and for a documented reason
specific to this codebase: since ADR 0026/0028 one physical `LsmEngine` can
be **shared** by several tablets (a table's split parent and child, or any
two tablets a node hosts), a table that merely overlaps the query range —
rather than being wholly contained in it — may hold a sibling tenant's
bytes too, particularly at L0 (the unpartitioned flush tier). Counting its
whole `file_size` anyway keeps the bias strictly one-directional: a tablet
that might need splitting is never silently missed. This tightens naturally
once data is compacted into range-partitioned, non-overlapping L1+ runs
(leveled compaction's whole point) — the estimate is loosest for
still-unflushed/L0 data on a heavily shared engine, exactly where
`auto_split_loop`'s materializing confirm step (below) corrects it before
any split actually commits.

`RaftKvNode::approx_bytes()` (`animus-cp-data`) is the per-tablet
call-through: it reads this group's own live `StorageScope` and calls
`approx_bytes_in_range` over its **physical** bounds. A `StorageScope`'s
range is frequently unbounded above (a fresh table's first, not-yet-split
tablet spans its whole prefix) — falling back to an engine-wide scan for
that case would defeat the entire point of a cheap per-tick gate, so
`StorageScope` grew a `physical_bounds()` helper that computes the standard
**prefix-upper-bound** trick (increment the last non-`0xFF` byte of the
scope's own key prefix, dropping trailing `0xFF` bytes first) instead of
leaving the physical range open-ended. This keeps the byte estimate scoped
to *this* tablet's own prefix even before it has ever been split — the
single most common case the loop must handle cheaply.

`CpGroup::approx_bytes()` (`animusd`) dispatches to either backend arm. Note
this is a strict improvement over its `approx_key_count` sibling: because
the default trait impl is correct on any engine, the byte estimate works on
**both** the LSM and memory backends (`approx_key_count` returns `None` on
the memory backend, since it has no cheap SSTable/memtable-count metadata to
read).

### 2. `auto_split_loop` gates on either threshold; the confirm step now also
   totals bytes

`--auto-split K` (keys) is kept working exactly as before — same behavior,
same tests, byte-for-byte. A new `--auto-split-bytes B` flag adds a second,
independent threshold. **Either, both, or neither** may be configured; when
both are set, exceeding **either** threshold fires a split (a byte-heavy
tablet of few huge values and a key-heavy tablet of many small ones are
genuinely different situations — some operational costs, like bulk-scan
iteration overhead, still track key count even though most of the
motivating costs above track bytes — so neither trigger subsumes the
other). The loop's cheap per-tick gate now checks both cheap estimates
(`approx_key_count`/`approx_bytes`) and materializes the tablet's live pairs
if *either* looks hot, or on the existing slow confirm cadence. The
materializing step (already reading every pair to get an authoritative key
count) now also sums `key.len() + value.len()` for an authoritative byte
total in the same pass, and triggers if either authoritative number exceeds
its configured threshold.

### 3. The split point becomes a **byte-weighted** median when bytes matter

This is the one place the old metric and the new one *must* diverge, not
just coexist: today's split point is the **positional** median
(`pairs[pairs.len() / 2]`), which bisects the tablet by **key count**. Under
skewed value sizes, positional bisection can produce one enormous half and
one tiny half — and the tiny half is immediately below any reasonable
threshold while the enormous half immediately re-triggers, potentially
against the very estimate this ADR just made byte-aware. So whenever a byte
threshold is configured, the split point is instead the **byte-weighted
median**: among every achievable interior split point (a key boundary — a
key's own bytes can never be divided across the split), pick the one whose
left-side byte total is closest to half the tablet's total bytes — the key
boundary that most nearly bisects *bytes*, not position. A key-count-only
configuration (no `--auto-split-bytes`)
keeps the plain positional median, unchanged from before this ADR — existing
key-count auto-split behavior and tests are untouched.

### CLI / config surface: additive, not a replacement

`--auto-split K` keeps working exactly as it does today. `--auto-split-bytes
B` is new and independent; `BoundNode::start_with` gained a new
`auto_split_bytes_threshold: Option<u64>` parameter alongside the existing
`auto_split_threshold: Option<usize>`, and a new
`start_cluster_with_auto_split_bytes` entry point configures both — the
existing `start_cluster_with_auto_split`/`start_cluster_auto_split` stay as
thin key-count-only wrappers over it, so every existing caller (including
`animusd/tests/cp_plane.rs`'s `start_cluster_auto_split(bound, 16)`) is
unaffected. We considered folding both thresholds into one config struct
threaded everywhere instead of two parallel `Option`s, but that would have
meant either breaking the existing pub functions' signatures (this repo's
"keep public signatures stable, additive only" convention) or adding an
equally-wide parallel struct-based entry point anyway — two plain
`Option`s threaded the same way `auto_split_threshold` already is, is the
smaller, more mechanical diff for the same effect.

## Consequences

- A tablet's auto-split decision now tracks the resource that snapshot
  build/ship, compaction, replica-move, and recovery windows actually scale
  with, closing the gap between the trigger and the costs it exists to
  bound.
- The byte estimate is scoped per-tablet even on a shared engine (ADR
  0026/0028) and even for a tablet whose range is still unbounded above (the
  common not-yet-split case), via the SSTable-overlap sum + the
  `physical_bounds` prefix-upper-bound trick — not a whole-engine number
  that would double-count co-resident siblings the way a naive
  `entries()`-based estimate would.
- `StorageEngine` gained one more additive trait method with a working
  default, so no existing implementor needs to change (the `merge_batch`
  precedent); only `LsmEngine` overrides it for the cheap, non-materializing
  path.
- The split point computation now branches on whether a byte threshold is
  configured — the one deliberate, documented behavioral change a
  key-count-only deployment would see if it turned bytes on. With bytes off,
  the split point stays byte-identical to before this ADR.
- **Deferred**: a byte estimate does not address a small-but-*hot* tablet —
  one that is well under any size threshold but receiving disproportionate
  QPS/load. **Load-based splitting** (à la CockroachDB's QPS-triggered
  splits, which split a range receiving sustained high QPS regardless of its
  size) is real future work this ADR does not attempt; it would need a
  per-tablet request-rate signal this codebase does not yet track anywhere
  (the ADR 0015 metrics seam records counters, not per-tablet rates), and a
  different kind of split point (there is no natural "half the load" key
  without knowing the access pattern within the range). Left as a follow-up
  ADR.
- **Not addressed**: the estimate's SSTable-overlap-counts-whole-file bias
  can be loose on a heavily-shared engine with many small, interleaved L0
  tables (many tablets, little data each) — the confirm step's materializing
  count is what keeps this from ever *missing* a split, but a cluster
  running with a very tight byte threshold on such a workload could see more
  confirm-cadence materialization than necessary. Not observed to matter in
  practice (L0 tables are transient — leveled compaction promptly
  re-partitions them), and bounded by the same `AUTO_SPLIT_COOLDOWN`/confirm
  cadence that already bounds `approx_key_count`'s analogous bias.

## Amendment (2026-08-14, ADR 0042/0043)

A stream's hidden per-stream table is exempt from auto-split entirely:
`animusd::auto_split_loop` skips a stream table's tablets, the same
mechanism it already uses to skip a GSI's hidden table. A stream shard's
range is fixed at `CreateStreamShards` time and is load-bearing for ADR
0042's routing contract (`token(pk)` maps deterministically to exactly one
shard) — auto-splitting one would silently break that mapping mid-stream,
which no byte threshold could ever justify trading against. Growing a
stream's shard count is ADR 0042's own committed roadmap item (generation-
cut resharding, grow-by-doubling only), a control-plane-triggered event
entirely distinct from this ADR's byte-driven auto-split.

**Update (2026-08-14, round-3 rewrite): reversed — there is no stream
table to exempt, and this ADR's auto-split is now what *drives* stream
shard lineage.** Round 3 (ADR 0042/0043) seals a streamed table's own
change log in place; a stream shard is a seal epoch of an ordinary base
tablet, and a shard-lineage branch is created **only** when this ADR's
own auto-split creates a new tablet (ADR 0043 §A4: "auto-scaling is tablet
topology, full stop" — stream parallelism is tablet count, with no
separate resharding mechanism at all). Far from being exempted, a streamed
table's tablets are auto-split exactly like any other table's, with one
addition: **the split key is rounded down to its own 8-byte token boundary**
when the source table is streamed (ADR 0042 §14's **F11**) — this
`byte_weighted_median`-chosen key would otherwise land mid-token, which
would risk separating one partition key's change records (and hence one
shard's lineage) across the split boundary; token-alignment preserves the
partition-key/shard affinity a change record's own token-leading key
already assumes. This also narrows a pre-existing residual `txn.rs` noted
in ADR 0018's PR3 amendment about a non-token-aligned split racing an
in-flight transaction's own token, for every streamed table.

## Amendment (2026-09-01): the key-count trigger removed

`--auto-split K` — the original, ADR-0002-era key-count trigger this ADR's
byte-based trigger was added *alongside* (the "CLI / config surface:
additive, not a replacement" section above) — is **deleted**, along with
`AutoSplitThresholds.keys`, `auto_split_loop`'s `key_hot`/
`over_key_threshold` checks, and the plain positional-median split-point
path it drove (`start_cluster_auto_split`/`start_cluster_with_auto_split`,
the key-count-only wrappers that section's "thin wrappers... for
back-compat" design left in place, are deleted with it).

The byte-weighted median this ADR introduced and the ADR 0042 §14
change-rate trigger cover every failure mode key count did, with nothing
left that is *specifically* a key-count concern: bytes bound
snapshot/compaction/replica-move/recovery cost directly (this ADR's own
Context section), and change-rate (ADR 0042 §14 Fork F) catches the one gap
bytes structurally can't — a high-churn, small-footprint streamed table.
`CpGroup::approx_key_count` itself is **not** removed — `/admin/raftkv`'s
`key_count` field still reads it for the Console's Tablets view, now purely
informational (no threshold to compare against).

**Default behavior is unchanged.** Auto-split of any kind was always
opt-in — no `DEFAULT_AUTO_SPLIT_*` constant ever existed, `--cluster N`/
`--cluster-control`+`--cluster-data` start with `AutoSplitThresholds`'s
fields at `None` unless a flag is passed, and `--config`/`--node` (the real
per-process deployment) never had an auto-split flag of any kind to begin
with. Removing `--auto-split K` therefore does not turn auto-split off
anywhere it used to be on by default — there was no such default to lose.
An operator who was relying on `--auto-split K` in a dev/test invocation
should switch to `--auto-split-bytes B` (a byte value tuned for the
workload) or `--auto-split-change-rate RATE` for a streamed table; there is
no drop-in numeric equivalent (bytes and keys are different units), so this
is a deliberate re-tuning, not a mechanical substitution.

## Amendment (2026-09-04, S-06): reachable from `--config`/`--node` and
`animusd data --config` via a config-file section, not just `--cluster N`

The paragraph immediately above ("`--config`/`--node` (the real per-process
deployment) never had an auto-split flag of any kind to begin with") was
true when written and is the gap S-06 closes — **not** by adding a
`--auto-split-bytes`/`--auto-split-change-rate` CLI flag to `--config`/
`--node` or `animusd data --config` (neither subcommand gained one), but by
giving `ClusterConfig` its own `cluster_settings: Option<ClusterSettings>`
section (`crates/animusd/src/config.rs`) that both real deployment shapes
now read: `run_single` (`--config`/`--node`) and `run_data_config`
(`animusd data --config`) both thread `cluster_settings.auto_split_bytes`/
`auto_split_change_rate` down to `BoundNode::start_with_growth`/
`BoundDataNode::start_data_with_growth`'s own parameters of the same name —
the same knobs `--cluster N`'s `--auto-split-bytes`/`--auto-split-change-
rate` flags already fed, just reached through a config file instead of a
dev-only in-process CLI flag. A `--cluster N` CLI flag and the identical
config-file field being set on the same `--config`/`--node` invocation is a
hard startup error (`main.rs`'s `resolve_cluster_settings`), never a silent
precedence rule.

Still a documented gap: `--cluster-control`/`--cluster-data` (the
in-process split-deployment dev mode) and the standalone `control`/`join`
subcommands have no route to this section at all — S-06 scoped only the
three real `--config`/`--node`-shaped deployment paths. See
`crates/animusd/CLAUDE.md`'s config.rs module-map entry and `animusd::
config::ClusterSettings`'s own doc for the full field list and per-role
applicability, and ADR 0040/0048's own amendment notes for the same
mechanism's `orphan_sweep_after_secs`/`quiesce_after_secs` fields.

## Amendment (2026-09-05, W-09): the deferred request-rate signal, closed

The "Deferred" bullet above (a small-but-hot tablet, well under any size
threshold but under disproportionate write load, that structurally cannot
split) is closed. `animusd::RequestRateTracker` (`lib.rs`, beside — and
sharing its EWMA machinery with — `ChangeRateTracker`, ADR 0042 §14's own
change-append-rate signal) is a per-node, per-tablet estimate of a tablet's
own **leader-side write** rate (ops/sec), observed at the two call sites
that together cover every leader-side non-transactional write:
`dynamo::kind_write_item_at_leader` (the ADR 0046 U3 evaluate-at-leader
funnel — a condition, an old-image echo, or an images-carrying table) and
`dynamo::fast_marker_write` (the ADR 0049 fast arm — an unconditioned
`Put`/`Delete` on a plain, unindexed/unstreamed table, which never reaches
the funnel at all and is in fact the *common* shape for an ordinary
write). Both tick the tablet's own tracker once per successful write, so
the signal needs no new counter plumbing beyond those two call sites — but
both are load-bearing: a version scoped to only the evaluate-at-leader
funnel (this ADR's original single-choke-point assumption) would be blind
to the exact write shape a plain, unconditioned `PutItem` burst produces,
which is precisely the scenario the deferred bullet named. `--auto-split-ops-rate
RATE` (`AutoSplitThresholds.ops_rate`) joins `auto_split_loop`'s existing
either/any-trigger-fires gate alongside bytes and (for streamed tables)
change-rate: once a led tablet's smoothed write rate sustains above `RATE`,
it splits via the identical `byte_weighted_median`/`trigger_split` path
every other trigger already uses.

**Writes only, deliberately, mirroring `ChangeRateTracker`'s own
precedent** — and for a structural reason, not just symmetry. Since ADR
0055 an eventually-consistent read (`ConsistentRead: false`, the DynamoDB
wire default) is served from *any* replica's own applied state and never
reaches the leader at all; folding reads into a leader-observed counter
would silently undercount a tablet whose real hot path is reads spread
across replicas, with no honest fix short of a second, cluster-wide
aggregation this signal deliberately stays simple enough to avoid. A
strong (`ConsistentRead: true`) read does reach the leader but is excluded
too, for the same reason: a tablet's write load is what actually drives
replica-move/recovery/compaction cost the way this ADR's own byte trigger
already targets, and a hot-but-small tablet under heavy `PutItem`/
`UpdateItem`/`DeleteItem` load — the exact failure mode the deferred bullet
named — is fully visible through writes alone. A future revision wanting
read pressure factored in would need its own, separately-reasoned signal.

**Unlike `auto_split_change_rate`, this trigger is not streamed-tables-only**
— nothing about counting writes needs a change log, so `RequestRateTracker`
observes every table's tablets, streamed or not; this is what actually
closes the deferred bullet's *general* case rather than only the
change-rate amendment's already-covered streamed one.

Knob shape mirrors `auto_split_change_rate` throughout: opt-in (`None` is a
true no-op, zero behavior change for an existing deployment), no
production-tuned default (an operator picks `RATE` per workload), threaded
through the identical layered-wrapper stack
(`BoundNode::start_with_growth`/`BoundDataNode::start_data_with_growth`,
every `start_cluster_with_growth*`/`start_split_cluster_with_growth`
variant, `run_node_with_cluster_settings`/`run_node_data_with_cluster_
settings`), reachable from `--cluster N`/`--cluster-control`+
`--cluster-data`'s own `--auto-split-ops-rate RATE` CLI flag and from
`--config`/`--node`'s and `animusd data --config`'s `cluster_settings.
auto_split_ops_rate` config-file field (S-06's mechanism, extended) — the
identical "CLI flag and config field both set is a hard startup error"
contract. Surfaced read-only via `/admin/metrics`'s `request_rates` array
(beside `stream_change_rates`) and `auto_split_ops_rate_threshold` (beside
`auto_split_bytes_threshold`, also newly added there). No CRD field on the
Kubernetes operator side, mirroring `auto_split_change_rate`'s own
precedent (`crates/animus-operator/src/desired/cluster_config.rs`'s mirror
`ClusterSettings` carries the field for shape-completeness but nothing
populates it from `AnimusClusterSpec` yet).

**A pre-existing, unrelated determinism-rule violation found and fixed in
the same change**: `ChangeRateTracker::observe` read `tokio::time::
Instant::now()` directly instead of going through the `Env` seam — a
violation of the root `CLAUDE.md`'s "no wall clock" rule that the
workspace's `disallowed_methods` lint does not catch inside `lib.rs`
(`animusd`'s package-level lint exemption, `crates/animusd/CLAUDE.md`).
Fixed as a pure refactor, in scope here because `RequestRateTracker` needed
the identical `RateSample` storage shape anyway: both trackers now take the
caller's own `env.now()` reading as an explicit `Nanos` parameter rather
than reading a clock internally, and both gained a `SimEnv`-driven unit
test over virtual time (no real sleep) proving the EWMA converges toward a
sustained rate and decays once observations slow or stop.
